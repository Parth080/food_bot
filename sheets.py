import base64
import binascii
import json
import logging
import os
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Asia/Kolkata")

# Sheet tab names
RAW_SHEET = "Raw Votes"
SUMMARY_SHEET = "Daily Summary"

# Row 1 on each tab — explicit names for Sheets / Excel export
RAW_HEADERS = [
    "Date (poll day)",
    "Time (vote logged)",
    "Slack user ID",
    "Voter display name",
    "Rating (1 to 5)",
    "Comment (optional, independent of rating)",
]
SUMMARY_HEADERS = [
    "Date (poll day)",
    "Rating 1 — vote count",
    "Rating 2 — vote count",
    "Rating 3 — vote count",
    "Rating 4 — vote count",
    "Rating 5 — vote count",
    "Total votes",
    "All comments (name: text, one per line)",
]

_SERVICE = None


def _normalize_private_key(pem: str) -> str:
    """Render/.env often store PEM as one line with literal \\n sequences."""
    pem = (pem or "").strip()
    if "\\n" in pem:
        pem = pem.replace("\\n", "\n")
    return pem


def _credentials_from_sa_env_vars() -> dict | None:
    """
    Build service-account dict from flat GOOGLE_SA_* env vars (easy copy-paste into Render).

    Required:
      GOOGLE_SA_PROJECT_ID, GOOGLE_SA_PRIVATE_KEY_ID, GOOGLE_SA_PRIVATE_KEY,
      GOOGLE_SA_CLIENT_EMAIL, GOOGLE_SA_CLIENT_ID

    Optional (defaults match Google’s JSON key file):
      GOOGLE_SA_TYPE (default service_account)
      GOOGLE_SA_AUTH_URI, GOOGLE_SA_TOKEN_URI,
      GOOGLE_SA_AUTH_PROVIDER_X509_CERT_URL, GOOGLE_SA_CLIENT_X509_CERT_URL,
      GOOGLE_SA_UNIVERSE_DOMAIN
    """
    project_id = (os.environ.get("GOOGLE_SA_PROJECT_ID") or "").strip()
    private_key_id = (os.environ.get("GOOGLE_SA_PRIVATE_KEY_ID") or "").strip()
    private_key = _normalize_private_key(os.environ.get("GOOGLE_SA_PRIVATE_KEY") or "")
    client_email = (os.environ.get("GOOGLE_SA_CLIENT_EMAIL") or "").strip()
    client_id = (os.environ.get("GOOGLE_SA_CLIENT_ID") or "").strip()

    if not all([project_id, private_key_id, private_key, client_email, client_id]):
        return None

    auth_uri = (
        os.environ.get("GOOGLE_SA_AUTH_URI") or "https://accounts.google.com/o/oauth2/auth"
    ).strip()
    token_uri = (
        os.environ.get("GOOGLE_SA_TOKEN_URI") or "https://oauth2.googleapis.com/token"
    ).strip()
    auth_provider = (
        os.environ.get("GOOGLE_SA_AUTH_PROVIDER_X509_CERT_URL")
        or "https://www.googleapis.com/oauth2/v1/certs"
    ).strip()
    client_x509 = (os.environ.get("GOOGLE_SA_CLIENT_X509_CERT_URL") or "").strip()
    if not client_x509:
        enc = urllib.parse.quote(client_email, safe="")
        client_x509 = (
            f"https://www.googleapis.com/robot/v1/metadata/x509/{enc}"
        )
    sa_type = (os.environ.get("GOOGLE_SA_TYPE") or "service_account").strip()
    universe = (os.environ.get("GOOGLE_SA_UNIVERSE_DOMAIN") or "googleapis.com").strip()

    return {
        "type": sa_type,
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key,
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": auth_provider,
        "client_x509_cert_url": client_x509,
        "universe_domain": universe,
    }


def _load_service_account_info() -> dict:
    """
    Credentials source (first match wins):

    1. GOOGLE_SA_* flat variables (best for Render “paste env” workflows)
    2. GOOGLE_CREDENTIALS_B64 — base64 of the full JSON key file
    3. GOOGLE_CREDENTIALS_JSON — raw JSON string (local .env)
    """
    assembled = _credentials_from_sa_env_vars()
    if assembled is not None:
        return assembled

    b64 = "".join((os.environ.get("GOOGLE_CREDENTIALS_B64") or "").split())
    raw = (os.environ.get("GOOGLE_CREDENTIALS_JSON") or "").strip()

    if b64:
        try:
            decoded = base64.b64decode(b64, validate=True)
        except (binascii.Error, ValueError) as e:
            raise ValueError(
                "GOOGLE_CREDENTIALS_B64 must be standard base64 of the JSON key file"
            ) from e
        return json.loads(decoded.decode("utf-8"))

    if raw:
        return json.loads(raw)

    raise ValueError(
        "Set GOOGLE_SA_PROJECT_ID, GOOGLE_SA_PRIVATE_KEY_ID, GOOGLE_SA_PRIVATE_KEY, "
        "GOOGLE_SA_CLIENT_EMAIL, GOOGLE_SA_CLIENT_ID — or GOOGLE_CREDENTIALS_B64 / "
        "GOOGLE_CREDENTIALS_JSON"
    )


def _get_service():
    """Builds (once per process) the Google Sheets API service from env creds."""
    global _SERVICE
    if _SERVICE is not None:
        return _SERVICE

    creds_dict = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    # Avoid discovery cache overhead and repeated client construction noise.
    _SERVICE = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _SERVICE


def ensure_sheet_headers():
    """
    Called once on startup. Creates header rows in both tabs if they don't exist.
    Extends existing single-row headers when new columns were added (Remarks, summary text).
    """
    try:
        service = _get_service()
        sheet = service.spreadsheets()

        # Raw Votes: ensure Date … Remarks (6 columns)
        result = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"{RAW_SHEET}!A1:F1")
            .execute()
        )
        row = (result.get("values") or [[]])[0]

        if not row:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{RAW_SHEET}!A1:F1",
                valueInputOption="RAW",
                body={"values": [RAW_HEADERS]},
            ).execute()
            logger.info("Created headers in Raw Votes tab")
        elif len(row) < len(RAW_HEADERS) or row[: len(RAW_HEADERS)] != RAW_HEADERS:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{RAW_SHEET}!A1:F1",
                valueInputOption="RAW",
                body={"values": [RAW_HEADERS]},
            ).execute()
            logger.info("Updated Raw Votes header row (added Remarks / normalized)")

        # Daily Summary: rating counts + aggregated comment column (8 columns)
        result2 = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"{SUMMARY_SHEET}!A1:H1")
            .execute()
        )
        row2 = (result2.get("values") or [[]])[0]

        if not row2:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A1:H1",
                valueInputOption="RAW",
                body={"values": [SUMMARY_HEADERS]},
            ).execute()
            logger.info("Created headers in Daily Summary tab")
        elif len(row2) < len(SUMMARY_HEADERS) or row2[: len(SUMMARY_HEADERS)] != SUMMARY_HEADERS:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A1:H1",
                valueInputOption="RAW",
                body={"values": [SUMMARY_HEADERS]},
            ).execute()
            logger.info("Updated Daily Summary header row (remark columns / normalized)")

    except HttpError as e:
        logger.error(f"Error ensuring sheet headers: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in ensure_sheet_headers: {e}")


def _read_raw_votes_rows(service) -> list[list[str]]:
    """Reads all data rows from Raw Votes (excluding header)."""
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{RAW_SHEET}!A2:F")
        .execute()
    )
    return result.get("values", [])


def get_user_vote_for_date(poll_date: str, user_id: str) -> str | None:
    """Returns existing rating (1..5) for user on poll_date, if any."""
    try:
        service = _get_service()
        rows = _read_raw_votes_rows(service)
        for row in rows:
            if len(row) >= 5 and row[0] == poll_date and row[2] == user_id and row[4] in {"1", "2", "3", "4", "5"}:
                return row[4]
        return None
    except HttpError as e:
        logger.error(f"Google Sheets HttpError checking duplicate vote: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error checking duplicate vote: {e}")
        return None


def get_counts_from_raw_votes(poll_date: str) -> dict:
    """Computes rating 1..5 counts for poll_date from Raw Votes rows."""
    counts = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    try:
        service = _get_service()
        rows = _read_raw_votes_rows(service)
        for row in rows:
            if len(row) < 5 or row[0] != poll_date:
                continue
            vote = row[4]
            if vote in counts:
                counts[vote] += 1
        return counts
    except HttpError as e:
        logger.error(f"Google Sheets HttpError reading counts: {e}")
        return counts
    except Exception as e:
        logger.error(f"Unexpected error reading counts: {e}")
        return counts


def _aggregate_comments_for_date(service, poll_date: str) -> str:
    """
    Reads Raw Votes for poll_date and builds newline-separated "Name: comment" lines
    for all rows with non-empty comments.
    """
    rows = _read_raw_votes_rows(service)
    lines: list[str] = []

    for row in rows:
        if not row or row[0] != poll_date:
            continue
        vote = row[4] if len(row) > 4 else ""
        remark = (row[5] if len(row) > 5 else "").strip()
        if not remark:
            continue
        name = row[3] if len(row) > 3 else ""
        line = f"{name}: {remark}" if name else remark
        lines.append(line)
    return "\n".join(lines)


def append_vote(
    poll_date: str,
    user_id: str,
    user_name: str,
    choice: str,
    remark: str = "",
):
    """
    Appends one row to the Raw Votes tab immediately after a vote is cast.
    Stores one row in Raw Votes. `choice` may be blank for comment-only submissions.
    """
    try:
        service = _get_service()
        now = datetime.now(ZoneInfo(APP_TIMEZONE)).strftime("%H:%M:%S")
        remark_cell = (remark or "").strip()

        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RAW_SHEET}!A:F",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={
                "values": [
                    [poll_date, now, user_id, user_name, choice, remark_cell]
                ]
            },
        ).execute()

        logger.info(f"Appended vote: {user_name} -> {choice} on {poll_date}")

    except HttpError as e:
        logger.error(f"Google Sheets HttpError appending vote: {e}")
    except Exception as e:
        logger.error(f"Unexpected error appending vote: {e}")


def update_daily_summary(poll_date: str, counts: dict):
    """
    Upserts a row in the Daily Summary tab for the given date.
    If a row for today exists, it updates it. Otherwise appends a new row.
    """
    try:
        service = _get_service()
        sheet = service.spreadsheets()

        # Read all existing dates in the summary tab
        result = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"{SUMMARY_SHEET}!A:A")
            .execute()
        )
        existing_dates = result.get("values", [])

        # Find if today's date already has a row (1-indexed, row 1 is header)
        target_row = None
        for i, row in enumerate(existing_dates):
            if row and row[0] == poll_date:
                target_row = i + 1  # 1-indexed sheet row
                break

        total = sum(counts.values())
        all_comments = _aggregate_comments_for_date(service, poll_date)
        row_data = [
            poll_date,
            counts.get("1", 0),
            counts.get("2", 0),
            counts.get("3", 0),
            counts.get("4", 0),
            counts.get("5", 0),
            total,
            all_comments,
        ]

        if target_row:
            # Update existing row
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A{target_row}:H{target_row}",
                valueInputOption="RAW",
                body={"values": [row_data]},
            ).execute()
        else:
            # Append new row
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A:H",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row_data]},
            ).execute()

        logger.info(f"Updated daily summary for {poll_date}: {counts}")

    except HttpError as e:
        logger.error(f"Google Sheets HttpError updating summary: {e}")
    except Exception as e:
        logger.error(f"Unexpected error updating summary: {e}")
