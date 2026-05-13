import base64
import binascii
import json
import logging
import os
import re
import urllib.parse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID")

# Sheet tab names
RAW_SHEET = "Raw Votes"
SUMMARY_SHEET = "Daily Summary"

# Row 1 on each tab — explicit names for Sheets / Excel export
RAW_HEADERS = [
    "Poll Slot (Date Time)",
    "Submitted At",
    "Slack User ID",
    "User Name",
    "Rating (1-5)",
    "Comment",
    "Message TS",
]
_RATINGS = frozenset({"1", "2", "3", "4", "5"})
SUMMARY_HEADERS = [
    "Poll Slot (Date Time)",
    "Rating 1 Count",
    "Rating 2 Count",
    "Rating 3 Count",
    "Rating 4 Count",
    "Rating 5 Count",
    "Total Ratings",
    "Comments (Name: Comment)",
]

_SERVICE = None

# Visual grouping in Raw Votes / Daily Summary (column A). Never matches real poll_slot keys.
_SECTION_DAY_PREFIX = "SECTION:day:"
_SECTION_SLOT_PREFIX = "SECTION:slot:"

_POLL_SLOT_KEY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2})?$")


def _is_poll_slot_key(cell: str) -> bool:
    """True if column A holds a real poll slot / legacy date key (not a section label)."""
    s = (cell or "").strip()
    if not s or s.startswith("SECTION:"):
        return False
    return bool(_POLL_SLOT_KEY_RE.match(s))


def _date_part_from_slot(slot: str) -> str:
    """Calendar day YYYY-MM-DD from a poll slot or date string."""
    s = (slot or "").strip()
    return s[:10] if len(s) >= 10 else ""


def _last_poll_slot_in_raw_rows(rows: list[list[str]]) -> str | None:
    """Last row's poll key in column A (skips section/spacer rows)."""
    for row in reversed(rows):
        if not row:
            continue
        if _is_poll_slot_key(row[0]):
            return (row[0] or "").strip()
    return None


def _section_rows_before_vote(poll_slot: str, rows: list[list[str]]) -> list[list[str]]:
    """
    Insert day + slot banners when the calendar day or poll slot changes.
    First-ever row gets day + slot headers so the sheet stays grouped.
    """
    day = _date_part_from_slot(poll_slot)
    if not day:
        return []

    last_slot = _last_poll_slot_in_raw_rows(rows)
    last_day = _date_part_from_slot(last_slot) if last_slot else None

    # Avoid duplicate slot banner if two appends race and both see the same "last" state.
    last_row = rows[-1] if rows else []
    if (
        last_row
        and (last_row[0] or "").strip() == f"{_SECTION_SLOT_PREFIX}{poll_slot}"
    ):
        return []

    empty7 = ["", "", "", "", "", "", ""]
    out: list[list[str]] = []
    if last_slot is None:
        out.append([f"{_SECTION_DAY_PREFIX}{day}", *empty7[1:]])
        out.append([f"{_SECTION_SLOT_PREFIX}{poll_slot}", *empty7[1:]])
        return out

    if day != last_day:
        out.append([f"{_SECTION_DAY_PREFIX}{day}", *empty7[1:]])
        out.append([f"{_SECTION_SLOT_PREFIX}{poll_slot}", *empty7[1:]])
    elif last_slot != poll_slot:
        out.append([f"{_SECTION_SLOT_PREFIX}{poll_slot}", *empty7[1:]])

    return out


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

    Optional (defaults match Google's JSON key file):
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

    1. GOOGLE_SA_* flat variables (best for Render "paste env" workflows)
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

        # Raw Votes: through Message TS (7 columns)
        result = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"{RAW_SHEET}!A1:G1")
            .execute()
        )
        row = (result.get("values") or [[]])[0]

        if not row:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{RAW_SHEET}!A1:G1",
                valueInputOption="RAW",
                body={"values": [RAW_HEADERS]},
            ).execute()
            logger.info("Created headers in Raw Votes tab")
        elif len(row) < len(RAW_HEADERS) or row[: len(RAW_HEADERS)] != RAW_HEADERS:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{RAW_SHEET}!A1:G1",
                valueInputOption="RAW",
                body={"values": [RAW_HEADERS]},
            ).execute()
            logger.info("Updated Raw Votes header row (Message TS / normalized)")

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
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{RAW_SHEET}!A2:G")
        .execute()
    )
    return result.get("values", [])


def read_raw_votes_rows_for_slot(service, slot: str) -> list[list[str]]:
    """Returns Raw Votes rows whose poll-slot column matches `slot` (no section rows).

    Used by VoteBuffer to hydrate its in-memory state on first access of a slot
    after a container restart.
    """
    rows = _read_raw_votes_rows(service)
    return [
        r
        for r in rows
        if r and _is_poll_slot_key(r[0]) and r[0] == slot
    ]


def _aggregate_comments_for_date(service, poll_date: str) -> str:
    """
    Reads Raw Votes for poll_date and builds newline-separated "Name: comment" lines
    for all rows with non-empty comments.
    """
    rows = _read_raw_votes_rows(service)
    lines: list[str] = []

    for row in rows:
        if not row or not _is_poll_slot_key(row[0]):
            continue
        if row[0] != poll_date:
            continue
        remark = (row[5] if len(row) > 5 else "").strip()
        if not remark:
            continue
        name = row[3] if len(row) > 3 else ""
        line = f"{name}: {remark}" if name else remark
        lines.append(line)
    return "\n".join(lines)


def _reorganize_daily_summary(service) -> None:
    """
    Sort summary rows by poll slot, group by calendar day with a banner row and blank line.
    Preserves header row 1; overwrites A2:H onward.
    """
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{SUMMARY_SHEET}!A2:H")
        .execute()
    )
    rows = result.get("values", [])
    data: list[list[str]] = []
    for r in rows:
        if r and _is_poll_slot_key(r[0]):
            data.append((r + [""] * 8)[:8])

    data.sort(key=lambda x: x[0])

    out: list[list[str]] = []
    prev_day: str | None = None
    for r in data:
        day = _date_part_from_slot(r[0])
        if day != prev_day:
            if out:
                out.append([""] * 8)
            out.append([f"{_SECTION_DAY_PREFIX}{day}", "", "", "", "", "", "", ""])
            prev_day = day
        out.append(r)

    sheet.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SUMMARY_SHEET}!A2:H5000",
    ).execute()
    if out:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SUMMARY_SHEET}!A2",
            valueInputOption="RAW",
            body={"values": out},
        ).execute()


def bulk_append_votes(rows: list[list[str]]) -> None:
    """Bulk append many vote rows in a single Sheets API call.

    rows: list of 7-column raw rows
        [poll_slot, submitted_at, user_id, user_name, choice, comment, message_ts]

    Section banners (new-day / new-slot) are inserted as needed before the
    appropriate row. The full state is read once, then we simulate appending
    each row to derive the correct banner sequence locally before issuing one
    `values.append` call.
    """
    if not rows:
        return
    try:
        service = _get_service()
        existing = _read_raw_votes_rows(service)
        out: list[list[str]] = []
        for row in rows:
            slot = (row[0] or "").strip() if row else ""
            if not slot:
                continue
            banners = _section_rows_before_vote(slot, existing + out)
            out.extend(banners)
            # Pad / trim to 7 columns to keep range consistent.
            padded = (row + ["", "", "", "", "", "", ""])[:7]
            out.append(padded)

        if not out:
            return

        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{RAW_SHEET}!A:G",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": out},
        ).execute()
        logger.info(
            "Bulk appended %d vote rows (%d total rows incl. banners)",
            len(rows),
            len(out),
        )
    except HttpError as e:
        logger.error(f"Google Sheets HttpError in bulk_append_votes: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in bulk_append_votes: {e}")
        raise


def update_daily_summary(poll_date: str, counts: dict):
    """
    Upserts a row in the Daily Summary tab for the given date.
    If a row for today exists, it updates it. Otherwise appends a new row.
    Always reorganizes to keep rows sorted and grouped by day.
    """
    try:
        service = _get_service()
        sheet = service.spreadsheets()

        result = (
            sheet.values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"{SUMMARY_SHEET}!A:A")
            .execute()
        )
        existing_dates = result.get("values", [])
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
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A{target_row}:H{target_row}",
                valueInputOption="RAW",
                body={"values": [row_data]},
            ).execute()
            logger.info(f"Updated daily summary for {poll_date}: {counts}")
        else:
            sheet.values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SUMMARY_SHEET}!A:H",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [row_data]},
            ).execute()
            logger.info(f"Appended new summary slot for {poll_date}: {counts}")

        try:
            _reorganize_daily_summary(service)
        except HttpError as e:
            logger.warning(f"Daily summary reorganize HttpError (data still saved): {e}")
        except Exception as e:
            logger.warning(f"Daily summary reorganize failed (data still saved): {e}")

    except HttpError as e:
        logger.error(f"Google Sheets HttpError updating summary: {e}")
    except Exception as e:
        logger.error(f"Unexpected error updating summary: {e}")
