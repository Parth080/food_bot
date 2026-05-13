#!/usr/bin/env python3
"""
End-to-end load test: **Slack → your server → Sheets → Slack UI**.

**Default flow (`--post-live-poll`, on by default)**  
1. `SLACK_BOT_TOKEN` — `chat.postMessage` posts a **real** poll in the channel (same blocks as production).  
2. `SLACK_SIGNING_SECRET` — signed HTTP POSTs to your Render app simulate button clicks.  
3. Server runs Bolt → `process_vote` → Google Sheets + `chat.update` on that **same** message `ts`.

That way `message_ts`, `block_id`, and channel match what Slack expects, so live counts should update
in-channel (not only the sheet).

**`--no-post-live-poll`** — Skip step 1; use `--message-ts` / `--poll-slot` / env / hardcoded defaults
(useful if the message no longer exists or `ts`/slot drift → `chat.update` often fails).

**Required in `.env`:** `SLACK_SIGNING_SECRET`; for default flow also `SLACK_BOT_TOKEN`.

**Precedence for URL/channel/ts/slot:** CLI → env → `DEFAULT_*` constants.

Usage:
  python scripts/load_test_votes.py
  python scripts/load_test_votes.py --count 500 --workers 100
  python scripts/load_test_votes.py --no-post-live-poll --message-ts <permalink_ts>
  (Default poll_slot is 2026-05-05 09:17 IST; override with --poll-slot if needed.)

Warning: Writes to your real sheet and spams Slack + your app.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv

# ── Hardcoded targets (override via .env or CLI when you run a new poll) ─────────
DEFAULT_BASE_URL = "https://ai-team-core--janta-poll-bot-web.modal.run"
DEFAULT_SLACK_REQUEST_PATH = "/slack/events"
DEFAULT_POLL_CHANNEL_ID = "C0ARC31G2HM"
# With --no-post-live-poll: set via --message-ts or LOAD_TEST_MESSAGE_TS (Slack permalink → ts).
DEFAULT_POLL_MESSAGE_TS = ""
DEFAULT_POLL_SLOT = "2026-05-05 09:17"


def _load_dotenv_from_repo() -> Path:
    """Load `.env` from repo root (parent of `scripts/`)."""
    repo_root = Path(__file__).resolve().parent.parent
    env_path = repo_root / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
    else:
        load_dotenv()
    return repo_root


def _sign_request(signing_secret: str, timestamp: str, raw_body: str) -> str:
    basestring = f"v0:{timestamp}:{raw_body}".encode("utf-8")
    digest = hmac.new(
        signing_secret.encode("utf-8"),
        basestring,
        hashlib.sha256,
    ).hexdigest()
    return f"v0={digest}"


def _build_block_actions_payload(
    *,
    channel_id: str,
    message_ts: str,
    poll_slot: str,
    user_id: str,
    vote: str,
    team_id: str,
    api_app_id: str,
) -> dict:
    action_id = f"vote_{vote}"
    block_id = f"food_poll_{poll_slot}"
    return {
        "type": "block_actions",
        "user": {
            "id": user_id,
            "username": "loadtest",
            "name": "Load Test",
            "team_id": team_id,
        },
        "team": {"id": team_id, "domain": "loadtest"},
        "api_app_id": api_app_id,
        "container": {
            "type": "message",
            "message_ts": message_ts,
            "channel_id": channel_id,
        },
        "channel": {"id": channel_id, "name": "general"},
        "trigger_id": f"loadtest.{time.time()}.{random.random()}",
        "actions": [
            {
                "type": "button",
                "action_id": action_id,
                "block_id": block_id,
                "value": vote,
                "text": {"type": "plain_text", "text": vote},
                "action_ts": str(time.time()),
            }
        ],
    }


def _post_one(
    *,
    url: str,
    signing_secret: str,
    payload_obj: dict,
    timeout: float,
) -> tuple[int, str]:
    raw_payload = json.dumps(payload_obj, separators=(",", ":"))
    body_str = urllib.parse.urlencode({"payload": raw_payload})
    body_bytes = body_str.encode("utf-8")
    ts = str(int(time.time()))
    sig = _sign_request(signing_secret, ts, body_str)

    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Slack-Request-Timestamp": ts,
            "X-Slack-Signature": sig,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")[:500]
            return resp.status, text
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace")[:500]
        return e.code, text
    except urllib.error.URLError as e:
        return -1, str(e.reason if hasattr(e, "reason") else e)


def _resolve_endpoint(
    cli_base: str | None,
    cli_path: str | None,
) -> tuple[str, str]:
    base = (cli_base or "").strip().rstrip("/")
    if not base:
        base = (os.environ.get("LOAD_TEST_BASE_URL") or "").strip().rstrip("/")
    if not base:
        base = DEFAULT_BASE_URL.rstrip("/")

    path = (cli_path or "").strip()
    if not path:
        path = (os.environ.get("LOAD_TEST_PATH") or "").strip()
    if not path:
        path = DEFAULT_SLACK_REQUEST_PATH
    if not path.startswith("/"):
        path = "/" + path
    return base, path


def _resolve_channel_id(cli_ch: str | None) -> str:
    channel_id = (cli_ch or "").strip()
    if not channel_id:
        channel_id = (
            os.environ.get("LOAD_TEST_CHANNEL_ID")
            or os.environ.get("SLACK_CHANNEL_ID")
            or ""
        ).strip()
    if not channel_id:
        channel_id = DEFAULT_POLL_CHANNEL_ID
    return channel_id


def _post_live_poll_via_slack(repo_root: Path, channel_id: str) -> tuple[str, str, str]:
    """
    Post a real poll message with the bot token. Returns (message_ts, poll_slot, team_id)
    for building synthetic block_actions (team_id from auth.test).
    """
    bot = (os.environ.get("SLACK_BOT_TOKEN") or "").strip()
    if not bot:
        raise ValueError(
            "SLACK_BOT_TOKEN is required to post a live poll (or pass --no-post-live-poll)"
        )

    sys.path.insert(0, str(repo_root))
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from poll import build_poll_blocks
    from poll_schedule_config import POLL_TIMEZONE
    from slack_sdk import WebClient

    client = WebClient(token=bot)
    auth = client.auth_test()
    if not auth.get("ok"):
        raise RuntimeError(f"Slack auth.test failed: {auth}")
    team_id = (auth.get("team_id") or "T00000000").strip()

    poll_slot = datetime.now(ZoneInfo(POLL_TIMEZONE)).strftime("%Y-%m-%d %H:%M")
    resp = client.chat_postMessage(
        channel=channel_id,
        blocks=build_poll_blocks(poll_slot),
        text=f"🍽️ Food poll for {poll_slot} — load test",
    )
    if not resp.get("ok"):
        raise RuntimeError(f"chat.postMessage failed: {resp}")

    return str(resp["ts"]), poll_slot, team_id


def _resolve_poll_target(
    cli_ch: str | None,
    cli_ts: str | None,
    cli_slot: str | None,
) -> tuple[str, str, str]:
    channel_id = _resolve_channel_id(cli_ch)

    message_ts = (cli_ts or "").strip()
    if not message_ts:
        message_ts = (os.environ.get("LOAD_TEST_MESSAGE_TS") or "").strip()
    if not message_ts:
        message_ts = DEFAULT_POLL_MESSAGE_TS

    poll_slot = (cli_slot or "").strip()
    if not poll_slot:
        poll_slot = (os.environ.get("LOAD_TEST_POLL_SLOT") or "").strip()
    if not poll_slot:
        poll_slot = DEFAULT_POLL_SLOT

    return channel_id, message_ts, poll_slot


def main() -> None:
    repo_root = _load_dotenv_from_repo()
    p = argparse.ArgumentParser(description="Load-test food poll vote endpoint with signed Slack payloads.")
    p.add_argument(
        "--base-url",
        default=None,
        help=f"Default: env LOAD_TEST_BASE_URL or {DEFAULT_BASE_URL!r}",
    )
    p.add_argument(
        "--path",
        default=None,
        help=f"Default: env LOAD_TEST_PATH or {DEFAULT_SLACK_REQUEST_PATH!r}",
    )
    p.add_argument(
        "--channel-id",
        default=None,
        help=f"Default: env SLACK_CHANNEL_ID / LOAD_TEST_CHANNEL_ID or {DEFAULT_POLL_CHANNEL_ID!r}",
    )
    p.add_argument(
        "--message-ts",
        default=None,
        help="Default: env LOAD_TEST_MESSAGE_TS or hardcoded ts for the Flam test poll",
    )
    p.add_argument(
        "--poll-slot",
        default=None,
        help='Default: env LOAD_TEST_POLL_SLOT or hardcoded slot for the Flam test poll',
    )
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--workers", type=int, default=25)
    p.add_argument("--timeout", type=float, default=90.0)
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible vote distribution")
    p.set_defaults(post_live_poll=True)
    p.add_argument(
        "--no-post-live-poll",
        dest="post_live_poll",
        action="store_false",
        help="Skip chat.postMessage; reuse --message-ts / env / defaults (often sheet-only updates).",
    )
    args = p.parse_args()

    base_url, path = _resolve_endpoint(args.base_url, args.path)

    signing_secret = (os.environ.get("SLACK_SIGNING_SECRET") or "").strip()
    if not signing_secret:
        p.error(f"Set SLACK_SIGNING_SECRET in {repo_root / '.env'} (or export it)")

    api_app_id = (os.environ.get("SLACK_APP_ID") or "A00000000").strip()
    team_id = (os.environ.get("SLACK_TEAM_ID") or "T00000000").strip()

    use_live_post = args.post_live_poll
    if use_live_post:
        channel_id = _resolve_channel_id(args.channel_id)
        try:
            message_ts, poll_slot, team_id = _post_live_poll_via_slack(repo_root, channel_id)
        except Exception as e:
            p.error(f"Live Slack poll post failed: {e}")
        print(
            "\n=== Step 1: Posted live poll to Slack ===\n"
            f"  channel={channel_id}\n"
            f"  message_ts={message_ts}\n"
            f"  poll_slot={poll_slot!r}\n"
        )
        time.sleep(1.5)
    else:
        channel_id, message_ts, poll_slot = _resolve_poll_target(
            args.channel_id, args.message_ts, args.poll_slot
        )
        if not message_ts.strip():
            p.error(
                "With --no-post-live-poll, set --message-ts or LOAD_TEST_MESSAGE_TS "
                "(open the poll in Slack → Copy link → ts is the number after the last `p` in the path)."
            )
        print("\n=== Step 1: Skipped live post (reuse message) ===\n")

    if args.seed is not None:
        random.seed(args.seed)

    url = f"{base_url}{path}"
    votes = ["1", "2", "3", "4", "5"]

    jobs: list[dict] = []
    for i in range(args.count):
        uid = f"ULOAD{i:06d}"
        v = random.choice(votes)
        jobs.append(
            _build_block_actions_payload(
                channel_id=channel_id,
                message_ts=message_ts,
                poll_slot=poll_slot,
                user_id=uid,
                vote=v,
                team_id=team_id,
                api_app_id=api_app_id,
            )
        )

    print(
        f"=== Step 2: Signed block_actions → {url}  "
        f"x{args.count}  workers={args.workers}  timeout={args.timeout}s ==="
    )
    print(f"channel={channel_id}  message_ts={message_ts}")
    print(f"poll_slot={poll_slot!r}  block_id=food_poll_{poll_slot}")

    status_counts: dict[int, int] = {}
    errors: list[str] = []

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = [
            ex.submit(_post_one, url=url, signing_secret=signing_secret, payload_obj=payload, timeout=args.timeout)
            for payload in jobs
        ]
        for fut in as_completed(futs):
            code, snippet = fut.result()
            status_counts[code] = status_counts.get(code, 0) + 1
            if code != 200:
                errors.append(f"{code}: {snippet[:200]}")

    elapsed = time.perf_counter() - t0
    print(f"\nDone in {elapsed:.2f}s ({args.count / elapsed:.1f} req/s)")
    print("Status histogram:")
    for code in sorted(status_counts.keys()):
        print(f"  {code}: {status_counts[code]}")

    if errors:
        print(f"\nNon-200 samples (up to 10 of {len(errors)}):")
        for line in errors[:10]:
            print(f"  {line}")

    print(
        "\n=== Done ===\n"
        "Open the poll message in Slack — counts should match sheet rows for this message_ts. "
        "If you skipped live post, parent chat.update may fail; use default flow for full UI updates."
    )


if __name__ == "__main__":
    main()
