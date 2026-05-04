#!/usr/bin/env python3
"""
Fire many concurrent synthetic Slack block_actions (vote button) requests against your bot.

This exercises the *real* Flask/Bolt handlers (same path as production), so:
  - Google Sheets writes run
  - chat_update should refresh the poll message in Slack (live counts), if SLACK_BOT_TOKEN works

Requirements:
  - SLACK_SIGNING_SECRET in env (same as the running app)
  - A real poll message: channel ID, message ts, and poll_slot string exactly as in the message header
    (e.g. food_poll_2026-05-04 14:30 -> poll_slot "2026-05-04 14:30")

Usage:
  export SLACK_SIGNING_SECRET=xoxb-...
  python scripts/load_test_votes.py \\
    --base-url https://your-service.onrender.com \\
    --channel-id C0123456789 \\
    --message-ts 1234567890.123456 \\
    --poll-slot "2026-05-04 14:30"

Optional:
  --path /slack/events   (default tries /slack/events then /)
  --count 500
  --workers 100          (thread pool size; requests still total --count)

Warning: This writes to your real sheet and spams Slack API. Use a test workspace/channel only.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def main() -> None:
    p = argparse.ArgumentParser(description="Load-test food poll vote endpoint with signed Slack payloads.")
    p.add_argument(
        "--base-url",
        default=os.environ.get("LOAD_TEST_BASE_URL", "").rstrip("/"),
        help="e.g. https://your-bot.onrender.com (no trailing slash)",
    )
    p.add_argument("--path", default=os.environ.get("LOAD_TEST_PATH", "/slack/events"))
    p.add_argument("--channel-id", required=True, help="Channel containing the poll (C…)")
    p.add_argument("--message-ts", required=True, help="Poll message ts (copy from Slack)")
    p.add_argument(
        "--poll-slot",
        required=True,
        help='Exact poll slot string from the message, e.g. "2026-05-04 14:30"',
    )
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--workers", type=int, default=100)
    p.add_argument("--timeout", type=float, default=90.0)
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible vote distribution")
    args = p.parse_args()

    if not args.base_url:
        p.error("Pass --base-url or set LOAD_TEST_BASE_URL")
    signing_secret = (os.environ.get("SLACK_SIGNING_SECRET") or "").strip()
    if not signing_secret:
        p.error("Set SLACK_SIGNING_SECRET in the environment")

    team_id = (os.environ.get("SLACK_TEAM_ID") or "T00000000").strip()
    api_app_id = (os.environ.get("SLACK_APP_ID") or "A00000000").strip()

    if args.seed is not None:
        random.seed(args.seed)

    url = f"{args.base_url}{args.path}"
    votes = ["1", "2", "3", "4", "5"]

    jobs: list[dict] = []
    for i in range(args.count):
        uid = f"ULOAD{i:06d}"
        v = random.choice(votes)
        jobs.append(
            _build_block_actions_payload(
                channel_id=args.channel_id,
                message_ts=args.message_ts,
                poll_slot=args.poll_slot,
                user_id=uid,
                vote=v,
                team_id=team_id,
                api_app_id=api_app_id,
            )
        )

    print(f"POST {url}  x{args.count}  workers={args.workers}  timeout={args.timeout}s")
    print(f"poll_slot={args.poll_slot!r}  block_id=food_poll_{args.poll_slot}")

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
        "\nSlack UI: open the poll message — the last successful chat_update wins; "
        "counts should reflect votes that made it through Sheets + Slack API limits."
    )


if __name__ == "__main__":
    main()
