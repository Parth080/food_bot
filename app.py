import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import Flask, request, jsonify

from poll import build_poll_blocks
from vote_handler import (
    COMMENT_MODAL_CALLBACK_ID,
    handle_comment_modal_submit,
    open_comment_modal,
    process_vote,
)
from sheets import ensure_sheet_headers
from poll_scheduler import start_scheduled_polls
from poll_schedule_config import POLL_TIMEZONE
from memory_hygiene import start_memory_hygiene

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)
start_memory_hygiene()

# If set, poll is posted here; if empty, posts in the channel where /startpoll was run.
CONFIGURED_POLL_CHANNEL_ID = (os.environ.get("SLACK_CHANNEL_ID") or "").strip()


def _slash_notify_ephemeral(body: dict, client, user_id: str, text: str) -> None:
    """Prefer in-channel ephemeral; slash commands always have response_url as fallback."""
    ru = body.get("response_url")
    try:
        client.chat_postEphemeral(
            channel=body["channel_id"],
            user=user_id,
            text=text,
        )
        return
    except Exception as e:
        logger.warning(f"chat_postEphemeral failed, using response_url: {e}")
    if not ru:
        return
    try:
        payload = json.dumps(
            {"response_type": "ephemeral", "text": text}
        ).encode("utf-8")
        req = urllib.request.Request(
            ru,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.URLError as e:
        logger.error(f"response_url ephemeral failed: {e}")


# ── Slack Bolt App ────────────────────────────────────────────────────────────
bolt_app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
)


def _post_food_poll_message(client, channel_id: str, poll_slot: str) -> None:
    client.chat_postMessage(
        channel=channel_id,
        blocks=build_poll_blocks(poll_slot),
        text=f"🍽️ Food poll for {poll_slot} — How was the food today?",
    )


def _build_poll_slot(slot_time: str | None = None) -> str:
    now = datetime.now(ZoneInfo(POLL_TIMEZONE))
    if slot_time:
        return f"{now.date()} {slot_time}"
    return now.strftime("%Y-%m-%d %H:%M")


def _post_scheduled_poll(slot_time: str | None = None) -> None:
    """Cron callback: post today's poll to SLACK_CHANNEL_ID."""
    channel = CONFIGURED_POLL_CHANNEL_ID
    if not channel:
        logger.error("Scheduled poll skipped: SLACK_CHANNEL_ID not set.")
        return
    poll_slot = _build_poll_slot(slot_time)
    try:
        _post_food_poll_message(bolt_app.client, channel, poll_slot)
        logger.info(f"Scheduled poll posted for {poll_slot} → {channel}")
    except Exception as e:
        logger.error(f"Scheduled poll failed: {e}")


start_scheduled_polls(_post_scheduled_poll, CONFIGURED_POLL_CHANNEL_ID)


# ── Slash Command: /startpoll ─────────────────────────────────────────────────
@bolt_app.command("/startpoll")
def handle_startpoll(ack, body, client):
    """
    Admin runs /startpoll in any channel.
    Poll goes to SLACK_CHANNEL_ID when set; otherwise to the invoking channel.
    """
    ack()

    poll_slot = _build_poll_slot()
    user_id = body["user_id"]
    post_channel = CONFIGURED_POLL_CHANNEL_ID or body["channel_id"]

    try:
        _post_food_poll_message(client, post_channel, poll_slot)
        logger.info(f"Poll posted by {user_id} for {poll_slot} → {post_channel}")

        _slash_notify_ephemeral(
            body,
            client,
            user_id,
            text=f"✅ Poll posted to <#{post_channel}> for *{poll_slot}*!",
        )
    except Exception as e:
        logger.error(f"Failed to post poll: {e}")
        hint = ""
        err = str(e)
        if "channel_not_found" in err:
            hint = (
                "\n\n`channel_not_found`: set *SLACK_CHANNEL_ID* to a real channel ID "
                "(e.g. `C0ABC…`), invite the bot to that channel (`/invite @Bot`), "
                "or *clear* SLACK_CHANNEL_ID to post in the channel where you run `/startpoll`."
            )
        _slash_notify_ephemeral(
            body,
            client,
            user_id,
            text=f"❌ Failed to post poll: {err}{hint}",
        )


# ── Vote Button Handlers ──────────────────────────────────────────────────────
@bolt_app.action("vote_1")
def on_vote_1(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("vote_2")
def on_vote_2(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("vote_3")
def on_vote_3(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("vote_4")
def on_vote_4(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("vote_5")
def on_vote_5(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("add_comment")
def on_add_comment(ack, body, client, action):
    ack()
    open_comment_modal(body, client, action)


@bolt_app.view(COMMENT_MODAL_CALLBACK_ID)
def on_comment_modal_submit(ack, body, client, view):
    ack()
    handle_comment_modal_submit(body, client, view)


# ── Flask App ─────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)
handler = SlackRequestHandler(bolt_app)

# Gunicorn on Render never runs `if __name__ == "__main__"` — ensure sheet row 1 headers exist.
logger.info("Ensuring Google Sheet column headers…")
ensure_sheet_headers()


@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    return handler.handle(request)


@flask_app.route("/slack/commands", methods=["POST"])
def slack_commands():
    return handler.handle(request)


@flask_app.route("/", methods=["POST"])
def slack_root_post():
    """Slack Request URL often set to https://<host>/ — same Bolt handler as /slack/events."""
    return handler.handle(request)


@flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "janta-poll-bot"}), 200


@flask_app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok", "message": "Janta Poll Bot is running"}), 200


# ── Startup ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting Janta Poll Bot...")
    ensure_sheet_headers()
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 3000)))
