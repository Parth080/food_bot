import os
import logging
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import Flask, request, jsonify

from poll import build_poll_blocks
from vote_handler import (
    VOTE_REMARKS_CALLBACK_ID,
    handle_remarks_modal_submit,
    open_vote_remarks_modal,
    process_vote,
)
from sheets import ensure_sheet_headers

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Slack Bolt App ────────────────────────────────────────────────────────────
bolt_app = App(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
)

CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]


# ── Slash Command: /startpoll ─────────────────────────────────────────────────
@bolt_app.command("/startpoll")
def handle_startpoll(ack, body, client):
    """
    Admin runs /startpoll in any channel.
    Bot posts the poll into the configured #janta channel.
    """
    ack()

    poll_date = str(date.today())
    user_id = body["user_id"]

    try:
        client.chat_postMessage(
            channel=CHANNEL_ID,
            blocks=build_poll_blocks(poll_date),
            text=f"🍽️ Food poll for {poll_date} — How was the food today?",
        )
        logger.info(f"Poll posted by {user_id} for {poll_date}")

        # Confirm to the admin privately
        client.chat_postEphemeral(
            channel=body["channel_id"],
            user=user_id,
            text=f"✅ Poll posted to <#{CHANNEL_ID}> for *{poll_date}*!",
        )
    except Exception as e:
        logger.error(f"Failed to post poll: {e}")
        client.chat_postEphemeral(
            channel=body["channel_id"],
            user=user_id,
            text=f"❌ Failed to post poll: {str(e)}",
        )


# ── Vote Button Handlers ──────────────────────────────────────────────────────
@bolt_app.action("vote_great")
def on_vote_great(ack, body, client, action):
    ack()
    process_vote(body, client, action)


@bolt_app.action("vote_okay")
def on_vote_okay(ack, body, client, action):
    ack()
    open_vote_remarks_modal(body, client, action)


@bolt_app.action("vote_bad")
def on_vote_bad(ack, body, client, action):
    ack()
    open_vote_remarks_modal(body, client, action)


@bolt_app.view(VOTE_REMARKS_CALLBACK_ID)
def on_vote_remarks_modal_submit(ack, body, client, view):
    ack()
    handle_remarks_modal_submit(body, client, view)


# ── Flask App ─────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)
handler = SlackRequestHandler(bolt_app)


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
