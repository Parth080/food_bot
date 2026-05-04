import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from slack_sdk.errors import SlackApiError

from poll import build_poll_blocks
from sheets import (
    append_vote,
    get_counts_from_raw_votes,
    get_user_comment_for_date,
    get_user_vote_for_date,
    update_daily_summary,
)
from poll_schedule_config import POLL_TIMEZONE

logger = logging.getLogger(__name__)

# Must match Slack modal callback_id and Bolt @app.view registration
COMMENT_MODAL_CALLBACK_ID = "comment_modal"


def _current_poll_slot() -> str:
    return datetime.now(ZoneInfo(POLL_TIMEZONE)).strftime("%Y-%m-%d %H:%M")


def _poll_slot_from_action(action: dict) -> str | None:
    """Poll messages use block_id `food_poll_{YYYY-MM-DD HH:MM}` on the actions block."""
    bid = action.get("block_id") or ""
    prefix = "food_poll_"
    if bid.startswith(prefix):
        return bid[len(prefix) :]
    return None


def open_comment_modal(body: dict, client, action: dict) -> None:
    """Opens a standalone comment modal; comments are independent of rating votes."""
    user_id = body["user"]["id"]
    poll_slot = _poll_slot_from_action(action) or _current_poll_slot()
    channel_id = body["container"]["channel_id"]
    message_ts = body["container"]["message_ts"]
    previous_comment = get_user_comment_for_date(poll_slot, user_id, message_ts)
    if previous_comment:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="You already submitted one comment for this poll. Thanks! 🙏",
        )
        logger.info(f"Duplicate comment blocked (modal): {user_id} on {poll_slot}")
        return
    meta = json.dumps(
        {
            "poll_date": poll_slot,
            "channel_id": channel_id,
            "message_ts": body["container"]["message_ts"],
        }
    )

    try:
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": COMMENT_MODAL_CALLBACK_ID,
                "private_metadata": meta,
                "title": {"type": "plain_text", "text": "Add comment"},
                "submit": {"type": "plain_text", "text": "Submit"},
                "close": {"type": "plain_text", "text": "Cancel"},
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Share any feedback for today's food (optional).",
                        },
                    },
                    {
                        "type": "input",
                        "block_id": "comment_block",
                        "optional": True,
                        "element": {
                            "type": "plain_text_input",
                            "action_id": "comment_text",
                            "multiline": True,
                            "max_length": 2000,
                            "placeholder": {
                                "type": "plain_text",
                                "text": "Type your comment here...",
                            },
                        },
                        "label": {"type": "plain_text", "text": "Comment"},
                    },
                ],
            },
        )
    except Exception as e:
        logger.error(f"views_open failed: {e}")
        client.chat_postEphemeral(
            channel=channel_id,
            user=body["user"]["id"],
            text="❌ Could not open feedback form. Please try again or contact an admin.",
        )


def handle_comment_modal_submit(body: dict, client, view: dict) -> None:
    """After user submits a standalone comment from the modal."""
    user_id = body["user"]["id"]
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
    except json.JSONDecodeError:
        logger.error("Invalid modal private_metadata")
        return

    poll_date = meta.get("poll_date") or _current_poll_slot()
    channel_id = meta.get("channel_id")
    message_ts = meta.get("message_ts")
    if not channel_id or not message_ts:
        logger.error("Comment modal metadata missing channel or ts")
        return

    values = view.get("state", {}).get("values", {})
    comment = (
        values.get("comment_block", {})
        .get("comment_text", {})
        .get("value", "")
        or ""
    ).strip()
    if not comment:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="Comment was empty, so nothing was saved.",
        )
        return
    if get_user_comment_for_date(poll_date, user_id, message_ts):
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="You already submitted one comment for this poll. 🙏",
        )
        logger.info(f"Duplicate comment blocked (submit): {user_id} on {poll_date}")
        return

    user_name = _get_user_name(client, user_id)
    append_vote(poll_date, user_id, user_name, choice="", remark=comment, message_ts=message_ts)
    counts = get_counts_from_raw_votes(poll_date)
    update_daily_summary(poll_date, counts)
    _refresh_poll_message(client, channel_id, message_ts, poll_date, counts)

    client.chat_postEphemeral(
        channel=channel_id,
        user=user_id,
        text="Thanks! Your comment was saved.",
    )


def process_vote(
    body: dict,
    client,
    action: dict,
    poll_date: str | None = None,
):
    """
    Central handler for recording a vote (1/2/3 direct, or 4/5 after modal).
    Called after ack() so we have full time to do Sheets writes.
    """
    user_id = body["user"]["id"]
    choice = action["value"]  # "1" | "2" | "3" | "4" | "5"
    poll_date = poll_date or _poll_slot_from_action(action) or _current_poll_slot()
    channel_id = body["container"]["channel_id"]
    message_ts = body["container"]["message_ts"]

    # --- Deduplication (message_ts = stable id for this poll message in Slack) ---
    previous = get_user_vote_for_date(poll_date, user_id, message_ts)
    if previous:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text=f"You already voted *{_label(previous)}* for this poll. Votes are final — thanks! 🙏",
        )
        logger.info(f"Duplicate vote blocked: {user_id} already voted {previous}")
        return

    # --- Get user's display name from Slack ---
    user_name = _get_user_name(client, user_id)

    # --- Write to Google Sheets ---
    append_vote(poll_date, user_id, user_name, choice, remark="", message_ts=message_ts)

    # --- Recalculate counts from Raw Votes (single source of truth) ---
    counts = get_counts_from_raw_votes(poll_date)
    update_daily_summary(poll_date, counts)

    # --- Update the poll message with live count ---
    refreshed = _refresh_poll_message(client, channel_id, message_ts, poll_date, counts)

    # --- Confirm to the voter privately ---
    thanks = f"Got your vote: {_label(choice)} ✅  Thanks {user_name.split()[0]}!"
    if not refreshed:
        thanks += (
            "\n\n_(The channel poll couldn’t be refreshed just now; your vote is saved in the sheet. "
            "If counts look wrong, check the thread under the poll.)_"
        )
    client.chat_postEphemeral(
        channel=channel_id,
        user=user_id,
        text=thanks,
    )

    logger.info(f"Vote processed: {user_name} ({user_id}) -> {choice}")


def _refresh_poll_message(
    client, channel_id: str, message_ts: str, poll_date: str, counts: dict
) -> bool:
    """
    Updates the original poll message with the latest vote counts.
    Retries on transient Slack errors; falls back to a thread reply if chat.update keeps failing.
    """
    blocks = build_poll_blocks(poll_date, counts)
    text = f"Food poll — {poll_date} | Total {sum(counts.values())}"

    for attempt in range(3):
        try:
            client.chat_update(
                channel=channel_id,
                ts=message_ts,
                blocks=blocks,
                text=text,
            )
            return True
        except SlackApiError as e:
            err = (e.response or {}).get("error", "") if e.response else ""
            logger.warning(
                "chat_update failed (attempt %s): error=%s — %s",
                attempt + 1,
                err,
                e,
            )
            if err == "ratelimited":
                retry_after = 1.0
                if e.response is not None and getattr(e.response, "headers", None):
                    ra = e.response.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_after = float(ra)
                        except ValueError:
                            pass
                time.sleep(retry_after)
            elif attempt < 2:
                time.sleep(0.6 * (attempt + 1))
        except Exception as e:
            logger.warning("chat_update failed (attempt %s): %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(0.6 * (attempt + 1))

    try:
        client.chat_postMessage(
            channel=channel_id,
            thread_ts=message_ts,
            text=text + " _(live counts — posted here because the poll message could not be edited)_",
            blocks=blocks,
        )
        logger.info("Posted poll refresh as thread reply after chat_update failures")
        return True
    except Exception as e:
        logger.error(f"Thread fallback for poll refresh failed: {e}")
    return False


def _get_user_name(client, user_id: str) -> str:
    """Fetches the user's real name from Slack. Falls back to user_id on error."""
    try:
        result = client.users_info(user=user_id)
        profile = result["user"]["profile"]
        return profile.get("real_name") or profile.get("display_name") or user_id
    except Exception as e:
        logger.warning(f"Could not fetch user name for {user_id}: {e}")
        return user_id


def _label(choice: str) -> str:
    return {
        "1": "1",
        "2": "2",
        "3": "3",
        "4": "4",
        "5": "5",
    }.get(choice, choice)
