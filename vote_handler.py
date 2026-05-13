import json
import logging
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from slack_sdk.errors import SlackApiError

from poll import build_poll_blocks
from poll_schedule_config import POLL_TIMEZONE
from vote_buffer import get_buffer

logger = logging.getLogger(__name__)

# Must match Slack modal callback_id and Bolt @app.view registration
COMMENT_MODAL_CALLBACK_ID = "comment_modal"

# Slack chat.update is Tier 3 (~50/min per workspace). During a vote burst we
# coalesce many concurrent refresh attempts into one in-flight update per
# (channel, message_ts) using a per-key lock + small debounce window.
_REFRESH_DEBOUNCE_SECS = 0.4
_refresh_locks: dict[tuple, threading.Lock] = {}
_refresh_locks_guard = threading.Lock()


def _current_poll_slot() -> str:
    return datetime.now(ZoneInfo(POLL_TIMEZONE)).strftime("%Y-%m-%d %H:%M")


def _poll_slot_from_action(action: dict) -> str | None:
    """Poll messages use block_id `food_poll_{YYYY-MM-DD HH:MM}` on the actions block."""
    bid = action.get("block_id") or ""
    prefix = "food_poll_"
    if bid.startswith(prefix):
        return bid[len(prefix):]
    return None


def open_comment_modal(body: dict, client, action: dict) -> None:
    """Opens a standalone comment modal; comments are independent of rating votes."""
    user_id = body["user"]["id"]
    poll_slot = _poll_slot_from_action(action) or _current_poll_slot()
    channel_id = body["container"]["channel_id"]
    message_ts = body["container"]["message_ts"]
    buffer = get_buffer()

    if buffer.get_user_comment(poll_slot, user_id, message_ts):
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
            "message_ts": message_ts,
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

    buffer = get_buffer()
    if buffer.get_user_comment(poll_date, user_id, message_ts):
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="You already submitted one comment for this poll. 🙏",
        )
        logger.info(f"Duplicate comment blocked (submit): {user_id} on {poll_date}")
        return

    user_name = _get_user_name(client, user_id)
    buffer.record_comment(poll_date, user_id, user_name, comment, message_ts)

    _refresh_throttled(client, channel_id, message_ts, poll_date)

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
    Central handler for recording a 1..5 vote.
    Called after ack() so we have full time to do buffer writes + Slack refresh.
    """
    user_id = body["user"]["id"]
    choice = action["value"]
    poll_date = poll_date or _poll_slot_from_action(action) or _current_poll_slot()
    channel_id = body["container"]["channel_id"]
    message_ts = body["container"]["message_ts"]

    buffer = get_buffer()
    previous = buffer.get_user_vote(poll_date, user_id, message_ts)
    if previous:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text=f"You already voted *{_label(previous)}* for this poll. Votes are final — thanks! 🙏",
        )
        logger.info(f"Duplicate vote blocked: {user_id} already voted {previous}")
        return

    user_name = _get_user_name(client, user_id)
    buffer.record_vote(poll_date, user_id, user_name, choice, message_ts)

    refreshed = _refresh_throttled(client, channel_id, message_ts, poll_date)

    first_name = user_name.split()[0] if user_name else "you"
    thanks = f"Got your vote: {_label(choice)} ✅  Thanks {first_name}!"
    if refreshed is False:
        thanks += (
            "\n\n_(The channel poll couldn't be refreshed just now; your vote is saved. "
            "If counts look wrong, check the thread under the poll.)_"
        )
    client.chat_postEphemeral(
        channel=channel_id,
        user=user_id,
        text=thanks,
    )

    logger.info(f"Vote processed: {user_name} ({user_id}) -> {choice}")


def _refresh_throttled(
    client, channel_id: str, message_ts: str, poll_date: str
) -> bool | None:
    """
    Coalesce concurrent refresh requests for the same poll message.

    First caller per (channel, ts) acquires the lock, debounces briefly so
    additional in-flight votes pile up, then sends one chat.update with the
    latest counts. Concurrent callers see `acquire(blocking=False)` fail and
    return immediately — the in-flight update will reflect their state.

    Returns:
        True   — update sent successfully
        False  — update failed after all retries / fallback
        None   — another thread is already handling this message
    """
    key = (channel_id, message_ts)
    with _refresh_locks_guard:
        lock = _refresh_locks.setdefault(key, threading.Lock())

    if not lock.acquire(blocking=False):
        return None

    try:
        time.sleep(_REFRESH_DEBOUNCE_SECS)
        counts = get_buffer().get_counts(poll_date)
        return _refresh_poll_message(client, channel_id, message_ts, poll_date, counts)
    finally:
        lock.release()


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
    """Returns the user's real name."""
    try:
        result = client.users_info(user=user_id)
        profile = result["user"]["profile"]
        return profile.get("real_name") or profile.get("display_name") or user_id
    except Exception as e:
        logger.warning(f"Could not fetch user name for {user_id}: {e}")
        return user_id


def _label(choice: str) -> str:
    return {"1": "1", "2": "2", "3": "3", "4": "4", "5": "5"}.get(choice, choice)
