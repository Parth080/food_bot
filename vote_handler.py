import json
import logging
import os
from datetime import date, datetime

from poll import build_poll_blocks
from sheets import (
    append_vote,
    get_counts_from_raw_votes,
    get_user_vote_for_date,
    update_daily_summary,
)

logger = logging.getLogger(__name__)

# Must match Slack modal callback_id and Bolt @app.view registration
VOTE_REMARKS_CALLBACK_ID = "vote_remarks_modal"


def _poll_date_from_action(action: dict) -> str | None:
    """Poll messages use block_id `food_poll_{YYYY-MM-DD}` on the actions block."""
    bid = action.get("block_id") or ""
    prefix = "food_poll_"
    if bid.startswith(prefix):
        return bid[len(prefix) :]
    return None


def open_vote_remarks_modal(body: dict, client, action: dict) -> None:
    """
    For Okay / Bad: open a modal with an optional remark before recording the vote.
    Great votes skip this and call process_vote directly.
    """
    user_id = body["user"]["id"]
    poll_date = _poll_date_from_action(action) or str(date.today())
    channel_id = body["container"]["channel_id"]

    previous = get_user_vote_for_date(poll_date, user_id)
    if previous:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text=f"You already voted *{_label(previous)}* today. Votes are final — thanks! 🙏",
        )
        logger.info(f"Duplicate vote blocked (modal): {user_id} already voted {previous}")
        return

    choice = action["value"]
    meta = json.dumps(
        {
            "poll_date": poll_date,
            "channel_id": channel_id,
            "message_ts": body["container"]["message_ts"],
            "choice": choice,
        }
    )

    try:
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": VOTE_REMARKS_CALLBACK_ID,
                "private_metadata": meta,
                "title": {"type": "plain_text", "text": "Food feedback"},
                "submit": {"type": "plain_text", "text": "Submit vote"},
                "close": {"type": "plain_text", "text": "Cancel"},
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*You chose {_label(choice)}.*\n"
                                "Optional: add a short note if you want (feedback helps the kitchen)."
                            ),
                        },
                    },
                    {
                        "type": "input",
                        "block_id": "remark_block",
                        "optional": True,
                        "element": {
                            "type": "plain_text_input",
                            "action_id": "remark_text",
                            "multiline": True,
                            "max_length": 2000,
                            "placeholder": {
                                "type": "plain_text",
                                "text": "e.g. Portion small, too oily, rice was great but …",
                            },
                        },
                        "label": {"type": "plain_text", "text": "Remarks"},
                    },
                ],
            },
        )
    except Exception as e:
        logger.error(f"views_open failed: {e}")
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="❌ Could not open feedback form. Please try again or contact an admin.",
        )


def handle_remarks_modal_submit(body: dict, client, view: dict) -> None:
    """After user submits Okay/Bad + remark from the modal."""
    user_id = body["user"]["id"]
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
    except json.JSONDecodeError:
        logger.error("Invalid modal private_metadata")
        return

    poll_date = meta.get("poll_date") or str(date.today())
    channel_id = meta.get("channel_id")
    message_ts = meta.get("message_ts")
    choice = meta.get("choice")

    if not channel_id or not message_ts or choice not in ("okay", "bad"):
        logger.error("Modal metadata missing channel, ts, or choice")
        return

    values = view.get("state", {}).get("values", {})
    remark = (
        values.get("remark_block", {})
        .get("remark_text", {})
        .get("value", "")
        or ""
    ).strip()

    if get_user_vote_for_date(poll_date, user_id):
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="You already voted today — this form was not saved. 🙏",
        )
        return

    synthetic_body = {
        "user": {"id": user_id},
        "container": {"channel_id": channel_id, "message_ts": message_ts},
    }
    synthetic_action = {"value": choice}
    process_vote(
        synthetic_body,
        client,
        synthetic_action,
        remark=remark,
        poll_date=poll_date,
    )


def process_vote(
    body: dict,
    client,
    action: dict,
    remark: str = "",
    poll_date: str | None = None,
):
    """
    Central handler for recording a vote (button Great, or Okay/Bad after modal).
    Called after ack() so we have full time to do Sheets writes.
    """
    user_id = body["user"]["id"]
    choice = action["value"]  # "great" | "okay" | "bad"
    poll_date = poll_date or _poll_date_from_action(action) or str(date.today())
    channel_id = body["container"]["channel_id"]
    message_ts = body["container"]["message_ts"]

    remark = (remark or "").strip()
    if choice == "great":
        remark = ""

    # --- Deduplication ---
    previous = get_user_vote_for_date(poll_date, user_id)
    if previous:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text=f"You already voted *{_label(previous)}* today. Votes are final — thanks! 🙏",
        )
        logger.info(f"Duplicate vote blocked: {user_id} already voted {previous}")
        return

    # --- Get user's display name from Slack ---
    user_name = _get_user_name(client, user_id)

    # --- Write to Google Sheets ---
    append_vote(poll_date, user_id, user_name, choice, remark=remark)

    # --- Recalculate counts from Raw Votes (single source of truth) ---
    counts = get_counts_from_raw_votes(poll_date)
    update_daily_summary(poll_date, counts)

    # --- Update the poll message with live count ---
    _refresh_poll_message(client, channel_id, message_ts, poll_date, counts)

    # --- Confirm to the voter privately ---
    thanks = f"Got your vote: {_label(choice)} ✅  Thanks {user_name.split()[0]}!"
    if remark and choice in ("okay", "bad"):
        thanks += f"\n_Your note:_ {remark}"
    client.chat_postEphemeral(
        channel=channel_id,
        user=user_id,
        text=thanks,
    )

    logger.info(f"Vote processed: {user_name} ({user_id}) -> {choice}")


def _refresh_poll_message(client, channel_id: str, message_ts: str, poll_date: str, counts: dict):
    """Updates the original poll message with the latest vote counts."""
    try:
        updated_at = datetime.now().strftime("%H:%M:%S")
        client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=build_poll_blocks(poll_date, counts, updated_at=updated_at),
            text=(
                f"Food poll — {poll_date} | Total {sum(counts.values())} | "
                f"Updated {updated_at}"
            ),
        )
    except Exception as e:
        logger.error(f"Failed to update poll message: {e}")


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
    return {"great": "😍 Great", "okay": "😐 Okay", "bad": "😞 Bad"}.get(choice, choice)
