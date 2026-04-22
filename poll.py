from datetime import datetime


def build_poll_blocks(poll_date: str, counts: dict = None, updated_at: str | None = None) -> list:
    """
    Builds the Slack Block Kit payload for the food poll.
    Optionally includes a live tally section if counts are passed.

    counts = {"great": 0, "okay": 0, "bad": 0}
    """
    if counts is None:
        counts = {"great": 0, "okay": 0, "bad": 0}

    total = sum(counts.values())

    if not updated_at:
        updated_at = datetime.now().strftime("%H:%M:%S")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🍽️  Food Rating — {poll_date}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*How was the food today?* Cast your vote below 👇\n"
                    "_Each person gets one vote. Results update live._\n"
                    "_Okay / Bad: you can add an optional note in a quick form after you click._"
                ),
            },
        },
        {
            "type": "actions",
            "block_id": f"food_poll_{poll_date}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "😍  Great", "emoji": True},
                    "value": "great",
                    "action_id": "vote_great",
                    "style": "primary",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "😐  Okay", "emoji": True},
                    "value": "okay",
                    "action_id": "vote_okay",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "😞  Bad", "emoji": True},
                    "value": "bad",
                    "action_id": "vote_bad",
                    "style": "danger",
                },
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Live results — {total} vote{'s' if total != 1 else ''}*\n"
                    f"😍  Great: *{counts['great']}*    "
                    f"😐  Okay: *{counts['okay']}*    "
                    f"😞  Bad: *{counts['bad']}*"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Last updated: `{updated_at}`",
                }
            ],
        },
    ]

    return blocks
