def build_poll_blocks(poll_slot: str, counts: dict = None) -> list:
    """
    Builds the Slack Block Kit payload for the food poll.
    Optionally includes a live tally section if counts are passed.

    counts = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    """
    if counts is None:
        counts = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}

    total = sum(counts.values())

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🍽️  Food Rating — {poll_slot}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*How was today's food?*\nRate from *1 (lowest)* to *5 (highest)*.",
            },
        },
        {
            "type": "actions",
            "block_id": f"food_poll_{poll_slot}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "1  😞", "emoji": True},
                    "value": "1",
                    "action_id": "vote_1",
                    "style": "danger",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "2  🙁", "emoji": True},
                    "value": "2",
                    "action_id": "vote_2",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "3  😐", "emoji": True},
                    "value": "3",
                    "action_id": "vote_3",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "4  🙂", "emoji": True},
                    "value": "4",
                    "action_id": "vote_4",
                    "style": "primary",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "5  🤩", "emoji": True},
                    "value": "5",
                    "action_id": "vote_5",
                    "style": "primary",
                },
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Add Comment", "emoji": True},
                    "value": "comment",
                    "action_id": "add_comment",
                }
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Live results — {total} vote{'s' if total != 1 else ''}*\n"
                    f"😞  1 (Lowest): *{counts['1']}*\n"
                    f"🙁  2: *{counts['2']}*\n"
                    f"😐  3: *{counts['3']}*\n"
                    f"🙂  4: *{counts['4']}*\n"
                    f"🤩  5 (Highest): *{counts['5']}*"
                ),
            },
        },
    ]

    return blocks
