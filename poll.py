def build_poll_blocks(poll_date: str, counts: dict = None) -> list:
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
                "text": f"🍽️  Food Rating — {poll_date}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Rate today's food from 1 to 5*",
            },
        },
        {
            "type": "actions",
            "block_id": f"food_poll_{poll_date}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "1", "emoji": True},
                    "value": "1",
                    "action_id": "vote_1",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "2", "emoji": True},
                    "value": "2",
                    "action_id": "vote_2",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "3", "emoji": True},
                    "value": "3",
                    "action_id": "vote_3",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "4", "emoji": True},
                    "value": "4",
                    "action_id": "vote_4",
                    "style": "primary",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "5", "emoji": True},
                    "value": "5",
                    "action_id": "vote_5",
                    "style": "primary",
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
                    f"1: *{counts['1']}*    "
                    f"2: *{counts['2']}*    "
                    f"3: *{counts['3']}*    "
                    f"4: *{counts['4']}*    "
                    f"5: *{counts['5']}*"
                ),
            },
        },
    ]

    return blocks
