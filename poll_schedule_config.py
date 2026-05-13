"""Schedule metadata for the food poll.

Scheduling is now driven by Modal cron functions defined in `modal_app.py`,
not by a process-local APScheduler. Modal cron expressions are in **UTC**, so
the IST times below are kept here only as documentation / display strings
(the `slot_time` argument passed to `post_scheduled_poll`).

To add or change a schedule:
    1. Add the IST time to POLL_SCHEDULE_IST below for documentation.
    2. Add a matching `@app.function(schedule=modal.Cron("MM HH * * *"))`
       in `modal_app.py` (converting IST → UTC; subtract 5h30m).
    3. `modal deploy modal_app.py`.

Examples (IST → UTC):
    17:00 IST = 11:30 UTC  → modal.Cron("30 11 * * *")
    09:00 IST = 03:30 UTC  → modal.Cron("30 3 * * *")
    13:00 IST = 07:30 UTC  → modal.Cron("30 7 * * *")
"""

# IANA timezone for the slot timestamps shown in Slack / written to the sheet.
POLL_TIMEZONE = "Asia/Kolkata"

# Documentation only — informs which Modal cron functions should exist.
POLL_SCHEDULE_IST = [
    "17:00",
]
