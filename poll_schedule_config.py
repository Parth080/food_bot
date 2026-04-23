"""
Automatic poll posting — edit times here and redeploy.

Times use 24-hour clock in Asia/Kolkata (IST). The process must stay running
(e.g. Render web service + Gunicorn) for jobs to fire.
"""

# IANA timezone for all scheduled triggers (do not change unless you move regions).
POLL_TIMEZONE = "Asia/Kolkata"

# Post the food poll at these local times. Empty list = no cron (only /startpoll).
# Example: lunch + dinner
POLL_SCHEDULE_IST = [
    "11:20",
    # "19:00",
]

# APScheduler day_of_week. None = every day.
# Examples: "mon-fri", "sat-sun", "mon,wed,fri"
POLL_SCHEDULE_DAY_OF_WEEK = None
