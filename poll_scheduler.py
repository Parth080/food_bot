import atexit
import logging
import re

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from poll_schedule_config import (
    POLL_SCHEDULE_DAY_OF_WEEK,
    POLL_SCHEDULE_IST,
    POLL_TIMEZONE,
)

logger = logging.getLogger(__name__)

_TIME_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")


def _parse_hhmm(s: str) -> tuple[int, int]:
    s = (s or "").strip()
    m = _TIME_RE.match(s)
    if not m:
        raise ValueError(f"Invalid time {s!r}; use HH:MM 24h (IST).")
    return int(m.group(1)), int(m.group(2))


def start_scheduled_polls(post_poll_fn, channel_id: str) -> BackgroundScheduler | None:
    """
    Registers cron jobs that call post_poll_fn() at configured IST times.

    channel_id must be set (SLACK_CHANNEL_ID); scheduled posts have no slash-command channel.
    """
    if not POLL_SCHEDULE_IST:
        logger.info("Scheduled polls off — POLL_SCHEDULE_IST is empty in poll_schedule_config.py.")
        return None

    if not (channel_id or "").strip():
        logger.error(
            "poll_schedule_config has times but SLACK_CHANNEL_ID is empty — "
            "scheduled polls disabled. Set SLACK_CHANNEL_ID to the channel ID."
        )
        return None

    scheduler = BackgroundScheduler(timezone=POLL_TIMEZONE)

    for i, tm in enumerate(POLL_SCHEDULE_IST):
        hour, minute = _parse_hhmm(tm)
        trig_kw: dict = {
            "hour": hour,
            "minute": minute,
            "timezone": POLL_TIMEZONE,
        }
        if POLL_SCHEDULE_DAY_OF_WEEK:
            trig_kw["day_of_week"] = POLL_SCHEDULE_DAY_OF_WEEK
        scheduler.add_job(
            post_poll_fn,
            CronTrigger(**trig_kw),
            id=f"food_poll_{hour:02d}{minute:02d}_{i}",
            replace_existing=True,
        )
        logger.info(
            "Scheduled poll: %02d:%02d %s — %s",
            hour,
            minute,
            POLL_TIMEZONE,
            POLL_SCHEDULE_DAY_OF_WEEK or "every day",
        )

    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    logger.info("Background poll scheduler started.")
    return scheduler
