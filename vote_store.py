import threading
import logging

logger = logging.getLogger(__name__)

# Thread-safe lock for concurrent vote writes
_lock = threading.Lock()

# Structure: { "2024-01-20": { "U0ABC123": "great", "U0DEF456": "okay" } }
_store: dict[str, dict[str, str]] = {}


def has_voted(poll_date: str, user_id: str) -> bool:
    with _lock:
        return user_id in _store.get(poll_date, {})


def get_previous_vote(poll_date: str, user_id: str) -> str | None:
    with _lock:
        return _store.get(poll_date, {}).get(user_id)


def record_vote(poll_date: str, user_id: str, choice: str):
    with _lock:
        if poll_date not in _store:
            _store[poll_date] = {}
        _store[poll_date][user_id] = choice
        logger.info(f"Recorded vote in memory: {user_id} -> {choice} ({poll_date})")


def get_counts(poll_date: str) -> dict:
    with _lock:
        votes = _store.get(poll_date, {}).values()
        return {
            "great": sum(1 for v in votes if v == "great"),
            "okay": sum(1 for v in votes if v == "okay"),
            "bad": sum(1 for v in votes if v == "bad"),
        }


def get_total(poll_date: str) -> int:
    counts = get_counts(poll_date)
    return sum(counts.values())
