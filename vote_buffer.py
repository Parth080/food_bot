"""In-memory vote buffer with periodic batched flush to Google Sheets.

Why this exists:
    Each vote previously triggered ~6 Sheets API calls (dedupe read, append,
    counts read, summary read, summary upsert, summary reorganize). Google's
    Sheets API allows ~60 reads + 60 writes per user per minute, so a burst
    of 500 simultaneous votes would exceed the quota by ~10x and queue requests
    for minutes. With this buffer:

      * Vote arrives → recorded in memory instantly → ack() returns fast.
      * A background flusher thread batches all pending rows into one
        `values.append` call every `flush_interval` seconds (or when the
        pending queue crosses `flush_threshold`).
      * Counts shown in Slack come from the in-memory state (always current).
      * Dedupe checks use the in-memory state (after lazy-loading existing
        rows from the sheet on the first access for each poll slot).

Crash safety:
    The buffer flushes on graceful shutdown via `shutdown()`. If a container
    is killed without warning, at most `flush_interval` seconds of votes may
    be lost. On a fresh container, the buffer rehydrates from the sheet the
    first time a poll slot is touched, so dedupe stays correct across restarts.

Single-container assumption:
    The Modal deployment runs the web app with `max_containers=1` so that
    only one process owns this buffer. Do NOT scale out without moving state
    to a shared store (Redis / Modal Dict).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_RATINGS = frozenset({"1", "2", "3", "4", "5"})
_APP_TZ = os.environ.get("APP_TIMEZONE", "Asia/Kolkata")


class VoteBuffer:
    def __init__(
        self,
        flush_interval: float = 3.0,
        flush_threshold: int = 50,
    ) -> None:
        self._lock = threading.RLock()
        # poll_slot -> { user_id -> entry }
        # entry: { vote, comment, user_name, message_ts, submitted_at }
        self._state: dict[str, dict[str, dict]] = {}
        self._loaded_slots: set[str] = set()
        # Pending rows queued for bulk append: each row is the 7-column raw vote row
        # [slot, submitted_at, user_id, user_name, choice, comment, message_ts]
        self._pending: list[list[str]] = []
        self._dirty_slots: set[str] = set()
        self._flush_interval = flush_interval
        self._flush_threshold = flush_threshold
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="vote-buffer-flush", daemon=True
        )
        self._thread.start()
        logger.info(
            "VoteBuffer started (interval=%.1fs, threshold=%d)",
            self._flush_interval,
            self._flush_threshold,
        )

    def shutdown(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
        try:
            self.flush()
        except Exception:
            logger.exception("Final flush on shutdown failed")

    # ── State helpers ──────────────────────────────────────────────────────
    def _now_str(self) -> str:
        return datetime.now(ZoneInfo(_APP_TZ)).strftime("%H:%M:%S")

    def _ensure_loaded(self, slot: str) -> None:
        """Hydrate in-memory state for `slot` from the sheet (once per slot)."""
        if slot in self._loaded_slots:
            return
        try:
            from sheets import _get_service, read_raw_votes_rows_for_slot
            service = _get_service()
            rows = read_raw_votes_rows_for_slot(service, slot)
        except Exception:
            logger.exception("Failed to hydrate slot %s from sheet", slot)
            rows = []
        with self._lock:
            if slot in self._loaded_slots:
                return
            slot_state = self._state.setdefault(slot, {})
            for row in rows:
                if len(row) < 5:
                    continue
                user_id = row[2]
                user_name = row[3] if len(row) > 3 else ""
                choice = row[4] if len(row) > 4 else ""
                comment = (row[5] if len(row) > 5 else "").strip()
                msg_ts = (row[6] if len(row) > 6 else "").strip()
                entry = slot_state.setdefault(user_id, {})
                if choice in _RATINGS and not entry.get("vote"):
                    entry["vote"] = choice
                if comment and not entry.get("comment"):
                    entry["comment"] = comment
                if user_name and not entry.get("user_name"):
                    entry["user_name"] = user_name
                if msg_ts and not entry.get("message_ts"):
                    entry["message_ts"] = msg_ts
            self._loaded_slots.add(slot)

    # ── Dedupe queries (same rules as the old sheets.py helpers) ───────────
    def get_user_vote(
        self, slot: str, user_id: str, message_ts: str | None
    ) -> str | None:
        self._ensure_loaded(slot)
        with self._lock:
            entry = self._state.get(slot, {}).get(user_id)
            if not entry:
                return None
            vote = entry.get("vote", "")
            if vote not in _RATINGS:
                return None
            ts = (message_ts or "").strip()
            row_ts = (entry.get("message_ts") or "").strip()
            if not ts or ts == row_ts or not row_ts:
                return vote
            return None

    def get_user_comment(
        self, slot: str, user_id: str, message_ts: str | None
    ) -> str | None:
        self._ensure_loaded(slot)
        with self._lock:
            entry = self._state.get(slot, {}).get(user_id)
            if not entry:
                return None
            comment = (entry.get("comment") or "").strip()
            if not comment:
                return None
            ts = (message_ts or "").strip()
            row_ts = (entry.get("message_ts") or "").strip()
            if not ts or ts == row_ts or not row_ts:
                return comment
            return None

    # ── Writes ─────────────────────────────────────────────────────────────
    def record_vote(
        self,
        slot: str,
        user_id: str,
        user_name: str,
        choice: str,
        message_ts: str,
    ) -> None:
        self._ensure_loaded(slot)
        now = self._now_str()
        ts = (message_ts or "").strip()
        with self._lock:
            entry = self._state.setdefault(slot, {}).setdefault(user_id, {})
            entry["vote"] = choice
            entry["user_name"] = user_name
            entry["message_ts"] = ts
            entry["submitted_at"] = now
            entry.setdefault("comment", "")
            self._pending.append([slot, now, user_id, user_name, choice, "", ts])
            self._dirty_slots.add(slot)
            should_flush = len(self._pending) >= self._flush_threshold
        if should_flush:
            self._flush_async()

    def record_comment(
        self,
        slot: str,
        user_id: str,
        user_name: str,
        comment: str,
        message_ts: str,
    ) -> None:
        self._ensure_loaded(slot)
        now = self._now_str()
        ts = (message_ts or "").strip()
        clean = (comment or "").strip()
        with self._lock:
            entry = self._state.setdefault(slot, {}).setdefault(user_id, {})
            entry["comment"] = clean
            entry["user_name"] = user_name
            entry["message_ts"] = ts
            entry["submitted_at"] = now
            entry.setdefault("vote", "")
            self._pending.append([slot, now, user_id, user_name, "", clean, ts])
            self._dirty_slots.add(slot)
            should_flush = len(self._pending) >= self._flush_threshold
        if should_flush:
            self._flush_async()

    # ── Read counts (used to refresh the Slack poll message) ──────────────
    def get_counts(self, slot: str) -> dict[str, int]:
        self._ensure_loaded(slot)
        counts = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
        with self._lock:
            for entry in self._state.get(slot, {}).values():
                v = entry.get("vote", "")
                if v in counts:
                    counts[v] += 1
        return counts

    # ── Flush mechanics ────────────────────────────────────────────────────
    def _flush_async(self) -> None:
        # Fire-and-forget; another in-flight flush is fine, the lock serialises.
        threading.Thread(
            target=self.flush, name="vote-buffer-flush-immediate", daemon=True
        ).start()

    def flush(self) -> None:
        with self._lock:
            if not self._pending and not self._dirty_slots:
                return
            to_append = list(self._pending)
            self._pending.clear()
            dirty = set(self._dirty_slots)
            self._dirty_slots.clear()

        if to_append:
            try:
                from sheets import bulk_append_votes
                bulk_append_votes(to_append)
                logger.info("Flushed %d rows to Raw Votes", len(to_append))
            except Exception:
                logger.exception(
                    "bulk_append_votes failed; re-queueing %d rows", len(to_append)
                )
                with self._lock:
                    # Preserve insertion order — prepend the failed batch.
                    self._pending = to_append + self._pending
                    self._dirty_slots.update(dirty)
                return

        for slot in dirty:
            counts = self.get_counts(slot)
            try:
                from sheets import update_daily_summary
                update_daily_summary(slot, counts)
            except Exception:
                logger.exception("Daily summary update failed for slot %s", slot)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self._flush_interval)
            if self._stop.is_set():
                break
            try:
                self.flush()
            except Exception:
                logger.exception("Flush loop error")


_BUFFER: VoteBuffer | None = None
_BUFFER_LOCK = threading.Lock()


def get_buffer() -> VoteBuffer:
    """Process-wide singleton. Started lazily; safe to call before/after start()."""
    global _BUFFER
    with _BUFFER_LOCK:
        if _BUFFER is None:
            interval = float(os.environ.get("VOTE_FLUSH_INTERVAL_SECONDS", "3"))
            threshold = int(os.environ.get("VOTE_FLUSH_THRESHOLD", "50"))
            _BUFFER = VoteBuffer(flush_interval=interval, flush_threshold=threshold)
        return _BUFFER
