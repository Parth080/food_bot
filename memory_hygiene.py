import gc
import logging
import os
import threading
import time
from ctypes import CDLL

logger = logging.getLogger(__name__)

_started = False


def _read_positive_int(env_key: str, default: int) -> int:
    raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        logger.warning("Invalid %s=%r, using default=%s", env_key, raw, default)
        return default


def _run_cleanup_cycle(use_malloc_trim: bool, libc) -> None:
    # Explicit GC helps long-running bots release cyclic refs periodically.
    collected = gc.collect()
    if use_malloc_trim and libc is not None:
        try:
            libc.malloc_trim(0)
        except Exception as e:
            logger.debug("malloc_trim failed: %s", e)
    logger.debug("Memory cleanup cycle complete (gc_collected=%s)", collected)


def start_memory_hygiene() -> None:
    """
    Starts a daemon thread that periodically runs garbage collection and, on Linux,
    optionally calls libc malloc_trim(0) to return free heap pages to the OS.
    """
    global _started
    if _started:
        return

    enabled = (os.environ.get("MEMORY_HYGIENE_ENABLED") or "1").strip().lower()
    if enabled in {"0", "false", "no"}:
        logger.info("Memory hygiene disabled (MEMORY_HYGIENE_ENABLED=%s)", enabled)
        return

    interval_seconds = _read_positive_int("MEMORY_CLEANUP_INTERVAL_SECONDS", 900)
    use_malloc_trim = (
        (os.environ.get("MEMORY_USE_MALLOC_TRIM") or "1").strip().lower()
        not in {"0", "false", "no"}
    )

    libc = None
    if use_malloc_trim:
        try:
            libc = CDLL("libc.so.6")
        except Exception:
            # Expected on non-Linux/macOS local runs.
            libc = None

    def _loop() -> None:
        while True:
            _run_cleanup_cycle(use_malloc_trim=use_malloc_trim, libc=libc)
            time.sleep(interval_seconds)

    thread = threading.Thread(target=_loop, name="memory-hygiene", daemon=True)
    thread.start()
    _started = True
    logger.info(
        "Memory hygiene started (interval=%ss, malloc_trim=%s)",
        interval_seconds,
        bool(libc and use_malloc_trim),
    )
