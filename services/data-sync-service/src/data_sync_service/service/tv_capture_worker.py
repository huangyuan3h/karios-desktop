"""Background worker for TradingView screener capture jobs."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from data_sync_service.db import tv_capture_jobs as jobdb
from data_sync_service.service.tv import process_capture_job

logger = logging.getLogger(__name__)

MAX_CONCURRENT_CAPTURES = 2
POLL_INTERVAL_S = 0.5

_executor: ThreadPoolExecutor | None = None
_dispatcher_thread: threading.Thread | None = None
_stop_event = threading.Event()
_active_lock = threading.Lock()
_active_count = 0


def _set_active(delta: int) -> int:
    global _active_count
    with _active_lock:
        _active_count += delta
        return _active_count


def _run_job(job_id: str) -> None:
    try:
        process_capture_job(job_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("capture job failed unexpectedly: %s", job_id)
        try:
            jobdb.mark_failed(job_id=job_id, error_message=str(exc) or exc.__class__.__name__)
        except Exception:
            pass
    finally:
        _set_active(-1)


def _dispatch_loop() -> None:
    while not _stop_event.is_set():
        try:
            with _active_lock:
                slots = max(0, MAX_CONCURRENT_CAPTURES - _active_count)
            if slots > 0:
                jobs = jobdb.claim_next_jobs(limit=slots)
                for job in jobs:
                    jid = str(job.get("id") or "")
                    if not jid:
                        continue
                    _set_active(1)
                    assert _executor is not None
                    _executor.submit(_run_job, jid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("tv capture dispatcher error: %s", exc)
        _stop_event.wait(POLL_INTERVAL_S)


def start_tv_capture_worker() -> None:
    global _executor, _dispatcher_thread
    if _dispatcher_thread is not None and _dispatcher_thread.is_alive():
        return
    _stop_event.clear()
    try:
        jobdb.reset_stale_running_jobs()
    except Exception as exc:  # noqa: BLE001
        logger.warning("reset stale tv capture jobs failed: %s", exc)
    _executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CAPTURES, thread_name_prefix="tv-cap")
    _dispatcher_thread = threading.Thread(
        target=_dispatch_loop,
        name="tv-capture-dispatcher",
        daemon=True,
    )
    _dispatcher_thread.start()
    logger.info("tv capture worker started (max_concurrent=%s)", MAX_CONCURRENT_CAPTURES)


def stop_tv_capture_worker() -> None:
    global _executor, _dispatcher_thread
    _stop_event.set()
    if _dispatcher_thread is not None:
        _dispatcher_thread.join(timeout=5)
        _dispatcher_thread = None
    if _executor is not None:
        _executor.shutdown(wait=False, cancel_futures=True)
        _executor = None


def wake_tv_capture_worker() -> None:
    """Hint dispatcher to poll sooner (no-op beyond clearing wait)."""
    _stop_event.set()
    _stop_event.clear()
