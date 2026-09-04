"""Uniform scheduler failure reporting (OPT-139).

Every cron ``run()`` must leave a ``sync_job_record`` row — success or
failure — so the watchdog, hub health and ``job_failed`` Bark pushes can see
it. Before this module, 15/50 jobs had no ``try`` and 21/50 never called
``insert_record`` (notably ``twin_star_reminder``: the most important push
had no execution record at all).

Three primitives; jobs keep their exact log lines and skip semantics —
the guard only adds records:

- :func:`record_success` / :func:`record_failure` — thin ``insert_record``
  wrappers so tests can patch a single target
  (``data_sync_service.scheduler._job_guard.insert_record``).
- :func:`run_guarded` — exception-path wrapper for ``run()`` bodies that
  do their own result mapping: on exception it records failure, logs the
  traceback and returns ``None`` (the cron keeps its exit-code convention).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

_ERROR_MAX_LEN = 500


def _error_message(error: BaseException | str) -> str:
    if isinstance(error, BaseException):
        return f"{type(error).__name__}: {error}"[:_ERROR_MAX_LEN]
    return str(error or "unknown error")[:_ERROR_MAX_LEN]


def record_success(job_id: str, *, last_ts_code: str | None = None) -> None:
    """Record a successful cron run (gate-skips count as success)."""
    insert_record(job_id, success=True, last_ts_code=last_ts_code)


def record_failure(
    job_id: str,
    error: BaseException | str,
    *,
    last_ts_code: str | None = None,
) -> None:
    """Record a failed cron run. Never raises (record path must not crash jobs)."""
    try:
        insert_record(
            job_id,
            success=False,
            last_ts_code=last_ts_code,
            error_message=_error_message(error),
        )
    except Exception:  # noqa: BLE001
        logger.warning("job_guard: failed to record failure for %s", job_id)


def run_guarded[T](
    job_id: str,
    fn: Callable[[], T],
    *,
    log: logging.Logger | None = None,
) -> T | None:
    """Run ``fn``; on exception record failure + log traceback, return ``None``.

    Success path returns ``fn()`` untouched — callers that map result dicts
    to success/failure keep their own logic and use :func:`record_success` /
    :func:`record_failure` for the record.

    NOTE: the exception path also returns ``None``, so ``fn`` must not
    return ``None`` on success (use explicit try/except in that case —
    see ``daily_sync_job``).
    """
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        record_failure(job_id, exc)
        (log or logger).exception("%s failed: %s", job_id, exc)
        return None


def record_dict_result(
    job_id: str,
    result: Any,
    *,
    ok_log: Callable[[Any], None] | None = None,
    fail_log: Callable[[Any], None] | None = None,
) -> Any:
    """Record a ``{"ok": bool, ...}`` service result; keep the job's own logs.

    Returns ``result`` unchanged. Non-dict results are recorded as success
    (legacy jobs that return ``None`` on success).
    """
    if not isinstance(result, dict) or result.get("ok", True):
        record_success(job_id)
        if ok_log is not None:
            ok_log(result)
    else:
        record_failure(job_id, result.get("error", "unknown"))
        if fail_log is not None:
            fail_log(result)
    return result
