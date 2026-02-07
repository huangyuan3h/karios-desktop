from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.background import BackgroundScheduler

from data_sync_service.scheduler import foo_job

_foo_status: Dict[str, Any] = {
    "last_run_at": None,
    "last_run_ok": None,
    "last_error": None,
    "trigger": None,
}


def _to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _handle_job_event(event: JobExecutionEvent) -> None:
    if event.job_id != "foo_job":
        return
    _foo_status["last_run_at"] = _to_iso(datetime.now(timezone.utc))
    if event.exception:
        _foo_status["last_run_ok"] = False
        _foo_status["last_error"] = str(event.exception)
    else:
        _foo_status["last_run_ok"] = True
        _foo_status["last_error"] = None


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="UTC", job_defaults={"coalesce": True, "max_instances": 1})
    trigger = foo_job.build_trigger()
    _foo_status["trigger"] = f"{trigger} ({foo_job.CRON_EXPRESSION})"
    scheduler.add_listener(_handle_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.add_job(
        foo_job.run,
        trigger,
        id=foo_job.JOB_ID,
        replace_existing=True,
    )
    return scheduler


def get_foo_status() -> Dict[str, Any]:
    return dict(_foo_status)
