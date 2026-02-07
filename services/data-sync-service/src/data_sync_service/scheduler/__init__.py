from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from data_sync_service.scheduler.jobs import run_foo_job


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="UTC", job_defaults={"coalesce": True, "max_instances": 1})
    scheduler.add_job(
        run_foo_job,
        IntervalTrigger(seconds=60),
        id="foo_job",
        replace_existing=True,
    )
    return scheduler
