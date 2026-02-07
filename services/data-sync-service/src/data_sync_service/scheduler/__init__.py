from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from data_sync_service.scheduler import stock_basic_job


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(
        timezone="UTC",
        job_defaults={"coalesce": True, "max_instances": 1},
    )
    scheduler.add_job(
        stock_basic_job.run,
        stock_basic_job.build_trigger(),
        id=stock_basic_job.JOB_ID,
        replace_existing=True,
    )
    return scheduler
