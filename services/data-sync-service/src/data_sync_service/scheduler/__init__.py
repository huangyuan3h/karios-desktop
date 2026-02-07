from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from data_sync_service.scheduler import adj_factor_job, daily_sync_job, stock_basic_job


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
    scheduler.add_job(
        daily_sync_job.run,
        daily_sync_job.build_trigger(),
        id=daily_sync_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        adj_factor_job.run,
        adj_factor_job.build_trigger(),
        id=adj_factor_job.JOB_ID,
        replace_existing=True,
    )
    return scheduler
