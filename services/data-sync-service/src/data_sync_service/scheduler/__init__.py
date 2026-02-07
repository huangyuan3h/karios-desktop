from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(
        timezone="UTC",
        job_defaults={"coalesce": True, "max_instances": 1},
    )
    # Register cron jobs: one module per job, add_job(module.run, module.build_trigger(), id=module.JOB_ID)
    return scheduler
