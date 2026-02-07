from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service import foo

logger = logging.getLogger(__name__)

JOB_ID = "foo_job"
CRON_EXPRESSION = "*/5 * * * *"
LOG_PATH = Path(__file__).resolve().parents[3] / "foo_job.log"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone="UTC")


def run() -> None:
    result = foo()
    logger.info("Foo job executed: %s", result)
    timestamp = datetime.now(timezone.utc).isoformat()
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp} foo ok\n")
