"""Trading-session briefs (2026-08-11) — 10:00 / 12:00 / 14:00 weekdays.

Matches the user's real trading rhythm:
  - open   (10:00): regime + panic, S-3 candidates, overnight news top5.
  - midday (12:00): candidate drift, held names near stop lines, news.
  - action (14:00): BUY cards + conditional-stop list (broker side) + alerts.
    (2026-08-14 · OPT-113: moved 14:30 → 14:00 to match the user's entry
    time and the 14:00 intraday-lock freeze snapshot.)

Each run assembles existing data blocks (portfolio_health, s3 candidates,
news selection) and stores a `trading-<type>` row in morning_briefs —
served by the existing GET /news/brief/latest?brief_type=... API.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.trading_brief import generate_trading_brief

logger = logging.getLogger(__name__)

JOB_ID = "trading_brief"
TIMEZONE = "Asia/Shanghai"
CRON_EXPRESSIONS = {
    "open": "0 10 * * 1-5",
    "midday": "0 12 * * 1-5",
    "action": "0 14 * * 1-5",
}


def build_trigger(brief_type: str) -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSIONS[brief_type], timezone=TIMEZONE)


def _run(brief_type: str) -> None:
    job_id = f"{JOB_ID}_{brief_type}"
    try:
        brief = generate_trading_brief(brief_type)
    except Exception as exc:  # noqa: BLE001
        insert_record(job_id, success=False, error_message=str(exc))
        logger.warning("%s failed: %s", job_id, exc)
        return
    n = len(brief.get("items") or [])
    insert_record(job_id, success=True, last_ts_code=f"{brief.get('briefDate')}|{n}")
    logger.info("%s ok: date=%s sections=%d", job_id, brief.get("briefDate"), n)


def run_open() -> None:
    _run("open")


def run_midday() -> None:
    _run("midday")


def run_action() -> None:
    _run("action")
