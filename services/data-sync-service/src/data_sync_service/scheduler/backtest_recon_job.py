"""Weekly backtest-vs-paper reconciliation (2026-08-11).

Monday 07:30 Asia/Shanghai — reconciles LAST FRIDAY: the S-3 backtest's
"should hold" list vs the paper book's actual holdings, per market, and
persists the snapshot (db/backtest_paper_recon). Feed the latest snapshot
to the decision agent / weekly review.

The backtest replay takes a few minutes (valid-window CN+HK simulate) —
fine for a weekly job.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.reconciliation import run_and_persist

logger = logging.getLogger(__name__)

JOB_ID = "backtest_paper_recon"
CRON_EXPRESSION = "30 7 * * 1"  # Monday 07:30 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _last_friday() -> str:
    d = datetime.now(tz=ZoneInfo(TIMEZONE)).date()
    while d.weekday() != 4:
        d -= timedelta(days=1)
    return d.isoformat()


def run() -> None:
    day = _last_friday()
    try:
        out = run_and_persist(day)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("backtest_paper_recon failed: %s", exc)
        return
    n = sum(1 for m in out["markets"].values() if m.get("available"))
    insert_record(JOB_ID, success=True, last_ts_code=f"{day}|{n}")
    logger.info("backtest_paper_recon ok: day=%s markets=%d", day, n)

    # E7 (webhook design §2): recon missing-trades pushes a webhook event.
    missing_markets = [
        m for m, md in out.get("markets", {}).items() if (md.get("missing") or 0) > 0
    ]
    if missing_markets:
        from data_sync_service.db.webhook import emit_event

        emit_event(
            "recon_missing",
            {"day": day, "markets": missing_markets},
            dedupe_key=f"recon_missing:{day}",
        )
