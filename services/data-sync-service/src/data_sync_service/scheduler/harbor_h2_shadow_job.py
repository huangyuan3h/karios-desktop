"""H2 Harbor shadow ledger updater (Live-track, display-only).

Weekdays 18:35 Asia/Shanghai — after close_sync (17:10), sleeve_etf_daily_sync
(17:25) and sleeve_paper_auto (18:20): rebuild the trailing-year Harbor
Timeline, blend the H2 hysteresis parking leg, and append the missing paper
days to ``data/backtest_reports/harbor_h2_shadow_latest.json``.

Idempotent: rows are keyed by date; a re-run on the same day records
"up to date" without rebuilding. Failures land in sync_job_record (low
severity — a validation line never pages the phone) and surface in the
system inbox + Scheduler page. Never touches paper_trades / user_trades /
watchlist automation.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID = "harbor_h2_shadow"
CRON_EXPRESSION = "35 18 * * 1-5"  # weekdays 18:35 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> dict:
    from data_sync_service.api import backtest_routes as br
    from data_sync_service.service import harbor_h2_shadow as sh
    from data_sync_service.service.parking_sleeve import blend_harbor_h2_timeline
    from data_sync_service.service.trade_calendar_utils import (
        last_open_date_on_or_before,
        shanghai_today,
    )

    try:
        prev = sh.load_shadow_report()
        end_d = last_open_date_on_or_before(shanghai_today())
        if end_d is None:
            insert_record(JOB_ID, success=True, error_message="no open day in range — skip")
            return {"ok": True, "skipped": "no open day"}
        end = end_d.isoformat()
        if prev and str((prev.get("latest") or {}).get("date") or "") >= end:
            insert_record(JOB_ID, success=True, last_ts_code=end, error_message="up to date")
            return {"ok": True, "upToDate": end}
        start = (end_d - timedelta(days=sh.WINDOW_DAYS)).isoformat()
        # force=True: bypass the in-memory timeline cache — a morning warmup
        # would otherwise serve pre-close rows and the report would fail with
        # "day series does not cover end" (2026-09-17 audit).
        harbor_result, _ = br._get_or_build_timeline(start, end, strategy="harbor", force=True)
        h2_result = blend_harbor_h2_timeline(harbor_result)
        report = sh.build_shadow_report(end, harbor_result, h2_result, prev)
        sh.save_shadow_report(report)
        latest = report.get("latest") or {}
        insert_record(
            JOB_ID,
            success=True,
            last_ts_code=end,
            error_message=(
                f"appended {report.get('appended', 0)} day(s); "
                f"spread {latest.get('spreadPt')}pt status={latest.get('status')}"
            ),
        )
        logger.info(
            "harbor_h2_shadow %s: appended=%s spread=%spt status=%s",
            end, report.get("appended", 0), latest.get("spreadPt"), latest.get("status"),
        )
        return {"ok": True, "end": end, "report": report}
    except Exception as exc:  # noqa: BLE001
        logger.exception("harbor_h2_shadow: %s", exc)
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        return {"ok": False, "error": str(exc)[:500]}
