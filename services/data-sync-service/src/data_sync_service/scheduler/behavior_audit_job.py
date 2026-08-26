"""Behavior audit cron (2026-08-14 · OPT-112).

Weekdays 18:45 Asia/Shanghai — after the close chain (17:30 scores /
17:42 S-3 intake / 17:45 paper update) so the registry is stable: runs the
REAL-book vs S-3 backtest behavior audit (simulate takes a few minutes)
and persists it. The watchlist banner then shows fresh results without the
user clicking 刷新对账.

Emits an `audit_issues` webhook event when the audit finds anything —
extra holdings (买了不该买 / 该卖没卖) or missing backtest holdings
(该持没买) — so the user's phone can surface drift.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.db.webhook import emit_event

logger = logging.getLogger(__name__)

JOB_ID = "behavior_audit"
CRON_EXPRESSION = "45 18 * * 1-5"  # weekdays 18:45 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    from data_sync_service.db.paper_trading import today_iso
    from data_sync_service.service.reconciliation import run_registry_and_persist

    day = today_iso()
    try:
        out = run_registry_and_persist(day)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("behavior_audit failed: %s", exc)
        return

    available = {m: v for m, v in out["markets"].items() if v.get("available")}
    # Bark 与系统一致：单轨100%择强时，STOCK类 missing 仅当 pick==STOCK 才算问题
    try:
        from data_sync_service.service.multi_asset_sleeve import _pick as sleeve_pick

        pick = sleeve_pick()
        is_stock_pick = (pick.get("key") if isinstance(pick, dict) else getattr(pick, "key", None)) == "STOCK"
        if not is_stock_pick:
            for m in ("CN", "HK"):
                if m in available and available[m].get("missingList"):
                    # suppress STOCK missing when single-track is GOLD/OIL/NASDAQ
                    available[m]["missingList"] = []
    except Exception:  # noqa: BLE001
        pass
    n_extra = sum(len(v.get("extraList") or []) for v in available.values())
    n_missing = sum(len(v.get("missingList") or []) for v in available.values())
    insert_record(
        JOB_ID,
        success=True,
        last_ts_code=f"{out.get('reconDate')}|extra={n_extra}|missing={n_missing}",
    )
    logger.info(
        "behavior_audit ok: day=%s markets=%d extra=%d missing=%d",
        out.get("reconDate"), len(available), n_extra, n_missing,
    )

    if n_extra > 0 or n_missing > 0:
        payload = {
            "day": day,
            "markets": {
                m: {
                    "expected": v.get("expected"),
                    "actual": v.get("actual"),
                    "extra": [
                        {"symbol": e.get("symbol"), "kind": e.get("kind")}
                        for e in (v.get("extraList") or [])
                    ],
                    "missing": [m2.get("symbol") for m2 in (v.get("missingList") or [])],
                }
                for m, v in available.items()
            },
        }
        emit_event("audit_issues", payload, dedupe_key=f"audit_issues:{day}")
