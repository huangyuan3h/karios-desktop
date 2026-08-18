"""Candidate-add diff (E5 · todo §14 #3 · P2 · 2026-08-12).

Runs at 17:35 Asia/Shanghai weekdays — after watchlist_automation (17:30)
writes fresh scores, before paper_s3_intake (17:42).

Evaluation (2026-08-12): candidate DISAPPEARANCES are usually the gate
closing (Weak regime / panic cooldown / circuit) — normal noise, not
push-worthy. Candidate ADDITIONS (new symbols qualifying vs the previous
trading day) are the actionable signal (gate reopened / new strong RS
stocks), so we push those only, one event per market per day.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.trade_calendar import last_trading_day
from data_sync_service.service.paper_s3 import build_s3_candidates

logger = logging.getLogger(__name__)

JOB_ID = "candidate_diff"
CRON_EXPRESSION = "35 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _symbol_of(c: dict) -> str:
    return str(c.get("symbol") or "")


def candidate_diff(*, trade_date: str | None = None) -> dict:
    """Compare today's S-3 candidates vs the previous trading day's."""
    day = trade_date or date.today().isoformat()
    added_by_market: dict[str, list[str]] = {}
    for market in ("CN", "HK"):
        try:
            today_cands = {_symbol_of(c) for c in build_s3_candidates(trade_date=day, market=market)}
            # last_trading_day is on-or-before semantics → step back one day
            # first so we get the STRICT previous trading day.
            prev_day = last_trading_day(market, date.fromisoformat(day) - timedelta(days=1))
            prev_cands = {
                _symbol_of(c)
                for c in build_s3_candidates(trade_date=prev_day.isoformat(), market=market)
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("candidate_diff[%s] failed: %s", market, exc)
            continue
        added = sorted(today_cands - prev_cands)
        if added:
            from data_sync_service.db.webhook import emit_event

            emit_event(
                "candidate_added",
                {"market": market, "date": day, "added": added, "count": len(added)},
                dedupe_key=f"candidate_added:{market}:{day}",
            )
            added_by_market[market] = added
    return {"ok": True, "date": day, "added_by_market": added_by_market}


def run() -> None:
    try:
        result = candidate_diff()
        if result["added_by_market"]:
            logger.info(
                "candidate diff: %s",
                {m: len(v) for m, v in result["added_by_market"].items()},
            )
    except Exception:  # noqa: BLE001
        logger.exception("candidate diff job failed")
