"""Live 14:30 satellite panel snapshot job (OPT-222).

Weekdays 14:30 Asia/Shanghai — while the session is open (CN afternoon
session), capture a market-wide realtime quote snapshot and build TODAY's
habit panel so the watchlist satellite card can show "what to buy at 14:30
if the gate opens" instead of yesterday's replay state.

Order matters: quotes are fetched first (the 14:30 print is time-sensitive),
then the frozen context is loaded and the quotes injected. The panel is
persisted as ``data/backtest_reports/satellite_live_panel_latest.json`` only
when complete (``decisionAvailable`` + quote coverage above the floor);
otherwise the previous snapshot stays and a failure lands in
``sync_job_record`` for the watchdog / health surfaces.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID = "satellite_live_panel"
# weekdays 14:30 Asia/Shanghai — PRIMARY capture, plus staggered in-session
# retries (14:33 / 14:37) for a transient quote failure or a restart inside the
# window. `run()` is idempotent (skips once today's panel is persisted), so the
# retries no-op on a healthy day and only act when the 14:30 capture was lost.
# Day-of-week uses NAMES: APScheduler reads 0 as Monday, so the old "1-5" meant
# Tue-Sat and silently skipped every Monday (bug fixed 2026-09-21).
CRON_EXPRESSION = "30,33,37 14 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> dict:
    from data_sync_service.db.trade_calendar import is_trading_day
    from data_sync_service.service import satellite_live as sl
    from data_sync_service.service.trade_calendar_utils import shanghai_today

    try:
        today = shanghai_today()
        if is_trading_day("SSE", today) is not True:
            insert_record(JOB_ID, success=True, error_message="not a trading day — skip")
            return {"ok": True, "skipped": "not_trading_day"}
        # Idempotent: a healthy 14:30 capture makes the 14:33/14:37 retries
        # no-ops (never re-fetch, never re-push). A lost capture is retried
        # inside the session; after the close nothing runs (no catch-up).
        existing = sl.load_live_panel()
        if existing and str(existing.get("tradeDate") or "") == today.isoformat():
            insert_record(JOB_ID, success=True, error_message="already captured today — skip")
            return {"ok": True, "skipped": "already_captured"}
        panel = sl.build_live_panel(today.isoformat())
        quotes = panel.pop("_quotes", None) or {}
        quote_persisted = True
        if quotes:
            try:
                stored = sl.persist_quotes(panel.get("tradeDate") or today.isoformat(), quotes)
                expected = int(panel.get("quoted") or 0)
                quote_persisted = expected > 0 and stored >= expected
                logger.info(
                    "[satellite_live_panel] 14:30 prints persisted: %d/%d",
                    stored,
                    expected,
                )
            except Exception as exc:  # noqa: BLE001
                quote_persisted = False
                logger.warning("[satellite_live_panel] 14:30 print persist failed: %s", exc)
        panel["quotePersisted"] = quote_persisted
        saved, note = sl.persist_if_complete(panel)
        insert_record(
            JOB_ID,
            success=saved,
            last_ts_code=panel.get("tradeDate"),
            error_message=(note if saved else f"not persisted: {note}")[:500],
        )
        if saved:
            _emit_action(panel)
            logger.info(
                "satellite_live_panel %s: %s (breadth=%s gap=%s pool=%s)",
                panel.get("tradeDate"),
                note,
                panel.get("breadth1430"),
                panel.get("gapCount"),
                panel.get("poolSize"),
            )
        else:
            logger.warning("satellite_live_panel not persisted: %s", note)
        return {"ok": saved, "panel": panel, "note": note}
    except Exception as exc:  # noqa: BLE001
        logger.exception("satellite_live_panel failed: %s", exc)
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        return {"ok": False, "error": str(exc)[:500]}


def _emit_action(panel: dict) -> None:
    """OPT-223: push today's action for the strategy the user selected.

    Composed from the same payload the card renders; delivered through the
    webhook (Bark) path and read by the in-app notification aggregator. A push
    failure must never fail the snapshot job.
    """
    try:
        from data_sync_service.api.settings_routes import selected_strategy_mode
        from data_sync_service.db.webhook import emit_event
        from data_sync_service.service.satellite_actions import (
            SATELLITE_MODES,
            compose_satellite_action,
        )

        mode = selected_strategy_mode()
        if mode not in SATELLITE_MODES:
            logger.info("satellite_live_panel: mode=%s has no satellite leg — no action push", mode)
            return
        parking = None
        try:
            from data_sync_service.service import multi_asset_sleeve as mas

            parking = mas._pick(as_of=str(panel.get("tradeDate") or ""))
        except Exception as exc:  # noqa: BLE001
            logger.warning("satellite_live_panel: parking pick failed: %s", exc)
        action = compose_satellite_action(panel, mode, parking=parking)
        emit_event(
            "satellite_action",
            action,
            dedupe_key=f"satellite_action:{panel.get('tradeDate')}:{mode}",
        )
        logger.info("satellite_live_panel: action pushed (%s)", action.get("summary"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("satellite_live_panel: action push failed: %s", exc)
