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

    today = None
    trading_day = False
    try:
        today = shanghai_today()
        if is_trading_day("SSE", today) is not True:
            insert_record(JOB_ID, success=True, error_message="not a trading day — skip")
            return {"ok": True, "skipped": "not_trading_day"}
        trading_day = True
        # Idempotent: a healthy 14:30 capture makes the 14:33/14:37 retries
        # no-ops for the panel, but the daily-adjust push still self-heals a
        # lost emit (emit_event dedupes by day+mode).
        existing = sl.load_live_panel()
        if existing and str(existing.get("tradeDate") or "") == today.isoformat():
            insert_record(JOB_ID, success=True, error_message="already captured today — skip")
            _emit_action(existing)
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
        # D) 14:30 今日调整 must fire on every trading day for the SELECTED
        # mode, including harbor/homeport (the panel is satellite-only). This
        # runs even when the satellite snapshot is incomplete; for satellite
        # modes the composer says the panel is missing instead of going silent.
        _emit_action(panel if saved else {"tradeDate": panel.get("tradeDate")})
        return {"ok": saved, "panel": panel, "note": note}
    except Exception as exc:  # noqa: BLE001
        logger.exception("satellite_live_panel failed: %s", exc)
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        # The fixed daily adjust must not depend on the satellite snapshot.
        if trading_day:
            _emit_action({"tradeDate": today.isoformat() if today else ""})
        return {"ok": False, "error": str(exc)[:500]}


def _emit_action(panel: dict) -> None:
    """Push today's 14:30 「今日调整」 for the strategy the user selected (OPT-223).

    Originally only SATELLITE_MODES emitted; default/live is ``homeport``, so
    Harbor parking trail exits never reached Bark. Now every selected mode
    gets a daily adjust payload (empty day = 「今日无需调整」). Delivered
    through the webhook (Bark) path and read by the in-app notification
    aggregator. A push failure must never fail the snapshot job.
    """
    try:
        from data_sync_service.api.settings_routes import selected_strategy_mode
        from data_sync_service.db.webhook import emit_event
        from data_sync_service.service.daily_adjust import (
            compose_daily_adjust,
            load_live_sleeve,
        )
        from data_sync_service.service.satellite_actions import SATELLITE_MODES

        mode = selected_strategy_mode()
        trade_date = str(panel.get("tradeDate") or "")[:10]
        if not trade_date:
            from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

            trade_date = shanghai_today_iso()
        parking = None
        if mode in SATELLITE_MODES:
            try:
                from data_sync_service.service import multi_asset_sleeve as mas

                parking = mas._pick(as_of=trade_date)
            except Exception as exc:  # noqa: BLE001
                logger.warning("satellite_live_panel: parking pick failed: %s", exc)
        # Every mode gets the live Harbor parking state appended (the default
        # book can hold a parking leg while a satellite mode is selected).
        sleeve = load_live_sleeve(trade_date)
        action = compose_daily_adjust(
            mode,
            trade_date=trade_date,
            sleeve=sleeve,
            panel=panel,
            parking=parking,
        )
        emit_event(
            "satellite_action",
            action,
            dedupe_key=f"satellite_action:{trade_date}:{mode}",
        )
        logger.info(
            "satellite_live_panel: daily adjust pushed (%s): %s",
            mode,
            action.get("summary"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("satellite_live_panel: action push failed: %s", exc)

