from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-not-found]
from apscheduler.triggers.date import DateTrigger

from data_sync_service.scheduler import (
    adj_factor_job,
    allocation_decide_job,
    alpha_radar_fetch_job,
    alpha_radar_ingest_job,
    alpha_radar_process_job,
    backtest_recon_job,
    behavior_audit_job,
    candidate_diff_job,
    close_catchup_job,
    close_sync_job,
    cn_industry_post_close_job,
    daily_sync_job,
    decision_action_job,
    decision_outcome_job,
    decision_snapshot_job,
    eastmoney_industry_job,
    etf_daily_job,
    fund_basic_job,
    hk_basic_job,
    hk_daily_job,
    hk_industry_job,
    index_basic_job,
    index_daily_job,
    intraday_alarm_job,
    intraday_score_job,
    macro_daily_job,
    minute_capture_job,
    morning_brief_job,
    news_enrich_job,
    news_fetch_job,
    paper_backtest_mirror_job,
    paper_chain_watchdog_job,
    paper_s3_intake_job,
    paper_trading_intake_job,
    paper_trading_update_job,
    research_report_job,
    rolling_oos_job,
    sleeve_paper_job,
    stock_basic_job,
    timeline_warmup_job,
    trading_brief_job,
    watchlist_automation_job,
    webhook_delivery_job,
    weekly_review_job,
)


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(
        timezone="UTC",
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            # Allow running missed jobs after wake-up/restart (seconds).
            "misfire_grace_time": 12 * 60 * 60,
        },
    )
    scheduler.add_job(
        stock_basic_job.run,
        stock_basic_job.build_trigger(),
        id=stock_basic_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        hk_basic_job.run,
        hk_basic_job.build_trigger(),
        id=hk_basic_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        hk_daily_job.run,
        hk_daily_job.build_trigger(),
        id=hk_daily_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        fund_basic_job.run,
        fund_basic_job.build_trigger(),
        id=fund_basic_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        etf_daily_job.run,
        etf_daily_job.build_trigger(),
        id=etf_daily_job.JOB_ID,
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
    scheduler.add_job(
        close_sync_job.run,
        close_sync_job.build_trigger(),
        id=close_sync_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        close_catchup_job.run,
        close_catchup_job.build_trigger(),
        id=close_catchup_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        news_fetch_job.run,
        news_fetch_job.build_trigger(),
        id=news_fetch_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        news_enrich_job.run,
        news_enrich_job.build_trigger(),
        id=news_enrich_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        alpha_radar_ingest_job.run,
        alpha_radar_ingest_job.build_trigger(),
        id=alpha_radar_ingest_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        alpha_radar_process_job.run,
        alpha_radar_process_job.build_trigger(),
        id=alpha_radar_process_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        alpha_radar_fetch_job.run,
        alpha_radar_fetch_job.build_trigger(),
        id=alpha_radar_fetch_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        index_daily_job.run,
        index_daily_job.build_trigger(),
        id=index_daily_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        macro_daily_job.run,
        macro_daily_job.build_trigger(),
        id=macro_daily_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        watchlist_automation_job.run,
        watchlist_automation_job.build_trigger(),
        id=watchlist_automation_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        intraday_score_job.run,
        intraday_score_job.build_trigger(),
        id=intraday_score_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        eastmoney_industry_job.run,
        eastmoney_industry_job.build_trigger(),
        id=eastmoney_industry_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        hk_industry_job.run,
        hk_industry_job.build_trigger(),
        id=hk_industry_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        index_basic_job.run,
        index_basic_job.build_trigger(),
        id=index_basic_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        cn_industry_post_close_job.run,
        cn_industry_post_close_job.build_trigger(),
        id=cn_industry_post_close_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        sleeve_paper_job.run,
        sleeve_paper_job.build_trigger(),
        id=sleeve_paper_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        minute_capture_job.run,
        minute_capture_job.build_trigger(),
        id=minute_capture_job.JOB_ID,
        replace_existing=True,
    )
    # OPT-049: paper-trading intake + update (after cn_industry_post_close).
    scheduler.add_job(
        paper_trading_intake_job.run,
        paper_trading_intake_job.build_trigger(),
        id=paper_trading_intake_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        paper_trading_update_job.run,
        paper_trading_update_job.build_trigger(),
        id=paper_trading_update_job.JOB_ID,
        replace_existing=True,
    )
    # Weekly backtest-vs-paper reconciliation (Monday morning, last Friday).
    scheduler.add_job(
        backtest_recon_job.run,
        backtest_recon_job.build_trigger(),
        id=backtest_recon_job.JOB_ID,
        replace_existing=True,
    )
    # Daily real-book behavior audit (weekdays 18:45 — fresh watchlist banner).
    scheduler.add_job(
        behavior_audit_job.run,
        behavior_audit_job.build_trigger(),
        id=behavior_audit_job.JOB_ID,
        replace_existing=True,
    )
    # Monthly rolling-OOS monitor (first Monday, strategy-fade early warning).
    scheduler.add_job(
        rolling_oos_job.run,
        rolling_oos_job.build_trigger(),
        id=rolling_oos_job.JOB_ID,
        replace_existing=True,
    )
    # Paper-chain watchdog (18:05 — self-heal the 17:30/17:42/17:45 chain).
    scheduler.add_job(
        paper_chain_watchdog_job.run,
        paper_chain_watchdog_job.build_trigger(),
        id=paper_chain_watchdog_job.JOB_ID,
        replace_existing=True,
    )
    # Candidate-add diff (17:35 weekdays — gate-reopen / new strong RS push).
    scheduler.add_job(
        candidate_diff_job.run,
        candidate_diff_job.build_trigger(),
        id=candidate_diff_job.JOB_ID,
        replace_existing=True,
    )
    # Intraday drawdown alarm (hourly 10-14 weekdays — backstop for broker conditional orders).
    scheduler.add_job(
        intraday_alarm_job.run,
        intraday_alarm_job.build_trigger(),
        id=intraday_alarm_job.JOB_ID,
        replace_existing=True,
    )
    # Webhook delivery (every minute — HMAC-signed event pushes).
    scheduler.add_job(
        webhook_delivery_job.run,
        webhook_delivery_job.build_trigger(),
        id=webhook_delivery_job.JOB_ID,
        replace_existing=True,
    )
    # Weekly review (Monday 07:40 — decision quality report for the agent).
    scheduler.add_job(
        weekly_review_job.run,
        weekly_review_job.build_trigger(),
        id=weekly_review_job.JOB_ID,
        replace_existing=True,
    )
    # Trading-session briefs (10:00 / 12:00 / 14:30 weekdays — user's rhythm).
    for _bt in ("open", "midday", "action"):
        scheduler.add_job(
            getattr(trading_brief_job, f"run_{_bt}"),
            trading_brief_job.build_trigger(_bt),
            id=f"{trading_brief_job.JOB_ID}_{_bt}",
            replace_existing=True,
        )
    # T4: weekly R5c allocation decision (Monday, before the update cron).
    scheduler.add_job(
        allocation_decide_job.run,
        allocation_decide_job.build_trigger(),
        id=allocation_decide_job.JOB_ID,
        replace_existing=True,
    )
    # G4: S-3 backtest paper intake (after regular intake, before update).
    scheduler.add_job(
        paper_s3_intake_job.run,
        paper_s3_intake_job.build_trigger(),
        id=paper_s3_intake_job.JOB_ID,
         replace_existing=True,
     )
    # Backtest mirror (2026-08-14): replay the engine trajectory into the
    # paper book daily — the backtest is the source of truth (runs after
    # hk_daily_full_sync so today's HK bars are settled).
    scheduler.add_job(
        paper_backtest_mirror_job.run,
        paper_backtest_mirror_job.build_trigger(),
        id=paper_backtest_mirror_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        news_enrich_job.run,
        news_enrich_job.build_trigger(),
        id=news_enrich_job.JOB_ID,
        replace_existing=True,
    )
    # TIP-012: research report ingestion (研报 → Alpha channel)
    scheduler.add_job(
        research_report_job.run,
        research_report_job.build_trigger(),
        id=research_report_job.JOB_ID,
        replace_existing=True,
    )
    # TIP-015: decision archive snapshot + T+1 outcome feedback
    scheduler.add_job(
        decision_snapshot_job.run,
        decision_snapshot_job.build_trigger(),
        id=decision_snapshot_job.JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        decision_outcome_job.run,
        decision_outcome_job.build_trigger(),
        id=decision_outcome_job.JOB_ID,
        replace_existing=True,
    )
    # TIP-015: extract brief actions → match executions → track outcomes
    scheduler.add_job(
        decision_action_job.run,
        decision_action_job.build_trigger(),
        id=decision_action_job.JOB_ID,
        replace_existing=True,
    )
    # Timeline warmup: past-year single-track (08:20 weekdays) -> file cache for <100ms loads
    scheduler.add_job(
        timeline_warmup_job.run,
        timeline_warmup_job.build_trigger(),
        id=timeline_warmup_job.JOB_ID,
        replace_existing=True,
    )
    # Track 3: Morning Brief (AM 08:30 + PM 12:30 Asia/Shanghai, weekdays)
    scheduler.add_job(
        lambda: morning_brief_job.run(brief_type="morning"),
        morning_brief_job.build_am_trigger(),
        id=morning_brief_job.JOB_ID_AM,
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: morning_brief_job.run(brief_type="midday"),
        morning_brief_job.build_pm_trigger(),
        id=morning_brief_job.JOB_ID_PM,
        replace_existing=True,
    )
    # EOD-chain restart catch-up: a few seconds after startup, re-run any
    # close-time step the restart skipped (see catchup_missed_eod_chain).
    scheduler.add_job(
        catchup_missed_eod_chain,
        DateTrigger(run_date=datetime.now(tz=UTC) + timedelta(seconds=3)),
        id="eod_chain_startup_catchup",
        replace_existing=True,
    )
    return scheduler


def _cst_now() -> datetime:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai"))


def catchup_missed_eod_chain() -> None:
    """Re-run today's EOD chain steps missed by a backend restart.

    The in-process APScheduler keeps jobs in an in-memory job store: when the
    process restarts AFTER a cron fire time, ``add_job`` schedules the NEXT
    occurrence and the missed run is silently dropped (misfire_grace_time
    only applies to jobs already in the store). A dev restart around the
    close-time window therefore loses the whole chain — 2026-08-10: 17:30
    watchlist_automation and 18:15 cn_industry_post_close_sync both skipped
    while close_sync (17:10) and hk_daily_full (17:51) fired fine.

    Fired once, ~3s after startup. Each step is guarded by its own
    ``sync_job_record`` today-row, so the normal scheduled path is untouched.
    Time-of-day guards sit AFTER each cron slot to avoid racing a fire that
    is about to happen (or just happened) normally.
    """
    logger = logging.getLogger(__name__)
    from data_sync_service.db.sync_job_record import get_today_run

    now = _cst_now()
    if now.weekday() >= 5:
        return
    close_ok = bool((get_today_run("stock_close_sync") or {}).get("success"))
    if not close_ok:
        return

    def already(job_type: str) -> bool:
        return get_today_run(job_type) is not None

    # watchlist_automation cron: 17:30 — catch up only after its slot
    # (17:35 avoids racing the normal 17:30 fire when started just before).
    # Each step is isolated: one failure must not silently skip the rest of
    # the chain (2026-08-12 robustness audit).
    if (now.hour, now.minute) >= (17, 35) and not already("watchlist_automation"):
        logger.info("eod chain catchup: watchlist_automation missed (restart) — re-running")
        try:
            watchlist_automation_job.run()
        except Exception:  # noqa: BLE001
            logger.warning("eod chain catchup: watchlist_automation run failed", exc_info=True)
    # paper_s3_intake cron: 17:42 — needs today's scores from the step above;
    # 17:45 avoids racing the normal 17:42 fire. Idempotent re-runs are safe
    # (per-symbol/day dedupe) even in the 17:42-17:45 double-run window.
    if (
        (now.hour, now.minute) >= (17, 45)
        and already("watchlist_automation")
        and not already("paper_s3_intake_CN")
    ):
        logger.info("eod chain catchup: paper_s3_intake missed (restart) — re-running")
        try:
            paper_s3_intake_job.run()
        except Exception:  # noqa: BLE001
            logger.warning("eod chain catchup: paper_s3_intake run failed", exc_info=True)
    # cn_industry_post_close_sync cron: 18:15 — 18:25 avoids the race.
    if (now.hour, now.minute) >= (18, 25) and not already("cn_industry_post_close_sync"):
        logger.info("eod chain catchup: cn_industry_post_close_sync missed (restart) — re-running")
        try:
            cn_industry_post_close_job.run()
        except Exception:  # noqa: BLE001
            logger.warning("eod chain catchup: cn_industry_post_close_sync run failed", exc_info=True)
    # paper_backtest_mirror cron: 18:05 — 18:10 avoids the race; needs the
    # daily HK bars (hk_daily_full_sync 17:30) which close_sync guards imply.
    if (now.hour, now.minute) >= (18, 10) and not already("paper_backtest_mirror"):
        logger.info("eod chain catchup: paper_backtest_mirror missed (restart) — re-running")
        try:
            paper_backtest_mirror_job.run()
        except Exception:  # noqa: BLE001
            logger.warning("eod chain catchup: paper_backtest_mirror run failed", exc_info=True)
