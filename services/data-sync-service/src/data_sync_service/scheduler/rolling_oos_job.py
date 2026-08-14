"""Rolling OOS monitor (2026-08-11) — strategy-fade early warning.

Monthly (first Monday 08:15 Asia/Shanghai): re-simulate the FIXED S-3 config
(CN + HK lines) over the latest rolling 3-month window and compare against
the frozen three-window baseline profile. The point is NOT parameter tuning
(S-3 params are frozen) — it is fade detection: if the recent window turns
negative / sub-baseline, the weekly review gets a warning flag months before
a full-window walk-forward would reveal it.

Deliverables per run:
- data/backtest_reports/rolling_oos_latest.json (CN + HK summaries, machine-
  readable for the decision agent / weekly review)
- sync_job_record row (success + warning status in last_ts_code)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.backtest_engine import BacktestConfig, simulate

logger = logging.getLogger(__name__)

JOB_ID = "rolling_oos"
CRON_EXPRESSION = "15 8 1-7 * 1"  # first Monday of each month 08:15 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"

ROLLING_DAYS = 90
REPORT_FILE = Path("data/backtest_reports/rolling_oos_latest.json")

# Same frozen configs as scripts/run_walk_forward.py (S-3 audit standard).
S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 3,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    # TIP-014 (2026-08-14): weak/neutral-day entry block + env-aware entry
    # style — mirrors scripts/run_walk_forward.py S3_CONFIG (S-3 audit standard).
    "neutral_block": True,
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
}

HK_S3_CONFIG: dict[str, float | int | str] = {
    **S3_CONFIG,
    "market": "HK",
    "gates": "regime",
    "trailing_stop_pct": -12.0,
    "rs_rank_min": 0.6,
    "exclude_boards": "",
    "drawdown_circuit_pct": 0.0,  # CN-only defence (2026-08-12, long-window)
}


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _rolling_window() -> tuple[str, str]:
    """Start = today − ROLLING_DAYS, end = today (engine skips non-trading days)."""
    today = datetime.now(tz=ZoneInfo(TIMEZONE)).date()
    start = today - timedelta(days=ROLLING_DAYS)
    return start.isoformat(), today.isoformat()


def _summarize(run) -> dict[str, float | int | None]:
    s = run.summary
    return {
        "closed": s.closed,
        "winRate": s.win_rate,
        "avgNetPnlPct": s.avg_net_pnl_pct,
        "totalNetPnlPct": s.total_net_pnl_pct,
        "maxDrawdownPct": s.max_drawdown_pct,
        "sharpe": s.sharpe,
    }


def run() -> None:
    start, end = _rolling_window()
    report: dict = {"windowStart": start, "windowEnd": end, "markets": {}}
    warnings: list[str] = []
    for market, cfg in (("CN", S3_CONFIG), ("HK", HK_S3_CONFIG)):
        try:
            run_cfg = BacktestConfig(start_date=start, end_date=end, **cfg)
            run = simulate(run_cfg)
            summary = _summarize(run)
            report["markets"][market] = summary
            flag = (
                (summary["totalNetPnlPct"] or 0) < 0
                or (summary["sharpe"] is not None and summary["sharpe"] < 0)
                or (summary["closed"] or 0) == 0
            )
            if flag:
                warnings.append(
                    f"{market}: {summary['totalNetPnlPct']:+.1f}% "
                    f"dd={summary['maxDrawdownPct']:.1f}% "
                    f"sharpe={summary['sharpe']} trades={summary['closed']}"
                )
        except Exception as exc:  # noqa: BLE001
            report["markets"][market] = {"error": str(exc)}
            warnings.append(f"{market}: {exc}")
            logger.warning("rolling_oos %s failed: %s", market, exc)
    report["warning"] = bool(warnings)
    report["warnings"] = warnings

    # E6 (webhook design §2): strategy-fade early warning becomes a push event.
    if warnings:
        from data_sync_service.db.webhook import emit_event

        emit_event(
            "oos_warning",
            {"window_start": start, "window_end": end, "warnings": warnings},
            dedupe_key=f"oos_warning:{start}",
        )

    try:
        REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
        REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=1))
    except OSError as exc:  # noqa: BLE001
        logger.warning("rolling_oos report write failed: %s", exc)

    last_ts = (
        "WARN:" + "; ".join(warnings)
        if warnings
        else f"{start}..{end} "
        + " ".join(
            f"{m}={report['markets'][m]['totalNetPnlPct']:+.1f}%"
            for m in ("CN", "HK")
            if "totalNetPnlPct" in report["markets"].get(m, {})
        )
    )
    insert_record(JOB_ID, success=not warnings, last_ts_code=last_ts[:500])
    logger.info("rolling_oos %s..%s %s", start, end, last_ts)
