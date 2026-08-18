"""Minute-bar capture job (TIP-014 Phase 3 / D7) — weekdays 16:35 Asia/Shanghai.

Captures the current session's 1-minute OHLCV bars for the symbols we care
about (CN/HK watchlist + open paper holdings + today's S-3 candidates) from
Tencent minute endpoints into bar_minute. Purpose: validate intraday entry
fills (尾盘执行) and re-sample 5m bars for entry-price research.

Data accumulates forward — no history available on this source (Eastmoney
push2his, the history-capable endpoint, is IP-rate-limited since 2026-08-14).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.minute_capture import capture_symbols

logger = logging.getLogger(__name__)

JOB_ID = "minute_capture"
# 16:35 — after HK close (16:00) + CN close (15:00) + a 35-min buffer so the
# last-minute data is stable. Before close_sync (17:10) which needs the daily
# bars; minute capture only touches bar_minute so ordering is independent.
CRON_EXPRESSION = "35 16 * * 1-5"
TIMEZONE = "Asia/Shanghai"
MAX_SYMBOLS = 60


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _symbols_to_capture() -> list[dict[str, str]]:
    """Watchlist registry + open paper holdings + recent S-3 candidates.

    Dedup by ts_code. kind="hk" for *.HK, else "cn" (ETF minute data is not
    available from these endpoints — skipped).
    """
    from data_sync_service.db.paper_trading import list_paper_trades

    out: dict[str, dict[str, str]] = {}
    for row in list_paper_trades(status="open"):
        sym = str(row.get("symbol") or "")
        ts = str(row.get("ts_code") or "")
        if sym.startswith("HK:") and ts.endswith(".HK"):
            out[ts] = {"ts_code": ts, "kind": "hk"}
        elif sym.startswith("CN:") and ts.endswith((".SH", ".SZ")):
            out[ts] = {"ts_code": ts, "kind": "cn"}
    return list(out.values())


def run() -> None:
    today = datetime.now(tz=UTC).date().isoformat()
    try:
        symbols = _symbols_to_capture()
        if not symbols:
            insert_record(JOB_ID, success=True, last_ts_code="0", error_message="no-symbols")
            logger.info("[minute_capture] no symbols to capture")
            return
        res = capture_symbols(trade_date=today, symbols=symbols, max_symbols=MAX_SYMBOLS)
        insert_record(
            JOB_ID,
            success=res["failed"] == 0,
            last_ts_code=str(res["stored"]),
            error_message=None if res["failed"] == 0 else f"failed={res['failed']}",
        )
        logger.info(
            "[minute_capture] ok=%d stored=%d failed=%d skipped=%d",
            res["ok"], res["stored"], res["failed"], res["skipped"],
        )
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        logger.warning("[minute_capture] failed: %s", exc)
