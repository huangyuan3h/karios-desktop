"""Last-hour (14:30–15:00) 5-minute bars for CN A-shares.

Probe 2026-09-03:
- baostock frequency=5: 1y in one query (~10s/symbol, 14:30..15:00 all present)
- tushare stk_mins: 10y capable, 8000 rows/call, but 1 call/min (this token
  also 1/hour after a burst) — unusable for a 5000-name year backfill
- akshare stock_zh_a_hist_min_em / stock_zh_a_minute: ~6–8 weeks only
- Tencent minute/query: current session only
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from data_sync_service.db.bar_5min import coverage_by_ts_code, upsert_5min_bars

logger = logging.getLogger(__name__)

LAST_HOUR_TIMES = frozenset({"1430", "1435", "1440", "1445", "1450", "1455", "1500"})
SOURCE_BAOSTOCK = "baostock"
SOURCE_TUSHARE = "tushare.stk_mins"
BAOSTOCK_SLEEP_SECONDS = 0.2
TUSHARE_SLEEP_SECONDS = 61.0
COVERAGE_RATIO = 0.85


def _num(val: Any) -> float | None:
    try:
        return float(val) if val is not None and val != "" else None
    except (TypeError, ValueError):
        return None


def to_baostock_code(ts_code: str) -> str | None:
    code, _, suffix = ts_code.partition(".")
    suf = suffix.upper()
    if not code or not suf:
        return None
    if suf.startswith("SH"):
        return f"sh.{code.zfill(6)}"
    if suf.startswith("SZ"):
        return f"sz.{code.zfill(6)}"
    return None


def filter_last_hour(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if str(r.get("time") or "") in LAST_HOUR_TIMES]


def parse_baostock_time(raw: str) -> str | None:
    """baostock time field: '20250903093500000' → '0935'."""
    s = str(raw).strip()
    if len(s) >= 12:
        hhmm = s[8:12]
        if hhmm.isdigit():
            return hhmm
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 12:
        return digits[8:12]
    return None


def rows_from_baostock(raw_rows: list[list[str]]) -> list[dict[str, Any]]:
    """Map baostock row [date, time, code, open, high, low, close, volume, amount]."""
    out: list[dict[str, Any]] = []
    for raw in raw_rows:
        if len(raw) < 8:
            continue
        trade_date = str(raw[0]).strip()
        hhmm = parse_baostock_time(raw[1])
        if not trade_date or hhmm is None:
            continue
        try:
            o, h, low, c = (float(raw[3]), float(raw[4]), float(raw[5]), float(raw[6]))
            vol = float(raw[7]) if raw[7] not in ("", None) else None
            amt = float(raw[8]) if len(raw) > 8 and raw[8] not in ("", None) else None
        except (TypeError, ValueError):
            continue
        if c <= 0:
            continue
        out.append(
            {
                "trade_date": trade_date,
                "time": hhmm,
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "vol": vol,
                "amount": amt,
            }
        )
    return out


def rows_from_tushare(frame_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map tushare stk_mins columns ts_code/trade_time/OHLCV/amount."""
    out: list[dict[str, Any]] = []
    for raw in frame_rows:
        trade_time = str(raw.get("trade_time") or "")
        date_part, _, time_part = trade_time.partition(" ")
        hhmm = time_part.replace(":", "")[:4]
        if len(date_part) < 10 or len(hhmm) != 4:
            continue
        try:
            close = float(raw["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if close <= 0:
            continue
        out.append(
            {
                "trade_date": date_part[:10],
                "time": hhmm,
                "open": _num(raw.get("open")),
                "high": _num(raw.get("high")),
                "low": _num(raw.get("low")),
                "close": close,
                "vol": _num(raw.get("vol")),
                "amount": _num(raw.get("amount")),
            }
        )
    return out


_BS_LOCK = threading.Lock()
_BS_LOGGED_IN = False


def _bs_ensure_login_unlocked() -> None:
    global _BS_LOGGED_IN
    import baostock as bs  # type: ignore[import-not-found]

    if _BS_LOGGED_IN:
        return
    lg = bs.login()
    if str(lg.error_code) != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    _BS_LOGGED_IN = True


def fetch_baostock_5min(ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """One baostock query; returns last-hour 5min rows (may be empty)."""
    global _BS_LOGGED_IN
    import baostock as bs  # type: ignore[import-not-found]

    bs_code = to_baostock_code(ts_code)
    if bs_code is None:
        return []

    def _query() -> list[list[str]]:
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,time,code,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="5",
            adjustflag="3",
        )
        if str(rs.error_code) != "0":
            raise RuntimeError(f"baostock query {bs_code}: {rs.error_msg}")
        rows: list[list[str]] = []
        while str(rs.error_code) == "0" and rs.next():
            rows.append(list(rs.get_row_data()))
        return rows

    with _BS_LOCK:
        _bs_ensure_login_unlocked()
        try:
            raw = _query()
        except Exception:  # noqa: BLE001
            try:
                bs.logout()
            except Exception:  # noqa: BLE001
                pass
            _BS_LOGGED_IN = False
            _bs_ensure_login_unlocked()
            raw = _query()
    return filter_last_hour(rows_from_baostock(raw))


def fetch_tushare_5min(ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Tushare stk_mins 5min. Caller must pace at TUSHARE_SLEEP_SECONDS."""
    import tushare as ts  # type: ignore[import-not-found]

    from data_sync_service.config import get_settings

    token = get_settings().tu_share_api_key
    if not token:
        raise RuntimeError("TU_SHARE_API_KEY is not set")
    ts.set_token(token)
    pro = ts.pro_api()
    start_dt = f"{start_date} 09:00:00"
    end_dt = f"{end_date} 16:00:00"
    df = pro.stk_mins(ts_code=ts_code, freq="5min", start_date=start_dt, end_date=end_dt)
    if df is None or df.empty:
        return []
    records = df.to_dict("records")
    return filter_last_hour(rows_from_tushare(records))


def list_cn_a_share_codes() -> list[str]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code FROM stock_basic
                WHERE (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ')
                  AND COALESCE(market, '') NOT IN ('ETF', 'HK', '北交所')
                  AND ts_code NOT LIKE '%.BJ'
                  AND delist_date IS NULL
                  AND name NOT LIKE '%ST%'
                ORDER BY ts_code
                """
            )
            return [str(r[0]) for r in cur.fetchall()]


def list_gap_codes(trade_date: str, *, min_gap: float = 0.03) -> list[str]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.ts_code
                FROM daily d
                JOIN stock_basic sb ON sb.ts_code = d.ts_code
                WHERE d.trade_date = %s
                  AND d.open IS NOT NULL AND d.pre_close IS NOT NULL AND d.pre_close > 0
                  AND d.open / d.pre_close - 1 > %s
                  AND (d.ts_code LIKE '%.SH' OR d.ts_code LIKE '%.SZ')
                  AND COALESCE(sb.market, '') NOT IN ('ETF', 'HK', '北交所')
                  AND sb.delist_date IS NULL
                  AND sb.name NOT LIKE '%ST%'
                ORDER BY d.ts_code
                """,
                (trade_date, min_gap),
            )
            return [str(r[0]) for r in cur.fetchall()]


def trading_day_count(start_date: str, end_date: str) -> int:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT trade_date) FROM daily
                WHERE trade_date >= %s AND trade_date <= %s AND ts_code = '000001.SZ'
                """,
                (start_date, end_date),
            )
            return int(cur.fetchone()[0] or 0)


def backfill_symbols(
    *,
    ts_codes: list[str],
    start_date: str,
    end_date: str,
    source: str = SOURCE_BAOSTOCK,
    max_symbols: int | None = None,
    sleep_seconds: float | None = None,
    skip_covered: bool = True,
) -> dict[str, Any]:
    """Fetch last-hour 5min for each symbol and upsert. Resume-safe."""
    fetch = fetch_baostock_5min if source == SOURCE_BAOSTOCK else fetch_tushare_5min
    delay = BAOSTOCK_SLEEP_SECONDS if source == SOURCE_BAOSTOCK else TUSHARE_SLEEP_SECONDS
    if sleep_seconds is not None:
        delay = sleep_seconds

    covered = coverage_by_ts_code(start_date, end_date) if skip_covered else {}
    need = trading_day_count(start_date, end_date) if skip_covered else 0
    if skip_covered and need > 0:
        min_days = max(1, int(need * COVERAGE_RATIO))
        pending = [c for c in ts_codes if covered.get(c, 0) < min_days]
    else:
        pending = list(ts_codes)
    if max_symbols is not None:
        pending = pending[:max_symbols]

    out = {
        "ok": 0,
        "failed": 0,
        "skipped": len(ts_codes) - len(pending),
        "stored": 0,
        "pending": len(pending),
        "source": source,
        "errors": [],
    }
    for i, ts_code in enumerate(pending):
        try:
            rows = fetch(ts_code, start_date, end_date)
            stored = upsert_5min_bars(ts_code, rows, source=source)
            out["ok"] += 1
            out["stored"] += stored
        except Exception as exc:  # noqa: BLE001
            out["failed"] += 1
            msg = f"{ts_code}: {exc}"[:200]
            if len(out["errors"]) < 20:
                out["errors"].append(msg)
            logger.warning("[bar_5min] %s", msg)
        if delay > 0:
            time.sleep(delay)
        if (i + 1) % 20 == 0:
            logger.info(
                "[bar_5min] progress %d/%d ok=%d stored=%d failed=%d",
                i + 1, len(pending), out["ok"], out["stored"], out["failed"],
            )
    return out
