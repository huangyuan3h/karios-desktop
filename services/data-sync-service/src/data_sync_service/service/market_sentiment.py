from __future__ import annotations

import math
import random
import sys
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import get_connection
from data_sync_service.db.daily import ensure_table as ensure_daily
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.stock_basic import fetch_ts_codes_by_market
from data_sync_service.db.market_sentiment import get_latest_date, list_days, upsert_daily_rows
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.service.realtime_quote import fetch_realtime_quotes


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _with_retry(fn, *, tries: int = 3, base_sleep_s: float = 0.4, max_sleep_s: float = 2.0):
    tries2 = max(1, min(int(tries), 5))
    last: Exception | None = None
    for i in range(tries2):
        try:
            return fn()
        except Exception as e:
            last = e
            if i >= tries2 - 1:
                raise
            sleep_s = min(float(max_sleep_s), float(base_sleep_s) * (2**i))
            sleep_s = sleep_s * (0.7 + random.random() * 0.6)
            time.sleep(max(0.0, sleep_s))
    if last is not None:
        raise last
    raise RuntimeError("Retry wrapper failed unexpectedly.")


def _akshare():
    try:
        import akshare as ak  # type: ignore[import-not-found]

        return ak
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for market sentiment sync.\n"
            "Install in data-sync-service: cd services/data-sync-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e


def _tushare_pro():
    try:
        import tushare as ts  # type: ignore[import-not-found]

        from data_sync_service.config import get_settings

        settings = get_settings()
        if not settings.tu_share_api_key:
            raise RuntimeError("TU_SHARE_API_KEY is not set")
        return ts.pro_api(settings.tu_share_api_key)
    except Exception as e:
        raise RuntimeError(f"Tushare is required for fallback. Original error: {e}") from e


def _to_records(df: Any) -> list[dict[str, Any]]:
    if hasattr(df, "to_dict"):
        return list(df.to_dict("records"))  # type: ignore[arg-type]
    raise RuntimeError("Unexpected AkShare return type (expected DataFrame).")


def _parse_money_to_cny(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else 0.0
    s = str(value).strip()
    if not s or s in ("-", "—", "N/A", "None"):
        return 0.0
    s2 = s.replace(",", "").replace(" ", "")
    mult = 1.0
    if "亿" in s2:
        mult = 1e8
        s2 = s2.replace("亿", "")
    elif "万" in s2:
        mult = 1e4
        s2 = s2.replace("万元", "").replace("万", "")
    keep = []
    for ch in s2:
        if ch.isdigit() or ch in (".", "-", "+"):
            keep.append(ch)
    num_s = "".join(keep)
    try:
        return float(num_s) * mult
    except Exception:
        return 0.0


def fetch_cn_market_breadth_eod(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")
    # Use tushare to avoid native crashes from AkShare's JS decoder (mini_racer).
    # This is EOD breadth, so tushare daily is sufficient and more stable.
    pro = _tushare_pro()
    td = _safe_trade_date(as_of)
    limit = 5000
    offset = 0
    up = 0
    down = 0
    flat = 0
    total_turnover_cny = 0.0
    total_volume = 0.0
    rows_n = 0
    while True:
        df = _with_retry(
            lambda: pro.daily(trade_date=td, limit=limit, offset=offset, fields="ts_code,pct_chg,vol,amount"),
            tries=2,
            base_sleep_s=0.6,
        )
        if df is None or getattr(df, "empty", False):
            break
        rows = _to_records(df)
        rows_n += len(rows)
        for r in rows:
            try:
                pct = float(r.get("pct_chg"))
            except Exception:
                pct = 0.0
            if pct > 0:
                up += 1
            elif pct < 0:
                down += 1
            else:
                flat += 1

            try:
                vol = float(r.get("vol") or 0.0)
            except Exception:
                vol = 0.0
            try:
                amt = float(r.get("amount") or 0.0)
            except Exception:
                amt = 0.0
            # Tushare daily.amount is in thousand RMB (K CNY).
            if math.isfinite(amt):
                total_turnover_cny += float(amt) * 1000.0
            if math.isfinite(vol):
                total_volume += float(vol)
        if len(rows) < limit:
            break
        offset += limit

    total = up + down + flat
    ratio = float(up) / float(down) if down > 0 else float(up)
    return {
        "date": d,
        "up_count": up,
        "down_count": down,
        "flat_count": flat,
        "total_count": total,
        "up_down_ratio": ratio,
        "total_turnover_cny": total_turnover_cny,
        "total_volume": total_volume,
        "raw": {"source": "tushare.daily", "trade_date": td, "rows": rows_n},
    }


def fetch_cn_market_breadth_intraday(as_of: date) -> dict[str, Any]:
    """
    Best-effort intraday breadth using realtime quotes (Tushare).
    """
    d = as_of.strftime("%Y-%m-%d")
    ensure_stock_basic()
    ts_codes = fetch_ts_codes_by_market("CN")
    requested = len(ts_codes)
    if not ts_codes:
        return {
            "date": d,
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "total_count": 0,
            "up_down_ratio": 0.0,
            "total_turnover_cny": 0.0,
            "total_volume": 0.0,
            "raw": {"source": "tushare.realtime_quote", "requested": 0, "matched": 0},
        }

    up = 0
    down = 0
    flat = 0
    total_turnover_cny = 0.0
    total_volume = 0.0
    matched = 0
    batches = 0
    errors: list[str] = []
    for i in range(0, len(ts_codes), 50):
        part = ts_codes[i : i + 50]
        batches += 1
        r = fetch_realtime_quotes(part)
        if not isinstance(r, dict) or not bool(r.get("ok")):
            err = r.get("error") if isinstance(r, dict) else "realtime_quote_failed"
            errors.append(str(err))
            continue
        items = r.get("items", []) or []
        for it in items:
            matched += 1
            try:
                pct = float(it.get("pct_chg"))
            except Exception:
                pct = None
            if pct is not None and math.isfinite(pct):
                if pct > 0:
                    up += 1
                elif pct < 0:
                    down += 1
                else:
                    flat += 1
            vol = _finite_float(it.get("volume"), 0.0)
            amt = _finite_float(it.get("amount"), 0.0)
            total_volume += vol
            total_turnover_cny += amt
        time.sleep(0.08)

    total = up + down + flat
    ratio = float(up) / float(down) if down > 0 else float(up)
    raw = {
        "source": "tushare.realtime_quote",
        "requested": requested,
        "matched": matched,
        "batches": batches,
    }
    if errors:
        raw["errors"] = errors[:3]
    return {
        "date": d,
        "up_count": up,
        "down_count": down,
        "flat_count": flat,
        "total_count": total,
        "up_down_ratio": ratio,
        "total_turnover_cny": total_turnover_cny,
        "total_volume": total_volume,
        "raw": raw,
    }


def _safe_trade_date(x: date) -> str:
    return x.strftime("%Y%m%d")


def _tushare_daily_pct_chg_map(as_of: date) -> dict[str, float]:
    """
    Return {ts_code -> pct_chg} for the given trade date (EOD).
    """
    pro = _tushare_pro()
    d = _safe_trade_date(as_of)
    # Prefer a minimal fields set to reduce payload size.
    df = _with_retry(lambda: pro.daily(trade_date=d, fields="ts_code,pct_chg"), tries=2, base_sleep_s=0.6)
    if df is None:
        return {}
    rows = _to_records(df)
    out: dict[str, float] = {}
    for r in rows:
        ts_code = str(r.get("ts_code") or "").strip()
        if not ts_code:
            continue
        try:
            v = float(r.get("pct_chg"))
        except Exception:
            continue
        if math.isfinite(v):
            out[ts_code] = v
    return out


def _tushare_yesterday_limitup_codes(as_of: date) -> tuple[date | None, list[str]]:
    """
    Find the most recent prior trade date with any limit-up list, and return its ts_codes.
    """
    pro = _tushare_pro()
    chosen_y: date | None = None
    codes: list[str] = []

    def _try_limit_list(trade_date: str) -> list[dict[str, Any]]:
        # Tushare provides different names across versions; try a few.
        for fn_name in ("limit_list_d", "limit_list"):
            fn = getattr(pro, fn_name, None)
            if not fn:
                continue
            # Try common signatures.
            for kwargs in (
                {"trade_date": trade_date, "limit_type": "U", "fields": "ts_code"},
                {"trade_date": trade_date, "limit_type": "U"},
                {"trade_date": trade_date, "fields": "ts_code"},
                {"trade_date": trade_date},
            ):
                try:
                    df = fn(**kwargs)  # type: ignore[misc]
                except TypeError:
                    continue
                if df is None:
                    continue
                rs = _to_records(df)
                if rs:
                    return rs
        return []

    for back in range(1, 10):
        y = as_of - timedelta(days=back)
        trade_date = _safe_trade_date(y)
        try:
            rs = _with_retry(lambda: _try_limit_list(trade_date), tries=2, base_sleep_s=0.6)
        except Exception:
            rs = []
        codes2: list[str] = []
        for r in rs:
            ts_code = str(r.get("ts_code") or "").strip()
            if ts_code:
                codes2.append(ts_code)
        if codes2:
            chosen_y = y
            codes = codes2
            break
    return chosen_y, codes


def fetch_cn_yesterday_limitup_premium_tushare(as_of: date) -> dict[str, Any]:
    """
    Fallback implementation for yesterday limit-up premium using tushare (EOD).
    """
    d = as_of.strftime("%Y-%m-%d")
    chosen_y, codes = _tushare_yesterday_limitup_codes(as_of)
    if not codes:
        return {"date": d, "premium": 0.0, "count": 0, "raw": {"source": "tushare", "y": None}}

    pct_map = _tushare_daily_pct_chg_map(as_of)
    vals: list[float] = []
    for ts_code in codes:
        if ts_code in pct_map:
            vals.append(float(pct_map[ts_code]))
    premium = float(sum(vals) / len(vals)) if vals else 0.0
    return {
        "date": d,
        "premium": premium,
        "count": len(codes),
        "raw": {
            "source": "tushare",
            "y": chosen_y.strftime("%Y-%m-%d") if chosen_y else None,
            "matched": len(vals),
        },
    }

def _fetch_cn_a_spot_change_pct() -> dict[str, float]:
    ak = _akshare()
    if not hasattr(ak, "stock_zh_a_spot_em"):
        raise RuntimeError("AkShare missing stock_zh_a_spot_em. Please upgrade AkShare.")
    try:
        df = _with_retry(lambda: ak.stock_zh_a_spot_em(), tries=3)
    except Exception:
        if not hasattr(ak, "stock_zh_a_spot"):
            raise
        df = _with_retry(lambda: ak.stock_zh_a_spot(), tries=2, base_sleep_s=0.8)
    rows = _to_records(df)
    out: dict[str, float] = {}
    for r in rows:
        code = str(r.get("代码") or r.get("code") or "").strip()
        if not code:
            continue
        chg = str(r.get("涨跌幅") or r.get("change_pct") or "").strip().replace("%", "")
        try:
            out[code] = float(chg)
        except Exception:
            continue
    return out


def fetch_cn_yesterday_limitup_premium(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")
    # On macOS, AkShare's JS decoder may crash the whole process (mini_racer / V8 fatal).
    # Prefer tushare to keep the backend stable.
    if sys.platform == "darwin":
        return fetch_cn_yesterday_limitup_premium_tushare(as_of)
    try:
        ak = _akshare()
        if not hasattr(ak, "stock_zt_pool_em"):
            raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")
        chosen_y: datetime.date | None = None
        codes: list[str] = []
        for back in range(1, 8):
            y = as_of - timedelta(days=back)
            try:
                df = ak.stock_zt_pool_em(date=_safe_trade_date(y))  # type: ignore[misc]
                rows = _to_records(df)
            except Exception:
                continue
            codes = []
            for r in rows:
                code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
                if code:
                    codes.append(code)
            if codes:
                chosen_y = y
                break
        if not codes:
            return {"date": d, "premium": 0.0, "count": 0, "raw": {"y": None, "searchedBackDays": 7}}

        chg_map = _fetch_cn_a_spot_change_pct()
        vals: list[float] = []
        for code in codes:
            if code in chg_map:
                vals.append(float(chg_map[code]))
        premium = float(sum(vals) / len(vals)) if vals else 0.0
        return {
            "date": d,
            "premium": premium,
            "count": len(codes),
            "raw": {
                "source": "akshare",
                "y": chosen_y.strftime("%Y-%m-%d") if chosen_y else None,
                "matched": len(vals),
            },
        }
    except Exception as e:
        # AkShare occasionally gets blocked/rate-limited and returns HTML ("<..."), causing decode errors.
        # Fallback to tushare (EOD) to keep the sentiment pipeline stable.
        try:
            out = fetch_cn_yesterday_limitup_premium_tushare(as_of)
            raw = out.get("raw") if isinstance(out, dict) else {}
            if isinstance(raw, dict):
                raw["akshareError"] = str(e)
            return out
        except Exception as e2:
            # Final fallback: return a safe default without throwing, to avoid polluting the whole sync step.
            return {
                "date": d,
                "premium": 0.0,
                "count": 0,
                "raw": {"source": "fallback", "akshareError": str(e), "tushareError": str(e2)},
            }

def fetch_cn_failed_limitup_rate(as_of: date) -> dict[str, Any]:
    d = as_of.strftime("%Y-%m-%d")

    def _codes(rs: list[dict[str, Any]]) -> set[str]:
        s: set[str] = set()
        for r in rs:
            code = str(r.get("代码") or r.get("code") or r.get("股票代码") or "").strip()
            if code:
                s.add(code)
        return s

    try:
        # Same safety consideration as premium: avoid AkShare on macOS to prevent native crashes.
        if sys.platform == "darwin":
            raise RuntimeError("akshare_disabled_on_darwin")
        ak = _akshare()
        if not hasattr(ak, "stock_zt_pool_em"):
            raise RuntimeError("AkShare missing stock_zt_pool_em. Please upgrade AkShare.")
        df_close = ak.stock_zt_pool_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
        close_rows = _to_records(df_close)
        failed_rows: list[dict[str, Any]] = []
        method = "fallback_strong_minus_close"
        if hasattr(ak, "stock_zt_pool_zbgc_em"):
            try:
                df_failed = ak.stock_zt_pool_zbgc_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
                failed_rows = _to_records(df_failed)
                method = "zbgc_over_zbgc_plus_close"
            except Exception:
                failed_rows = []
                method = "fallback_strong_minus_close"
        elif hasattr(ak, "stock_zt_pool_zb_em"):
            try:
                df_failed = ak.stock_zt_pool_zb_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
                failed_rows = _to_records(df_failed)
                method = "zb_over_zb_plus_close"
            except Exception:
                failed_rows = []
                method = "fallback_strong_minus_close"

        close = _codes(close_rows)
        close_count = len(close)
        failed = _codes(failed_rows)
        failed_count = len(failed)
        if method in ("zbgc_over_zbgc_plus_close", "zb_over_zb_plus_close"):
            denom = failed_count + close_count
            rate = (float(failed_count) / float(denom) * 100.0) if denom > 0 else 0.0
            ever_count = denom
        else:
            if not hasattr(ak, "stock_zt_pool_strong_em"):
                raise RuntimeError("AkShare missing stock_zt_pool_strong_em. Please upgrade AkShare.")
            df_ever = ak.stock_zt_pool_strong_em(date=_safe_trade_date(as_of))  # type: ignore[misc]
            ever_rows = _to_records(df_ever)
            ever = _codes(ever_rows)
            ever_count = len(ever)
            failed_count = max(0, ever_count - close_count)
            rate = (float(failed_count) / float(ever_count) * 100.0) if ever_count > 0 else 0.0
        return {
            "date": d,
            "failed_rate": rate,
            "ever_count": ever_count,
            "close_count": close_count,
            "raw": {
                "source": "akshare",
                "method": method,
                "failedRows": len(failed_rows),
                "closeRows": len(close_rows),
            },
        }
    except Exception as e:
        # Keep pipeline stable if AkShare is blocked/rate-limited and returns HTML ("<...").
        # We do not have a reliable cross-source definition here, so return a conservative fallback.
        return {
            "date": d,
            "failed_rate": 0.0,
            "ever_count": 0,
            "close_count": 0,
            "raw": {"source": "fallback", "note": "akshare_failed", "akshareError": str(e)},
        }


def _finite_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _is_shanghai_trading_time() -> bool:
    """
    Best-effort CN A-share trading time check in Asia/Shanghai.
    """
    now = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    if now.weekday() >= 5:  # 5/6 = weekend
        return False
    minutes = now.hour * 60 + now.minute
    in_morning = minutes >= 9 * 60 + 30 and minutes <= 11 * 60 + 30
    in_afternoon = minutes >= 13 * 60 and minutes <= 15 * 60
    return in_morning or in_afternoon


def _limit_pct_for(ts_code: str, name: str | None) -> float:
    n = (name or "").upper()
    if "ST" in n:
        return 5.0
    t = (ts_code or "").upper()
    if t.endswith(".BJ"):
        return 30.0
    code = t.split(".", 1)[0]
    if code.startswith(("300", "301", "688")):
        return 20.0
    return 10.0


def _prev_open_date(exchange: str, d0: date) -> date | None:
    """
    Return previous open trading date before d0, or None if calendar missing.
    """
    # Prefer trade calendar when available.
    if is_trading_day(exchange, d0) is not None:
        xs = get_open_dates(exchange=exchange, start_date=d0 - timedelta(days=40), end_date=d0)
        xs2 = [x for x in xs if x < d0]
        if xs2:
            return xs2[-1]

    # Fallback: derive from daily table.
    ensure_daily()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(trade_date) FROM daily WHERE trade_date < %s",
                (d0.isoformat(),),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def _daily_rows_for_date(d0: date) -> list[tuple[str, float | None, float | None, float | None, float | None, str | None]]:
    """
    Return tuples: (ts_code, pre_close, high, close, pct_chg, name).
    """
    ensure_daily()
    ensure_stock_basic()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.ts_code, d.pre_close, d.high, d.close, d.pct_chg, b.name
                FROM daily d
                LEFT JOIN stock_basic b ON b.ts_code = d.ts_code
                WHERE d.trade_date = %s
                """,
                (d0.isoformat(),),
            )
            rows = cur.fetchall()
    out: list[tuple[str, float | None, float | None, float | None, float | None, str | None]] = []
    for r in rows:
        ts_code = str(r[0] or "")
        if not ts_code:
            continue
        out.append((ts_code, r[1], r[2], r[3], r[4], str(r[5]) if r[5] is not None else None))
    return out


def _close_limit_up_pool_codes(d0: date) -> list[str]:
    """
    Derive "close-at-limit-up" pool from daily table (DB-first).
    """
    rows = _daily_rows_for_date(d0)
    codes: list[str] = []
    for ts_code, pre_close, _high, close, pct_chg, name in rows:
        if pre_close is None or close is None:
            continue
        try:
            pre = float(pre_close)
            c = float(close)
        except Exception:
            continue
        if not (pre > 0.0 and math.isfinite(pre) and math.isfinite(c)):
            continue
        limit_pct = _limit_pct_for(ts_code, name)
        limit_price = pre * (1.0 + limit_pct / 100.0)
        tol = max(0.01, abs(limit_price) * 0.0015)
        if abs(c - limit_price) <= tol:
            codes.append(ts_code)
            continue
        # Fallback: some data sources round pct_chg; allow a pct-based check.
        try:
            p = float(pct_chg) if pct_chg is not None else None
        except Exception:
            p = None
        if p is not None and math.isfinite(p) and p >= (limit_pct - 0.2):
            codes.append(ts_code)
    return codes


def _avg_pct_chg_from_db(trade_date: date, ts_codes: list[str]) -> tuple[float, int]:
    if not ts_codes:
        return 0.0, 0
    ensure_daily()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, pct_chg
                FROM daily
                WHERE trade_date = %s AND ts_code = ANY(%s)
                """,
                (trade_date.isoformat(), ts_codes),
            )
            rows = cur.fetchall()
    vals: list[float] = []
    for _ts, pct in rows:
        try:
            v = float(pct)
        except Exception:
            continue
        if math.isfinite(v):
            vals.append(v)
    return (float(sum(vals) / len(vals)) if vals else 0.0), len(vals)


def _avg_pct_chg_from_realtime(ts_codes: list[str]) -> tuple[float, int]:
    if not ts_codes:
        return 0.0, 0
    vals: list[float] = []
    # Keep batch size conservative.
    for i in range(0, len(ts_codes), 50):
        part = ts_codes[i : i + 50]
        r = fetch_realtime_quotes(part)
        if not isinstance(r, dict) or not bool(r.get("ok")):
            continue
        for it in r.get("items", []) or []:
            try:
                v = float(it.get("pct_chg"))
            except Exception:
                continue
            if math.isfinite(v):
                vals.append(v)
        time.sleep(0.08)
    return (float(sum(vals) / len(vals)) if vals else 0.0), len(vals)


def _failed_limitup_rate_from_db(trade_date: date) -> tuple[float, int, int]:
    """
    Approximate failed limit-up rate using daily table:
      ever = high touched limit price
      close = close at limit price
    """
    rows = _daily_rows_for_date(trade_date)
    ever = 0
    close = 0
    for ts_code, pre_close, high, close0, pct_chg, name in rows:
        if pre_close is None or high is None or close0 is None:
            continue
        try:
            pre = float(pre_close)
            h = float(high)
            c = float(close0)
        except Exception:
            continue
        if not (pre > 0.0 and math.isfinite(pre) and math.isfinite(h) and math.isfinite(c)):
            continue
        limit_pct = _limit_pct_for(ts_code, name)
        limit_price = pre * (1.0 + limit_pct / 100.0)
        tol = max(0.01, abs(limit_price) * 0.0015)
        touched = h >= (limit_price - tol)
        closed = abs(c - limit_price) <= tol
        if touched:
            ever += 1
            if closed:
                close += 1
            continue
        # Fallback: pct-based touched check (weaker).
        try:
            p = float(pct_chg) if pct_chg is not None else None
        except Exception:
            p = None
        if p is not None and math.isfinite(p) and p >= (limit_pct - 0.2):
            ever += 1
            close += 1
    failed = max(0, ever - close)
    rate = (float(failed) / float(ever) * 100.0) if ever > 0 else 0.0
    return rate, ever, close


def compute_cn_sentiment_for_date(d: str) -> dict[str, Any]:
    ts = now_iso()
    as_of = d
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    raw: dict[str, Any] = {}
    errors: list[str] = []
    up = 0
    down = 0
    flat = 0
    ratio = 0.0
    market_turnover_cny = 0.0
    market_volume = 0.0

    breadth: dict[str, Any] | None = None
    try:
        breadth = fetch_cn_market_breadth_eod(dt)
    except Exception as e:
        errors.append(f"breadth_failed: {e}")
        raw["breadthError"] = str(e)

    today_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()
    should_try_intraday = dt == today_cn and (
        not breadth
        or int(breadth.get("total_count") or 0) == 0
        or _finite_float(breadth.get("total_turnover_cny"), 0.0) == 0.0
    )
    if should_try_intraday:
        try:
            breadth_rt = fetch_cn_market_breadth_intraday(dt)
            if int(breadth_rt.get("total_count") or 0) > 0:
                breadth = breadth_rt
        except Exception as e:
            errors.append(f"breadth_intraday_failed: {e}")

    if breadth:
        raw["breadth"] = breadth
        up = int(breadth.get("up_count") or 0)
        down = int(breadth.get("down_count") or 0)
        flat = int(breadth.get("flat_count") or 0)
        ratio = _finite_float(breadth.get("up_down_ratio"), 0.0)
        market_turnover_cny = _finite_float(breadth.get("total_turnover_cny"), 0.0)
        market_volume = _finite_float(breadth.get("total_volume"), 0.0)

    # Premium%: DB-first. Derive yesterday close-limit-up pool from DB, then:
    # - If today's daily pct_chg exists in DB: use it
    # - If today's daily is not ready: use realtime_quote
    premium = 0.0
    try:
        y = _prev_open_date("SSE", dt)
        if y is None:
            raise RuntimeError("trade calendar missing for premium computation")
        pool = _close_limit_up_pool_codes(y)
        premium_db, matched_db = _avg_pct_chg_from_db(dt, pool)
        if matched_db > 0:
            premium = premium_db
            raw["yesterdayLimitUpPremium"] = {
                "date": dt.isoformat(),
                "premium": premium,
                "count": len(pool),
                "matched": matched_db,
                "y": y.isoformat(),
                "source": "db.daily",
            }
        else:
            # Intraday/near-close realtime fallback for "today" when daily is not ready.
            now_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
            if now_cn.date() == dt:
                premium_rt, matched_rt = _avg_pct_chg_from_realtime(pool)
                premium = premium_rt
                raw["yesterdayLimitUpPremium"] = {
                    "date": dt.isoformat(),
                    "premium": premium,
                    "count": len(pool),
                    "matched": matched_rt,
                    "y": y.isoformat(),
                    "source": "tushare.realtime_quote",
                }
                errors.append(f"premium_realtime_from: {y.isoformat()}")
            else:
                raw["yesterdayLimitUpPremium"] = {
                    "date": dt.isoformat(),
                    "premium": 0.0,
                    "count": len(pool),
                    "matched": 0,
                    "y": y.isoformat(),
                    "source": "db.daily",
                }
                errors.append(f"premium_missing_daily_for: {dt.isoformat()}")
    except Exception as e:
        errors.append(f"yesterday_limitup_premium_failed: {e}")
        raw["yesterdayLimitUpPremiumError"] = str(e)

    failed_rate = 0.0
    try:
        # Failed% (炸板率): DB-first from daily table. Intraday not reliable; keep 0 and mark.
        # If daily rows for today are not ready, return 0 with a rule so UI doesn't misinterpret it.
        rate, ever_cnt, close_cnt = _failed_limitup_rate_from_db(dt)
        failed_rate = _finite_float(rate, 0.0)
        raw["failedLimitUpRate"] = {
            "date": dt.isoformat(),
            "failed_rate": failed_rate,
            "ever_count": ever_cnt,
            "close_count": close_cnt,
            "source": "db.daily",
        }
        if ever_cnt == 0:
            now_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
            if now_cn.date() == dt and _is_shanghai_trading_time():
                errors.append("failed_rate_intraday_unavailable")
    except Exception as e:
        errors.append(f"failed_limitup_rate_failed: {e}")
        raw["failedLimitUpRateError"] = str(e)

    rules: list[str] = []
    risk_mode = "normal"
    turnover_high = market_turnover_cny >= 1.5e12
    turnover_hot = market_turnover_cny >= 1.8e12
    turnover_euphoric = market_turnover_cny >= 2.5e12
    breadth_good = ratio >= 1.2
    breadth_hot = ratio >= 1.5
    breadth_euphoric = ratio >= 2.0
    premium_good = premium >= 0.0
    premium_hot = premium >= 0.5
    premium_euphoric = premium >= 3.0
    bullish_override = turnover_high and breadth_good and premium_good

    if turnover_euphoric and breadth_euphoric and premium_euphoric and failed_rate <= 35.0:
        risk_mode = "euphoric"
        rules.append("euphoric(turnover>=2.5T && breadth>=2.0 && premium>=3.0 && failed<=35)")
    elif turnover_hot and breadth_hot and premium_hot and failed_rate <= 50.0:
        risk_mode = "hot"
        rules.append("hot(turnover>=1.8T && breadth>=1.5 && premium>=0.5 && failed<=50)")
    else:
        if premium < 0.0 and failed_rate >= 70.0:
            risk_mode = "no_new_positions"
            rules.append("premium<0 && failedLimitUpRate>=70 => no_new_positions")
        elif failed_rate >= 70.0:
            risk_mode = "caution"
            rules.append("failedLimitUpRate>=70 => caution")
        elif premium < 0.0:
            risk_mode = "caution"
            rules.append("premium<0 => caution")
        if risk_mode in ("caution", "no_new_positions") and bullish_override and failed_rate <= 85.0:
            risk_mode = "normal"
            rules.append("bullish_override(turnover_high && breadth_ratio>=1.2 && premium>=0)")
    if errors and risk_mode == "normal":
        risk_mode = "caution"
    if errors:
        rules.extend(errors[:3])

    return {
        "date": d,
        "asOfDate": as_of,
        "up": up,
        "down": down,
        "flat": flat,
        "ratio": ratio,
        "marketTurnoverCny": market_turnover_cny,
        "marketVolume": market_volume,
        "premium": premium,
        "failedRate": failed_rate,
        "riskMode": risk_mode,
        "rules": rules,
        "updatedAt": ts,
        "raw": raw,
    }


def sync_cn_sentiment(*, date_str: str, force: bool) -> dict[str, Any]:
    d = date_str
    if not force:
        cached = list_days(as_of_date=d, days=1)
        if cached and str(cached[-1].get("date") or "") == d:
            return {"asOfDate": d, "days": 1, "items": [cached[-1]]}

    try:
        out = compute_cn_sentiment_for_date(d)
    except Exception as e:
        cached2 = list_days(as_of_date=d, days=1)
        if cached2:
            # Persist a "stale" row for today so Dashboard can move forward.
            last = dict(cached2[-1])
            last_date = str(last.get("date") or "")
            last_rules = (last.get("rules") if isinstance(last.get("rules"), list) else []) or []
            last["date"] = d
            last["updatedAt"] = now_iso()
            last["rules"] = [
                *[str(x) for x in last_rules],
                f"stale_from: {last_date}" if last_date else "stale_from: unknown",
                f"sync_failed: {type(e).__name__}: {e}",
            ]
            row2 = {
                "date": d,
                "as_of_date": d,
                "up_count": int(last.get("upCount") or 0),
                "down_count": int(last.get("downCount") or 0),
                "flat_count": int(last.get("flatCount") or 0),
                "total_count": int(last.get("totalCount") or 0),
                "up_down_ratio": float(last.get("upDownRatio") or 0.0),
                "market_turnover_cny": float(last.get("marketTurnoverCny") or 0.0),
                "market_volume": float(last.get("marketVolume") or 0.0),
                "yesterday_limitup_premium": float(last.get("yesterdayLimitUpPremium") or 0.0),
                "failed_limitup_rate": float(last.get("failedLimitUpRate") or 0.0),
                "risk_mode": str(last.get("riskMode") or "caution"),
                "rules": last.get("rules") if isinstance(last.get("rules"), list) else [],
                "updated_at": str(last.get("updatedAt") or now_iso()),
                "raw": {"stale": True, "error": str(e), "sourceDate": last_date},
            }
            upsert_daily_rows([row2])
            cached3 = list_days(as_of_date=d, days=1)
            if cached3:
                return {"asOfDate": d, "days": 1, "items": [cached3[-1]]}
            return {"asOfDate": d, "days": 1, "items": [last]}
        out = {
            "date": d,
            "asOfDate": d,
            "up": 0,
            "down": 0,
            "flat": 0,
            "ratio": 0.0,
            "premium": 0.0,
            "failedRate": 0.0,
            "riskMode": "caution",
            "rules": [f"compute_failed: {type(e).__name__}: {e}"],
            "updatedAt": now_iso(),
            "raw": {"error": str(e)},
        }

    rules_raw = out.get("rules") or []
    rules_list = [str(x) for x in rules_raw] if isinstance(rules_raw, list) else [str(rules_raw)]
    # Always persist a row for the requested date so the dashboard can advance.
    # If some sub-components failed, we keep the partial values and attach failure rules.
    row = {
        "date": out.get("date") or d,
        "as_of_date": out.get("asOfDate") or d,
        "up_count": out.get("up") or 0,
        "down_count": out.get("down") or 0,
        "flat_count": out.get("flat") or 0,
        "total_count": int(out.get("up", 0)) + int(out.get("down", 0)) + int(out.get("flat", 0)),
        "up_down_ratio": out.get("ratio") or 0.0,
        "market_turnover_cny": out.get("marketTurnoverCny") or 0.0,
        "market_volume": out.get("marketVolume") or 0.0,
        "yesterday_limitup_premium": out.get("premium") or 0.0,
        "failed_limitup_rate": out.get("failedRate") or 0.0,
        "risk_mode": out.get("riskMode") or "caution",
        "rules": rules_list,
        "updated_at": out.get("updatedAt") or now_iso(),
        "raw": out.get("raw") if isinstance(out.get("raw"), dict) else {"raw": out.get("raw")},
    }
    upsert_daily_rows([row])
    cached = list_days(as_of_date=d, days=1)
    if cached:
        return {"asOfDate": d, "days": 1, "items": [cached[-1]]}

    items = [
        {
            "date": str(out.get("date") or d),
            "upCount": int(out.get("up") or 0),
            "downCount": int(out.get("down") or 0),
            "flatCount": int(out.get("flat") or 0),
            "totalCount": int(out.get("up", 0)) + int(out.get("down", 0)) + int(out.get("flat", 0)),
            "upDownRatio": float(out.get("ratio") or 0.0),
            "marketTurnoverCny": float(out.get("marketTurnoverCny") or 0.0),
            "marketVolume": float(out.get("marketVolume") or 0.0),
            "yesterdayLimitUpPremium": float(out.get("premium") or 0.0),
            "failedLimitUpRate": float(out.get("failedRate") or 0.0),
            "riskMode": str(out.get("riskMode") or "caution"),
            "rules": rules_list,
            "updatedAt": str(out.get("updatedAt") or now_iso()),
        }
    ]
    return {"asOfDate": d, "days": 1, "items": items}


def get_cn_sentiment(*, days: int = 10, as_of_date: str | None = None) -> dict[str, Any]:
    d = (as_of_date or "").strip() or (get_latest_date() or "")
    if not d:
        return {"asOfDate": "", "days": days, "items": []}
    items = list_days(as_of_date=d, days=days)
    return {"asOfDate": d, "days": max(1, min(int(days), 30)), "items": items}
