from __future__ import annotations

import math
import time
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from cachetools import TTLCache

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.index_daily import fetch_last_closes_vol_batch
from data_sync_service.db.industry_fund_flow import get_rows_by_date
from data_sync_service.db.macro_daily import fetch_last_closes as fetch_macro_last_closes
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.stock_basic import fetch_ts_codes
from data_sync_service.service.market_sentiment import (
    fetch_cn_market_breadth_eod,
    fetch_cn_market_breadth_intraday,
)
from data_sync_service.service.macro_snapshot_on_demand import (
    _is_data_stale,
    fetch_hsi_on_demand,
)
from data_sync_service.service.realtime_quote import fetch_realtime_quotes, fetch_realtime_quotes_batched

INDEX_SIGNALS = [
    {"ts_code": "000001.SH", "name": "上证指数"},
    {"ts_code": "399006.SZ", "name": "创业板指"},
]

HK_INDEX_SIGNALS = [
    {"series_id": "HSI", "name": "恒生指数"},
]

HISTORY_DAYS = 80
REGIME_CACHE_TTL_SECONDS = 600
INDEX_SIGNALS_CACHE_TTL_SECONDS = 60
BREADTH_CACHE_TTL_SECONDS = 60
HISTORICAL_BREADTH_CACHE_TTL_SECONDS = 3600

_regime_cache: TTLCache = TTLCache(maxsize=32, ttl=REGIME_CACHE_TTL_SECONDS)
_index_signals_cache: TTLCache = TTLCache(maxsize=32, ttl=INDEX_SIGNALS_CACHE_TTL_SECONDS)
_breadth_cache_live: TTLCache = TTLCache(maxsize=32, ttl=BREADTH_CACHE_TTL_SECONDS)
_breadth_cache_hist: TTLCache = TTLCache(maxsize=32, ttl=HISTORICAL_BREADTH_CACHE_TTL_SECONDS)
_liquidity_cache_live: TTLCache = TTLCache(maxsize=32, ttl=BREADTH_CACHE_TTL_SECONDS)
_liquidity_cache_hist: TTLCache = TTLCache(maxsize=32, ttl=HISTORICAL_BREADTH_CACHE_TTL_SECONDS)


def clear_index_signals_cache() -> None:
    """Clear in-process index signals TTL cache (for tests)."""
    _index_signals_cache.clear()
    clear_market_breadth_cache()


def clear_market_breadth_cache() -> None:
    """Clear all-market breadth and liquidity TTL caches (for tests and force refresh)."""
    _breadth_cache_live.clear()
    _breadth_cache_hist.clear()
    _liquidity_cache_live.clear()
    _liquidity_cache_hist.clear()


def clear_market_regime_cache() -> None:
    """Clear in-process market regime TTL cache (for tests)."""
    _regime_cache.clear()
    clear_index_signals_cache()


BREADTH_DEEP_GREEN_MIN_RATIO = 0.6


def _ema(values: list[float], period: int) -> list[float]:
    if not values or len(values) < period:
        return []
    multiplier = 2.0 / (period + 1)
    ema_vals: list[float] = []
    sma = sum(values[:period]) / period
    ema_vals.append(sma)
    for v in values[period:]:
        ema_vals.append((v - ema_vals[-1]) * multiplier + ema_vals[-1])
    return ema_vals


def _macd_histogram(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> list[float]:
    if len(closes) < slow + signal:
        return []
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    if not ema_fast or not ema_slow:
        return []
    offset = slow - fast
    dif = [ema_fast[i + offset] - ema_slow[i] for i in range(len(ema_slow))]
    if len(dif) < signal:
        return []
    dea = _ema(dif, signal)
    offset_dif = len(dif) - len(dea)
    hist = [dif[i + offset_dif] - dea[i] for i in range(len(dea))]
    return hist


def _today_iso_date() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _is_shanghai_trading_time_at(now: datetime) -> bool:
    """
    Best-effort CN A-share trading time check in Asia/Shanghai.
    """
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    in_morning = minutes >= 9 * 60 + 30 and minutes <= 11 * 60 + 30
    in_afternoon = minutes >= 13 * 60 and minutes <= 15 * 60
    return in_morning or in_afternoon


def _is_shanghai_sync_window_at(now: datetime) -> bool:
    """
    Sync window: trading hours + lunch break + after-hours until 20:00.
    Realtime quotes remain available after market close.
    """
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    in_trading = _is_shanghai_trading_time_at(now)
    in_lunch = minutes > 11 * 60 + 30 and minutes < 13 * 60
    in_after_hours = minutes > 15 * 60 and minutes <= 20 * 60
    return in_trading or in_lunch or in_after_hours


def _is_shanghai_trading_time() -> bool:
    return _is_shanghai_trading_time_at(datetime.now(tz=ZoneInfo("Asia/Shanghai")))


def _is_shanghai_sync_window() -> bool:
    return _is_shanghai_sync_window_at(datetime.now(tz=ZoneInfo("Asia/Shanghai")))


def _get_trade_minutes(now: datetime) -> int:
    """
    Get elapsed trading minutes in Shanghai timezone.
    Morning: 9:30-11:30 (120 min), Afternoon: 13:00-15:00 (120 min).
    Total: 240 minutes.
    """
    minutes = now.hour * 60 + now.minute
    if minutes <= 9 * 60 + 30:
        return 0
    if minutes <= 11 * 60 + 30:
        return minutes - (9 * 60 + 30)
    if minutes < 13 * 60:
        return 120
    if minutes <= 15 * 60:
        return 120 + (minutes - 13 * 60)
    return 240


def _estimate_full_day_volume(current_vol: float, trade_minutes: int) -> float | None:
    """Estimate full-day volume from current partial volume."""
    if trade_minutes <= 0:
        return None
    return current_vol * 240.0 / trade_minutes


def _trade_date_from_trade_time(trade_time: str | None) -> str | None:
    if not trade_time:
        return None
    s = str(trade_time).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _hsi_series_stale(series_raw: list[tuple[str, float]]) -> bool:
    if not series_raw:
        return True
    return _is_data_stale(series_raw[-1][0])


def _merge_on_demand_into_series(
    series_raw: list[tuple[str, float]],
    metrics: dict[str, Any],
) -> list[tuple[str, float]]:
    as_of = str(metrics.get("asOfDate") or "")
    close = metrics.get("close")
    if not as_of or close is None:
        return series_raw
    try:
        c = float(close)
    except (TypeError, ValueError):
        return series_raw
    if not math.isfinite(c):
        return series_raw
    if series_raw and series_raw[-1][0] == as_of:
        return [*series_raw[:-1], (as_of, c)]
    if not series_raw or series_raw[-1][0] < as_of:
        return [*series_raw, (as_of, c)]
    return series_raw


def _hsi_source_label(*, used_realtime: bool, on_demand_src: str | None) -> str:
    if used_realtime:
        return "tushare.realtime_quote"
    if on_demand_src:
        return on_demand_src
    return "db.macro_daily"


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def _realtime_pct_or_price(item: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (pct_chg, price) for breadth computation; pct may be derived from price/pre_close."""
    price = _safe_float(item.get("price"))
    pre_close = _safe_float(item.get("pre_close"))
    pct = _safe_float(item.get("pct_chg"))
    if pct is not None:
        return pct, price
    if price is not None and pre_close is not None and pre_close > 0:
        return (price - pre_close) / pre_close * 100.0, price
    return None, price


def _market_data_cache_key(kind: str, as_of_date: str | None) -> tuple[str, str]:
    return (kind, str(as_of_date or "").strip())


def _breadth_cache_for(as_of_date: str | None) -> TTLCache:
    return _breadth_cache_hist if as_of_date else _breadth_cache_live


def _liquidity_cache_for(as_of_date: str | None) -> TTLCache:
    return _liquidity_cache_hist if as_of_date else _liquidity_cache_live


def _get_breadth_above_ma20_ratio(*, as_of_date: str | None = None) -> dict[str, Any]:
    key = _market_data_cache_key("ma20", as_of_date)
    cache = _breadth_cache_for(as_of_date)
    cached = cache.get(key)
    if cached is not None:
        return dict(cached)
    result = _compute_breadth_above_ma20_ratio(as_of_date=as_of_date)
    cache[key] = dict(result)
    return result


def _compute_breadth_above_ma20_ratio(*, as_of_date: str | None = None) -> dict[str, Any]:
    """
    Compute realtime breadth: ratio of CN A-shares with price above MA20.
    Returns {ratio, total, above_count}; caches per-request via single call.
    """
    ensure_stock_basic()
    codes_all = fetch_ts_codes()
    ts_codes = [c for c in codes_all if c.endswith((".SZ", ".SH", ".BJ"))]
    if not ts_codes:
        return {"ratio": 0.0, "total": 0, "above_count": 0}

    batch = fetch_last_ohlcv_batch(ts_codes, days=30)
    ma20_by_code: dict[str, float] = {}
    last_close_by_code: dict[str, float] = {}
    for code, rows in batch.items():
        if len(rows) < 20:
            continue
        closes = []
        for r in rows:
            try:
                c = float(r[4])
            except Exception:
                continue
            if math.isfinite(c):
                closes.append(c)
        if len(closes) >= 20:
            ma20 = sum(closes[-20:]) / 20.0
            ma20_by_code[code] = ma20
            last_close_by_code[code] = closes[-1]

    above = 0
    total = 0
    rt_price: dict[str, float] = {}
    if _is_shanghai_sync_window() and not as_of_date:
        eligible = [c for c in ts_codes if c in ma20_by_code]
        for it in fetch_realtime_quotes_batched(eligible):
            code = str(it.get("ts_code") or "").strip()
            if not code:
                continue
            _, price = _realtime_pct_or_price(it)
            if price is not None:
                rt_price[code] = price

    for code, ma20 in ma20_by_code.items():
        price = rt_price.get(code) if rt_price else None
        if price is None:
            price = last_close_by_code.get(code)
        if price is None:
            continue
        total += 1
        if price > ma20:
            above += 1

    ratio = float(above) / float(total) if total > 0 else 0.0
    return {"ratio": ratio, "total": total, "above_count": above}


def _get_market_liquidity_and_mainline(
    *, as_of_date: str | None = None, breadth_ratio: float
) -> dict[str, Any]:
    key = _market_data_cache_key("liquidity", as_of_date)
    cache = _liquidity_cache_for(as_of_date)
    cached = cache.get(key)
    if cached is not None:
        return dict(cached)
    result = _compute_market_liquidity_and_mainline(as_of_date=as_of_date, breadth_ratio=breadth_ratio)
    cache[key] = dict(result)
    return result


def _compute_market_liquidity_and_mainline(
    *, as_of_date: str | None = None, breadth_ratio: float
) -> dict[str, Any]:
    """
    Fetch total market turnover and max industry inflow for deep green criteria.
    Returns {total_turnover_cny, max_industry_inflow, turnover_above_1_5T, mainline_inflow_above_5B}.
    """
    dt: date | None = None
    if as_of_date:
        try:
            dt = date.fromisoformat(as_of_date)
        except Exception:
            dt = None
    if dt is None:
        dt = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()

    total_turnover_cny = 0.0
    max_industry_inflow = 0.0

    try:
        breadth = fetch_cn_market_breadth_eod(dt)
        if breadth and "total_turnover_cny" in breadth:
            total_turnover_cny = float(breadth.get("total_turnover_cny") or 0.0)
    except Exception:
        pass

    today_cn = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date()
    if dt == today_cn and total_turnover_cny == 0.0:
        try:
            breadth_rt = fetch_cn_market_breadth_intraday(dt)
            if breadth_rt and "total_turnover_cny" in breadth_rt:
                turnover_rt = float(breadth_rt.get("total_turnover_cny") or 0.0)
                if turnover_rt > 0.0:
                    total_turnover_cny = turnover_rt
        except Exception:
            pass

    date_str = dt.isoformat()
    try:
        rows = get_rows_by_date(date_str)
        if rows:
            inflows = [float(r.get("net_inflow") or 0.0) for r in rows]
            max_industry_inflow = max(inflows) if inflows else 0.0
    except Exception:
        pass

    turnover_above_1_5T = total_turnover_cny >= 1.5e12
    mainline_inflow_above_5B = max_industry_inflow >= 5e9

    return {
        "total_turnover_cny": total_turnover_cny,
        "max_industry_inflow": max_industry_inflow,
        "turnover_above_1_5T": turnover_above_1_5T,
        "mainline_inflow_above_5B": mainline_inflow_above_5B,
    }


def _quote_error_message(resp: dict[str, Any] | None) -> str | None:
    if not isinstance(resp, dict):
        return "invalid_quote_response"
    err = resp.get("error")
    if err is None:
        return None
    msg = str(err).strip()
    return msg or None


def _fetch_realtime_quote_map(codes: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """Fetch realtime quotes with per-symbol fallback so one bad symbol cannot poison the batch."""
    clean_codes = [str(c).strip() for c in codes if str(c or "").strip()]
    if not clean_codes:
        return {}, {}

    def add_items(resp: dict[str, Any] | None, target: dict[str, dict[str, Any]]) -> None:
        if not isinstance(resp, dict) or not bool(resp.get("ok")):
            return
        for item in resp.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            code = str(item.get("ts_code") or "").strip()
            if code:
                target[code] = item

    quotes: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    batch_resp = fetch_realtime_quotes(clean_codes)
    if isinstance(batch_resp, dict) and bool(batch_resp.get("ok")):
        add_items(batch_resp, quotes)
        missing = [code for code in clean_codes if code not in quotes]
        if not missing:
            return quotes, errors
        for code in missing:
            one_resp = fetch_realtime_quotes([code])
            before = len(quotes)
            add_items(one_resp, quotes)
            if code not in quotes or len(quotes) == before:
                errors[code] = _quote_error_message(one_resp) or "missing_from_batch_quote"
            time.sleep(0.03)
        return quotes, errors

    batch_error = _quote_error_message(batch_resp) or "batch_quote_failed"
    for code in clean_codes:
        one_resp = fetch_realtime_quotes([code])
        before = len(quotes)
        add_items(one_resp, quotes)
        if code not in quotes or len(quotes) == before:
            errors[code] = _quote_error_message(one_resp) or batch_error
        time.sleep(0.03)
    return quotes, errors


def _apply_realtime_quotes(
    quotes: dict[str, dict[str, Any]],
    rt_price: dict[str, float],
    rt_time: dict[str, str | None],
    rt_pct: dict[str, float | None],
    rt_vol: dict[str, float],
) -> None:
    for ts_code, item in quotes.items():
        pct_rt, price = _realtime_pct_or_price(item)
        if price is None:
            continue
        rt_price[ts_code] = price
        rt_pct[ts_code] = pct_rt
        trade_time_raw = item.get("trade_time")
        rt_time[ts_code] = str(trade_time_raw) if trade_time_raw else None
        vol_str = item.get("volume")
        if vol_str:
            try:
                rt_vol[ts_code] = float(vol_str)
            except (ValueError, TypeError):
                pass


def get_index_signals(
    *, as_of_date: str | None = None, include_breadth: bool = True
) -> list[dict[str, Any]]:
    """
    Return index traffic-light signals using MA20/MA5/MA60, 3-day confirmation,
    volume expansion, and breadth gating.

    When include_breadth is False, skips the all-market breadth scan (slow) and
    never emits deep_green; use for lightweight APIs such as GET /macro/snapshot.

    Results are cached in-process for INDEX_SIGNALS_CACHE_TTL_SECONDS per
    (as_of_date, include_breadth).
    """
    key = (str(as_of_date or "").strip(), bool(include_breadth))
    cached = _index_signals_cache.get(key)
    if cached is not None:
        return cached

    out = _compute_index_signals(as_of_date=as_of_date, include_breadth=include_breadth)
    _index_signals_cache[key] = out
    return out


def _compute_index_signals(
    *, as_of_date: str | None = None, include_breadth: bool = True
) -> list[dict[str, Any]]:
    use_as_of = str(as_of_date).strip() if as_of_date else None
    rt_price: dict[str, float] = {}
    rt_time: dict[str, str | None] = {}
    rt_pct: dict[str, float | None] = {}
    rt_vol: dict[str, float] = {}
    rt_quote_errors: dict[str, str] = {}
    if _is_shanghai_sync_window() and not use_as_of:
        cn_codes = [x["ts_code"] for x in INDEX_SIGNALS]
        cn_quotes, cn_errors = _fetch_realtime_quote_map(cn_codes)
        _apply_realtime_quotes(cn_quotes, rt_price, rt_time, rt_pct, rt_vol)
        rt_quote_errors.update(cn_errors)

        # HK/offshore symbols must not share the CN quote batch. Some providers fail
        # unsupported offshore symbols with hard errors; isolate them so CN realtime remains usable.
        hk_codes = [x["series_id"] for x in HK_INDEX_SIGNALS]
        hk_quotes, hk_errors = _fetch_realtime_quote_map(hk_codes)
        _apply_realtime_quotes(hk_quotes, rt_price, rt_time, rt_pct, rt_vol)
        rt_quote_errors.update(hk_errors)

    if include_breadth:
        breadth = _get_breadth_above_ma20_ratio(as_of_date=use_as_of)
        liquidity = _get_market_liquidity_and_mainline(
            as_of_date=use_as_of, breadth_ratio=breadth["ratio"]
        )
    else:
        breadth = {"ratio": 0.0, "total": 0, "above_count": 0}
        liquidity = {
            "total_turnover_cny": 0.0,
            "max_industry_inflow": 0.0,
            "turnover_above_1_5T": False,
            "mainline_inflow_above_5B": False,
        }
    breadth_ok_deep = breadth["ratio"] >= 0.5
    liquidity_ok = liquidity["turnover_above_1_5T"]
    mainline_ok = liquidity["mainline_inflow_above_5B"]

    out: list[dict[str, Any]] = []
    index_codes = [it["ts_code"] for it in INDEX_SIGNALS]
    series_by_code = fetch_last_closes_vol_batch(
        index_codes,
        days=HISTORY_DAYS,
        as_of_date=use_as_of,
    )
    for it in INDEX_SIGNALS:
        ts_code = it["ts_code"]
        name = it["name"]
        series_cv = series_by_code.get(ts_code, [])
        series = [(d, c) for d, c, _ in series_cv]
        series_vol = [v for _, _, v in series_cv]

        used_realtime = False
        trade_time = rt_time.get(ts_code)
        rt_close = rt_price.get(ts_code)
        if rt_close is not None:
            rt_date = _trade_date_from_trade_time(trade_time) or _today_iso_date()
            if series:
                last_date = series[-1][0]
                if last_date == rt_date:
                    series = [*series[:-1], (rt_date, rt_close)]
                    if series_vol:
                        series_vol = series_vol[:-1] + [0.0]
                    used_realtime = True
                elif last_date < rt_date:
                    series = [*series, (rt_date, rt_close)]
                    series_vol = series_vol + [0.0]
                    used_realtime = True
            else:
                series = [(rt_date, rt_close)]
                series_vol = [0.0]
                used_realtime = True

        if len(series) < 23:
            closes_short = [c for _, c in series]
            pct_short = None
            if used_realtime and ts_code in rt_pct and rt_pct[ts_code] is not None:
                pct_short = rt_pct[ts_code]
            elif len(closes_short) >= 2:
                p0, p1 = closes_short[-2], closes_short[-1]
                if p0 is not None and p0 > 0 and p1 is not None:
                    pct_short = (p1 - p0) / p0 * 100.0
            item_out = {
                "tsCode": ts_code,
                "name": name,
                "asOfDate": series[-1][0] if series else None,
                "close": series[-1][1] if series else None,
                "ma5": None,
                "ma20": None,
                "ma60": None,
                "ma20Prev": None,
                "signal": "unknown",
                "positionRange": "—",
                "rules": ["insufficient data for MA20"],
                "realtime": used_realtime,
                "tradeTime": trade_time if used_realtime else None,
                "source": "tushare.realtime_quote" if used_realtime else "db.index_daily",
                "pctChg": pct_short,
            }
            if not used_realtime and ts_code in rt_quote_errors:
                item_out["quoteError"] = rt_quote_errors[ts_code]
            out.append(item_out)
            continue

        closes = [c for _, c in series]
        ma5 = sum(closes[-5:]) / 5.0
        ma20 = sum(closes[-20:]) / 20.0
        ma20_prev = sum(closes[-21:-1]) / 20.0
        ma60 = sum(closes[-60:]) / 60.0 if len(closes) >= 60 else None
        close = closes[-1]

        ema10_vals = _ema(closes, 10) if len(closes) >= 10 else []
        ema10 = ema10_vals[-1] if ema10_vals else None
        close_above_ema10 = ema10 is not None and close > ema10

        macd_hist = _macd_histogram(closes)
        macd_hist_turning_up = False
        if len(macd_hist) >= 2:
            macd_hist_turning_up = macd_hist[-1] > macd_hist[-2]

        ma20_slope_up = ma20 > ma20_prev
        ma5_above_ma20 = ma5 > ma20
        ma20_above_ma60 = ma60 is not None and ma20 > ma60
        ma_full_bull = ma5_above_ma20 and ma20_above_ma60

        vol_ratio = None
        vol_above_ma5 = False
        vol_above_ma5_strong = False
        estimated_vol: float | None = None
        if len(series_vol) >= 20:
            avg_vol_5 = sum(series_vol[-5:]) / 5.0
            avg_vol_20 = sum(series_vol[-20:]) / 20.0
            current_vol = series_vol[-1] if series_vol else 0.0
            if used_realtime and ts_code in rt_vol and rt_vol[ts_code] > 0:
                now_sh = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
                trade_minutes = _get_trade_minutes(now_sh)
                estimated_vol = _estimate_full_day_volume(rt_vol[ts_code], trade_minutes)
                if estimated_vol is not None and avg_vol_5 > 0:
                    vol_ratio = estimated_vol / avg_vol_5
                    vol_above_ma5 = estimated_vol > avg_vol_5 * 0.8
                    vol_above_ma5_strong = estimated_vol > avg_vol_5 * 1.3
                else:
                    vol_above_ma5 = False
                    vol_above_ma5_strong = False
            else:
                if avg_vol_5 > 0:
                    vol_ratio = current_vol / avg_vol_5
                vol_above_ma5 = current_vol > avg_vol_5
                vol_above_ma5_strong = current_vol > avg_vol_5 * 1.2

        signal = "yellow"
        position = "30%"
        rules: list[str] = []

        if close < ma20 or not ma5_above_ma20:
            signal = "red"
            position = "0%-10%"
            if close < ma20:
                rules.append("price<MA20")
            if not ma5_above_ma20:
                rules.append("MA5<MA20")
            if close > ma20 and macd_hist_turning_up:
                signal = "yellow"
                position = "30%"
                rules.append("MACD hist turning up override")
        elif close > ma20:
            if (not ma20_slope_up) or (not vol_above_ma5) or (not ma5_above_ma20):
                signal = "yellow"
                position = "30%"
                if not ma20_slope_up:
                    rules.append("MA20 slope down")
                if not vol_above_ma5:
                    rules.append("Vol<MA5Vol*0.8(est)")
                if not ma5_above_ma20:
                    rules.append("MA5<MA20")
            else:
                signal = "green"
                position = "50%-60%"
                rules.append("price>MA20 && MA5>MA20 && MA20 up && volRatio>0.8(est)")

            if (
                ma_full_bull
                and close_above_ema10
                and liquidity_ok
                and (breadth_ok_deep or mainline_ok)
            ):
                signal = "deep_green"
                position = "80%-100%"
                rules.append(
                    "MA5>MA20>MA60 && price>EMA10 && turnover>=1.5T && (breadth>=50% || mainline>=5B)"
                )
        else:
            if close < ma5 and close >= ma20:
                rules.append("close<MA5 but hold MA20")
            elif abs(close - ma20) / ma20 <= 0.01:
                rules.append("close near MA20")
            else:
                rules.append("range/sideways")

        pct_chg: float | None = None
        if used_realtime and rt_pct.get(ts_code) is not None:
            pct_chg = rt_pct[ts_code]
        if pct_chg is None and len(closes) >= 2:
            prev_c, cur_c = closes[-2], closes[-1]
            if prev_c is not None and prev_c > 0 and cur_c is not None:
                pct_chg = (cur_c - prev_c) / prev_c * 100.0

        item_out = {
            "tsCode": ts_code,
            "name": name,
            "asOfDate": series[-1][0],
            "close": close,
            "ma5": ma5,
            "ma20": ma20,
            "ma60": ma60,
            "ma20Prev": ma20_prev,
            "ema10": ema10,
            "signal": signal,
            "positionRange": position,
            "rules": rules,
            "realtime": used_realtime,
            "tradeTime": trade_time if used_realtime else None,
            "source": "tushare.realtime_quote" if used_realtime else "db.index_daily",
            "pctChg": pct_chg,
            "volRatio": vol_ratio,
            "estimatedVol": estimated_vol,
            "totalTurnover": liquidity["total_turnover_cny"],
            "maxIndustryInflow": liquidity["max_industry_inflow"],
        }
        if not used_realtime and ts_code in rt_quote_errors:
            item_out["quoteError"] = rt_quote_errors[ts_code]
        out.append(item_out)

    for it in HK_INDEX_SIGNALS:
        series_id = it["series_id"]
        name = it["name"]
        series_raw = fetch_macro_last_closes(series_id, days=HISTORY_DAYS)
        if not series_raw or _hsi_series_stale(series_raw):
            metrics, src = fetch_hsi_on_demand()
            if metrics.get("close") is not None and metrics.get("asOfDate"):
                series_raw = _merge_on_demand_into_series(series_raw, metrics)
                hsi_on_demand_source = src
            else:
                hsi_on_demand_source = None
        else:
            hsi_on_demand_source = None
        if not series_raw:
            item_out = {
                "tsCode": series_id,
                "name": name,
                "asOfDate": None,
                "close": None,
                "ma5": None,
                "ma20": None,
                "ma60": None,
                "ma20Prev": None,
                "signal": "unknown",
                "positionRange": "—",
                "rules": ["no data in macro_daily"],
                "realtime": False,
                "tradeTime": None,
                "source": "db.macro_daily",
                "pctChg": None,
                "volRatio": None,
                "estimatedVol": None,
            }
            if series_id in rt_quote_errors:
                item_out["quoteError"] = rt_quote_errors[series_id]
            out.append(item_out)
            continue

        used_realtime = False
        trade_time = rt_time.get(series_id)
        rt_close = rt_price.get(series_id)
        series = series_raw
        if rt_close is not None:
            rt_date = _trade_date_from_trade_time(trade_time) or _today_iso_date()
            if series:
                last_date = series[-1][0]
                if last_date == rt_date:
                    series = [*series[:-1], (rt_date, rt_close)]
                    used_realtime = True
                elif last_date < rt_date:
                    series = [*series, (rt_date, rt_close)]
                    used_realtime = True
            else:
                series = [(rt_date, rt_close)]
                used_realtime = True

        if len(series) < 23:
            closes_short = [c for _, c in series]
            pct_short = None
            if used_realtime and series_id in rt_pct and rt_pct[series_id] is not None:
                pct_short = rt_pct[series_id]
            elif len(closes_short) >= 2:
                p0, p1 = closes_short[-2], closes_short[-1]
                if p0 is not None and p0 > 0 and p1 is not None:
                    pct_short = (p1 - p0) / p0 * 100.0
            item_out = {
                "tsCode": series_id,
                "name": name,
                "asOfDate": series[-1][0] if series else None,
                "close": series[-1][1] if series else None,
                "ma5": None,
                "ma20": None,
                "ma60": None,
                "ma20Prev": None,
                "signal": "unknown",
                "positionRange": "—",
                "rules": ["insufficient data for MA20"],
                "realtime": used_realtime,
                "tradeTime": trade_time if used_realtime else None,
                "source": _hsi_source_label(
                    used_realtime=used_realtime,
                    on_demand_src=hsi_on_demand_source,
                ),
                "pctChg": pct_short,
                "volRatio": None,
                "estimatedVol": None,
            }
            if not used_realtime and series_id in rt_quote_errors:
                item_out["quoteError"] = rt_quote_errors[series_id]
            out.append(item_out)
            continue

        closes = [c for _, c in series]
        ma5 = sum(closes[-5:]) / 5.0
        ma20 = sum(closes[-20:]) / 20.0
        ma20_prev = sum(closes[-21:-1]) / 20.0
        ma60 = sum(closes[-60:]) / 60.0 if len(closes) >= 60 else None
        close = closes[-1]

        macd_hist = _macd_histogram(closes)
        macd_hist_turning_up = False
        if len(macd_hist) >= 2:
            macd_hist_turning_up = macd_hist[-1] > macd_hist[-2]

        ma20_slope_up = ma20 > ma20_prev
        ma5_above_ma20 = ma5 > ma20
        ma20_above_ma60 = ma60 is not None and ma20 > ma60

        signal = "yellow"
        position = "30%"
        rules: list[str] = []

        if close < ma20 or not ma5_above_ma20:
            signal = "red"
            position = "0%-10%"
            if close < ma20:
                rules.append("price<MA20")
            if not ma5_above_ma20:
                rules.append("MA5<MA20")
            if close > ma20 and macd_hist_turning_up:
                signal = "yellow"
                position = "30%"
                rules.append("MACD hist turning up override")
        elif close > ma20:
            if not ma20_slope_up or not ma5_above_ma20:
                signal = "yellow"
                position = "30%"
                if not ma20_slope_up:
                    rules.append("MA20 slope down")
                if not ma5_above_ma20:
                    rules.append("MA5<MA20")
            else:
                signal = "green"
                position = "50%-60%"
                rules.append("price>MA20 && MA5>MA20 && MA20 up")

            if ma20_above_ma60 and ma60 is not None and close > ma5:
                signal = "green"
                position = "60%-80%"
                rules.append("MA5>MA20>MA60 && price>MA5")
        else:
            if close < ma5 and close >= ma20:
                rules.append("close<MA5 but hold MA20")
            elif abs(close - ma20) / ma20 <= 0.01:
                rules.append("close near MA20")
            else:
                rules.append("range/sideways")

        pct_chg: float | None = None
        if used_realtime and rt_pct.get(series_id) is not None:
            pct_chg = rt_pct[series_id]
        if pct_chg is None and len(closes) >= 2:
            prev_c, cur_c = closes[-2], closes[-1]
            if prev_c is not None and prev_c > 0 and cur_c is not None:
                pct_chg = (cur_c - prev_c) / prev_c * 100.0

        item_out = {
            "tsCode": series_id,
            "name": name,
            "asOfDate": series[-1][0],
            "close": close,
            "ma5": ma5,
            "ma20": ma20,
            "ma60": ma60,
            "ma20Prev": ma20_prev,
            "signal": signal,
            "positionRange": position,
            "rules": rules,
            "realtime": used_realtime,
            "tradeTime": trade_time if used_realtime else None,
            "source": _hsi_source_label(
                used_realtime=used_realtime,
                on_demand_src=hsi_on_demand_source,
            ),
            "pctChg": pct_chg,
            "volRatio": None,
            "estimatedVol": None,
        }
        if not used_realtime and series_id in rt_quote_errors:
            item_out["quoteError"] = rt_quote_errors[series_id]
        out.append(item_out)
    return out


def _signal_rank(signal: str) -> int:
    if signal in ("green", "light_green", "deep_green"):
        return 3
    if signal == "yellow":
        return 2
    if signal == "red":
        return 1
    return 0


def _regime_from_signals(index_signals: list[dict[str, Any]]) -> tuple[str, str | None]:
    if len(index_signals) < 2:
        return "Weak", None
    by_name = {str(x.get("name") or x.get("tsCode") or ""): str(x.get("signal") or "") for x in index_signals}
    sse = by_name.get("上证指数") or str(index_signals[0].get("signal") or "")
    cyb = by_name.get("创业板指") or str(index_signals[1].get("signal") or "")
    g1 = sse in ("green", "light_green", "deep_green")
    g2 = cyb in ("green", "light_green", "deep_green")
    if g1 and g2:
        return "Strong", None
    if g1 or g2:
        r1 = _signal_rank(sse)
        r2 = _signal_rank(cyb)
        if r1 == r2:
            return "Diverging", "mixed"
        return ("Diverging", "sse_stronger") if r1 > r2 else ("Diverging", "cyb_stronger")
    return "Weak", None


def get_market_regime(
    *,
    as_of_date: str | None = None,
    include_breadth: bool = True,
) -> dict[str, Any]:
    """
    Return market regime derived from index traffic lights.

    When include_breadth is False, skips the all-market breadth scan (use on TrendOK hot path).
    Results are cached in-process for REGIME_CACHE_TTL_SECONDS per (as_of_date, include_breadth).
    """
    key = (str(as_of_date or "").strip(), bool(include_breadth))
    cached = _regime_cache.get(key)
    if cached is not None:
        return cached

    signals = get_index_signals(as_of_date=as_of_date, include_breadth=include_breadth)
    regime, bias = _regime_from_signals(signals)
    result: dict[str, Any] = {"regime": regime, "bias": bias, "indexSignals": signals}
    _regime_cache[key] = result
    return result
