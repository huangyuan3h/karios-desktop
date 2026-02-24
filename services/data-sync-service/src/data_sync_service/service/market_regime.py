from __future__ import annotations

import math
import time
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.db.index_daily import fetch_last_closes_vol, fetch_last_closes_vol_upto
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.db.stock_basic import fetch_ts_codes
from data_sync_service.service.realtime_quote import fetch_realtime_quotes

INDEX_SIGNALS = [
    {"ts_code": "000001.SH", "name": "上证指数"},
    {"ts_code": "399006.SZ", "name": "创业板指"},
]

HISTORY_DAYS = 80
BREADTH_MIN_RATIO = 0.5


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
    Sync window includes trading time and lunch break.
    """
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    in_lunch = minutes > 11 * 60 + 30 and minutes < 13 * 60
    return _is_shanghai_trading_time_at(now) or in_lunch


def _is_shanghai_trading_time() -> bool:
    return _is_shanghai_trading_time_at(datetime.now(tz=ZoneInfo("Asia/Shanghai")))


def _is_shanghai_sync_window() -> bool:
    return _is_shanghai_sync_window_at(datetime.now(tz=ZoneInfo("Asia/Shanghai")))


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


def _get_breadth_above_ma20_ratio(*, as_of_date: str | None = None) -> dict[str, Any]:
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
        for i in range(0, len(ts_codes), 50):
            part = [c for c in ts_codes[i : i + 50] if c in ma20_by_code]
            if not part:
                continue
            r = fetch_realtime_quotes(part)
            if isinstance(r, dict) and bool(r.get("ok")):
                for it in r.get("items", []) or []:
                    code = str(it.get("ts_code") or "").strip()
                    if not code:
                        continue
                    _, price = _realtime_pct_or_price(it)
                    if price is not None:
                        rt_price[code] = price
            time.sleep(0.08)

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


def get_index_signals(*, as_of_date: str | None = None) -> list[dict[str, Any]]:
    """
    Return index traffic-light signals using MA20/MA5/MA60, 3-day confirmation,
    volume expansion, and breadth gating.
    """
    use_as_of = str(as_of_date).strip() if as_of_date else None
    rt_price: dict[str, float] = {}
    rt_time: dict[str, str | None] = {}
    if _is_shanghai_sync_window() and not use_as_of:
        res = fetch_realtime_quotes([x["ts_code"] for x in INDEX_SIGNALS])
        if isinstance(res, dict) and bool(res.get("ok")):
            for it in res.get("items", []) or []:
                ts_code = str(it.get("ts_code") or "").strip()
                if not ts_code:
                    continue
                price = _safe_float(it.get("price"))
                if price is None:
                    continue
                rt_price[ts_code] = price
                rt_time[ts_code] = it.get("trade_time")

    breadth = _get_breadth_above_ma20_ratio(as_of_date=use_as_of)
    breadth_ok = breadth["ratio"] >= BREADTH_MIN_RATIO

    out: list[dict[str, Any]] = []
    for it in INDEX_SIGNALS:
        ts_code = it["ts_code"]
        name = it["name"]
        if use_as_of:
            series_cv = fetch_last_closes_vol_upto(ts_code, use_as_of, days=HISTORY_DAYS)
            series = [(d, c) for d, c, _ in series_cv]
            series_vol = [v for _, _, v in series_cv]
        else:
            series_cv = fetch_last_closes_vol(ts_code, days=HISTORY_DAYS)
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
            out.append(
                {
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
                }
            )
            continue

        closes = [c for _, c in series]
        ma5 = sum(closes[-5:]) / 5.0
        ma20 = sum(closes[-20:]) / 20.0
        ma20_prev = sum(closes[-21:-1]) / 20.0
        ma60 = sum(closes[-60:]) / 60.0 if len(closes) >= 60 else None
        close = closes[-1]

        ma20_3d_ago = sum(closes[-23:-3]) / 20.0
        ma20_2d_ago = sum(closes[-22:-2]) / 20.0
        ma20_1d_ago = sum(closes[-21:-1]) / 20.0
        ma20_today = ma20
        confirm_3day = (
            closes[-3] > ma20_3d_ago and closes[-2] > ma20_2d_ago and closes[-1] > ma20_today
        )

        ma20_slope_up = ma20 > ma20_prev
        ma5_above_ma20 = ma5 > ma20
        ma5_above_ma60 = ma60 is not None and ma5 > ma60
        ma20_above_ma60 = ma60 is not None and ma20 > ma60
        ma_full_bull = ma5_above_ma20 and ma20_above_ma60

        vol_ok = False
        if len(series_vol) >= 20:
            avg_vol_5 = sum(series_vol[-5:]) / 5.0
            avg_vol_20 = sum(series_vol[-20:]) / 20.0
            vol_inc_3 = (
                series_vol[-1] > series_vol[-2]
                and series_vol[-2] > series_vol[-3]
                if len(series_vol) >= 3
                else False
            )
            vol_ok = avg_vol_5 > avg_vol_20 and vol_inc_3

        signal = "yellow"
        position = "30%-40%"
        rules: list[str] = []

        if close < ma20 and ma20 < ma20_prev:
            signal = "red"
            position = "0%-20%"
            rules.append("close<MA20 && MA20 down")
        elif close > ma20:
            if not confirm_3day:
                signal = "yellow"
                position = "30%-40%"
                rules.append("break above MA20 first day (3d confirm pending)")
            elif not ma20_slope_up or not ma5_above_ma20:
                signal = "yellow"
                position = "30%-40%"
                if not ma20_slope_up:
                    rules.append("MA20 not yet turning up")
                if not ma5_above_ma20:
                    rules.append("MA5<=MA20")
            elif ma_full_bull and vol_ok:
                signal = "deep_green"
                position = "80%-100%"
                rules.append("MA5>MA20>MA60 && volume expansion")
            else:
                signal = "light_green"
                position = "60%-70%"
                rules.append("3d confirm && MA20 up && MA5>MA20")

            if signal in ("light_green", "deep_green") and not breadth_ok:
                signal = "yellow"
                position = "30%-40%"
                rules.append("breadth<50% (structural market)")
        else:
            if close < ma5 and close >= ma20:
                rules.append("close<MA5 but hold MA20")
            elif abs(close - ma20) / ma20 <= 0.01:
                rules.append("close near MA20")
            else:
                rules.append("range/sideways")

        out.append(
            {
                "tsCode": ts_code,
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
                "source": "tushare.realtime_quote" if used_realtime else "db.index_daily",
            }
        )
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


def get_market_regime(*, as_of_date: str | None = None) -> dict[str, Any]:
    """
    Return market regime derived from index traffic lights.
    """
    signals = get_index_signals(as_of_date=as_of_date)
    regime, bias = _regime_from_signals(signals)
    return {"regime": regime, "bias": bias, "indexSignals": signals}
