from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db.index_daily import fetch_last_closes, fetch_last_closes_upto
from data_sync_service.service.realtime_quote import fetch_realtime_quotes

INDEX_SIGNALS = [
    {"ts_code": "000001.SH", "name": "上证指数"},
    {"ts_code": "399006.SZ", "name": "创业板指"},
]


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


def get_index_signals(*, as_of_date: str | None = None) -> list[dict[str, Any]]:
    """
    Return index traffic-light signals using MA20/MA5.
    During trading hours, try to use realtime quotes from tushare.
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

    out: list[dict[str, Any]] = []
    for it in INDEX_SIGNALS:
        ts_code = it["ts_code"]
        name = it["name"]
        if use_as_of:
            series = fetch_last_closes_upto(ts_code, use_as_of, days=30)
        else:
            series = fetch_last_closes(ts_code, days=30)
        used_realtime = False
        trade_time = rt_time.get(ts_code)
        rt_close = rt_price.get(ts_code)
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

        if len(series) < 21:
            out.append(
                {
                    "tsCode": ts_code,
                    "name": name,
                    "asOfDate": series[-1][0] if series else None,
                    "close": series[-1][1] if series else None,
                    "ma5": None,
                    "ma20": None,
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
        close = closes[-1]
        signal = "yellow"
        position = "40%-50%"
        rules: list[str] = []

        if close > ma20 and ma20 > ma20_prev:
            signal = "green"
            position = "80%-100%"
            rules.append("close>MA20 && MA20 up")
        elif close < ma20 and ma20 < ma20_prev:
            signal = "red"
            position = "0%-20%"
            rules.append("close<MA20 && MA20 down")
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
    if signal == "green":
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
    g1 = sse == "green"
    g2 = cyb == "green"
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
