from __future__ import annotations

from typing import Any

from datetime import datetime
from zoneinfo import ZoneInfo

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.service.market_quotes import symbol_to_ts_code
from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.realtime_quote import fetch_realtime_quotes
from data_sync_service.service.trendok import _ema, _macd, _rsi


def _safe_float(val: Any) -> float | None:
    try:
        if val is None:
            return None
        num = float(val)
        return num if num == num else None
    except Exception:
        return None


def _get_regime(as_of_date: str | None) -> str:
    try:
        info = get_market_regime(as_of_date=as_of_date or "")
        return str(info.get("regime") or "Weak")
    except Exception:
        return "Weak"


def _regime_target(regime: str) -> float:
    if regime == "Strong":
        return 0.25
    if regime == "Diverging":
        return 0.15
    return 0.05


def _next_tranche(current_pct: float, base_target: float) -> float:
    if base_target <= 0:
        return 0.0
    step = base_target / 3.0
    if current_pct < step * 0.9:
        return step
    if current_pct < step * 1.9:
        return step * 2.0
    return base_target


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def _quote_trade_date(q: dict[str, Any]) -> str | None:
    tt = str(q.get("trade_time") or "").strip()
    if not tt:
        return None
    if len(tt) >= 10 and tt[4] == "-" and tt[7] == "-":
        return tt[:10]
    if len(tt) >= 8 and tt[:8].isdigit():
        return f"{tt[:4]}-{tt[4:6]}-{tt[6:8]}"
    return None


def _merge_realtime_bar(
    bars: list[tuple[str, str, str, str, str, str]],
    quote: dict[str, Any],
) -> list[tuple[str, str, str, str, str, str]]:
    if not bars:
        return bars
    price = _safe_float(quote.get("price"))
    if price is None:
        return bars
    date = _quote_trade_date(quote) or _shanghai_today_iso()
    last = bars[-1]
    last_date = str(last[0])
    close_s = str(price)
    open_s = str(quote.get("open") or close_s)
    high_s = str(quote.get("high") or close_s)
    low_s = str(quote.get("low") or close_s)
    vol_s = str(quote.get("volume") or last[5])
    next_bar = (date, open_s, high_s, low_s, close_s, vol_s)
    if date == last_date:
        return [*bars[:-1], next_bar]
    if date > last_date:
        return [*bars, next_bar]
    return bars


def _compute_rows(items: list[dict[str, Any]], realtime: bool) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    ts_codes: list[str] = []
    for it in items or []:
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym:
            continue
        ts_code = symbol_to_ts_code(sym)
        cleaned.append(
            {
                "symbol": sym,
                "ts_code": ts_code,
                "position_pct": it.get("position_pct"),
                "entry_price": it.get("entry_price"),
                "max_price": it.get("max_price"),
            }
        )
        if ts_code:
            ts_codes.append(ts_code)

    if not cleaned:
        return []

    bars_by_code = fetch_last_ohlcv_batch(ts_codes, days=120)
    if realtime and ts_codes:
        quotes = fetch_realtime_quotes(ts_codes)
        items_q = quotes.get("items") if isinstance(quotes, dict) else None
        if quotes.get("ok") and isinstance(items_q, list):
            by_code = {str(x.get("ts_code")): x for x in items_q if x and x.get("ts_code")}
            for code, bars in list(bars_by_code.items()):
                qt = by_code.get(code)
                if qt:
                    bars_by_code[code] = _merge_realtime_bar(bars, qt)
    out: list[dict[str, Any]] = []
    regime_by_symbol: dict[str, str] = {}
    for it in cleaned:
        sym = it["symbol"]
        ts_code = it["ts_code"]
        position_pct = _safe_float(it.get("position_pct")) or 0.0
        position_pct = max(0.0, min(1.0, position_pct))
        entry_price = _safe_float(it.get("entry_price"))
        max_price = _safe_float(it.get("max_price"))

        if not ts_code:
            out.append({"symbol": sym, "missingData": ["unsupported_market"]})
            continue

        bars = bars_by_code.get(ts_code, [])
        if not bars:
            out.append({"symbol": sym, "missingData": ["no_bars"]})
            continue

        dates: list[str] = []
        closes: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        vols: list[float] = []
        for d, open_s, high_s, low_s, close_s, vol_s in bars:
            c = _safe_float(close_s)
            h = _safe_float(high_s)
            l = _safe_float(low_s)
            v = _safe_float(vol_s)
            if c is None:
                continue
            closes.append(c)
            highs.append(h if h is not None else c)
            lows.append(l if l is not None else c)
            vols.append(v if v is not None else 0.0)
            dates.append(str(d))

        if len(closes) < 30:
            out.append({"symbol": sym, "missingData": ["bars_lt_60"]})
            continue

        ema20 = _ema(closes, 20)[-1]
        ema30 = _ema(closes, 30)[-1]
        macd_line, _signal, hist = _macd(closes)
        macd_last = macd_line[-1] if macd_line else 0.0
        hist_last = hist[-1] if hist else 0.0
        rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0
        high20 = max(highs[-20:])

        vol_short = sum(vols[-20:]) / 20.0 if len(vols) >= 20 else 0.0
        vol_long = vol_short
        vol_ok = vol_long > 0 and vols[-1] > vol_long * 1.2

        breakout_ok = (
            closes[-1] >= 0.99 * high20
            and ema20 > ema30
            and hist_last > 0.0
            and 55.0 <= rsi14 <= 82.0
            and vol_ok
        )
        trailing_stop = None
        if entry_price and entry_price > 0:
            if not max_price or max_price <= 0:
                max_price = closes[-1]
            trailing_stop = max_price * (1.0 - 0.10)
        stop_ok = trailing_stop is not None and closes[-1] <= trailing_stop
        trend_broken = closes[-1] < ema20 * 0.98 or macd_last < 0.0
        sell_ok = bool(stop_ok or trend_broken)

        regime = _get_regime(dates[-1] if dates else None)
        regime_by_symbol[sym] = regime

        out.append(
            {
                "symbol": sym,
                "asOfDate": dates[-1] if dates else None,
                "regime": regime,
                "currentPct": round(position_pct, 4),
                "breakoutOk": bool(breakout_ok),
                "sellOk": bool(sell_ok),
            }
        )

    for r in out:
        sym = str(r.get("symbol") or "")
        regime = regime_by_symbol.get(sym, "Weak")
        base_target = _regime_target(regime)
        if r.get("sellOk"):
            r["action"] = "exit"
            r["reason"] = "trend_weak"
            r["targetPct"] = 0.0
            continue
        if r.get("breakoutOk"):
            current_pct = float(r.get("currentPct") or 0.0)
            next_target = _next_tranche(current_pct, base_target)
            if next_target > current_pct:
                r["action"] = "buy_add"
                r["reason"] = "breakout"
                r["targetPct"] = round(float(next_target), 4)
            else:
                r["action"] = "hold"
                r["reason"] = "no_action"
                r["targetPct"] = round(float(current_pct), 4)
        else:
            r["action"] = "hold"
            r["reason"] = "no_action"
            r["targetPct"] = round(float(r.get("currentPct") or 0.0), 4)

    return out


def compute_watchlist_momentum_alerts(items: list[dict[str, Any]], realtime: bool = False) -> list[dict[str, Any]]:
    return _compute_rows(items, bool(realtime))
