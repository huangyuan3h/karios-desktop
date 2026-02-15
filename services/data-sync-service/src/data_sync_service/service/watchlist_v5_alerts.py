from __future__ import annotations

from typing import Any

from data_sync_service.db.daily import fetch_last_ohlcv_batch
from data_sync_service.service.market_quotes import symbol_to_ts_code
from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi


def _safe_float(val: Any) -> float | None:
    try:
        if val is None:
            return None
        num = float(val)
        return num if num == num else None
    except Exception:
        return None


def _clamp(val: float, lo: float, hi: float) -> float:
    return lo if val < lo else hi if val > hi else val


def _next_tranche(current_pct: float, base_target: float) -> float:
    if base_target <= 0:
        return 0.0
    step = base_target / 3.0
    if current_pct < step:
        return step
    if current_pct < 2 * step:
        return 2 * step
    return base_target


def _get_regime(as_of_date: str | None) -> str:
    try:
        info = get_market_regime(as_of_date=as_of_date or "")
        return str(info.get("regime") or "Weak")
    except Exception:
        return "Weak"


def _regime_target(regime: str) -> float:
    if regime == "Strong":
        return 1.0
    if regime == "Diverging":
        return 0.66
    if regime == "Weak":
        return 0.3
    return 0.0


def compute_watchlist_v5_alerts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Compute V5-style alerts for watchlist items with optional position percentage.
    This mirrors the core logic of watchlist_trend_v5 without entry-price stop.
    """
    cleaned: list[dict[str, Any]] = []
    ts_codes: list[str] = []
    for it in items or []:
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym:
            continue
        ts_code = symbol_to_ts_code(sym)
        cleaned.append({"symbol": sym, "ts_code": ts_code, "position_pct": it.get("position_pct")})
        if ts_code:
            ts_codes.append(ts_code)

    if not cleaned:
        return []

    bars_by_code = fetch_last_ohlcv_batch(ts_codes, days=120)
    out: list[dict[str, Any]] = []
    for it in cleaned:
        sym = it["symbol"]
        ts_code = it["ts_code"]
        position_pct = _safe_float(it.get("position_pct")) or 0.0
        position_pct = _clamp(position_pct, 0.0, 1.0)

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
        for d, open_s, high_s, low_s, close_s, _vol_s in bars:
            c = _safe_float(close_s)
            h = _safe_float(high_s)
            l = _safe_float(low_s)
            if c is None:
                continue
            closes.append(c)
            highs.append(h if h is not None else c)
            lows.append(l if l is not None else c)
            dates.append(str(d))

        if len(closes) < 30:
            out.append({"symbol": sym, "missingData": ["bars_lt_30"]})
            continue

        ema20_series = _ema(closes, 20)
        ema30_series = _ema(closes, 30)
        if not ema20_series or not ema30_series:
            out.append({"symbol": sym, "missingData": ["ema_unavailable"]})
            continue

        ema20 = ema20_series[-1]
        ema30 = ema30_series[-1]
        ema20_up = len(ema20_series) > 1 and ema20_series[-1] >= ema20_series[-2]
        ema30_up = len(ema30_series) > 1 and ema30_series[-1] >= ema30_series[-2]

        macd_line, _signal, hist = _macd(closes)
        macd_last = macd_line[-1] if macd_line else 0.0
        hist_last = hist[-1] if hist else 0.0
        hist_prev = hist[-2] if len(hist) > 1 else hist_last

        rsi14 = _rsi(closes, 14)
        rsi_last = rsi14[-1] if rsi14 else 50.0

        high20 = max(highs[-20:])
        close = closes[-1]
        breakout_ok = (
            close >= 0.98 * high20
            and ema20 > ema30
            and ema20_up
            and ema30_up
            and macd_last > 0.0
            and hist_last > 0.0
            and hist_last >= hist_prev
            and 58.0 <= rsi_last <= 85.0
        )

        sell_ok = close < ema20 * 0.97 or ema20 < ema30 or macd_last < 0.0
        regime = _get_regime(dates[-1] if dates else None)
        base_target = _regime_target(regime)

        action = "hold"
        reason = "no_action"
        target_pct: float | None = None
        if sell_ok:
            action = "exit"
            target_pct = 0.0
            reason = "trend_weak"
        elif breakout_ok:
            next_target = _next_tranche(position_pct, base_target)
            if next_target > position_pct:
                action = "buy_add"
                target_pct = next_target
                reason = "breakout_tranche"
        else:
            if base_target < 0.66 and position_pct > base_target:
                action = "trim"
                target_pct = base_target
                reason = "trim_weak"

        out.append(
            {
                "symbol": sym,
                "asOfDate": dates[-1] if dates else None,
                "regime": regime,
                "currentPct": round(position_pct, 4),
                "baseTarget": round(base_target, 4),
                "targetPct": round(target_pct, 4) if isinstance(target_pct, float) else None,
                "action": action,
                "reason": reason,
                "breakoutOk": bool(breakout_ok),
                "sellOk": bool(sell_ok),
                "ema20": round(ema20, 6),
                "ema30": round(ema30, 6),
                "macd": round(macd_last, 6),
                "rsi14": round(rsi_last, 3),
                "high20": round(high20, 6),
            }
        )
    return out
