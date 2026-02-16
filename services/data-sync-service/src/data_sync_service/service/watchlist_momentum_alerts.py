from __future__ import annotations

from typing import Any
import math

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


def _clip01(val: float) -> float:
    if val <= 0:
        return 0.0
    if val >= 1:
        return 1.0
    return val


def _calc_volatility(closes: list[float], window: int = 20) -> float:
    if len(closes) < window + 1:
        return 0.0
    rets: list[float] = []
    for i in range(-window, 0):
        c0 = closes[i - 1]
        c1 = closes[i]
        if c0 > 0:
            rets.append((c1 / c0) - 1.0)
    if len(rets) < 3:
        return 0.0
    mean = sum(rets) / float(len(rets))
    var = sum((r - mean) ** 2 for r in rets) / float(len(rets))
    return var ** 0.5


def _linear_fit_slope_r2(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n < 2:
        return 0.0, 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / float(n)
    sxy = 0.0
    sxx = 0.0
    for i, y in enumerate(values):
        dx = i - x_mean
        dy = y - y_mean
        sxy += dx * dy
        sxx += dx * dx
    if sxx == 0:
        return 0.0, 0.0
    slope = sxy / sxx
    ss_tot = sum((y - y_mean) ** 2 for y in values)
    ss_res = sum((values[i] - (slope * (i - x_mean) + y_mean)) ** 2 for i in range(n))
    r2 = 0.0 if ss_tot == 0 else max(0.0, 1.0 - ss_res / ss_tot)
    return slope, r2


def _quality_momentum(closes: list[float], window: int = 20) -> float:
    if len(closes) < window:
        return 0.0
    span = closes[-window:]
    if any(c <= 0 for c in span):
        return 0.0
    log_prices = [math.log(c) for c in span]
    slope, r2 = _linear_fit_slope_r2(log_prices)
    annualized = (math.exp(slope) ** 252) - 1.0
    return annualized * r2


def _get_regime(as_of_date: str | None) -> str:
    try:
        info = get_market_regime(as_of_date=as_of_date or "")
        return str(info.get("regime") or "Weak")
    except Exception:
        return "Weak"


def _regime_target(regime: str, weak_target: float) -> float:
    if regime == "Strong":
        return 1.0
    if regime == "Diverging":
        return 0.66
    if regime == "Weak":
        return weak_target
    return 0.0


def _rank_targets(codes: list[str], base_target: float, weights: list[float]) -> dict[str, float]:
    if not codes or base_target <= 0:
        return {}
    weights = weights[: len(codes)]
    total = sum(weights) if weights else 0.0
    if total <= 0:
        total = float(len(codes))
        weights = [1.0] * len(codes)
    scale = base_target / total
    return {code: weight * scale for code, weight in zip(codes, weights, strict=False)}


def _compute_rows(items: list[dict[str, Any]], max_positions: int, weak_target: float) -> list[dict[str, Any]]:
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
    scored: list[tuple[str, float]] = []
    regime_by_symbol: dict[str, str] = {}
    for it in cleaned:
        sym = it["symbol"]
        ts_code = it["ts_code"]
        position_pct = _safe_float(it.get("position_pct")) or 0.0
        position_pct = max(0.0, min(1.0, position_pct))

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

        if len(closes) < 60:
            out.append({"symbol": sym, "missingData": ["bars_lt_60"]})
            continue

        ema20 = _ema(closes, 20)[-1]
        ema60 = _ema(closes, 60)[-1]
        macd_line, _signal, hist = _macd(closes)
        macd_last = macd_line[-1] if macd_line else 0.0
        hist_last = hist[-1] if hist else 0.0
        rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0
        high20 = max(highs[-20:])

        trend_ok = ema20 > ema60 and macd_last > 0.0 and 50.0 <= rsi14 <= 85.0
        breakout_ok = closes[-1] >= 0.98 * high20 and trend_ok and hist_last > 0.0
        sell_ok = closes[-1] < ema20 * 0.97 or ema20 < ema60 or macd_last < 0.0
        if breakout_ok:
            score = _quality_momentum(closes, 20)
            scored.append((sym, score))

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
                "score": round(score, 6) if breakout_ok else None,
            }
        )

    scored.sort(key=lambda x: (-x[1], x[0]))
    selected = [code for code, _score in scored[: max_positions]]
    weights = [0.4, 0.3, 0.2, 0.1]
    if max_positions < len(weights):
        weights = weights[:max_positions]

    for r in out:
        sym = str(r.get("symbol") or "")
        regime = regime_by_symbol.get(sym, "Weak")
        base_target = _regime_target(regime, weak_target)
        target_by_code = _rank_targets(selected, base_target, weights)
        if r.get("sellOk"):
            r["action"] = "exit"
            r["reason"] = "trend_weak"
            r["targetPct"] = 0.0
            continue
        if sym in target_by_code:
            r["action"] = "buy_add" if r.get("breakoutOk") else "hold"
            r["reason"] = "momentum_rank"
            r["targetPct"] = round(float(target_by_code[sym]), 4)
        else:
            r["action"] = "rank_out" if (r.get("currentPct") or 0) > 0 else "hold"
            r["reason"] = "rank_out"
            r["targetPct"] = 0.0

    return out


def compute_watchlist_momentum_alerts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _compute_rows(items, max_positions=4, weak_target=1.0)


def compute_watchlist_momentum_plan(items: list[dict[str, Any]]) -> dict[str, Any]:
    rows = _compute_rows(items, max_positions=4, weak_target=1.0)
    holdings: list[dict[str, Any]] = []
    total_current = 0.0
    total_target = 0.0
    regime_counts: dict[str, int] = {}
    for r in rows:
        if r.get("missingData"):
            continue
        current_pct = float(r.get("currentPct") or 0.0)
        total_current += current_pct
        target_pct = r.get("targetPct")
        if target_pct is None:
            target_pct = current_pct
        total_target += float(target_pct)
        regime = str(r.get("regime") or "")
        if regime:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
        if target_pct and float(target_pct) > 0:
            holdings.append(
                {
                    "symbol": r.get("symbol"),
                    "action": r.get("action"),
                    "currentPct": round(current_pct, 4),
                    "targetPct": round(float(target_pct), 4),
                    "reason": r.get("reason"),
                }
            )
    holdings.sort(key=lambda x: (-float(x.get("targetPct") or 0.0), str(x.get("symbol") or "")))
    dominant_regime = None
    if regime_counts:
        dominant_regime = max(regime_counts.items(), key=lambda x: x[1])[0]
    return {
        "summary": {
            "regime": dominant_regime,
            "totalCurrentPct": round(total_current, 4),
            "totalTargetPct": round(total_target, 4),
        },
        "holdings": holdings,
        "rows": rows,
    }
