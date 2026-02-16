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


def _compute_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        sell_ok = closes[-1] < ema20 * 0.98 or macd_last < 0.0

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
            r["action"] = "buy_add"
            r["reason"] = "breakout"
            r["targetPct"] = round(float(base_target), 4)
        else:
            r["action"] = "hold"
            r["reason"] = "no_action"
            r["targetPct"] = round(float(r.get("currentPct") or 0.0), 4)

    return out


def compute_watchlist_momentum_alerts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _compute_rows(items)


def compute_watchlist_momentum_plan(items: list[dict[str, Any]]) -> dict[str, Any]:
    rows = _compute_rows(items)
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
