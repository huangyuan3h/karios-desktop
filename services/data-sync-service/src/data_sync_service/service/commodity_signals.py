"""Commodity sleeve signals: gold / oil / bond (daily, 10d horizon).

Findings from G1 scan 2026-08-23 (880b 2023-01~2026-08):
- Gold 518880: trend MA20>MA60 20d excess +0.54% win72%; RSI<30 +0.72% win75% -> BUY on oversold or trend hold
- Oil QDII 513350: MA10 above -0.32% (bad), RSI<30 +2.74% win86% -> mean-reversion BUY on RSI<30, SELL on above MA10
- Bond 511260: all excess ~0 -> hold as cash, no tactical signal (carry only)

This module only hints, never trades. Reuses daily table (fund_daily backfilled 2023-01).
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.daily import fetch_last_bars

logger = logging.getLogger(__name__)

COMMODITY_MAP = {
    "GOLD": {"ts": "518880.SH", "symbol": "ETF:518880", "name": "华安黄金ETF"},
    "OIL_QDII": {"ts": "513350.SH", "symbol": "ETF:513350", "name": "富国油气QDII"},
    "OIL_QDII2": {"ts": "159518.SZ", "symbol": "ETF:159518", "name": "嘉实油气QDII"},
    "BOND10Y": {"ts": "511260.SH", "symbol": "ETF:511260", "name": "10年国债ETF"},
    "BOND5Y": {"ts": "511010.SH", "symbol": "ETF:511010", "name": "5年国债ETF"},
}

def _closes(ts: str, days: int = 250) -> list[float]:
    try:
        bars = fetch_last_bars(ts, days=days)
    except Exception:
        return []
    out = []
    for b in bars:
        try:
            c = float(b.get("close"))
            if c > 0:
                out.append(c)
        except:  # noqa
            pass
    return out

def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    import numpy as np
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    # Wilder smoothing simplified with rolling mean
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))

def _ma(closes: list[float], n: int) -> float | None:
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n

def signal_for(ts: str) -> dict[str, Any]:
    closes = _closes(ts, 250)
    if len(closes) < 60:
        return {"ts": ts, "ok": False, "n": len(closes), "note": "insufficient bars"}
    close = closes[-1]
    ma10 = _ma(closes, 10)
    ma20 = _ma(closes, 20)
    ma60 = _ma(closes, 60)
    ma200 = _ma(closes, 200)
    rsi14 = _rsi(closes, 14)
    # generic signals
    above_ma10 = close > ma10 if ma10 else False
    ma20_gt_ma60 = (ma20 > ma60) if (ma20 and ma60) else False
    info = {
        "ts": ts, "close": round(close, 3),
        "ma10": round(ma10,3) if ma10 else None,
        "ma20": round(ma20,3) if ma20 else None,
        "ma60": round(ma60,3) if ma60 else None,
        "ma200": round(ma200,3) if ma200 else None,
        "rsi14": round(rsi14,1) if rsi14 else None,
        "above_ma10": above_ma10,
        "ma20_gt_ma60": ma20_gt_ma60,
        "ok": True,
    }
    # asset-specific hint
    action = "HOLD"
    reason = ""
    if ts in ("518880.SH","518800.SH","159934.SZ"):  # gold
        if rsi14 is not None and rsi14 < 30:
            action, reason = "BUY", f"RSI {rsi14:.1f}<30 超卖反弹（G1 +0.72% excess）"
        elif ma20_gt_ma60:
            action, reason = "HOLD", "MA20>MA60 趋势持有（20d +0.54%）"
        elif not above_ma10:
            action, reason = "HOLD", "跌破MA10但未到超卖，观望"
        else:
            action, reason = "DONT_BUY", "无趋势/无超卖，不追高"
    elif ts in ("513350.SH","159518.SZ"):  # oil QDII mean reversion
        if rsi14 is not None and rsi14 < 30:
            action, reason = "BUY", f"RSI {rsi14:.1f}<30 油价超卖反弹（G1 +2.7% excess win86%）"
        elif above_ma10:
            action, reason = "SELL", "站上MA10（G1 above -0.32% 弱），不追高/止盈"
        else:
            action, reason = "HOLD", "在MA10下，等待RSI<30"
    elif ts in ("511260.SH","511010.SH"):  # bond
        # bond no 10d edge, keep as cash proxy
        action, reason = "HOLD", "债ETF 年化3.7% vol2.5% carry，无10日择时"
    else:
        action, reason = "HOLD", ""
    info["action"] = action
    info["reason"] = reason
    return info

def all_signals() -> dict[str, Any]:
    out = {}
    for key, meta in COMMODITY_MAP.items():
        ts = meta["ts"]
        sig = signal_for(ts)
        sig.update(meta)
        out[key] = sig
    # summary
    return {"as_of": "today", "signals": out}
