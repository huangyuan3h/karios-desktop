# ruff: noqa: E701,E722
"""Multi-asset sleeve: who is strong buy who (replaces single NASDAQ sleeve).

Candidates (all tradable in CN stock account, fund_daily 2023-01):
- GOLD 518880.SH 华安黄金
- OIL 513350.SH 富国油气QDII (proxy for crude)
- NASDAQ 513100.SH 纳指100
- BOND 511260.SH 10年国债 (low-vol ballast, rarely top but keeps sharpe)

Rule (G2 prototype 2026-08-23, 661d 2023-11~2026-08):
  lookback = 60d return (mom60) > 20d, filtered by price > MA200 (avoid falling knife)
  daily pick = argmax mom60 among above-MA200 candidates, traded next day, 0.05% cost
  Prototype: mom60+MA200 ann31.2% vol30 sharpe1.03 vs fixed NASDAQ 10.9% / equal4 19.9% sharpe1.51
  => rotation beats single asset, equal weight best sharpe. We keep rotation as tactical sleeve,
     equal4 as strategic note.

States: same as third_asset_sleeve but etf is dynamic (GOLD/OIL/NASDAQ/BOND).
"""
from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.daily import fetch_last_bars

logger = logging.getLogger(__name__)

CANDIDATES = [
    {"key": "GOLD", "ts": "518880.SH", "symbol": "ETF:518880", "name": "华安黄金ETF"},
    {"key": "OIL", "ts": "513350.SH", "symbol": "ETF:513350", "name": "富国油气QDII"},
    {"key": "NASDAQ", "ts": "513100.SH", "symbol": "ETF:513100", "name": "纳指100"},
    {"key": "BOND10", "ts": "511260.SH", "symbol": "ETF:511260", "name": "10年国债ETF"},
]

LOOKBACK = 60
MA_WINDOW = 200
COST = 0.0005
MIN_IDLE_PCT = 20.0

def _closes(ts: str, days: int = 260) -> list[float]:
    try:
        bars = fetch_last_bars(ts, days=days)
    except Exception:
        return []
    out=[]
    for b in bars:
        try:
            c=float(b.get("close"))
            if c>0: out.append(c)
        except: pass
    return out

def _pick() -> dict[str, Any] | None:
    """Return today's pick: Nasdaq-first, weak -> rotate to strongest.

    Rule (optimized 2026-08-24): Nasdaq is default when above MA200 and mom60>0
    and rank <=1 (top2) among above-MA200 candidates. Otherwise pick max mom60
    among above-MA200. This matches walk-forward OOS2+19.3/train+17.9/valid+14.4
    all-positive, vs pure max_mom valid -1.0 and pure Nasdaq-first valid -25.4.
    """
    closes_map={}
    for c in CANDIDATES:
        closes_map[c["key"]] = _closes(c["ts"], 260)
        if len(closes_map[c["key"]]) < MA_WINDOW+LOOKBACK:
            logger.warning("multi-sleeve %s insufficient bars %s", c["key"], len(closes_map[c["key"]]))
            return None
    # compute mom60 and MA200 at t-1
    mom={}
    above={}
    for c in CANDIDATES:
        closes = closes_map[c["key"]]
        # t-1 values (exclude last close to avoid lookahead)
        closes_t1 = closes[:-1]
        if len(closes_t1) < MA_WINDOW:
            continue
        ma200 = sum(closes_t1[-MA_WINDOW:])/MA_WINDOW
        close_t1 = closes_t1[-1]
        mom60 = close_t1 / closes_t1[-LOOKBACK] -1 if closes_t1[-LOOKBACK]!=0 else -1e9
        mom[c["key"]] = mom60
        above[c["key"]] = close_t1 >= ma200
    filtered = {k: v for k,v in mom.items() if above.get(k)}
    if not filtered:
        return None
    # Nasdaq-first, weak -> rotate
    nasdaq_key = "NASDAQ"
    if above.get(nasdaq_key) and mom.get(nasdaq_key, -1) > 0:
        # rank check
        sorted_mom = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
        rank = [k for k,_ in sorted_mom].index(nasdaq_key) if nasdaq_key in filtered else 99
        if rank <= 1:
            pick_key = nasdaq_key
            pick = next(x for x in CANDIDATES if x["key"]==pick_key)
            closes_pick = closes_map[pick_key]
            return {
                "key": pick_key, "ts": pick["ts"], "symbol": pick["symbol"], "name": pick["name"],
                "mom60": round(filtered[pick_key]*100,2),
                "close": round(closes_pick[-1],3),
                "ma200": round(sum(closes_pick[-MA_WINDOW:])/MA_WINDOW,3),
                "above_ma200": above[pick_key],
                "all_mom": {k: round(v*100,2) for k,v in mom.items()},
                "all_above": above,
            }
    pick_key = max(filtered, key=lambda k: filtered[k])
    pick = next(x for x in CANDIDATES if x["key"]==pick_key)
    # current price for display
    closes_pick = closes_map[pick_key]
    return {
        "key": pick_key, "ts": pick["ts"], "symbol": pick["symbol"], "name": pick["name"],
        "mom60": round(filtered[pick_key]*100,2),
        "close": round(closes_pick[-1],3),
        "ma200": round(sum(closes_pick[-MA_WINDOW:])/MA_WINDOW,3),
        "above_ma200": above[pick_key],
        "all_mom": {k: round(v*100,2) for k,v in mom.items()},
        "all_above": above,
    }

def _idle_pct(holdings: list[dict[str, Any]]) -> float:
    deployed=0.0
    for h in holdings:
        try: deployed+=float(h.get("positionPct", h.get("sleeve_pct") or 0))
        except: pass
    return max(0.0, 100-min(deployed,100))

def build_multi_asset_sleeve(*, day: str, cn_block: dict[str, Any], holdings_override=None) -> dict[str, Any]:
    holdings = holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    idle = _idle_pct(holdings)
    # market state: same as third_asset_sleeve - s3 buy setup forces sell to A-share
    regime = cn_block.get("regime")
    panic = bool((cn_block.get("panicCooldown") or {}).get("active"))
    circuit = bool(cn_block.get("circuitBlocked"))
    cands = cn_block.get("s3Candidates") or []
    gate_open = regime in ("Strong","Diverging") and not panic and not circuit
    s3_buy_setup = gate_open and len(cands)>0

    # find held sleeve etf (any candidate)
    held=None
    for h in holdings:
        sym=str(h.get("symbol") or "").upper()
        ts=str(h.get("ts_code") or "").upper()
        for c in CANDIDATES:
            if sym==c["symbol"] or ts==c["ts"]:
                held=h
                break
    pick = _pick()
    out: dict[str, Any] = {"active": False, "action": "NONE", "idlePct": round(idle,1), "s3BuySetup": s3_buy_setup}
    if pick is None:
        out["note"]="候选数据不足 260 根"
        return out
    out.update({"pick": pick, "holding": bool(held)})

    if s3_buy_setup:
        out.update({"active": True, "action": "SELL_TO_A_SHARE", "message": f"A股有买点（{regime} {len(cands)}候选）→ 卖出 {pick['symbol']} 回 A 股", "label": "卖出转A股"})
        return out
    # if holding, check if still top? If not top, suggest rotation
    if held:
        held_sym=str(held.get("symbol") or "").upper()
        if held_sym != pick["symbol"]:
            out.update({"active": True, "action": "ROTATE", "message": f"轮动：卖出 {held_sym} → 买入 {pick['symbol']} ({pick['name']} mom60 {pick['mom60']}%)", "label": f"轮动至 {pick['key']}"})
            return out
        # holding is still pick, check MA200 break
        if not pick["above_ma200"]:
            out.update({"active": True, "action": "SELL_TO_REPO", "message": f"{pick['symbol']} 跌破200日线 → 卖出转逆回购", "label": "卖出转repo"})
            return out
        out.update({"active": True, "action": "HOLD", "message": f"持有 {pick['symbol']}（mom60 {pick['mom60']}% 全市场最强且站上200日线）", "label": "持有"})
        return out
    # not holding
    if not pick["above_ma200"]:
        out.update({"active": True, "action": "DONT_BUY", "message": f"{pick['symbol']} 虽最强但已破200日线，不买", "label": "今日不买"})
        return out
    if idle >= MIN_IDLE_PCT:
        out.update({"active": True, "action": "BUY", "message": f"闲置 {idle:.0f}% 且 {pick['symbol']} 最强（mom60 {pick['mom60']}%）→ 建议买入", "label": f"买入 {pick['key']}"})
        return out
    out.update({"active": True, "action": "DONT_BUY", "message": f"{pick['symbol']} 最强但资金已部署 闲置{idle:.0f}% → 不买", "label": "不买"})
    return out
