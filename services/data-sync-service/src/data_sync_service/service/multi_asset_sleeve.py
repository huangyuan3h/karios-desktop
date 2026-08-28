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

import numpy as np

from data_sync_service.db.daily import fetch_last_bars

logger = logging.getLogger(__name__)

CANDIDATES = [
    {"key": "GOLD", "ts": "518880.SH", "symbol": "ETF:518880", "name": "华安黄金ETF"},
    {"key": "OIL", "ts": "513350.SH", "symbol": "ETF:513350", "name": "富国油气QDII"},
    {"key": "NASDAQ", "ts": "513110.SH", "symbol": "ETF:513110", "name": "华泰柏瑞纳指100QDII"},
    {"key": "NASDAQ", "ts": "513100.SH", "symbol": "ETF:513100", "name": "广发纳指100QDII"},
    {"key": "BOND10", "ts": "511260.SH", "symbol": "ETF:511260", "name": "10年国债ETF"},
]

# NASDAQ has two tradable aliases (513110/513100) – keep both, dedupe by key in _pick.
MULTI_ASSET_SYMBOLS = {c["symbol"] for c in CANDIDATES}
MULTI_ASSET_TS_CODES = {c["ts"] for c in CANDIDATES}

def is_multi_asset_symbol(symbol: str) -> bool:
    sym = str(symbol or "").upper()
    # also treat ts_code with .SH/.SZ as symbol; accept both 513110/513100 as NASDAQ
    if sym in MULTI_ASSET_SYMBOLS or sym in MULTI_ASSET_TS_CODES:
        return True
    bare = sym.replace(".SH","").replace(".SZ","").replace("ETF:","")
    return bare in {s.replace("ETF:","") for s in MULTI_ASSET_SYMBOLS} or bare in {"513110","513100","513500"}

def _etf_market_data(ts: str) -> dict[str, Any]:
    """Fetch ETF bars and compute close/MA200 for holding display."""
    try:
        closes = _closes(ts, 260)
        if len(closes) < MA_WINDOW:
            return {"ok": False, "n": len(closes)}
        ma200 = sum(closes[-MA_WINDOW:]) / MA_WINDOW
        close = closes[-1]
        return {"ok": True, "close": close, "ma200": ma200, "above": close >= ma200}
    except Exception:
        return {"ok": False, "n": 0}

LOOKBACK = 60
MA_WINDOW = 200
COST = 0.0005
MIN_IDLE_PCT = 20.0
MIN_HOLD_DAYS = 5  # 2026-08-24 固化：最少持有5天防抖动，三窗 OOS2+17.4/train+19.3/valid+0.1 全过 vs 每天切+7.1
TRAILING_PCT = 8.0  # 2026-08-28 固化：峰值 -8% 硬切 GC001，长窗 +32.4pt DD13.7→11.2（sleeve-exit-study）

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
    NASDAQ aliases (513110/513100) are merged by best mom/price.
    """
    # Group closes by key (NASDAQ has two aliases, keep best liquidity/mom)
    raw_map: dict[str, list[list[float]]] = {}
    for c in CANDIDATES:
        closes = _closes(c["ts"], 260)
        if len(closes) < MA_WINDOW+LOOKBACK:
            logger.warning("multi-sleeve %s %s insufficient bars %s", c["key"], c["ts"], len(closes))
            continue
        raw_map.setdefault(c["key"], []).append(closes)
    closes_map: dict[str, list[float]] = {}
    for key, lists in raw_map.items():
        if not lists:
            return None
        # pick alias with highest mom proxy (latest close / close 60 ago) for ranking
        best = max(lists, key=lambda lst: lst[-1]/lst[-LOOKBACK] if lst[-LOOKBACK]!=0 else -1)
        closes_map[key] = best
    if any(k not in closes_map for k in ["GOLD","OIL","NASDAQ","BOND10"]):
        # allow missing if at least 3 present; log but don't hard fail
        if len(closes_map) < 3:
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

def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains=0.0
    losses=0.0
    for i in range(1, period+1):
        d=closes[-i] - closes[-i-1]
        if d>0: gains+=d
        else: losses+=-d
    if losses==0:
        return 100.0
    rs=(gains/period)/(losses/period)
    return 100 - 100/(1+rs)

PULSE_STATS = {
    "oil_rsi80": {"n":35, "mean":3.85, "median":4.70, "win":82.9, "vs_nas_mean":2.13, "recent_nas_mean":-1.06},
    "nas_mom20_neg5": {"n":63, "mean":4.66, "win":71.4, "vs_nas_mean":0.59},
    "oil_vol_low": {"n":129, "mean_gn":2.46, "win":65.1},
}

def build_pulse_hints(*, day: str | None = None) -> list[dict[str, Any]]:
    """Observation layer for §22.7 R1-R5 (no position change, hint only).

    Returns 3 hints with active flag for today (t-1 close, no lookahead):
    - R4 oil RSI>80 -> gold>oil +3.85% win82.9% (but gold>nas -1% recent -> vs sleeve≈0)
    - R2 nas mom20<-5% -> gold>oil +4.66% win71.4%
    - R3 oil vol20 low20% -> gold>nas +2.46% win65.1% (most stable vs sleeve)
    """
    from datetime import date as _date
    d = day or _date.today().isoformat()
    hints: list[dict[str, Any]] = []
    try:
        closes_gold=_closes("518880.SH", 260)
        closes_oil=_closes("513350.SH", 260)
        closes_nas=_closes("513100.SH", 260)
        # R4 oil RSI>80
        rsi_oil=_rsi(closes_oil[:-1]) if len(closes_oil)>=15 else None  # t-1
        active_rsi = rsi_oil is not None and rsi_oil > 80
        hints.append({
            "id":"R4_oil_rsi80", "label":"油超买 RSI>80",
            "active": bool(active_rsi), "value": round(rsi_oil,1) if rsi_oil is not None else None, "threshold":80,
            "stats": PULSE_STATS["oil_rsi80"],
            "note": "金强于油 +3.85% win82.9% n35，但金>纳指 +2.1%全期/-1.06%近期，vs sleeve≈0（纳指强势年）",
            "action":"观察：若sleeve持有OIL，可考虑切GOLD；持有NASDAQ则不切",
        })
        # R2 nas mom20<-5%
        mom_nas=None
        if len(closes_nas)>=21:
            mom_nas=closes_nas[-2]/closes_nas[-22]-1 if closes_nas[-22]!=0 else None
        active_nas = mom_nas is not None and mom_nas < -0.05
        hints.append({
            "id":"R2_nas_mom20_neg5", "label":"纳指弱势 mom20<-5%",
            "active": bool(active_nas), "value": round(mom_nas*100,2) if mom_nas is not None else None, "threshold":-5.0,
            "stats": PULSE_STATS["nas_mom20_neg5"],
            "note": "金强于油 +4.66% win71.4% n63，但金>纳指 +0.59% vs sleeve≈0",
            "action":"观察：纳指弱时金相对油安全",
        })
        # R3 oil vol low20% (approx threshold 0.0126 from 2023-11+ distribution)
        vol_oil=None
        if len(closes_oil)>=21:
            rets=[closes_oil[i]/closes_oil[i-1]-1 for i in range(len(closes_oil)-20, len(closes_oil)-1)]
            vol_oil=float(np.std(rets)) if rets else None
        active_vol = vol_oil is not None and vol_oil < 0.0126
        hints.append({
            "id":"R3_oil_vol_low", "label":"油低波 vol20<20%分位",
            "active": bool(active_vol), "value": round(vol_oil,4) if vol_oil is not None else None, "threshold":0.0126,
            "stats": PULSE_STATS["oil_vol_low"],
            "note": "金强于纳指 +2.46% win65.1% n129（双期稳定 early+2.26%/recent+2.75%），唯一金>纳指>2%天平",
            "action":"观察：油低波时金相对纳指 +2.5%",
        })
    except Exception as exc:
        logger.warning("pulse hints failed: %s", exc)
    return hints

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
    # if holding, check if still top? If not top, suggest rotation (with 5-day min hold防抖)
    if held:
        held_sym=str(held.get("symbol") or "").upper()
        if held_sym != pick["symbol"]:
            # min-hold check: if held <5 days and still above MA200, keep it
            try:
                from datetime import date as _date
                entry=str(held.get("entryDate") or held.get("entry_date") or "")
                if entry:
                    held_days=(_date.fromisoformat(day) - _date.fromisoformat(entry[:10])).days
                    if held_days < MIN_HOLD_DAYS:
                        # check held still above MA200
                        held_ts=str(held.get("ts_code") or held_sym.replace("ETF:","")+".SH")
                        md=_etf_market_data(held_ts)
                        if md.get("ok") and md.get("above"):
                            out.update({"active": True, "action": "HOLD", "message": f"持有 {held_sym}（已持{held_days}天<5天防抖，虽非最强但仍站上200日线）", "label": "持有（防抖）"})
                            return out
            except Exception:
                pass
            out.update({"active": True, "action": "ROTATE", "message": f"轮动：卖出 {held_sym} → 买入 {pick['symbol']} ({pick['name']} mom60 {pick['mom60']}%)", "label": f"轮动至 {pick['key']}"})
            return out
        # holding is still pick, check trailing -8% then MA200 break (trail8 固化 2026-08-28)
        # peak = max close since entry (incl. current close)
        try:
            held_ts = str(held.get("ts_code") or held_sym.replace("ETF:","")+".SH")
            entry = str(held.get("entryDate") or held.get("entry_date") or "")
            if entry:
                bars = fetch_last_bars(held_ts, days=500)
                peak = 0.0
                cur_close = 0.0
                for b in bars:
                    d = str(b.get("trade_date") or b.get("date") or "")
                    if d < entry[:10]:
                        continue
                    c = float(b.get("close") or 0)
                    if c > peak:
                        peak = c
                    if d == day:
                        cur_close = c
                    elif not cur_close and d > day:
                        break
                # fallback: use pick close if bars missing
                if peak > 0 and cur_close > 0 and cur_close < peak * (1 - TRAILING_PCT/100):
                    out.update({"active": True, "action": "SELL_TO_REPO", "message": f"{held_sym}峰值回撤{((peak-cur_close)/peak*100):.1f}% ≥{TRAILING_PCT:.0f}% → 卖出转逆回购（硬切）", "label": "卖出转repo(止损)"})
                    return out
        except Exception as exc:
            logger.warning("multi-sleeve trailing check failed: %s", exc)
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
