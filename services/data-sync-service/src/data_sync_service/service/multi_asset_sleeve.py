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

States: Harbor parking — park the idle fraction in the mom60+MA200 ETF argmax
(GOLD/OIL/NASDAQ/BOND10); no candidate above MA200 -> REPO.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from data_sync_service.db.daily import fetch_last_bars
from data_sync_service.service.harbor import pick_parking

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
    bare = sym.replace(".SH", "").replace(".SZ", "").replace("ETF:", "")
    return bare in {s.replace("ETF:", "") for s in MULTI_ASSET_SYMBOLS} or bare in {
        "513110",
        "513100",
        "513500",
    }


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
# Harbor ETF risk exit (causal: trigger on the decision print, no same-day credit).
TRAILING_PCT = 8.0


def _etf_trail_exit(held: dict[str, Any], *, day: str) -> dict[str, Any] | None:
    """If held ETF close < peak_since_entry × (1 − TRAILING_PCT%), return SELL_TO_REPO."""
    held_sym = str(held.get("symbol") or "").upper()
    held_ts = str(held.get("ts_code") or held_sym.replace("ETF:", "") + ".SH")
    entry = str(held.get("entryDate") or held.get("entry_date") or "")
    if not entry:
        return None
    try:
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
        if peak > 0 and cur_close > 0 and cur_close < peak * (1 - TRAILING_PCT / 100):
            dd = (peak - cur_close) / peak * 100
            return {
                "active": True,
                "action": "SELL_TO_REPO",
                "message": (f"{held_sym}峰值回撤{dd:.1f}% ≥{TRAILING_PCT:.0f}% → 转逆回购"),
                "label": "卖出转repo(止损)",
            }
    except Exception as exc:
        logger.warning("multi-sleeve trailing check failed: %s", exc)
    return None


def _closes(ts: str, days: int = 260) -> list[float]:
    """Latest closes including today's bar if present (display layer)."""
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
        except:
            pass
    return out


def _signal_closes(ts: str, days: int = 260) -> list[float]:
    """Closes through the latest COMPLETED bar (today's close after the daily sync).

    Harbor clock: signal at T close (job runs 18:20) -> execute T+1 open.
    During the session the daily table has no today bar, so this naturally
    falls back to the previous session.
    """
    from data_sync_service.service.trade_calendar_utils import shanghai_today

    today = shanghai_today().isoformat()
    try:
        bars = fetch_last_bars(ts, days=days + 5)
    except Exception:
        return []
    out = []
    for b in bars:
        d = str(b.get("date") or b.get("trade_date") or "")
        if d > today:
            continue
        try:
            c = float(b.get("close"))
            if c > 0:
                out.append(c)
        except:
            pass
    return out


def _signal_series(ts: str, days: int = 260) -> dict[str, float]:
    """{date: close} through the latest COMPLETED bar (Harbor T-close signal).

    Same source/cut as ``_signal_closes``; the series form lets the shared
    ``harbor.pick_parking`` rule run on the exact same data as the timeline.
    """
    from data_sync_service.service.trade_calendar_utils import shanghai_today

    today = shanghai_today().isoformat()
    try:
        bars = fetch_last_bars(ts, days=days + 5)
    except Exception:
        return {}
    out: dict[str, float] = {}
    for b in bars:
        d = str(b.get("date") or b.get("trade_date") or "")
        if d > today:
            continue
        try:
            c = float(b.get("close"))
        except (TypeError, ValueError):
            continue
        if c > 0:
            out[d] = c
    return out


def _pick() -> dict[str, Any] | None:
    """Today's ETF-leg pick via the shared Harbor rule (single source).

    Delegates to ``harbor.pick_parking`` so the Live decision, the frozen
    backtest (`build_harbor_timeline`) and the evaluation scripts all use ONE
    implementation (mom60 index, MA200 gate, NASDAQ alias selection, coverage).
    """
    etf_close = {c["ts"]: _signal_series(c["ts"], 260) for c in CANDIDATES}
    as_of = max((max(mp) for mp in etf_close.values() if mp), default="")
    if not as_of:
        return None
    return pick_parking(etf_close, as_of)


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[-i] - closes[-i - 1]
        if d > 0:
            gains += d
        else:
            losses += -d
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - 100 / (1 + rs)


PULSE_STATS = {
    "oil_rsi80": {
        "n": 35,
        "mean": 3.85,
        "median": 4.70,
        "win": 82.9,
        "vs_nas_mean": 2.13,
        "recent_nas_mean": -1.06,
    },
    "nas_mom20_neg5": {"n": 63, "mean": 4.66, "win": 71.4, "vs_nas_mean": 0.59},
    "oil_vol_low": {"n": 129, "mean_gn": 2.46, "win": 65.1},
}


def build_pulse_hints(*, day: str | None = None) -> list[dict[str, Any]]:
    """Observation layer for §22.7 R1-R5 (no position change, hint only).

    Returns 3 hints with active flag for today (t-1 close, no lookahead):
    - R4 oil RSI>80 -> gold>oil +3.85% win82.9% (but gold>nas -1% recent -> vs sleeve≈0)
    - R2 nas mom20<-5% -> gold>oil +4.66% win71.4%
    - R3 oil vol20 low20% -> gold>nas +2.46% win65.1% (most stable vs sleeve)
    """
    hints: list[dict[str, Any]] = []
    try:
        closes_oil = _signal_closes("513350.SH", 260)
        closes_nas = _signal_closes("513100.SH", 260)
        # R4 oil RSI>80
        rsi_oil = _rsi(closes_oil) if len(closes_oil) >= 15 else None  # t-1 close
        active_rsi = rsi_oil is not None and rsi_oil > 80
        hints.append(
            {
                "id": "R4_oil_rsi80",
                "label": "油超买 RSI>80",
                "active": bool(active_rsi),
                "value": round(rsi_oil, 1) if rsi_oil is not None else None,
                "threshold": 80,
                "stats": PULSE_STATS["oil_rsi80"],
                "note": "金强于油 +3.85% win82.9% n35，但金>纳指 +2.1%全期/-1.06%近期，vs sleeve≈0（纳指强势年）",
                "action": "观察：若sleeve持有OIL，可考虑切GOLD；持有NASDAQ则不切",
            }
        )
        # R2 nas mom20<-5%
        mom_nas = None
        if len(closes_nas) >= 21:
            mom_nas = closes_nas[-1] / closes_nas[-21] - 1 if closes_nas[-21] != 0 else None
        active_nas = mom_nas is not None and mom_nas < -0.05
        hints.append(
            {
                "id": "R2_nas_mom20_neg5",
                "label": "纳指弱势 mom20<-5%",
                "active": bool(active_nas),
                "value": round(mom_nas * 100, 2) if mom_nas is not None else None,
                "threshold": -5.0,
                "stats": PULSE_STATS["nas_mom20_neg5"],
                "note": "金强于油 +4.66% win71.4% n63，但金>纳指 +0.59% vs sleeve≈0",
                "action": "观察：纳指弱时金相对油安全",
            }
        )
        # R3 oil vol low20% (approx threshold 0.0126 from 2023-11+ distribution)
        vol_oil = None
        if len(closes_oil) >= 21:
            rets = [
                closes_oil[i] / closes_oil[i - 1] - 1
                for i in range(len(closes_oil) - 20, len(closes_oil))
            ]
            vol_oil = float(np.std(rets)) if rets else None
        active_vol = vol_oil is not None and vol_oil < 0.0126
        hints.append(
            {
                "id": "R3_oil_vol_low",
                "label": "油低波 vol20<20%分位",
                "active": bool(active_vol),
                "value": round(vol_oil, 4) if vol_oil is not None else None,
                "threshold": 0.0126,
                "stats": PULSE_STATS["oil_vol_low"],
                "note": "金强于纳指 +2.46% win65.1% n129（双期稳定 early+2.26%/recent+2.75%），唯一金>纳指>2%天平",
                "action": "观察：油低波时金相对纳指 +2.5%",
            }
        )
    except Exception as exc:
        logger.warning("pulse hints failed: %s", exc)
    return hints


def _idle_pct(holdings: list[dict[str, Any]]) -> float:
    deployed = 0.0
    for h in holdings:
        try:
            deployed += float(h.get("positionPct", h.get("sleeve_pct") or 0))
        except:
            pass
    return max(0.0, 100 - min(deployed, 100))


def build_multi_asset_sleeve(
    *, day: str, cn_block: dict[str, Any], holdings_override=None
) -> dict[str, Any]:
    """Live Harbor hint: park idle cash in the mom60+MA200 ETF argmax.

    Rule (frozen by B11, `docs/backtests/stable/etf-parking-baseline-2026-09-13.md`):
    no STOCK gate, no idle floor; no ETF above MA200 -> REPO (exit if held).
    Exit = causal trail8. Position size = idle% (paper sleeve_pct).
    """
    holdings = (
        holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    )
    idle = _idle_pct(holdings)
    regime = cn_block.get("regime")
    panic = bool((cn_block.get("panicCooldown") or {}).get("active"))
    circuit = bool(cn_block.get("circuitBlocked"))
    cands = cn_block.get("s3Candidates") or []
    gate_open = regime in ("Strong", "Diverging") and not panic and not circuit
    s3_buy_setup = gate_open and len(cands) > 0

    held = None
    for h in holdings:
        sym = str(h.get("symbol") or "").upper()
        ts = str(h.get("ts_code") or "").upper()
        for c in CANDIDATES:
            if sym == c["symbol"] or ts == c["ts"]:
                held = h
                break

    etf_pick = _pick()
    out: dict[str, Any] = {
        "active": False,
        "action": "NONE",
        "idlePct": round(idle, 1),
        "s3BuySetup": s3_buy_setup,
        "mode": "harbor",
        "strategy": "港湾",
    }

    # Harbor (P1, B11): park idle cash in the mom60+MA200 argmax ETF —
    # no STOCK gate, no idle floor. No candidate above MA200 -> REPO.
    out.update({"pick": etf_pick, "holding": bool(held), "etfPick": etf_pick})

    if held:
        trail = _etf_trail_exit(held, day=day)
        if trail is not None:
            out.update(trail)
            return out
    held_sym = str((held or {}).get("symbol") or "").upper()
    pick_sym = str((etf_pick or {}).get("symbol") or "").upper()

    if etf_pick is None:
        if held:
            out.update(
                {
                    "active": True,
                    "action": "SELL_TO_REPO",
                    "message": f"全候选跌破200日线 → 卖出 {held_sym} 转逆回购",
                    "label": "卖出转repo",
                }
            )
            return out
        out.update(
            {
                "active": True,
                "action": "DONT_BUY",
                "message": "无 ETF 站上200日线 → 闲置留现金",
                "label": "不买",
            }
        )
        return out

    if held and held_sym == pick_sym:
        out.update(
            {
                "active": True,
                "action": "HOLD",
                "message": f"港湾停车：持有 {etf_pick['symbol']}（mom60 {etf_pick['mom60']}%）",
                "label": "持有",
            }
        )
        return out
    if held:
        out.update(
            {
                "active": True,
                "action": "ROTATE",
                "message": (
                    f"港湾停车轮动：卖出 {held_sym} → {etf_pick['symbol']} "
                    f"({etf_pick['name']} mom60 {etf_pick['mom60']}%)"
                ),
                "label": f"轮动至 {etf_pick['key']}",
            }
        )
        return out
    if idle <= 0:
        out.update(
            {
                "active": True,
                "action": "DONT_BUY",
                "message": "无闲置现金（股票仓已满）",
                "label": "不买",
            }
        )
        return out
    out.update(
        {
            "active": True,
            "action": "BUY",
            "message": (
                f"港湾停车：{etf_pick['symbol']}（{etf_pick['name']} mom60 {etf_pick['mom60']}%）"
                f" · 闲置 {idle:.0f}% → 买入"
            ),
            "label": f"买入 {etf_pick['key']}",
        }
    )
    return out
