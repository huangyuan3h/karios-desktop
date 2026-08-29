"""择强单轨 (Pick-Strong Track) — single source of truth for mom_compare.

Canonical product strategy: equal-asset pool STOCK ∪ {GOLD,OIL,NASDAQ,BOND10},
t-1 mom60, ETF must be ≥ MA200, argmax → 100% hard switch, else REPO.

Used by:
- GET /api/backtest/timeline
- scripts/fused_timeline_walk.py / pick_strong_grid.py (should call here)
- live multi_asset_sleeve (ETF+_STOCK merge; keep messaging aligned)

Truth doc: docs/modules/pick-strong-track.md
"""
from __future__ import annotations

from typing import Any

import psycopg

from data_sync_service.config import get_settings

# Single NASDAQ series (matches fused_timeline_walk / hardening grid).
MULTI_TS: dict[str, str] = {
    "GOLD": "518880.SH",
    "OIL": "513350.SH",
    "NASDAQ": "513110.SH",
    "BOND10": "511260.SH",
}

LOOKBACK = 60
MA_WINDOW = 200
MODE = "mom_compare"
# Absorbed into fused NAV 2026-08-29 (Q8 / pick_strong_trail8 report).
TRAILING_PCT = 8.0


def fetch_etf_closes() -> dict[str, dict[str, float]]:
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    out: dict[str, dict[str, float]] = {}
    for key, ts in MULTI_TS.items():
        cur.execute(
            "select trade_date, close from daily where ts_code=%s order by trade_date",
            (ts,),
        )
        out[key] = {str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}
    conn.close()
    return out


def build_mom_compare_timeline(
    *,
    calendar: list[str],
    positions_by_day: list[dict[str, Any]],
    close_by_ts_day: dict[str, dict[str, float]],
    etf_close: dict[str, dict[str, float]] | None = None,
    lookback: int = LOOKBACK,
    ma_window: int = MA_WINDOW,
    trail_pct: float = TRAILING_PCT,
) -> dict[str, Any]:
    """Replay 择强单轨 NAV (absolute) + daily rows for UI.

    navSingle = 100% to pick (STOCK basket avg ret / ETF ret / 0 for REPO).
    navBase = 100% stock basket when any position, else 0 (fused baseline).
    ETF legs: peak since entry −trail_pct% → REPO (same day earns repo).
    """
    etf_close = etf_close or fetch_etf_closes()
    snap_by_day = {str(s.get("date")): s for s in positions_by_day}
    ts_days = {ts: sorted(mp.keys()) for ts, mp in close_by_ts_day.items()}

    etf_ret: dict[str, dict[str, float]] = {}
    for k, mp in etf_close.items():
        days = sorted(mp.keys())
        ret: dict[str, float] = {}
        for i in range(1, len(days)):
            d, prev = days[i], days[i - 1]
            if mp[prev] != 0:
                ret[d] = mp[d] / mp[prev] - 1.0
        etf_ret[k] = ret

    def mom_at(ts: str, prev_day: str) -> float | None:
        mp = close_by_ts_day.get(ts)
        days = ts_days.get(ts)
        if not mp or not days:
            return None
        try:
            pi = days.index(prev_day)
        except ValueError:
            return None
        if pi < lookback:
            return None
        prev_c, ago = mp.get(prev_day), mp.get(days[pi - lookback])
        if not prev_c or not ago:
            return None
        return prev_c / ago - 1.0

    nav_single = 1.0
    nav_base = 1.0
    peak = 1.0
    max_dd = 0.0
    rows: list[dict[str, Any]] = []
    prev_syms: set[str] = set()
    prev_map: dict[str, str] = {}
    held_etf: str | None = None
    etf_peak = 0.0
    trail_exits = 0

    for idx, day in enumerate(calendar):
        if idx == 0:
            continue
        prev = calendar[idx - 1]
        stock_poses: list[dict[str, Any]] = []
        snap_prev = snap_by_day.get(prev)
        if snap_prev:
            for pos in snap_prev.get("positions") or []:
                entry = str(pos.get("entry_date") or "")
                if entry and day <= entry:
                    continue
                stock_poses.append(pos)

        stock_rets: list[float] = []
        stock_moms: list[float] = []
        stock_syms: list[str] = []
        cn_cnt = hk_cnt = 0
        for pos in stock_poses:
            ts = str(pos.get("ts_code") or "")
            closes = close_by_ts_day.get(ts) or {}
            today_c, prev_c = closes.get(day), closes.get(prev)
            if today_c and prev_c and prev_c != 0:
                stock_rets.append(today_c / prev_c - 1.0)
            m = mom_at(ts, prev)
            if m is not None:
                stock_moms.append(m)
            sym = str(pos.get("symbol") or ts)
            if len(stock_syms) < 3:
                stock_syms.append(sym)
            if ts.endswith(".HK") or ts.startswith("HK"):
                hk_cnt += 1
            else:
                cn_cnt += 1
        stock_ret = sum(stock_rets) / len(stock_rets) if stock_rets else 0.0
        stock_mom = sum(stock_moms) / len(stock_moms) if stock_moms else -1e9

        candidates: dict[str, float] = {}
        if stock_poses:
            candidates["STOCK"] = stock_mom
        for k, ts in MULTI_TS.items():
            mp = etf_close.get(k) or {}
            if prev not in mp:
                continue
            days_k = sorted(mp.keys())
            try:
                pi = days_k.index(prev)
            except ValueError:
                continue
            if pi < max(lookback, ma_window) - 1:
                continue
            ma = sum(mp[days_k[j]] for j in range(pi - ma_window + 1, pi + 1)) / ma_window
            if mp[prev] < ma:
                continue
            ago = mp[days_k[pi - lookback]]
            candidates[k] = mp[prev] / ago - 1.0 if ago else -1e9

        pick = max(candidates, key=lambda kk: candidates[kk]) if candidates else "REPO"

        # ETF trail8: peak since consecutive hold of same ETF −trail% → REPO.
        if trail_pct > 0 and pick not in ("STOCK", "REPO"):
            mp = etf_close.get(pick) or {}
            close = mp.get(day)
            if held_etf != pick:
                held_etf = pick
                etf_peak = float(close) if close is not None else 0.0
            elif close is not None:
                if etf_peak > 0 and close < etf_peak * (1.0 - trail_pct / 100.0):
                    pick = "REPO"
                    trail_exits += 1
                    held_etf = None
                    etf_peak = 0.0
                else:
                    etf_peak = max(etf_peak, float(close))
        else:
            held_etf = None
            etf_peak = 0.0

        pick_ts = "STOCK_BASKET" if pick == "STOCK" else ("GC001" if pick == "REPO" else MULTI_TS.get(pick, ""))

        if pick == "STOCK":
            single_ret = stock_ret
        elif pick == "REPO":
            single_ret = 0.0
        else:
            single_ret = etf_ret.get(pick, {}).get(day, 0.0)
        base_ret = stock_ret if stock_poses else 0.0

        nav_single *= 1.0 + single_ret
        nav_base *= 1.0 + base_ret
        peak = max(peak, nav_single)
        if peak > 0:
            max_dd = max(max_dd, (peak - nav_single) / peak)

        # exits vs prev calendar snap (display)
        cur_snap = snap_by_day.get(day) or snap_prev
        cur_syms = {
            str(p.get("ts_code") or p.get("symbol") or "")
            for p in ((cur_snap or {}).get("positions") or [])
        }
        cur_syms = {s for s in cur_syms if s}
        sold = sorted(prev_syms - cur_syms)
        sold_labels = [prev_map.get(ts, ts) for ts in sold]
        prev_syms = cur_syms
        prev_map = {}
        if cur_snap:
            for p in cur_snap.get("positions") or []:
                ts = str(p.get("ts_code") or p.get("symbol") or "")
                sym = str(p.get("symbol") or ts)
                if ts:
                    prev_map[ts] = sym

        if cn_cnt and hk_cnt:
            stock_market = "A+H"
        elif hk_cnt:
            stock_market = "HK"
        elif cn_cnt:
            stock_market = "A股"
        else:
            stock_market = "空仓"

        deployed_pct = min(1.0, 0.1 * len(stock_poses)) if stock_poses else 0.0
        rows.append(
            {
                "date": day,
                "deployedPct": round(deployed_pct * 100, 1),
                "idlePct": round(max(0.0, 1.0 - deployed_pct) * 100, 1),
                "positions": len(stock_poses),
                "cnPositions": cn_cnt,
                "hkPositions": hk_cnt,
                "stockMarket": stock_market,
                "stockSymbols": stock_syms,
                "exits": sold_labels,
                "exitsCount": len(sold_labels),
                "pick": pick,
                "pickTs": pick_ts,
                "stockMom": round(stock_mom * 100, 2) if stock_moms else None,
                "navBase": round(nav_base, 6),
                "navSingle": round(nav_single, 6),
                "navMulti": round(nav_single, 6),
                "navBaseReturnPct": round((nav_base - 1) * 100, 2),
                "navSingleReturnPct": round((nav_single - 1) * 100, 2),
                "navMultiReturnPct": round((nav_single - 1) * 100, 2),
            }
        )

    return {
        "ok": True,
        "mode": MODE,
        "strategy": "择强单轨",
        "lookback": lookback,
        "maWindow": ma_window,
        "trailPct": trail_pct,
        "trailExits": trail_exits,
        "rows": rows,
        "summary": {
            "fusedPct": round((nav_single - 1) * 100, 2),
            "basePct": round((nav_base - 1) * 100, 2),
            "maxDdFusedPct": round(max_dd * 100, 1),
        },
    }
