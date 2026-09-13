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

from datetime import date, timedelta
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
    ma_window_by_key: dict[str, int] | None = None,
    etf_mom_floor: float | None = None,
) -> dict[str, Any]:
    """Replay 择强单轨 NAV (absolute) + daily rows for UI.

    navSingle = 100% to pick (STOCK basket avg ret / ETF ret / 0 for REPO).
    navBase = 100% stock basket when any position, else 0 (fused baseline).
    ETF legs: peak since entry −trail_pct% → REPO (same day earns repo).

    TIP-016 E (2026-09-09, pre-registered): absolute-strength floor on the
    ETF leg — when set and the winning candidate is an ETF below the floor
    (mom60 fraction), fall to REPO instead of 100%-switching into the
    least-weak name. None = off (frozen).
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
        for k, _ts in MULTI_TS.items():
            mp = etf_close.get(k) or {}
            if prev not in mp:
                continue
            days_k = sorted(mp.keys())
            try:
                pi = days_k.index(prev)
            except ValueError:
                continue
            ma_w = (ma_window_by_key or {}).get(k, ma_window)
            if pi < max(lookback, ma_w) - 1:
                continue
            ma = sum(mp[days_k[j]] for j in range(pi - ma_w + 1, pi + 1)) / ma_w
            if mp[prev] < ma:
                continue
            ago = mp[days_k[pi - lookback]]
            candidates[k] = mp[prev] / ago - 1.0 if ago else -1e9

        pick = max(candidates, key=lambda kk: candidates[kk]) if candidates else "REPO"

        # TIP-016 E (pre-registered): absolute-strength floor on the ETF leg.
        if etf_mom_floor is not None and pick not in ("STOCK", "REPO"):
            if candidates.get(pick, 0.0) < etf_mom_floor:
                pick = "REPO"

        # ETF trail8: peak since consecutive hold of same ETF −trail% → REPO.
        # OPT-177: decide from the PREVIOUS close (causal) — using today's close
        # to zero out today's return was a 1-day look-ahead.
        if trail_pct > 0 and pick not in ("STOCK", "REPO"):
            mp = etf_close.get(pick) or {}
            close = mp.get(prev)
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

        pick_ts = (
            "STOCK_BASKET"
            if pick == "STOCK"
            else ("GC001" if pick == "REPO" else MULTI_TS.get(pick, ""))
        )

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


def _pair_nav_to_calendar(
    nav: list[float] | None,
    own_cal: list[str] | None,
    calendar: list[str],
) -> list[float] | None:
    """Pair an engine nav_curve (len = len(own_cal)+1) onto the merged calendar.

    Missing days forward-fill the last own value (ret 0 that day, e.g. CN
    closed while HK traded). The terminal forced-close point (nav_curve[-1])
    binds to the own calendar's last day — same convention as
    scripts/run_walk_forward_dual.nav_curve_on_calendar.
    """
    if not nav or not own_cal:
        return None
    n = min(len(nav), len(own_cal))
    by_day = {str(d): float(nav[i]) for i, d in enumerate(own_cal[:n])}
    if len(nav) > len(own_cal) and own_cal:
        by_day[str(own_cal[-1])] = float(nav[-1])
    out: list[float] = []
    last = float(nav[0])
    for d in calendar:
        v = by_day.get(str(d))
        if v is not None:
            last = v
        out.append(last)
    return out


def _circuit_flags_by_day(
    trades: list[Any] | None,
    calendar: list[str],
    threshold: float = -25.0,
    window_days: int = 30,
    min_trades: int = 3,
) -> dict[str, bool]:
    """Mirror the engine's realized-drawdown circuit state per day (TIP-016 B).

    Same formula as backtest_engine._circuit_halted: trades closed within the
    last ``window_days`` natural days, >= ``min_trades`` of them, realized
    pnl_pct simple-sum <= ``threshold`` -> circuit ON. Display-only (the
    posture band); the engine state remains authoritative.
    """
    closes = sorted((str(t.close_date or ""), float(t.pnl_pct or 0.0)) for t in (trades or []))
    out: dict[str, bool] = {}
    for day in calendar:
        try:
            cutoff = (date.fromisoformat(day) - timedelta(days=window_days)).isoformat()
        except ValueError:
            out[day] = False
            continue
        recent = [p for d, p in closes if cutoff <= d <= day]
        out[day] = len(recent) >= min_trades and sum(recent) <= threshold
    return out


def build_twin_star_timeline(
    *,
    core_rows: list[dict[str, Any]],
    core_summary: dict[str, Any],
    sat_rows: list[dict[str, Any]],
    core_weight: float = 0.5,
    sat_weight: float = 0.5,
    opportunity: bool = True,
    sat_blotter: list[dict[str, Any]] | None = None,
    sim_nav_cn: list[float] | None = None,
    sim_cal_cn: list[str] | None = None,
    sim_nav_hk: list[float] | None = None,
    sim_cal_hk: list[str] | None = None,
    cn_trades: list[Any] | None = None,
    hk_trades: list[Any] | None = None,
    sentiment_by_day: dict[str, str] | None = None,
    flow_by_day: dict[str, dict[str, float | None]] | None = None,
) -> dict[str, Any]:
    """Blend 择强单轨 (core) + S-gap 卫星 (sat) into 机会双子星 (Opportunity Twin-Star) rows.

    Opportunity mode (default, frozen 2026-09-01 v3):
    satellite capital follows the core when idle; on days the satellite occupied
    capital (overnight hold OR exit-at-close — see satActive), its return
    replaces the core return for that sat_weight slice:
        opp_ret = core_ret if not sat_active else core_ret + sat_weight*(sat_ret - core_ret)
    Satellite fills: skip_t1_limit + pool_mode=strict (do not refill from worse
    S-gap ranks). Exit days must stay active so round-trip costs enter the blend.

    Fixed 50/50 daily-return blending (opportunity=False) is kept for audit.

    OPT-152 实盘口径 (2026-09-09): when the S-3 sim NAV curves are supplied,
    rows also carry navSim/navSimMulti — the product-structured curve. The
    navSingle replay assumes 100% of capital on the daily pick; the product
    instead earns what the S-3 sim actually earns on STOCK days (real
    position sizing, gated entries, costs — e.g. a single-stock pump day at
    deployedPct=10 adds ~4.5pt, not +45). ETF/REPO picks keep the replay
    return (择强 100% 硬切 IS the product behaviour on those legs). The
    satellite opportunity blend (idle 100% core / active 50/50) is applied on
    top of the product core exactly like on the benchmark core.

    TIP-016 posture annotation (2026-09-09): when line trades are supplied,
    rows carry cnCircuit/hkCircuit (the engine's realized 30-day circuit
    state, mirrored formula) and sentiment (CN risk mode) per day — the
    timeline chart renders them as posture bands ("why idle / why bleeding").
    """
    sat_by_day = {r["date"]: r for r in sat_rows}
    nav = 1.0
    peak = 1.0
    max_dd = 0.0
    prev_core = 1.0
    prev_sat = 1.0
    last_sat_row: dict[str, Any] | None = None
    blended: list[dict[str, Any]] = []

    calendar = [str(r["date"]) for r in core_rows]
    sim_cn = _pair_nav_to_calendar(sim_nav_cn, sim_cal_cn, calendar)
    sim_hk = _pair_nav_to_calendar(sim_nav_hk, sim_cal_hk, calendar)
    has_sim = sim_cn is not None
    circuit_cn = _circuit_flags_by_day(cn_trades, calendar) if cn_trades else {}
    circuit_hk = _circuit_flags_by_day(hk_trades, calendar) if hk_trades else {}
    sent_map = sentiment_by_day or {}
    flow_map = flow_by_day or {}
    nav_sim = 1.0
    nav_sim_multi = 1.0
    sim_peak = 1.0
    sim_max_dd = 0.0
    sim_multi_peak = 1.0
    sim_multi_max_dd = 0.0
    for idx, r in enumerate(core_rows):
        day = r["date"]
        sat_r = sat_by_day.get(day) or last_sat_row
        last_sat_row = sat_r or last_sat_row
        if sat_r is None:
            core_nav = float(r.get("navSingle") or 1.0)
            blended.append(
                {
                    **r,
                    "coreNav": round(core_nav, 6),
                    "coreNavReturnPct": round((core_nav - 1) * 100, 2),
                    "satNav": None,
                    "satNavReturnPct": None,
                    "satPositions": None,
                    "satSlots": None,
                    "satActive": None,
                    "gapCount": None,
                    "strictCount": None,
                    "skipT1Count": None,
                    "filledToday": None,
                    "idleSlots": None,
                    "gateOpen": None,
                    "cnCircuit": bool(circuit_cn.get(day, False)),
                    "hkCircuit": bool(circuit_hk.get(day, False)),
                    "sentiment": sent_map.get(day),
                    "flow": flow_map.get(day),
                    "navSim": None,
                    "navSimReturnPct": None,
                    "navSimMulti": None,
                    "navSimMultiReturnPct": None,
                }
            )
            continue
        sat_nav = float(sat_r["satNav"])
        core_nav = float(r["navSingle"])
        core_ret = core_nav / prev_core - 1 if prev_core > 0 else 0.0
        sat_ret = sat_nav / prev_sat - 1 if prev_sat > 0 else 0.0
        prev_core = core_nav
        prev_sat = sat_nav
        core_nav_pct = round((core_nav - 1) * 100, 2)

        # OPT-152: product core return for the day. STOCK pick → S-3 sim joint
        # NAV return (CN+HK 50/50 daily-rebalanced); ETF/REPO → replay return.
        if has_sim and str(r.get("pick")) == "STOCK":
            prev_cn = sim_cn[idx - 1] if idx > 0 else sim_cn[0]
            ret_cn = sim_cn[idx] / prev_cn - 1.0 if prev_cn > 0 else 0.0
            if sim_hk is not None:
                prev_hk = sim_hk[idx - 1] if idx > 0 else sim_hk[0]
                ret_hk = sim_hk[idx] / prev_hk - 1.0 if prev_hk > 0 else 0.0
                sim_core_ret = 0.5 * ret_cn + 0.5 * ret_hk
            else:
                sim_core_ret = ret_cn
        else:
            sim_core_ret = None

        if "satActive" in sat_r and sat_r["satActive"] is not None:
            has_sat = bool(sat_r["satActive"])
        else:
            # Legacy rows without satActive: overnight positions only (understates costs).
            has_sat = int(sat_r.get("satPositions") or 0) > 0
        if opportunity:
            ret = core_ret + sat_weight * (sat_ret - core_ret) if has_sat else core_ret
        else:
            ret = core_weight * core_ret + sat_weight * sat_ret
        nav *= 1.0 + ret
        peak = max(peak, nav)
        if peak > 0:
            max_dd = max(max_dd, (peak - nav) / peak)

        if has_sim:
            sim_ret = sim_core_ret if sim_core_ret is not None else core_ret
            sim_multi_ret = sim_ret + sat_weight * (sat_ret - sim_ret) if has_sat else sim_ret
            nav_sim *= 1.0 + sim_ret
            nav_sim_multi *= 1.0 + sim_multi_ret
            sim_peak = max(sim_peak, nav_sim)
            sim_multi_peak = max(sim_multi_peak, nav_sim_multi)
            if sim_peak > 0:
                sim_max_dd = max(sim_max_dd, (sim_peak - nav_sim) / sim_peak)
            if sim_multi_peak > 0:
                sim_multi_max_dd = max(
                    sim_multi_max_dd, (sim_multi_peak - nav_sim_multi) / sim_multi_peak
                )
        sat_pos = int(sat_r.get("satPositions") or 0)
        sat_slots = int(sat_r.get("satSlots") or sat_pos)
        blended.append(
            {
                **r,
                "navSingle": round(nav, 6),
                "navMulti": round(nav, 6),
                "navSingleReturnPct": round((nav - 1) * 100, 2),
                "navMultiReturnPct": round((nav - 1) * 100, 2),
                "coreNav": round(core_nav, 6),
                "coreNavReturnPct": core_nav_pct,
                "satNav": round(sat_nav, 6),
                "satNavReturnPct": round((sat_nav - 1) * 100, 2),
                "satPositions": sat_pos,
                "satSlots": sat_slots,
                "satActive": has_sat,
                "gapCount": sat_r.get("gapCount"),
                "strictCount": sat_r.get("strictCount"),
                "skipT1Count": sat_r.get("skipT1Count"),
                "filledToday": sat_r.get("filledToday"),
                "idleSlots": sat_r.get("idleSlots"),
                "gateOpen": sat_r.get("gateOpen"),
                "cnCircuit": bool(circuit_cn.get(day, False)),
                "hkCircuit": bool(circuit_hk.get(day, False)),
                "sentiment": sent_map.get(day),
                "flow": flow_map.get(day),
                "navSim": round(nav_sim, 6) if has_sim else None,
                "navSimReturnPct": round((nav_sim - 1) * 100, 2) if has_sim else None,
                "navSimMulti": round(nav_sim_multi, 6) if has_sim else None,
                "navSimMultiReturnPct": round((nav_sim_multi - 1) * 100, 2) if has_sim else None,
            }
        )
    sat_active_days = sum(1 for row in blended if row.get("satActive"))
    return {
        "ok": True,
        "mode": "opportunity_twin_star",
        "strategy": "机会双子星 (Opportunity Twin-Star)",
        "coreMode": MODE,
        "coreWeight": core_weight,
        "satWeight": sat_weight,
        "opportunity": bool(opportunity),
        "rows": blended,
        "blotter": list(sat_blotter or []),
        "summary": {
            "fusedPct": round((nav - 1) * 100, 2),
            "corePct": round((core_summary.get("fusedPct") or 0.0), 2),
            "basePct": round(core_summary.get("basePct") or 0.0, 2),
            "maxDdFusedPct": round(max_dd * 100, 1),
            "satActiveDays": sat_active_days,
            # OPT-152 product-structured curve (实盘口径).
            "simPct": round((nav_sim - 1) * 100, 2) if has_sim else None,
            "simMultiPct": round((nav_sim_multi - 1) * 100, 2) if has_sim else None,
            "simMaxDdPct": round(sim_max_dd * 100, 1) if has_sim else None,
            "simMultiMaxDdPct": round(sim_multi_max_dd * 100, 1) if has_sim else None,
        },
    }
