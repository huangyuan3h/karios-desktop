"""港湾 (Harbor) baseline: S-3 stock core + idle-cash ETF parking.

Validated in B11 (`docs/backtests/stable/etf-parking-baseline-2026-09-13.md`):
park the whole idle fraction in the mom60+MA200 argmax ETF (GOLD/OIL/NASDAQ/
BOND10), causal trail8 exit, 0.05%/side cost; no idle floor, no STOCK gate.

`build_harbor_timeline` mirrors the row shape of
`pick_strong_track.build_mom_compare_timeline` so the UI chart keeps working:
  navBase   = S-3 engine NAV (stock core alone)
  navSingle = Harbor NAV (engine return + idle * parking return)
Used by GET /api/backtest/timeline?strategy=harbor.
"""

from __future__ import annotations

import bisect
import csv
from pathlib import Path
from typing import Any

LOOKBACK = 60
MA_WINDOW = 200
TRAIL_PCT = 8.0
COST = 0.0005
MODE = "harbor"
STRATEGY_LABEL = "港湾"

MULTI_TS: dict[str, str] = {
    "GOLD": "518880.SH",
    "OIL": "513350.SH",
    "NASDAQ": "513110.SH",
    "BOND10": "511260.SH",
}
NASDAQ_ALIASES = ("513110.SH", "513100.SH")


def load_etf_closes() -> dict[str, dict[str, float]]:
    """Adjusted ETF closes from the research panel; fall back to `daily`."""
    wanted = {*MULTI_TS.values(), *NASDAQ_ALIASES}
    csv_path = Path(__file__).resolve().parents[3] / "data" / "etf" / "etf_daily.csv"
    out: dict[str, dict[str, float]] = {}
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                ts = str(row.get("ts_code") or "")
                if ts not in wanted:
                    continue
                d = str(row.get("trade_date") or "")
                d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                try:
                    c = float(row.get("close_adj") or 0)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    out.setdefault(ts, {})[d] = c
    if out:
        return out
    from data_sync_service.service.pick_strong_track import fetch_etf_closes

    for key, ts in MULTI_TS.items():
        out[ts] = fetch_etf_closes().get(key) or {}
    return out


def build_harbor_timeline(
    *,
    calendar: list[str],
    positions_by_day: list[dict[str, Any]],
    engine_nav_by_day: dict[str, float],
    etf_close: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    """Replay Harbor NAV (engine + idle parking) with UI rows."""
    etf_close = etf_close or load_etf_closes()
    # Engine calendar can carry phantom sessions (holidays) where no ETF bar
    # exists; a decision there would spuriously exit to REPO. Trade only real
    # sessions present in the ETF series (Live has no decision on holidays).
    session_days = {d for mp in etf_close.values() for d in mp}
    calendar = [d for d in calendar if d in session_days]
    snap_by_day = {str(s.get("date")): s for s in positions_by_day}
    days_by_ts = {ts: sorted(mp.keys()) for ts, mp in etf_close.items()}

    def c_at(ts: str, d: str) -> float | None:
        return etf_close.get(ts, {}).get(d)

    def idx_of(ts: str, d: str) -> int | None:
        ds = days_by_ts.get(ts) or []
        i = bisect.bisect_left(ds, d)
        return i if i < len(ds) and ds[i] == d else None

    def ma(ts: str, d: str) -> float | None:
        i = idx_of(ts, d)
        if i is None or i < MA_WINDOW - 1:
            return None
        return sum(etf_close[ts][days_by_ts[ts][j]] for j in range(i - MA_WINDOW + 1, i + 1)) / MA_WINDOW

    def mom(ts: str, d: str) -> float | None:
        i = idx_of(ts, d)
        if i is None or i < LOOKBACK:
            return None
        a = etf_close[ts][days_by_ts[ts][i - LOOKBACK]]
        return etf_close[ts][d] / a - 1.0 if a else None

    def ret(ts: str, d: str, prev: str) -> float:
        c, p = c_at(ts, d), c_at(ts, prev)
        return c / p - 1.0 if c and p else 0.0

    nav_base = 1.0
    nav_harbor = 1.0
    peak = 1.0
    max_dd = 0.0
    held_key: str | None = None
    held_ts: str | None = None
    etf_peak = 0.0
    trail_exits = 0
    trades = 0
    rows: list[dict[str, Any]] = []
    prev_syms: set[str] = set()
    prev_map: dict[str, str] = {}

    for i, day in enumerate(calendar):
        if i == 0:
            continue
        prev = calendar[i - 1]
        snap_prev = snap_by_day.get(prev) or {}
        stock_poses: list[dict[str, Any]] = []
        for pos in snap_prev.get("positions") or []:
            entry = str(pos.get("entry_date") or "")
            if entry and day <= entry:
                continue
            stock_poses.append(pos)

        deployed = sum(float(p.get("position_pct") or 0.0) for p in stock_poses)
        idle = max(0.0, 1.0 - min(1.0, deployed))

        cn_cnt = hk_cnt = 0
        stock_syms: list[str] = []
        for pos in stock_poses:
            ts = str(pos.get("ts_code") or "")
            sym = str(pos.get("symbol") or ts)
            if len(stock_syms) < 3:
                stock_syms.append(sym)
            if ts.endswith(".HK") or ts.startswith("HK"):
                hk_cnt += 1
            else:
                cn_cnt += 1

        pool: dict[str, tuple[float, str]] = {}
        for key, ts in MULTI_TS.items():
            aliases = NASDAQ_ALIASES if key == "NASDAQ" else (ts,)
            best_ts, best_mom = None, -1e9
            for a in aliases:
                m, mm = mom(a, prev), ma(a, prev)
                c = c_at(a, prev)
                if m is None or mm is None or c is None:
                    continue
                if c >= mm and m > best_mom:
                    best_ts, best_mom = a, m
            if best_ts is not None:
                pool[key] = (best_mom, best_ts)
        pick_key = max(pool, key=lambda k: pool[k][0]) if pool else None

        sides = 0
        if held_key != pick_key:
            sides += int(held_key is not None) + int(pick_key is not None)
            held_key = pick_key
            held_ts = pool[pick_key][1] if pick_key is not None else None
            etf_peak = (c_at(held_ts, prev) or 0.0) if held_ts else 0.0
        parking_ret = 0.0
        if held_ts is not None:
            c = c_at(held_ts, prev) or 0.0
            etf_peak = max(etf_peak, c)
            if etf_peak > 0 and c and c < etf_peak * (1.0 - TRAIL_PCT / 100.0):
                held_key = held_ts = None
                etf_peak = 0.0
                trail_exits += 1
                sides += 1
            else:
                parking_ret = ret(held_ts, day, prev)
        if sides:
            trades += 1

        eng_prev = engine_nav_by_day.get(prev)
        eng_day = engine_nav_by_day.get(day)
        r_eng = eng_day / eng_prev - 1.0 if eng_prev and eng_day else 0.0
        nav_base *= 1.0 + r_eng
        nav_harbor *= 1.0 + r_eng + idle * (parking_ret - COST * sides)
        peak = max(peak, nav_harbor)
        max_dd = max(max_dd, (peak - nav_harbor) / peak if peak > 0 else 0.0)

        cur_syms = {
            str(p.get("ts_code") or p.get("symbol") or "")
            for p in ((snap_by_day.get(day) or snap_prev).get("positions") or [])
        }
        cur_syms = {s for s in cur_syms if s}
        sold = sorted(prev_syms - cur_syms)
        sold_labels = [prev_map.get(ts, ts) for ts in sold]
        prev_syms = cur_syms
        prev_map = {}
        for pos in (snap_by_day.get(day) or snap_prev).get("positions") or []:
            ts = str(pos.get("ts_code") or pos.get("symbol") or "")
            if ts:
                prev_map[ts] = str(pos.get("symbol") or ts)

        if cn_cnt and hk_cnt:
            stock_market = "A+H"
        elif hk_cnt:
            stock_market = "HK"
        elif cn_cnt:
            stock_market = "A股"
        else:
            stock_market = "空仓"

        rows.append(
            {
                "date": day,
                "deployedPct": round(min(1.0, deployed) * 100, 1),
                "idlePct": round(idle * 100, 1),
                "positions": len(stock_poses),
                "cnPositions": cn_cnt,
                "hkPositions": hk_cnt,
                "stockMarket": stock_market,
                "stockSymbols": stock_syms,
                "exits": sold_labels,
                "exitsCount": len(sold_labels),
                "pick": held_key or "REPO",
                "pickTs": held_ts or "GC001",
                "stockMom": None,
                "parkingPct": round(idle * parking_ret * 100, 2),
                "navBase": round(nav_base, 6),
                "navSingle": round(nav_harbor, 6),
                "navMulti": round(nav_harbor, 6),
                "navBaseReturnPct": round((nav_base - 1) * 100, 2),
                "navSingleReturnPct": round((nav_harbor - 1) * 100, 2),
                "navMultiReturnPct": round((nav_harbor - 1) * 100, 2),
            }
        )

    return {
        "ok": True,
        "mode": MODE,
        "strategy": STRATEGY_LABEL,
        "lookback": LOOKBACK,
        "maWindow": MA_WINDOW,
        "trailPct": TRAIL_PCT,
        "trailExits": trail_exits,
        "parkingTrades": trades,
        "rows": rows,
        "summary": {
            "fusedPct": round((nav_harbor - 1) * 100, 2),
            "basePct": round((nav_base - 1) * 100, 2),
            "maxDdFusedPct": round(max_dd * 100, 1),
            "parkingTrades": trades,
            "trailExits": trail_exits,
        },
    }
