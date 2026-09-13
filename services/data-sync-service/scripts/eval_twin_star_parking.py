#!/usr/bin/env python3
"""Habit twin-star re-fit on the parking core (H-TWIN-PARK · 2026-09-13).

Core  = S-3 CN engine + idle-cash ETF parking (P1, B11).
Sat   = Live definition: strict S-gap, clip4 4x25% standalone, body=3,
        same_1430 signal+fill, 30bps RT costs (COSTS_ROUNDTRIP).
Blend = blend_nav_opportunity: 100% core when idle, else 50/50.
See docs/designs/twin-star-parking-refit-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_twin_star_parking.py --windows OOS2,train,valid --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_twin_star_parking.py --windows long
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

GOLD, OIL, BOND10 = "518880.SH", "513350.SH", "511260.SH"
NASDAQ_ALIAS = ("513110.SH", "513100.SH")
CAND = {"GOLD": GOLD, "OIL": OIL, "NASDAQ": NASDAQ_ALIAS[0], "BOND10": BOND10}
MA, LB, TRAIL, COST = 200, 60, 0.08, 0.0005
WINS = ("OOS2", "train", "valid", "long")
SAT_WEIGHTS = (0.4, 0.5, 0.6)


def _load_etf_closes() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    wanted = {GOLD, OIL, BOND10, *NASDAQ_ALIAS}
    with ETF_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            ts = row["ts_code"]
            if ts not in wanted:
                continue
            d = str(row["trade_date"])
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            try:
                c = float(row["close_adj"])
            except (TypeError, ValueError):
                continue
            if c > 0:
                out.setdefault(ts, {})[d] = c
    return out


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 3 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "cagr": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    years = (n - 1) / 252.0
    cagr = ((nav[-1] / nav[0]) ** (1 / years) - 1) * 100 if years > 0 and nav[-1] > 0 else 0.0
    peak, mdd = nav[0], 0.0
    for v in nav:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    std = float(np.std(rets)) if rets else 0.0
    sharpe = float(np.mean(rets) / std * (252**0.5)) if std > 0 else 0.0
    return {
        "n_days": n,
        "total_pct": round(total, 1),
        "cagr": round(cagr, 2),
        "max_dd": round(100 * mdd, 1),
        "sharpe": round(sharpe, 2),
    }


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}%/{m['cagr']:.1f}%/{m['max_dd']:.1f}%/{m['sharpe']:.2f}"


def _parking_core_by_day(px, start: str, end: str, stock_px: dict | None = None) -> dict[str, float]:
    """S-3 engine NAV + idle parking P1, keyed by engine calendar day."""
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    cal = list(data.calendar)
    eng = engine_nav_by_day_from_run(cal, run.nav_curve)
    snap_by = {str(x.get("date")): x for x in run.positions_by_day}

    days = {ts: sorted(m) for ts, m in px.items()}

    def c_at(ts, d):
        return px.get(ts, {}).get(d)

    def idx_of(ts, d):
        ds = days.get(ts) or []
        i = bisect.bisect_left(ds, d)
        return i if i < len(ds) and ds[i] == d else None

    def ma(ts, d):
        i = idx_of(ts, d)
        if i is None or i < MA - 1:
            return None
        return float(np.mean([px[ts][days[ts][j]] for j in range(i - MA + 1, i + 1)]))

    def mom(ts, d):
        i = idx_of(ts, d)
        if i is None or i < LB:
            return None
        a = px[ts][days[ts][i - LB]]
        return px[ts][d] / a - 1.0 if a else None

    def ret(ts, d, prev):
        c, p = c_at(ts, d), c_at(ts, prev)
        return c / p - 1.0 if c and p else 0.0

    nav = 1.0
    out = {cal[0]: 1.0}
    held_key = held_ts = None
    peak = 0.0
    for i in range(1, len(cal)):
        day, prev = cal[i], cal[i - 1]
        snap = snap_by.get(prev) or {}
        dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
        idle = max(0.0, 1.0 - min(1.0, dep))
        r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) else 0.0

        etf_mom, etf_ts = {}, {}
        for key, ts in CAND.items():
            aliases = NASDAQ_ALIAS if key == "NASDAQ" else (ts,)
            ba, bm = None, -1e9
            for a in aliases:
                m, mm = mom(a, prev), ma(a, prev)
                if m is None or mm is None or c_at(a, prev) is None:
                    continue
                if c_at(a, prev) >= mm and m > bm:
                    ba, bm = a, m
            if ba:
                etf_mom[key], etf_ts[key] = bm, ba
        best = max(etf_mom, key=etf_mom.get) if etf_mom else None
        best_ts = etf_ts.get(best) if best else None

        sides = 0
        if held_key != best:
            if held_key is not None:
                sides += 1
            if best is not None:
                sides += 1
            held_key, held_ts = best, best_ts
            peak = c_at(best_ts, prev) or 0.0 if best_ts else 0.0
        sr = 0.0
        if held_ts is not None:
            c = c_at(held_ts, prev) or 0.0
            peak = max(peak, c)
            if peak > 0 and c and c < peak * (1 - TRAIL):
                held_key = held_ts = None
                peak = 0.0
                sides += 1
            else:
                sr = ret(held_ts, day, prev)
        nav *= 1.0 + r_eng + idle * (sr - COST * sides)
        out[day] = nav
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_etf_closes()
    results: dict[str, dict] = {}
    for w in [x.strip() for x in args.windows.split(",") if x.strip()]:
        s, e = WINDOWS[w]
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        core_by_day = _parking_core_by_day(px, s, e)
        print(f"  core computed ({len(core_by_day)} days), loading sgap ctx ...", flush=True)
        ctx = load_sgap_context(s, e)
        sat = replay_sgap_from_context(
            ctx,
            start=s,
            end=e,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
            body=3,
            fill_mode=FILL_SAME_1430,
            fill_hhmm="1430",
        )
        rows = sat.get("rows") or []
        dates = [str(r["date"]) for r in rows]
        sat_nav = [float(r.get("satNav") or 1.0) for r in rows]
        active = [bool(r.get("satActive")) for r in rows]
        slots = [int(r.get("satPositions") or 0) for r in rows]
        if sat_nav and sat_nav[0] > 0:
            base = sat_nav[0]
            sat_nav = [v / base for v in sat_nav]

        cal_sorted = sorted(core_by_day)
        core_nav: list[float] = []
        last = 1.0
        j = 0
        for d in dates:
            while j < len(cal_sorted) and cal_sorted[j] <= d:
                last = core_by_day[cal_sorted[j]]
                j += 1
            core_nav.append(last)

        n = min(len(core_nav), len(sat_nav), len(active))
        core_nav, sat_nav, active = core_nav[:n], sat_nav[:n], active[:n]
        core_m = _stats(core_nav)
        sat_m = _stats(sat_nav)
        twin_by_w = {sw: blend_nav_opportunity(core_nav, sat_nav, active, sat_weight=sw) for sw in SAT_WEIGHTS}
        twin_m = {sw: _stats(nav) for sw, nav in twin_by_w.items()}
        primary = twin_m[0.5]
        pct_active = round(100.0 * sum(1 for a in active if a) / max(1, n), 1)
        summary = sat.get("summary") or {}

        results[w] = {
            "core": core_m,
            "satellite": sat_m,
            "twin": twin_m,
            "delta_core_total": round(primary["total_pct"] - core_m["total_pct"], 1),
            "delta_core_cagr": round(primary["cagr"] - core_m["cagr"], 2),
            "delta_core_mdd": round(primary["max_dd"] - core_m["max_dd"], 1),
            "delta_core_sharpe": round(primary["sharpe"] - core_m["sharpe"], 2),
            "pct_active": pct_active,
            "fills": summary.get("fillCount"),
            "avg_held_days": summary.get("avgHeldDays"),
        }
        d = results[w]
        print(
            f"  停车场核心 {_fmt(core_m)}\n"
            f"  卫星 standalone {_fmt(sat_m)}\n"
            f"  双子星 50/50 {_fmt(primary)}  Δcore tot {d['delta_core_total']:+.1f} cagr {d['delta_core_cagr']:+.2f} "
            f"dd {d['delta_core_mdd']:+.1f} sr {d['delta_core_sharpe']:+.2f}\n"
            f"  twin 40/60 {_fmt(twin_m[0.4])} | 60/40 {_fmt(twin_m[0.6])}\n"
            f"  sat active {pct_active:.0f}%  fills {summary.get('fillCount')}  hold {summary.get('avgHeldDays')}d",
            flush=True,
        )
        del ctx

    print("\n## 汇总（total / CAGR / maxDD / Sharpe）")
    print("| 窗口 | 停车场核心 | 卫星 standalone | 双子星 50/50 | Δcore tot | Δcagr | Δdd | Δsr |")
    print("|---|---|---|---|---|---|---|---|")
    for w in [x.strip() for x in args.windows.split(",") if x.strip()]:
        d = results[w]
        print(
            f"| {w} | {_fmt(d['core'])} | {_fmt(d['satellite'])} | {_fmt(d['twin'][0.5])} | "
            f"{d['delta_core_total']:+.1f} | {d['delta_core_cagr']:+.2f} | {d['delta_core_mdd']:+.1f} | {d['delta_core_sharpe']:+.2f} |"
        )

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "twin_star_parking_2026-09-13.json").write_text(
            json.dumps(
                {
                    "tag": "twin-star-parking-2026-09-13",
                    "prereg": "docs/designs/twin-star-parking-refit-prereg-2026-09-13.md",
                    "results": results,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
