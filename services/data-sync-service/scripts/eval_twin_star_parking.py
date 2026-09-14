#!/usr/bin/env python3
"""Habit twin-star re-fit on the parking core (H-TWIN-PARK · 2026-09-13).

Core  = S-3 CN engine + idle-cash ETF parking (P1, B11) via the canonical
        `service.harbor.parking_replay` (same NAV as eval_etf_parking_baseline).
Sat   = Live habit definition: strict S-gap, clip4 4x25% standalone, body=3,
        same_1430 signal/fill/exit, C1 3%, `rank_key="amp_1430"` (14:30-knowable
        amplitude — NOT the full-day amp lookahead key), `gate_1430=True`
        (R-wide breadth from the 14:30 panel, not the 15:00 close), 30bps RT.
Blend = blend_nav_opportunity: 100% core when idle, else 50/50.
See docs/designs/twin-star-parking-refit-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_twin_star_parking.py --windows OOS2,train,valid --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_twin_star_parking.py --windows long
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

GOLD, OIL, BOND10 = "518880.SH", "513350.SH", "511260.SH"
NASDAQ_ALIAS = ("513110.SH", "513100.SH")
COST = 0.0005
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
    """S-3 engine NAV + idle parking P1, keyed by engine calendar day.

    Core leg = canonical Harbor parking state machine
    (`service.harbor.parking_replay`, OPT-180 single source), identical to
    `eval_etf_parking_baseline` / Timeline / Live — no local replay loop
    (the old local loop re-liquidated the parking leg on phantom engine
    calendar days and diverged from the frozen B11 NAV).
    """
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    etf_days = {d for mp in px.values() for d in mp}
    cal = [d for d in data.calendar if d in etf_days]
    eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
    snap_by = {str(x.get("date")): x for x in run.positions_by_day}

    idle_by_day: dict[str, float] = {}
    for idx in range(1, len(cal)):
        prev = cal[idx - 1]
        snap = snap_by.get(prev) or {}
        dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
        idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))

    nav = 1.0
    out = {cal[0]: 1.0}
    for rec in parking_replay(px, cal, idle_by_day=idle_by_day):
        day, prev = str(rec["date"]), str(rec["prev"])
        idle = idle_by_day.get(prev, 0.0)
        r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
        sides = int(rec["sides"])
        nav *= 1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * sides)
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
            exit_hhmm="1430",
            max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
            gate_1430=True,
        )
        rows = sat.get("rows") or []
        dates = [str(r["date"]) for r in rows]
        sat_nav = [float(r.get("satNav") or 1.0) for r in rows]
        active = [bool(r.get("satActive")) for r in rows]
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
