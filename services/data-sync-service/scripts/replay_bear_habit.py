#!/usr/bin/env python3
"""C: habit Live recipe bear replay 2021-2023 — READ-ONLY diagnostic.

Frozen habit recipe (C1 3% same_1430, body=3, day-3 1430 exit, amp rank,
top-1/3, R-wide 0.5, clip4 strict, opp_50) replayed on 2021/2022/2023
(full calendar years, 2022 = real bear). Gross basis, same as all prior
habit docs (sat-live-caliber, sat-exit-hhmm) so numbers are comparable.

Slippage handling (2026-09-05 A-stage decision): the engine stays gross;
data-jitter coverage is shown as a per-fill drag budget instead of NAV
surgery:
  base   = 10bps/side (paper_cost_model CN default) -> 20bps round-trip
  stress = 30bps/side (covers A-stage p90 jitter 29bps) -> 60bps round-trip
Per satellite fill = 12.5% of twin NAV, so drag per fill = 0.125 * rt_bps.
Rule: whatever this shows, no Live parameter changes (P0-7 C is diagnostic).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/replay_bear_habit.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

BEAR_WINDOWS = {
    "2021": ("2021-01-01", "2021-12-31"),
    "2022": ("2022-01-01", "2022-12-31"),
    "2023": ("2023-01-01", "2023-12-31"),
}
FULL_START = "2021-01-01"
FULL_END = "2023-12-31"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

# Slippage-only round-trip budgets (bps of traded notional), per-fill twin
# drag = 0.125 (clip4 12.5% NAV slot) * rt_bps / 10000 * 100 pt.
BASE_RT_BPS = 20.0
STRESS_RT_BPS = 60.0
CLIP_NAV = 0.125


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets) / std * (252**0.5))
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _drag_pt(fills: int, rt_bps: float) -> float:
    return round(fills * CLIP_NAV * rt_bps / 100.0, 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--report-name", default="sat_bear_replay_2026-09-05.json")
    args = ap.parse_args()

    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)
    for hhmm in ("1000", "1430"):
        m = (ctx.get("px_by_hhmm") or {}).get(hhmm) or {}
        days = {d for per in m.values() for d in per}
        print(f"  {hhmm} coverage: {len(m)} names / {len(days)} days", flush=True)

    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}
    for wname, (s, e) in BEAR_WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = replay_sgap_from_context(
            ctx,
            start=s,
            end=e,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
            fill_mode=FILL_SAME_1430,
            fill_hhmm="1430",
            exit_hhmm="1430",
            max_open_to_1430_pct=0.03,
        )
        rows = sat["rows"]
        dates = [r["date"] for r in rows]
        sat_nav = [float(r["satNav"]) for r in rows]
        if sat_nav and sat_nav[0] > 0:
            sat_nav = [v / sat_nav[0] for v in sat_nav]
        active = [bool(r.get("satActive")) for r in rows]
        print("  warming frozen pick-strong core ...", flush=True)
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(
            cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
            score="mom", top2=False, trail_pct=8.0,
        )
        pk_map = r["nav"]
        last = 1.0
        core = []
        for d in dates:
            v = pk_map.get(d)
            if v is not None:
                last = v
            core.append(last)
        n = min(len(core), len(sat_nav))
        twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
        core_m, twin_m, sat_m = _stats(core[:n]), _stats(twin), _stats(sat_nav[:n])
        summary = sat.get("summary") or {}
        fills = int(summary.get("fillCount") or 0)
        d_core = round(twin_m["total_pct"] - core_m["total_pct"], 1)
        base_drag, stress_drag = _drag_pt(fills, BASE_RT_BPS), _drag_pt(fills, STRESS_RT_BPS)
        print(f"  core {_fmt(core_m)}", flush=True)
        print(
            f"  twin {_fmt(twin_m)}  Δcore {d_core:+.1f}  sat {_fmt(sat_m)}  "
            f"fills {fills} skipC1 {summary.get('skipC1Count')} "
            f"skipNoPrint1430 {summary.get('skipNoPrint1430', 0)}",
            flush=True,
        )
        print(f"  fillSrc {summary.get('fillSrc')}", flush=True)
        print(
            f"  jitter budget: base {base_drag:.1f}pt -> Δ {d_core - base_drag:+.1f} | "
            f"stress {stress_drag:.1f}pt -> Δ {d_core - stress_drag:+.1f}",
            flush=True,
        )
        results[wname] = {
            "core": core_m, "twin": twin_m, "sat": sat_m,
            "delta_core_pt": d_core,
            "delta_core_sharpe": round(twin_m["sharpe"] - core_m["sharpe"], 2),
            "delta_core_dd": round(twin_m["max_dd"] - core_m["max_dd"], 1),
            "fills": fills,
            "skipC1": summary.get("skipC1Count"),
            "skipNoPrint1430": summary.get("skipNoPrint1430", 0),
            "fillSrc": summary.get("fillSrc"),
            "jitter_base_drag_pt": base_drag,
            "jitter_stress_drag_pt": stress_drag,
            "delta_after_base_pt": round(d_core - base_drag, 1),
            "delta_after_stress_pt": round(d_core - stress_drag, 1),
        }

    print("\n| 年 | core tot/sr/dd | twin tot/sr/dd | Δcore | fills | Δ-base | Δ-stress |")
    print("|---|---|---|---|---|---|---|")
    for wname in BEAR_WINDOWS:
        r = results[wname]
        print(
            f"| {wname} | {_fmt(r['core'])} | {_fmt(r['twin'])} | "
            f"{r['delta_core_pt']:+.1f} | {r['fills']} | "
            f"{r['delta_after_base_pt']:+.1f} | {r['delta_after_stress_pt']:+.1f} |"
        )

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "tag": "sat-bear-replay-2026-09-05",
            "rule": "read-only diagnostic; no Live changes whatever it shows",
            "recipe": "C1 3% + same_1430 + body=3 + day-3 1430 sell, strict, 4x12.5%, opp_50",
            "basis": "gross (cost=0), same as sat-live-caliber; slippage as per-fill jitter budget",
            "windows": results,
            "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        path = REPORT_DIR / args.report_name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
