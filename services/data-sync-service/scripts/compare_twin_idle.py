#!/usr/bin/env python3
"""Twin-star idle-cash test: what happens to the satellite sleeve's unfilled half.

The frozen 机会双子星 blend is opp_50: when the satellite is active, 50% core
+ 50% satellite (the satellite book itself only fills ~3.3 of 4 slots, so the
rest earns 0); when idle, 100% core.

This post-hoc test recomputes the blend daily from the frozen habit satellite
book + frozen pick-strong core, comparing:
  opp50     frozen binary blend (base)
  idle_core unfilled satellite notional routed to the core (PS-G50-X x50)
  idle_repo unfilled satellite notional earns GC001-like yield (0.7%/yr)
  idle_sleeve unfilled satellite notional rides the multi-asset ETF sleeve pick

Read-only; Live untouched. Bar: three-window twin total within 5pt of base;
adoption needs all three > base with no Sharpe/DD regression.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_twin_idle.py --save-report
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

from data_sync_service.service.ps_g50_blend import (  # noqa: E402
    blend_nav_idle_to_core,
    blend_nav_opportunity,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2024-08-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REPO_ANNUAL = 0.007
SLOTS_PCT = 0.25
BASE_ID = "opp50"


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


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[int], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    slots = [int(r.get("satSlots") or 0) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, slots, active


def _pick_strong_nav(dates: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
    r = build_nav_from_cache(
        cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
        score="mom", top2=False, trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    out: list[float] = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        out.append(last)
    return out


def _blend_idle_repo(core_nav, sat_nav, sat_active, sat_slots, *, sat_weight=0.5, repo_annual=REPO_ANNUAL):
    n = min(len(core_nav), len(sat_nav), len(sat_active), len(sat_slots))
    repo_daily = repo_annual / 252.0
    out = [1.0]
    for i in range(1, n):
        c0, s0 = core_nav[i - 1], sat_nav[i - 1]
        core_ret = core_nav[i] / c0 - 1.0 if c0 > 0 else 0.0
        sat_ret = sat_nav[i] / s0 - 1.0 if s0 > 0 else 0.0
        if sat_active[i]:
            idle = max(0.0, 1.0 - sat_slots[i] * SLOTS_PCT)
            ret = core_ret + sat_weight * ((sat_ret + idle * repo_daily) - core_ret)
        else:
            ret = core_ret
        out.append(out[-1] * (1.0 + ret))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Twin-star satellite idle-cash test (frozen habit caliber)\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)
    etf_close = fetch_etf_closes()

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
        )
        dates, sat_nav, slots, active = _sat_series(sat)
        core = _pick_strong_nav(dates, s, e, etf_close)
        n = min(len(core), len(sat_nav))
        core, sat_nav, slots, active = core[:n], sat_nav[:n], slots[:n], active[:n]

        variants = {
            "opp50": blend_nav_opportunity(core, sat_nav, active, sat_weight=0.5),
            "idle_core": blend_nav_idle_to_core(core, sat_nav, slots, core_weight=0.5),
            "idle_repo": _blend_idle_repo(core, sat_nav, active, slots),
        }
        row: dict[str, dict] = {}
        # average idle notional on active days, as % of portfolio
        idle_vals = [max(0.0, 1.0 - slots[i] * SLOTS_PCT) for i in range(n) if active[i]]
        avg_idle_active = float(np.mean(idle_vals)) if idle_vals else 0.0
        for vid, nav in variants.items():
            m = _stats(nav)
            row[vid] = {"stats": m}
            print(f"  {vid:<10} twin {_fmt(m)}", flush=True)
        print(
            f"  (active days {sum(active)}/{n}; avg unfilled sat sleeve "
            f"{avg_idle_active * 100:.1f}% → portfolio idle ~{avg_idle_active * 0.5 * 100:.1f}% on active days)",
            flush=True,
        )
        base = row[BASE_ID]["stats"]
        for rec in row.values():
            rec["delta_base_pt"] = round(rec["stats"]["total_pct"] - base["total_pct"], 1)
            rec["delta_base_sharpe"] = round(rec["stats"]["sharpe"] - base["sharpe"], 2)
            rec["delta_base_dd"] = round(rec["stats"]["max_dd"] - base["max_dd"], 1)
        row["_occupancy"] = {"avg_idle_active_sleeve": round(avg_idle_active, 3), "active_days": int(sum(active)), "days": n}
        results[wname] = row

    print("\n## Twin NAV tot/sr/dd (Δ vs opp50 in parens)\n")
    ids = ["opp50", "idle_core", "idle_repo"]
    print("| 窗口 | " + " | ".join(ids) + " |")
    print("|" + "|".join(["------"] * (1 + len(ids))) + "|")
    for wname in WINDOWS:
        cells = []
        for vid in ids:
            rec = results[wname][vid]
            extra = f" ({rec['delta_base_pt']:+.1f})" if vid != BASE_ID else ""
            cells.append(f"{_fmt(rec['stats'])}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Verdict (vs opp50)\n")
    for vid in ids:
        if vid == BASE_ID:
            continue
        d_tot = [results[w][vid]["delta_base_pt"] for w in WF_WINDOWS]
        d_sr = [results[w][vid]["delta_base_sharpe"] for w in WF_WINDOWS]
        d_dd = [results[w][vid]["delta_base_dd"] for w in WF_WINDOWS]
        flags = []
        if any(d < -5.0 for d in d_tot):
            flags.append("REJECT/total")
        if any(s < 0 for s in d_sr):
            flags.append("worse_sharpe")
        if any(d > 0 for d in d_dd):
            flags.append("worse_dd")
        tag = "+".join(flags) if flags else ("PASS+" if all(d > 0 for d in d_tot) else "PASS")
        print(
            f"- {vid}: tot " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot, strict=True))
            + "  sr " + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, d_sr, strict=True))
            + "  dd " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_dd, strict=True))
            + f" → {tag}"
        )

    payload = {
        "tag": "twin-idle-2026-09-12",
        "protocol": (
            "frozen habit sat (C1 3% same_1430 amp_1430 clip4 body=3 day-3 14:30) + frozen "
            "pick-strong trail8; post-hoc blend. opp50=frozen; idle_core=unfilled sat notional "
            "to core; idle_repo=unfilled sat notional at 0.7%/yr."
        ),
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "twin_idle_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
