#!/usr/bin/env python3
"""Deeper executable sat search vs pick-strong: pool_mode × idle-to-core weights.

One DB load. Window-local empty books (same protocol as pick-strong).

  pool strict   — top 1/3, skip limit-up (slots may idle)
  pool replace  — top 1/3 of *fillable* (same count, next-best low-amp)
  pool fallback — all fillable (quality dump, control)

Blends per satellite: x50/x70/x80 idle→core, opp_50, static_50.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_ps_g50x_deep.py --save-report
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.ps_g50_blend import (  # noqa: E402
    blend_nav_idle_to_core,
    blend_nav_opportunity,
    blend_nav_static,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
POOL_MODES = ("strict", "replace", "fallback")
CORE_WEIGHTS = (0.5, 0.7, 0.8)
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


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
    return {"n_days": n, "total_pct": total, "max_dd": mdd, "sharpe": sharpe}


def _pick_strong_nav(dates: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
    r = build_nav_from_cache(
        cache,
        lookback=60,
        ma_window=200,
        min_hold=1,
        cost=0.0,
        score="mom",
        top2=False,
        trail_pct=8.0,
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


def _fmt(m: dict) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("PS-G50-X deep: window-local executable sat × pool_mode × idle→core\n")
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)

    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}

    for wname, (s, e) in WINDOWS.items():
        pk_dates = None
        pk = None
        core_m = None
        pools: dict[str, dict] = {}
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        for mode in POOL_MODES:
            sat = replay_sgap_from_context(
                ctx, start=s, end=e, skip_t1_limit=True, pool_mode=mode
            )
            dates, sat_nav, slots, active = _sat_series(sat)
            if pk is None:
                pk_dates = dates
                pk = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(pk)
                print(f"  单轨择强  {_fmt(core_m)}", flush=True)
            n = min(len(pk), len(sat_nav))
            pk_n, sat_n, slots_n, act_n = pk[:n], sat_nav[:n], slots[:n], active[:n]
            sat_m = _stats(sat_n)
            blends = {
                "static_50": _stats(blend_nav_static(pk_n, sat_n, core_weight=0.5)),
                "opp_50": _stats(blend_nav_opportunity(pk_n, sat_n, act_n, sat_weight=0.5)),
            }
            for w in CORE_WEIGHTS:
                blends[f"x{int(w * 100)}"] = _stats(
                    blend_nav_idle_to_core(pk_n, sat_n, slots_n, core_weight=w)
                )
            mean_slots = float(np.mean(slots_n)) if slots_n else 0.0
            pools[mode] = {
                "sat": sat_m,
                "mean_slots": round(mean_slots, 2),
                "idle_days": sum(1 for k in slots_n if k == 0),
                "blends": blends,
            }
            print(
                f"  sat[{mode:8s}] {_fmt(sat_m)}  slots={mean_slots:.1f}  "
                f"x70 {_fmt(blends['x70'])}  opp {_fmt(blends['opp_50'])}",
                flush=True,
            )
        results[wname] = {
            "start": s,
            "end": e,
            "n_days": len(pk_dates or []),
            "pick_strong_single": core_m,
            "pools": pools,
        }

    # Leaderboard: executable blends vs core on past_year, then 3-window (OOS2/train/valid)
    wf = ("OOS2", "train", "valid")
    cands: list[tuple] = []
    for mode in POOL_MODES:
        for bk in ["opp_50", "static_50"] + [f"x{int(w * 100)}" for w in CORE_WEIGHTS]:
            py = results["past_year"]["pools"][mode]["blends"][bk]
            core_py = results["past_year"]["pick_strong_single"]
            dt = py["total_pct"] - core_py["total_pct"]
            wf_ok = 0
            for w in wf:
                b = results[w]["pools"][mode]["blends"][bk]
                c = results[w]["pick_strong_single"]
                if (b["total_pct"] - c["total_pct"]) >= -15 and b["sharpe"] >= c["sharpe"] - 0.05:
                    wf_ok += 1
            # Prefer: not collapsing past_year, then sharpe>=core, then wf_ok, then total
            py_ok = 1 if dt >= -15 else 0
            sr_ok = 1 if py["sharpe"] >= core_py["sharpe"] - 0.05 else 0
            dd_ok = 1 if py["max_dd"] <= core_py["max_dd"] + 0.3 else 0
            cands.append((py_ok, sr_ok, dd_ok, wf_ok, dt, py["sharpe"], mode, bk, py))

    cands.sort(reverse=True)
    best_mode, best_bk, best_py = cands[0][6], cands[0][7], cands[0][8]
    core_py = results["past_year"]["pick_strong_single"]

    lines = [
        "| 窗口 | 单轨择强 | 最优候选 | Δtotal | Δsr | Δdd |",
        "|------|--------:|--------:|-------:|----:|----:|",
    ]
    label = f"{best_mode}/{best_bk}"
    for w in WINDOWS:
        c = results[w]["pick_strong_single"]
        b = results[w]["pools"][best_mode]["blends"][best_bk]
        lines.append(
            f"| {w} | {_fmt(c)} | {_fmt(b)} | "
            f"{b['total_pct'] - c['total_pct']:+.1f}pt | "
            f"{b['sharpe'] - c['sharpe']:+.2f} | "
            f"{b['max_dd'] - c['max_dd']:+.1f} |"
        )
    table = "\n".join(lines)
    print("\n" + table)

    top5 = ", ".join(f"{m}/{k} py{p['total_pct']:+.1f} sr{p['sharpe']:.2f}" for *_, m, k, p in cands[:5])
    verdict = (
        f"BEST executable vs 单轨: {label}. "
        f"past_year {_fmt(best_py)} vs core {_fmt(core_py)} "
        f"(Δ {best_py['total_pct'] - core_py['total_pct']:+.1f}pt "
        f"sr{best_py['sharpe'] - core_py['sharpe']:+.2f} "
        f"dd{best_py['max_dd'] - core_py['max_dd']:+.1f}). "
        f"Top5: {top5}"
    )
    print(f"\n{verdict}")

    if args.save_report:
        payload = {
            "tag": f"ps-g50x-deep-{date.today().isoformat()}",
            "generated_at": datetime.now(UTC).isoformat(),
            "protocol": "window-local empty book, skip_t1_limit, idle-to-core",
            "windows": {k: {"start": v[0], "end": v[1]} for k, v in WINDOWS.items()},
            "per_window": results,
            "best": {"pool_mode": best_mode, "blend": best_bk, "label": label},
            "markdown_table": table,
            "verdict": verdict,
        }
        out = REPORT_DIR / f"ps_g50x_deep_{date.today().isoformat()}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print(f"\nreport -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
