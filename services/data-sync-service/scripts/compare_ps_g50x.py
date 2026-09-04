#!/usr/bin/env python3
"""Fit PS-G50-X (executable S-gap, idle slots follow pick-strong) vs single track.

One S-gap replay (skip_t1_limit, no rank-fallback) over the union window, then
in-memory blends:

  static_50     — frozen PS-G50 recipe on executable sat (cash earns 0)
  opp_50        — 机会双子星 binary satActive
  x50 / x70 / x80 — idle→core with core_weight 0.5 / 0.7 / 0.8

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_ps_g50x.py
  PYTHONPATH=src:scripts python3 scripts/compare_ps_g50x.py --save-report
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
from data_sync_service.service.state_bucket_track import build_sgap_timeline  # noqa: E402
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
CORE_WEIGHTS_X = (0.5, 0.7, 0.8)
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
    pk: list[float] = []
    last = 1.0
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        pk.append(last)
    return pk


def _slice_sat(sat_rows: list[dict], start: str, end: str) -> tuple[list[str], list[float], list[int], list[bool]]:
    dates: list[str] = []
    nav: list[float] = []
    slots: list[int] = []
    active: list[bool] = []
    for r in sat_rows:
        d = r["date"]
        if d < start or d > end:
            continue
        dates.append(d)
        nav.append(float(r["satNav"]))
        slots.append(int(r.get("satSlots") or r.get("satPositions") or 0))
        active.append(bool(r.get("satActive")))
    if nav:
        # Rebase so window starts at 1.0
        base = nav[0]
        if base > 0:
            nav = [v / base for v in nav]
    return dates, nav, slots, active


def _fmt(m: dict) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("PS-G50-X fit: executable S-gap (skip_t1_limit) · idle slots → pick-strong\n")
    print(f"loading S-gap {FULL_START}~{FULL_END} (once)...", flush=True)
    sat = build_sgap_timeline(
        start=FULL_START,
        end=FULL_END,
        skip_t1_limit=True,
        limit_fallback=False,
    )
    sat_rows = sat["rows"]
    print(f"  sat days={len(sat_rows)} final {sat['summary']}", flush=True)

    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}
    variant_keys = ["static_50", "opp_50"] + [f"x{int(w * 100)}" for w in CORE_WEIGHTS_X]

    for wname, (s, e) in WINDOWS.items():
        dates, sat_nav, slots, active = _slice_sat(sat_rows, s, e)
        if len(dates) < 5:
            continue
        pk = _pick_strong_nav(dates, s, e, etf_close)
        n = min(len(pk), len(sat_nav))
        pk, sat_nav, slots, active = pk[:n], sat_nav[:n], slots[:n], active[:n]
        core_m = _stats(pk)
        sat_m = _stats(sat_nav)
        mean_slots = float(np.mean(slots)) if slots else 0.0
        idle_days = sum(1 for k in slots if k == 0)
        blends = {
            "static_50": _stats(blend_nav_static(pk, sat_nav, core_weight=0.5)),
            "opp_50": _stats(blend_nav_opportunity(pk, sat_nav, active, sat_weight=0.5)),
        }
        for w in CORE_WEIGHTS_X:
            blends[f"x{int(w * 100)}"] = _stats(
                blend_nav_idle_to_core(pk, sat_nav, slots, core_weight=w)
            )
        print(f"=== {wname} ({s}~{e}) n={n} meanSlots={mean_slots:.1f} idleDays={idle_days} ===")
        print(f"  单轨择强  {_fmt(core_m)}", flush=True)
        print(f"  sat only  {_fmt(sat_m)}", flush=True)
        for k in variant_keys:
            b = blends[k]
            dt = b["total_pct"] - core_m["total_pct"]
            ds = b["sharpe"] - core_m["sharpe"]
            print(f"  {k:10s} {_fmt(b)}  vs core {dt:+.1f}pt sr{ds:+.2f}", flush=True)
        results[wname] = {
            "start": s,
            "end": e,
            "n_days": n,
            "mean_sat_slots": round(mean_slots, 2),
            "idle_days": idle_days,
            "pick_strong_single": core_m,
            "sat_only": sat_m,
            "blends": blends,
        }

    # Markdown
    hdr = "| 窗口 | 单轨 | static_50 | opp_50 | x50 | x70 | x80 |"
    sep = "|------|-----:|----------:|-------:|----:|----:|----:|"
    lines = [hdr, sep]
    for w in WINDOWS:
        if w not in results:
            continue
        r = results[w]
        cells = [_fmt(r["pick_strong_single"])]
        for k in variant_keys:
            cells.append(_fmt(r["blends"][k]))
        lines.append(f"| {w} | " + " | ".join(cells) + " |")
    table = "\n".join(lines)
    print("\n" + table)

    # Prefer variants that pass past_year −15pt total gate, then most such windows,
    # then mean sharpe.
    cands = [f"x{int(w * 100)}" for w in CORE_WEIGHTS_X]
    scored = []
    py_core_t = results.get("past_year", {}).get("pick_strong_single", {}).get("total_pct", 0)
    for k in cands:
        ok = 0
        srs = []
        py_b = results.get("past_year", {}).get("blends", {}).get(k, {})
        py_dt = py_b.get("total_pct", 0) - py_core_t
        py_pass = 1 if py_dt >= -15 else 0
        for _w, r in results.items():
            core = r["pick_strong_single"]
            b = r["blends"][k]
            srs.append(b["sharpe"])
            if b["sharpe"] >= core["sharpe"] - 0.05 and (b["total_pct"] - core["total_pct"]) >= -15:
                ok += 1
        scored.append((py_pass, ok, float(np.mean(srs) if srs else 0), k))
    scored.sort(reverse=True)
    best = scored[0][3] if scored else "x70"
    py = results.get("past_year", {})
    core_py = py.get("pick_strong_single", {})
    best_py = py.get("blends", {}).get(best, {})
    verdict = (
        f"Recommend {best} (PS-G50-X idle→core). past_year vs 单轨: "
        f"{best_py.get('total_pct', '—'):.1f}% vs {core_py.get('total_pct', '—'):.1f}% "
        f"sr {best_py.get('sharpe', 0):.2f} vs {core_py.get('sharpe', 0):.2f} "
        f"dd {best_py.get('max_dd', 0):.1f} vs {core_py.get('max_dd', 0):.1f}. "
        "Executable skip_t1_limit; unfilled sat notional follows pick-strong."
    )
    print(f"\n{verdict}")

    if args.save_report:
        payload = {
            "tag": f"ps-g50x-{date.today().isoformat()}",
            "baseline_id": "PS-G50-X",
            "baseline_name_zh": "G50 空槽回核",
            "generated_at": datetime.now(UTC).isoformat(),
            "fill_model": "skip_t1_limit, no rank-fallback, idle slots → core",
            "windows": {k: {"start": v[0], "end": v[1]} for k, v in WINDOWS.items()},
            "per_window": results,
            "markdown_table": table,
            "recommended": best,
            "verdict": verdict,
        }
        out = REPORT_DIR / f"ps_g50x_{date.today().isoformat()}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print(f"\nreport -> {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
