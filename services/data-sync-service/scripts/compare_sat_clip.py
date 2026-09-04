#!/usr/bin/env python3
"""Satellite clip concentration vs frozen-v3 15×10% (5% NAV) opportunity twin-star.

Live freeze (2026-09-02): n4_c25 — 4 × 25% sat → 12.5% NAV
  (state_bucket_track MAX_POS=4 POSITION_PCT=0.25;
   Watchlist SAT_MAX_POS / SAT_SLOT_OF_SLEEVE).
This script still compares against the v3 15×10% baseline.

  base     15 × 10% sat  →  5.0% NAV  (v3 contrast)
  n10_c10  10 × 10% sat  →  5.0% NAV  (same clip, fewer slots)
  n5_c20    5 × 20% sat  → 10.0% NAV
  n4_c25    4 × 25% sat  → 12.5% NAV  (frozen live)
  n3_c33    3 × 33% sat  → 16.5% NAV

Protocol matches opportunity_twin_star_v3: window-local empty book, strict
S-gap skip_t1_limit, opp_50 blend. Three-window walk-forward + past_year /
aligned. >5pt worse than frozen twin on any of OOS2/train/valid → reject.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_sat_clip.py
  PYTHONPATH=src:scripts python3 scripts/compare_sat_clip.py --save-report
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
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0

# max_pos × sat_clip so the sat book tops out around 100% (base keeps frozen 150%).
VARIANTS: tuple[dict[str, object], ...] = (
    {"id": "base", "max_pos": 15, "clip": 0.10, "label": "15×10% sat · 5% NAV"},
    {"id": "n10_c10", "max_pos": 10, "clip": 0.10, "label": "10×10% sat · 5% NAV"},
    {"id": "n5_c20", "max_pos": 5, "clip": 0.20, "label": "5×20% sat · 10% NAV"},
    {"id": "n4_c25", "max_pos": 4, "clip": 0.25, "label": "4×25% sat · 12.5% NAV"},
    {"id": "n3_c33", "max_pos": 3, "clip": 0.33, "label": "3×33% sat · 16.5% NAV"},
)


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
    slots = [int(r.get("satPositions") or 0) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, slots, active


def _occupancy(slots: list[int], active: list[bool], clip: float) -> dict[str, float]:
    n = len(slots)
    if n == 0:
        return {"avg_pos": 0.0, "avg_pos_active": 0.0, "pct_active": 0.0, "avg_sat_invested": 0.0}
    avg_pos = float(np.mean(slots))
    act_idx = [i for i, a in enumerate(active) if a]
    avg_pos_active = float(np.mean([slots[i] for i in act_idx])) if act_idx else 0.0
    pct_active = 100.0 * len(act_idx) / n
    invested = [min(1.0, s * clip) for s in slots]
    return {
        "avg_pos": round(avg_pos, 2),
        "avg_pos_active": round(avg_pos_active, 2),
        "pct_active": round(pct_active, 1),
        "avg_sat_invested": round(float(np.mean(invested)) * 100, 1),
    }


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Satellite clip concentration vs frozen twin-star (opp_50, strict)\n")
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)
    etf_close = fetch_etf_closes()

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        core: list[float] | None = None
        core_m: dict[str, float] | None = None
        row: dict[str, dict] = {}
        for var in VARIANTS:
            sat = replay_sgap_from_context(
                ctx,
                start=s,
                end=e,
                skip_t1_limit=True,
                pool_mode="strict",
                max_pos=int(var["max_pos"]),
                position_pct=float(var["clip"]),
            )
            dates, sat_nav, slots, active = _sat_series(sat)
            if core is None:
                core = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(core)
                print(f"  core     {_fmt(core_m)}", flush=True)
            n = min(len(core), len(sat_nav))
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            sat_m = _stats(sat_nav[:n])
            occ = _occupancy(slots[:n], active[:n], float(var["clip"]))
            delta_core = round(twin_m["total_pct"] - core_m["total_pct"], 1)
            row[str(var["id"])] = {
                "label": var["label"],
                "max_pos": var["max_pos"],
                "clip": var["clip"],
                "nav_pct": round(50.0 * float(var["clip"]), 1),
                "sat": sat_m,
                "twin": twin_m,
                "delta_core_pt": delta_core,
                **occ,
            }
            print(
                f"  {var['id']:<8} twin {_fmt(twin_m)}  Δcore {delta_core:+.1f}  "
                f"sat {_fmt(sat_m)}  pos {occ['avg_pos_active']:.1f} (act {occ['pct_active']:.0f}%)",
                flush=True,
            )
        assert core_m is not None
        base_twin = row["base"]["twin"]["total_pct"]
        for vid, rec in row.items():
            rec["delta_base_pt"] = round(rec["twin"]["total_pct"] - base_twin, 1)
        results[wname] = {"core": core_m, "variants": row}

    print("\n## Twin NAV (total/Sharpe/maxDD) vs frozen base\n")
    hdr = "| 窗口 | 核心 | " + " | ".join(v["id"] for v in VARIANTS) + " |"
    sep = "|" + "|".join(["------"] * (2 + len(VARIANTS))) + "|"
    print(hdr)
    print(sep)
    for wname in WINDOWS:
        cells = [_fmt(results[wname]["core"])]
        for v in VARIANTS:
            rec = results[wname]["variants"][str(v["id"])]
            mark = rec["twin"]
            extra = f" ({rec['delta_base_pt']:+.1f})" if v["id"] != "base" else ""
            cells.append(f"{_fmt(mark)}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Walk-forward vs frozen twin (OOS2/train/valid, reject if any Δ < -5pt)\n")
    verdicts: dict[str, str] = {}
    for v in VARIANTS:
        vid = str(v["id"])
        if vid == "base":
            verdicts[vid] = "baseline"
            continue
        deltas = [results[w]["variants"][vid]["delta_base_pt"] for w in WF_WINDOWS]
        worst = min(deltas)
        ok = all(d >= -REJECT_PT for d in deltas)
        tag = "PASS" if ok else "REJECT"
        if ok and all(d > 0 for d in deltas):
            tag = "PASS+"
        verdicts[vid] = tag
        print(
            f"- {vid} ({v['label']}): "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, deltas, strict=True))
            + f" → {tag}"
        )

    payload = {
        "tag": "sat-clip-concentration-2026-09-02",
        "protocol": "window-local strict S-gap + opp_50, same windows as opportunity_twin_star_v3",
        "variants": VARIANTS,
        "windows": results,
        "verdicts": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_clip_concentration_2026-09-02.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
