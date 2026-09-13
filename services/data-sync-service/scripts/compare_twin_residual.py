#!/usr/bin/env python3
"""Twin-star residual directions: structural gap threshold + CHURN re-verify.

The 双子星 signal/param space is heavily scanned and frozen. Two genuinely
open satellite knobs remain:
  1. the overnight-gap threshold defining the S-gap pool (hardcoded >3%,
     never scanned);
  2. the CHURN entry filter (T-1 extreme turnover), a first-principles §五
     dimension-6 "partial absorption" candidate: train +2.4 / valid +1.5 but
     OOS2 -1.0 -> PASS/worse, never re-run under the current Live habit
     caliber (C1 3% + amp_1430 + day-3 14:30 exit).

Frozen habit caliber otherwise. Walk-forward bar: three-window twin total
within 5pt of base, fill count not collapsing, and for adoption the house
PASS+ (all three windows total > base, no Sharpe/DD regression).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_twin_residual.py --save-report
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

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2024-08-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0
BASE_ID = "base"

VARIANTS = (
    {"id": "base", "label": "frozen (gap>3%, no churn)", "min_gap_pct": None, "max_t1_turnover_mult": None},
    {"id": "gap2", "label": "gap>2%", "min_gap_pct": 0.02, "max_t1_turnover_mult": None},
    {"id": "gap4", "label": "gap>4%", "min_gap_pct": 0.04, "max_t1_turnover_mult": None},
    {"id": "gap5", "label": "gap>5%", "min_gap_pct": 0.05, "max_t1_turnover_mult": None},
    {"id": "churn3", "label": "skip T-1 turn>3x", "min_gap_pct": None, "max_t1_turnover_mult": 3.0},
    {"id": "churn4", "label": "skip T-1 turn>4x", "min_gap_pct": None, "max_t1_turnover_mult": 4.0},
    {"id": "churn5", "label": "skip T-1 turn>5x", "min_gap_pct": None, "max_t1_turnover_mult": 5.0},
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


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, active


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Twin residual: S-gap threshold + CHURN filter (frozen habit caliber)\n", flush=True)
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
                ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
                max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
                fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
                rank_key="amp_1430",
                min_gap_pct=var["min_gap_pct"] if var["min_gap_pct"] is not None else 0.03,
                max_t1_turnover_mult=var["max_t1_turnover_mult"],
            )
            dates, sat_nav, active = _sat_series(sat)
            if core is None:
                core = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(core)
                print(f"  core {_fmt(core_m)}", flush=True)
            n = min(len(core), len(sat_nav))
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            sat_m = _stats(sat_nav[:n])
            summary = sat.get("summary") or {}
            row[str(var["id"])] = {
                "label": var["label"],
                "sat": sat_m,
                "twin": twin_m,
                "fillCount": summary.get("fillCount"),
                "skipChurn": summary.get("skipChurn"),
                "gapCountAvg": summary.get("gapCountAvg"),
            }
            print(
                f"  {var['id']:<8} twin {_fmt(twin_m)}  sat {_fmt(sat_m)}  "
                f"fills {summary.get('fillCount')}  churnSkip {summary.get('skipChurn')}",
                flush=True,
            )
        assert core_m is not None
        base = row[BASE_ID]
        for rec in row.values():
            rec["delta_base_pt"] = round(rec["twin"]["total_pct"] - base["twin"]["total_pct"], 1)
            rec["delta_base_sharpe"] = round(rec["twin"]["sharpe"] - base["twin"]["sharpe"], 2)
            rec["delta_base_dd"] = round(rec["twin"]["max_dd"] - base["twin"]["max_dd"], 1)
        results[wname] = {"core": core_m, "variants": row}

    print("\n## Twin NAV tot/sr/dd (Δ vs base in parens)\n")
    print("| 窗口 | 核心 | " + " | ".join(v["id"] for v in VARIANTS) + " |")
    print("|" + "|".join(["------"] * (2 + len(VARIANTS))) + "|")
    for wname in WINDOWS:
        cells = [_fmt(results[wname]["core"])]
        for v in VARIANTS:
            rec = results[wname]["variants"][str(v["id"])]
            extra = f" ({rec['delta_base_pt']:+.1f})" if v["id"] != BASE_ID else ""
            cells.append(f"{_fmt(rec['twin'])}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Walk-forward verdict (vs base)\n")
    for v in VARIANTS:
        vid = str(v["id"])
        if vid == BASE_ID:
            continue
        d_tot = [results[w]["variants"][vid]["delta_base_pt"] for w in WF_WINDOWS]
        d_sr = [results[w]["variants"][vid]["delta_base_sharpe"] for w in WF_WINDOWS]
        d_dd = [results[w]["variants"][vid]["delta_base_dd"] for w in WF_WINDOWS]
        flags: list[str] = []
        if any(d < -REJECT_PT for d in d_tot):
            flags.append("REJECT/total")
        if any(s < 0 for s in d_sr):
            flags.append("worse_sharpe")
        if any(d > 0 for d in d_dd):
            flags.append("worse_dd")
        tag = "+".join(flags) if flags else ("PASS+" if all(d > 0 for d in d_tot) else "PASS")
        print(
            f"- {vid} ({v['label']}):\n"
            f"    Δbase tot " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot, strict=True))
            + "  sr " + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, d_sr, strict=True))
            + "  dd " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_dd, strict=True))
            + f" → {tag}"
        )

    payload = {
        "tag": "twin-residual-2026-09-12",
        "protocol": (
            "frozen habit caliber C1 3% same_1430 amp_1430 rank clip4 4x25% body=3 day-3 14:30 exit; "
            "frozen pick-strong trail8; opp_50. Structural gap threshold + T-1 CHURN filter only. "
            "Bar: 3-window twin total within 5pt of base; adoption needs PASS+."
        ),
        "variants": [{k: v for k, v in var.items()} for var in VARIANTS],
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "twin_residual_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
