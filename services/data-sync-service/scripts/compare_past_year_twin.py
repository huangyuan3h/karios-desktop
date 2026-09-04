#!/usr/bin/env python3
"""Past-year three-way: pick-strong single vs twin-star v3 15×5% vs clip4 4×12.5%.

Windows:
  protocol  2025-08-01~2026-08-07   twin-star freeze table past_year
  product   2025-08-28~2026-08-28   pick-strong-track.md §2
  trailing  2025-09-02~2026-09-02   true 1y as of 2026-09-02

Protocol matches opportunity_twin_star_v3 / clip4: window-local empty book,
strict S-gap skip_t1_limit, opp_50. Past_year is display-only (not the 3-window
reject gate). Freeze if clip4 beats both core and v3 on product + trailing.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_past_year_twin.py
  PYTHONPATH=src:scripts python3 scripts/compare_past_year_twin.py --save-report
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

from compare_sat_clip import _fmt, _occupancy, _pick_strong_nav, _sat_series, _stats  # noqa: E402

WINDOWS = {
    "protocol": ("2025-08-01", "2026-08-07"),
    "product": ("2025-08-28", "2026-08-28"),
    "trailing": ("2025-09-02", "2026-09-02"),
}
CTX_START = "2025-08-01"
CTX_END = "2026-09-02"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

VARIANTS: tuple[dict[str, object], ...] = (
    {"id": "v3_15x5", "max_pos": 15, "clip": 0.10, "label": "twin v3 15×5% NAV"},
    {"id": "clip4", "max_pos": 4, "clip": 0.25, "label": "twin v3.1 4×12.5% NAV"},
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Past-year three-way: 单轨 vs 双子星 v3 15×5% vs clip4 4×12.5%\n")
    print(f"loading context {CTX_START}~{CTX_END} ...", flush=True)
    ctx = load_sgap_context(CTX_START, CTX_END)
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
        v3_twin = row["v3_15x5"]["twin"]["total_pct"]
        for rec in row.values():
            rec["delta_v3_pt"] = round(rec["twin"]["total_pct"] - v3_twin, 1)
        results[wname] = {"start": s, "end": e, "core": core_m, "variants": row}

    print("\n## Past-year three-way (total/Sharpe/maxDD)\n")
    print("| 窗口 | 单轨择强 | 双子星 v3 15×5% | clip4 4×12.5% | Δclip4 vs 单轨 | Δclip4 vs v3 |")
    print("|------|----------|-----------------|---------------|----------------|--------------|")
    verdicts: dict[str, str] = {}
    for wname in WINDOWS:
        rec = results[wname]
        core_m = rec["core"]
        v3 = rec["variants"]["v3_15x5"]
        c4 = rec["variants"]["clip4"]
        d_core = c4["delta_core_pt"]
        d_v3 = c4["delta_v3_pt"]
        beats_both = d_core > 0 and d_v3 > 0
        dd_ok = c4["twin"]["max_dd"] <= core_m["max_dd"] + 0.2
        tag = "PASS+" if beats_both and dd_ok else ("PASS" if beats_both else "HOLD")
        if d_core < -5 or d_v3 < -5:
            tag = "REJECT"
        verdicts[wname] = tag
        print(
            f"| {wname} {rec['start']}~{rec['end']} | {_fmt(core_m)} | "
            f"{_fmt(v3['twin'])} ({v3['delta_core_pt']:+.1f}) | "
            f"**{_fmt(c4['twin'])}** | {d_core:+.1f}pt | {d_v3:+.1f}pt |"
        )
        print(f"  → {tag}", flush=True)

    product_ok = verdicts.get("product") in {"PASS", "PASS+"}
    trailing_ok = verdicts.get("trailing") in {"PASS", "PASS+"}
    freeze = product_ok and trailing_ok
    freeze_note = (
        "FREEZE: clip4 beats 单轨 and v3 15×5% on product + trailing past_year; DD tied to core."
        if freeze
        else "DO NOT FREEZE: clip4 did not beat both on product and trailing."
    )
    print(f"\n{freeze_note}", flush=True)

    payload = {
        "tag": "past-year-twin-vs-core-2026-09-02",
        "protocol": "window-local strict S-gap + opp_50; same engine as compare_sat_clip.py",
        "compare": ["pick_strong trail8", "twin v3 15×5%", "twin v3.1 clip4 4×12.5%"],
        "windows": results,
        "verdicts": verdicts,
        "freeze": freeze,
        "freeze_note": freeze_note,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
        "reproduce": "PYTHONPATH=src:scripts python3 scripts/compare_past_year_twin.py --save-report",
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "past_year_twin_vs_core_2026-09-02.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
