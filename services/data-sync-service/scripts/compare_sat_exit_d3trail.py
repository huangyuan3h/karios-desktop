#!/usr/bin/env python3
"""Day-3 conditional order: trail 2% off the exit-day high, else 14:30 (D-series).

User rule (2026-09-04): on the last holding day set a conditional sell order
— sell when price prints 2% below the day's running high; if never
triggered, sell at the fixed 14:30 print (Live habit recipe).

Design (zero Live/engine-calendar risk):
- Entry side is byte-identical to the frozen habit recipe (C1 3%, 14:30 buy,
  body=3, clip4). Only the body-exit *price* changes, never the exit date,
  so slot occupancy and the fill set are unchanged vs c1_x1430.
- Full-session 5-min bars come from vendor zips (see d3trail_series.py);
  bar_5min in Postgres only keeps sparse prints. Missing series falls back
  to the 14:30 print (same as baseline); the fallback rate is reported via
  exitPxSrc provenance (d3trail vs bar_1430).
- Fill = trigger level exactly (peak * 0.98), no extra slippage — documented
  assumption. A real conditional order may fill slightly worse.
- Sanity variant d3t50 (50% trail, never triggers) must reproduce c1_x1430
  NAV exactly; the script aborts otherwise.

Score tot + Sharpe + maxDD vs frozen C1-14:30 baseline and vs core.
Do not rewrite Live.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_sat_exit_d3trail.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from compare_sat_exit_hhmm import (  # noqa: E402
    WF_WINDOWS,
    WINDOWS,
    _fmt,
    _habit_line,
    _habit_tag,
    _occupancy,
    _pick_strong_nav,
    _sat_series,
    _stats,
    fetch_etf_closes,
)
from d3trail_series import extract_series  # noqa: E402

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0
BASE_ID = "c1_x1430"
TRAIL_PCT = 0.02

VARIANTS: tuple[dict[str, object], ...] = (
    {"id": "c1_x1430", "label": "C1 3% · day-3 14:30 (frozen habit)", "trail": None},
    {"id": "c1_x1430_d3t50", "label": "sanity: 50% trail (never fires)", "trail": 0.50},
    {"id": "c1_x1430_d3t2", "label": "C1 3% · day-3 high-2% conditional, else 14:30", "trail": TRAIL_PCT},
)

BASE_KWARGS = {
    "skip_t1_limit": True,
    "pool_mode": "strict",
    "max_pos": 4,
    "position_pct": 0.25,
    "fill_mode": FILL_SAME_1430,
    "fill_hhmm": "1430",
    "exit_hhmm": "1430",
    "max_open_to_1430_pct": 0.03,
}


def _exit_pairs(sat: dict) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for b in sat.get("blotter") or []:
        if b.get("kind") == "fill" and b.get("exitDate"):
            out.add((str(b["ts"]), str(b["exitDate"])))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--report-name", default="sat_exit_d3trail_2026-09-04.json")
    args = ap.parse_args()

    print("Day-3 conditional order: high-2% trail vs fixed 14:30\n")
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)

    # Pass 1: baseline replay per window -> collect (ts, exit_date) pairs.
    pairs: set[tuple[str, str]] = set()
    base_by_window: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        sat = replay_sgap_from_context(ctx, start=s, end=e, **BASE_KWARGS)
        base_by_window[wname] = sat
        pairs |= _exit_pairs(sat)
    print(f"  exit pairs to extract: {len(pairs)}", flush=True)

    series = extract_series(pairs)
    missing = len(pairs) - len(series)
    print(
        f"  extracted {len(series)} series, missing {missing} "
        f"({100.0 * missing / max(len(pairs), 1):.1f}% fall back to 14:30)",
        flush=True,
    )
    ctx["d3trail_series"] = series

    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        core: list[float] | None = None
        core_m: dict[str, float] | None = None
        row: dict[str, dict] = {}
        navs: dict[str, list[float]] = {}
        for var in VARIANTS:
            trail = var["trail"]
            if trail is None:
                sat = base_by_window[wname]
            else:
                sat = replay_sgap_from_context(
                    ctx, start=s, end=e, exit_day_trail_pct=float(trail), **BASE_KWARGS
                )
            dates, sat_nav, slots, active = _sat_series(sat)
            if core is None:
                print("  warming frozen pick-strong core ...", flush=True)
                core = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(core)
                print(f"  core     {_fmt(core_m)}", flush=True)
            assert core is not None and core_m is not None
            n = min(len(core), len(sat_nav))
            navs[str(var["id"])] = sat_nav[:n]
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            sat_m = _stats(sat_nav[:n])
            occ = _occupancy(slots[:n], active[:n], 0.25)
            summary = sat.get("summary") or {}
            fill_src = summary.get("fillSrc") or {}
            row[str(var["id"])] = {
                "label": var["label"],
                "trail_pct": trail,
                "sat": sat_m,
                "twin": twin_m,
                "delta_core_pt": round(twin_m["total_pct"] - core_m["total_pct"], 1),
                "delta_core_sharpe": round(twin_m["sharpe"] - core_m["sharpe"], 2),
                "delta_core_dd": round(twin_m["max_dd"] - core_m["max_dd"], 1),
                "fillCount": summary.get("fillCount"),
                "avgHeldDays": summary.get("avgHeldDays"),
                "fillSrcExit": (fill_src.get("exit") if isinstance(fill_src, dict) else None),
                **occ,
            }
            trig = ((fill_src.get("exit") or {}) if isinstance(fill_src, dict) else {}).get("d3trail", 0)
            print(
                f"  {var['id']:<16} twin {_fmt(twin_m)}  sat {_fmt(sat_m)}  "
                f"fills {summary.get('fillCount')} d3trail {trig}",
                flush=True,
            )
        # Sanity: 50% trail must reproduce the baseline NAV exactly.
        b, z = navs[BASE_ID], navs["c1_x1430_d3t50"]
        max_diff = max(abs(x - y) for x, y in zip(b, z, strict=True)) if b else 0.0
        print(f"  sanity d3t50 vs baseline max NAV diff: {max_diff:.2e}", flush=True)
        if max_diff > 1e-9:
            print("  ABORT: sanity variant diverged from baseline", flush=True)
            return 2
        assert core_m is not None
        base = row[BASE_ID]["twin"]
        for rec in row.values():
            rec["delta_base_pt"] = round(rec["twin"]["total_pct"] - base["total_pct"], 1)
            rec["delta_base_sharpe"] = round(rec["twin"]["sharpe"] - base["sharpe"], 2)
            rec["delta_base_dd"] = round(rec["twin"]["max_dd"] - base["max_dd"], 1)
        results[wname] = {"core": core_m, "variants": row}

    print("\n## Twin NAV tot/sr/dd vs frozen habit 14:30\n")
    ran = [v for v in VARIANTS if str(v["id"]) in results[next(iter(WINDOWS))]["variants"]]
    print("| 窗口 | 核心 | " + " | ".join(v["id"] for v in ran) + " |")
    print("|" + "|".join(["------"] * (2 + len(ran))) + "|")
    for wname in WINDOWS:
        cells = [_fmt(results[wname]["core"])]
        for v in ran:
            rec = results[wname]["variants"][str(v["id"])]
            extra = f" ({rec['delta_base_pt']:+.1f})" if v["id"] != BASE_ID else ""
            cells.append(f"{_fmt(rec['twin'])}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Walk-forward vs core (tot / Sharpe / maxDD)\n")
    verdicts: dict[str, str] = {}
    for v in ran:
        vid = str(v["id"])
        vs_core_tot = [results[w]["variants"][vid]["delta_core_pt"] for w in WF_WINDOWS]
        vs_core_sr = [results[w]["variants"][vid]["delta_core_sharpe"] for w in WF_WINDOWS]
        vs_core_dd = [results[w]["variants"][vid]["delta_core_dd"] for w in WF_WINDOWS]
        harvest = _habit_tag(vs_core_tot, vs_core_sr, vs_core_dd)
        if vid in (BASE_ID, "c1_x1430_d3t50"):
            verdicts[vid] = f"baseline/{harvest}"
            print(f"- {vid}: vs core " + _habit_line(vs_core_tot, vs_core_sr, vs_core_dd) + f" → {harvest}")
            continue
        d_tot = [results[w]["variants"][vid]["delta_base_pt"] for w in WF_WINDOWS]
        d_sr = [results[w]["variants"][vid]["delta_base_sharpe"] for w in WF_WINDOWS]
        d_dd = [results[w]["variants"][vid]["delta_base_dd"] for w in WF_WINDOWS]
        tot_ok = all(d >= -REJECT_PT for d in d_tot)
        risk_flags: list[str] = []
        if any(s < 0 for s in d_sr):
            risk_flags.append("worse_sharpe")
        if any(d > 0 for d in d_dd):
            risk_flags.append("worse_dd")
        if not tot_ok:
            tag = "REJECT/total"
        elif risk_flags:
            tag = "PASS/" + "+".join(risk_flags)
        elif all(d > 0 for d in d_tot):
            tag = "PASS+"
        else:
            tag = "PASS"
        verdicts[vid] = f"{tag}/{harvest}"
        print(
            f"- {vid} ({v['label']}):\n"
            f"    vs {BASE_ID} tot "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot, strict=True))
            + "  sr "
            + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, d_sr, strict=True))
            + "  dd "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_dd, strict=True))
            + f" → {tag}\n"
            f"    vs core "
            + _habit_line(vs_core_tot, vs_core_sr, vs_core_dd)
            + f" → {harvest}"
        )

    payload = {
        "tag": args.report_name.removesuffix(".json"),
        "protocol": (
            "window-local empty book; clip4 C1 3% same_1430 fill; body=3; "
            "day-3 exit = 5-min high-2% conditional else 14:30 print "
            "(vendor-zip full session, bars <=1430); frozen pick-strong trail8; "
            "opp_50. Fill = trigger level, no extra slippage (assumption). "
            "d3t50 sanity must equal baseline. Score tot+Sharpe+maxDD vs "
            "frozen habit baseline and vs core. Do not rewrite Live."
        ),
        "variants": [
            {"id": v["id"], "label": v["label"], "trail_pct": v["trail"]} for v in VARIANTS
        ],
        "series_extracted": len(series),
        "series_missing": missing,
        "windows": results,
        "verdicts": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / args.report_name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
