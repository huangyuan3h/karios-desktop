#!/usr/bin/env python3
"""H-SGAP-ALT-1 Stage 1: other 14:30 microstructure orderings of the S-gap pool.

Same event universe / execution as the frozen S-gap leg (gap>3%, R-wide open,
skip_t1 + C1, 14:30 buy -> day-3 14:30 sell, 30bp). For each candidate intraday
"position" feature (both directions) report the *selected* tercile (the one the
engine would buy) mean net 3-day return, and compare to the incumbent amp_1430.

Dev windows OOS2 + train only; valid is NOT touched (Stage 2 confirmation).
Read-only. Usage: PYTHONPATH=src python3 scripts/diag_sgap_alt_features.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    COSTS_ROUNDTRIP,
    R_WIDE_THRESHOLD,
    _breadth_at_1430,
    _cached_day_features,
    _intraday_px,
    _same_1430_skip_reason,
    load_sgap_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
# (feature, direction) -> engine would buy the lowest ("asc") or highest ("desc") values.
ARMS = [
    ("range_pos", "asc"),
    ("range_pos", "desc"),
    ("low_recovery", "asc"),
    ("low_recovery", "desc"),
    ("high_giveback", "asc"),
    ("high_giveback", "desc"),
]
CONTROLS = [("amp_ref", "asc"), ("gap_pct", "asc")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--margin", type=float, default=0.002)
    args = ap.parse_args()

    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    hl_map = ctx.get("px_hl_1430") or {}
    raw_marks = (ctx.get("px_by_hhmm") or {}).get("1500") or {}
    q_close = ctx.get("close_by_ts") or {}
    gate_1430 = True  # match the engine's zero-lookahead habit gate

    arms = CONTROLS + ARMS
    # per window, per arm: selected-tercile forward returns
    acc: dict[str, dict[str, list[float]]] = {w: {f"{f}:{d}": [] for f, d in arms} for w in WINDOWS}
    day_counts: dict[str, dict[str, int]] = {w: {"days": 0, "names": 0} for w in WINDOWS}

    for w, (s, e) in WINDOWS.items():
        for day in cal:
            if day <= s or day > e:
                continue
            ei = idx_by_day.get(day, -1)
            if ei < 0 or ei + 2 >= len(cal):
                continue
            exit_day = cal[ei + 2]
            feat_all, breadth = _cached_day_features(ctx, day)
            if gate_1430:
                b1430 = _breadth_at_1430(ctx, day)
                if b1430 is not None:
                    breadth = b1430
            if breadth <= R_WIDE_THRESHOLD:
                continue
            rows = []
            for ts, d in feat_all.items():
                if not d.get("is_gap"):
                    continue
                di = date_idx.get(ts, {}).get(day, -1)
                series = per_ts.get(ts)
                if di < 0 or not series:
                    continue
                bar = series[di]
                px = _intraday_px(ctx, ts, day, "1430")
                # L4 basis: daily open/pre_close are qfq, px is raw bar_5min.
                # Scale to raw via the same-day ratio (raw 15:00 mark / qfq close)
                # or the C1/skip checks misfire (first-principles §二.8).
                qc = (q_close.get(ts) or {}).get(day)
                rm = (raw_marks.get(ts) or {}).get(day)
                k = (float(rm) / float(qc)) if (rm and qc and float(qc) > 0) else None
                open_px = bar.get("open")
                pre_close = bar.get("pre_close")
                if k:
                    open_px = float(open_px) * k if open_px else None
                    pre_close = float(pre_close) * k if pre_close else None
                reason = _same_1430_skip_reason(
                    ts=ts,
                    px=px,
                    open_px=open_px,
                    pre_close=pre_close,
                    skip_t1_limit=True,
                    max_open_to_1430_pct=0.03,
                    near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                hl = (hl_map.get(ts) or {}).get(day)
                if not hl:
                    continue
                hi, lo = float(hl[0]), float(hl[1])
                open_px = bar.get("open") or 0
                pre_close = bar.get("pre_close") or 0
                if hi <= lo or lo <= 0 or open_px <= 0 or pre_close <= 0:
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                rows.append(
                    (
                        {
                            "range_pos": (px - lo) / (hi - lo),
                            "low_recovery": (px - lo) / lo,
                            "high_giveback": px / hi - 1.0,
                            "amp_ref": (hi - lo) / px,
                            "gap_pct": open_px / pre_close - 1.0,
                        },
                        fwd,
                    )
                )
            if len(rows) < 9:
                continue
            day_counts[w]["days"] += 1
            day_counts[w]["names"] += len(rows)
            for f, direction in arms:
                vals = sorted(rows, key=lambda r, _f=f: r[0][_f])
                if direction == "asc":
                    cut = vals[: max(1, len(vals) // 3)]
                else:
                    cut = vals[-max(1, len(vals) // 3):]
                acc[w][f"{f}:{direction}"].extend(v for _, v in cut)

    print(f"\ncoverage: " + " | ".join(
        f"{w} days={day_counts[w]['days']} names={day_counts[w]['names']}" for w in WINDOWS))

    def stat(w, key):
        v = acc[w][key]
        return (float(np.mean(v)) if v else float("nan")), len(v)

    base = {w: stat(w, "amp_ref:asc")[0] for w in WINDOWS}
    print("\n| arm | OOS2 sel mean (Δ vs amp) | train sel mean (Δ vs amp) | pass? |")
    print("|-----|--------------------------|---------------------------|-------|")
    report = {"windows": WINDOWS, "coverage": day_counts, "base_amp": base, "arms": {}}
    survivors = []
    for f, d in CONTROLS + ARMS:
        key = f"{f}:{d}"
        o, n1 = stat("OOS2", key)
        t, n2 = stat("train", key)
        do, dt = o - base["OOS2"], t - base["train"]
        is_arm = (f, d) in ARMS
        ok = is_arm and do >= args.margin and dt >= args.margin
        if ok:
            survivors.append((key, min(do, dt)))
        report["arms"][key] = {
            "OOS2": o, "train": t, "dOOS2": do, "dtrain": dt,
            "nOOS2": n1, "ntrain": n2, "survivor": ok,
        }
        tag = "  ".join((
            f"OOS2 {o*100:+.3f}% ({do*100:+.3f})",
            f"train {t*100:+.3f}% ({dt*100:+.3f})",
            "**PASS**" if ok else ("n/a" if not is_arm else "no"),
        ))
        print(f"| {key:22s} | " + tag + " |")

    print(f"\nStage-1 survivors (need both windows ≥ +{args.margin*100:.2f}%): "
          f"{[s[0] for s in survivors] or 'NONE'}")
    if survivors:
        best = max(survivors, key=lambda x: x[1])
        report["stage2_candidate"] = best[0]
        print(f"→ Stage 2 single arm: {best[0]}")
    else:
        report["stage2_candidate"] = None
        print("→ REJECT family (no arm beats amp_1430 in both dev windows).")

    if args.save_report:
        out = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "sgap_alt_features.json"
        out.write_text(json.dumps(report, indent=2, default=float))
        print(f"saved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
