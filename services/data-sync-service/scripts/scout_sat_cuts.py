#!/usr/bin/env python3
"""Scout battery S: satellite eligible fills cut by gap-size / weekday / breadth-change.

Eligible set = S4 (R-wide gate + skip_t1 + C1 3% cap). Forward 3-day net
(1430 -> day-3 1430, minus COSTS_ROUNDTRIP). Windows OOS2+train ONLY;
valid NOT touched. All cuts reported (no cherry-pick). At most ONE forward
candidate per pre-reg rules in designs/scout-breakthrough-2026-09-05.md.

Read-only vs Postgres. Saves nothing.
"""
from __future__ import annotations

import sys
from datetime import date as _date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    COSTS_ROUNDTRIP,
    R_WIDE_THRESHOLD,
    _cached_day_features,
    _intraday_px,
    _same_1430_skip_reason,
    load_sgap_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
GAP_BINS = ("3-4%", "4-6%", "6-8%", "8%+")
WD_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri")


def _gap_bin(g: float) -> str:
    if g < 0.04:
        return GAP_BINS[0]
    if g < 0.06:
        return GAP_BINS[1]
    if g < 0.08:
        return GAP_BINS[2]
    return GAP_BINS[3]


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]

    # breadth history for Δ5d (cached day features make 2nd pass free)
    breadth_by_day: dict[str, float] = {}
    for day in cal:
        _, br = _cached_day_features(ctx, day)
        breadth_by_day[day] = br

    acc_gap: dict[str, dict[str, list[float]]] = {
        w: {b: [] for b in GAP_BINS} for w in WINDOWS
    }
    acc_wd: dict[str, dict[str, list[float]]] = {
        w: {b: [] for b in WD_NAMES} for w in WINDOWS
    }
    delta_vals: dict[str, list[float]] = {w: [] for w in WINDOWS}
    recs: dict[str, list[tuple[float, float]]] = {w: [] for w in WINDOWS}
    for w, (s, e) in WINDOWS.items():
        for day in cal:
            if day <= s or day > e:
                continue
            ei = idx_by_day.get(day, -1)
            if ei < 0 or ei + 2 >= len(cal):
                continue
            exit_day = cal[ei + 2]
            feat_all, breadth = _cached_day_features(ctx, day)
            if breadth <= R_WIDE_THRESHOLD:
                continue
            d5 = cal[idx_by_day[day] - 5] if idx_by_day[day] >= 5 else None
            dbr = breadth - breadth_by_day.get(d5, breadth) if d5 else 0.0
            wd = WD_NAMES[_date.fromisoformat(day).weekday()] if _date.fromisoformat(day).weekday() < 5 else "?"
            for ts, d in feat_all.items():
                if not d.get("is_gap"):
                    continue
                di = date_idx.get(ts, {}).get(day, -1)
                series = per_ts.get(ts)
                if di < 0 or not series:
                    continue
                bar = series[di]
                px = _intraday_px(ctx, ts, day, "1430")
                reason = _same_1430_skip_reason(
                    ts=ts, px=px, open_px=bar.get("open"), pre_close=bar.get("pre_close"),
                    skip_t1_limit=True, max_open_to_1430_pct=0.03, near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                if not bar.get("open") or not bar.get("pre_close"):
                    continue
                gap = float(d.get("gap") or 0.0)
                if not gap > 0.03:
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                acc_gap[w][_gap_bin(gap)].append(fwd)
                if wd in acc_wd[w]:
                    acc_wd[w][wd].append(fwd)
                delta_vals[w].append(dbr)
                recs[w].append((dbr, fwd))

    def _show(title: str, table: dict[str, dict[str, list[float]]], cols: tuple) -> None:
        print(f"\n## {title}")
        print("| window | " + " | ".join(cols) + " |")
        print("|" + "|".join(["------"] * (1 + len(cols))) + "|")
        for w in WINDOWS:
            cells = []
            for c in cols:
                v = table[w][c]
                m = float(np.mean(v)) * 100 if v else 0.0
                hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
                cells.append(f"{m:+.2f}%/{hit:.0f}%/{len(v)}")
            print(f"| {w} | " + " | ".join(cells) + " |")

    _show("S-a gap size (at open, knowable)", acc_gap, GAP_BINS)
    _show("S-b weekday (entry day)", acc_wd, WD_NAMES)

    print("\n## S-c breadth-change terciles (descriptive)")
    print("| window | lowΔ | midΔ | highΔ |")
    print("|------|------|------|-------|")
    for w in WINDOWS:
        dv = sorted(delta_vals[w])
        if not dv:
            print(f"| {w} | — | — | — |")
            continue
        q1, q2 = dv[len(dv) // 3], dv[2 * len(dv) // 3]
        buckets: dict[str, list[float]] = {"lowΔ": [], "midΔ": [], "highΔ": []}
        for dbr, fwd in recs[w]:
            buckets["lowΔ" if dbr < q1 else ("midΔ" if dbr < q2 else "highΔ")].append(fwd)
        cells = []
        for c in ("lowΔ", "midΔ", "highΔ"):
            v = buckets[c]
            m = float(np.mean(v)) * 100 if v else 0.0
            cells.append(f"{m:+.2f}%/{len(v)}")
        print(f"| {w} | " + " | ".join(cells) + f" |  (q1={q1:+.3f} q2={q2:+.3f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
