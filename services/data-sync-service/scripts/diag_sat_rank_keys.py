#!/usr/bin/env python3
"""H1 diagnostic: which no-lookahead rank key predicts 3-day habit returns?

For each R-wide-open day in OOS2+train, take all S-gap names passing the
habit fill-side skips (skip_t1 + C1 3%), compute candidate rank keys known
at/before 14:30, and report mean forward 3-day net return per daily tercile.

Keys: gap_pct (known at open), runup = 1430/open-1, |runup|,
drift = 1430/1000-1, |drift|. Reference: full-day amp tercile (current rank).

Selection windows only (OOS2+train); valid is NOT touched (no peeking).
Read-only vs Postgres. Prints a table, saves nothing.
"""

from __future__ import annotations

import sys
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
KEYS = ("gap_pct", "runup", "abs_runup", "drift", "abs_drift", "amp_ref")


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    acc: dict[str, dict[str, list[float]]] = {k: {"t1": [], "t2": [], "t3": []} for k in KEYS}
    n_days = 0
    n_elig = 0
    for s, e in WINDOWS.values():
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
            n_days += 1
            rows: list[tuple[str, dict[str, float | None], float]] = []
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
                    ts=ts,
                    px=px,
                    open_px=bar.get("open"),
                    pre_close=bar.get("pre_close"),
                    skip_t1_limit=True,
                    max_open_to_1430_pct=0.03,
                    near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px1000 = _intraday_px(ctx, ts, day, "1000")
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                open_px = bar.get("open") or 0
                pre_close = bar.get("pre_close") or 0
                if open_px <= 0 or pre_close <= 0 or px1000 is None or px1000 <= 0:
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                keys: dict[str, float | None] = {
                    "gap_pct": open_px / pre_close - 1,
                    "runup": px / open_px - 1,
                    "abs_runup": abs(px / open_px - 1),
                    "drift": px / px1000 - 1,
                    "abs_drift": abs(px / px1000 - 1),
                    "amp_ref": float(d.get("amp") or 0),
                }
                rows.append((ts, keys, fwd))
            n_elig += len(rows)
            if len(rows) < 9:
                continue
            for k in KEYS:
                vals = sorted((r[1][k], r[2]) for r in rows if r[1][k] is not None)
                if len(vals) < 9:
                    continue
                n = len(vals)
                cuts = [vals[: n // 3], vals[n // 3 : 2 * n // 3], vals[2 * n // 3 :]]
                for terc, cut in zip(("t1", "t2", "t3"), cuts, strict=True):
                    acc[k][terc].extend(v for _, v in cut)
    print(f"\ndays={n_days} eligible_names={n_elig}")
    print("\n| key | n | T1(low) mean/hit | T2 mean/hit | T3(high) mean/hit | spread T1-T3 |")
    print("|-----|---|------------------|-------------|-------------------|----------------|")
    for k in KEYS:
        cells = []
        means = []
        for t in ("t1", "t2", "t3"):
            v = acc[k][t]
            m = float(np.mean(v)) * 100 if v else 0.0
            hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
            means.append(m)
            cells.append(f"{m:+.2f}%/{hit:.0f}%")
        n = len(acc[k]["t1"]) + len(acc[k]["t2"]) + len(acc[k]["t3"])
        print(f"| {k} | {n} | " + " | ".join(cells) + f" | {means[0] - means[2]:+.2f} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
