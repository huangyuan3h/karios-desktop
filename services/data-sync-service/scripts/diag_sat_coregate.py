#!/usr/bin/env python3
"""C-gate diagnostic battery: split eligible satellite fills by core pick.

Selection windows OOS2+train; valid NOT touched (pre-reg §2).
Same eligible set as S4 (R-wide gate + skip_t1 + C1 3% upside cap).
Forward 3-day net (1430 -> day-3 1430, minus COSTS_ROUNDTRIP).

Core pick per entry day D comes from the FROZEN core
(pick_strong_grid.build_nav_from_cache, mom60+MA200+trail8, warmed per
window exactly like the twin replay). Buckets: STOCK / RISK_ETF(OIL,NASDAQ)
/ DEFENSIVE(GOLD,BOND10) / REPO. At most ONE bucket (or DEFENSIVE+REPO)
goes to walk-forward, and only if BOTH windows agree it is the worst.

Read-only vs Postgres. Saves nothing.
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
from pick_strong_grid import (  # noqa: E402
    build_nav_from_cache,
    fetch_etf_closes,
    warm_window,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
BUCKETS = ("STOCK", "RISK_ETF", "DEFENSIVE", "REPO")


def _bucket(pick: str) -> str:
    if pick == "STOCK":
        return "STOCK"
    if pick in ("OIL", "NASDAQ"):
        return "RISK_ETF"
    if pick in ("GOLD", "BOND10"):
        return "DEFENSIVE"
    if pick == "REPO":
        return "REPO"
    if pick.startswith("TOP2:"):
        keys = pick[5:].split("+")
        if "STOCK" in keys:
            return "STOCK"
        if any(k in ("OIL", "NASDAQ") for k in keys):
            return "RISK_ETF"
        return "DEFENSIVE"
    return "REPO"


def main() -> int:
    print("loading sgap context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    print(f"cal days: {len(cal)} names: {len(per_ts)}", flush=True)
    print("fetching etf closes ...", flush=True)
    etf_close = fetch_etf_closes()

    acc: dict[str, dict[str, list[float]]] = {
        w: {b: [] for b in BUCKETS} for w in WINDOWS
    }
    for w, (s, e) in WINDOWS.items():
        print(f"warming frozen core for {w} ...", flush=True)
        cache = warm_window(s, e, etf_close)
        core = build_nav_from_cache(
            cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
            score="mom", top2=False, trail_pct=8.0,
        )
        pick_map: dict[str, str] = core.get("pick_map") or {}
        n_pick = len(pick_map)
        n_repo = sum(1 for v in pick_map.values() if v == "REPO")
        print(f"  picks: {n_pick} days, REPO {n_repo}", flush=True)
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
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                acc[w][_bucket(pick_map.get(day, "REPO"))].append(fwd)

    print("\n## C-gate diagnostic (eligible gap fills split by core pick on entry day)")
    print("| window | STOCK | RISK_ETF | DEFENSIVE | REPO |")
    print("|------|-------|----------|-----------|------|")
    for w in WINDOWS:
        cells = []
        for b in BUCKETS:
            v = acc[w][b]
            m = float(np.mean(v)) * 100 if v else 0.0
            hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
            cells.append(f"{m:+.2f}%/{hit:.0f}%/{len(v)}")
        print(f"| {w} | " + " | ".join(cells) + " |")
    print("\ntrigger share (of eligible fills):")
    for w in WINDOWS:
        tot = sum(len(acc[w][b]) for b in BUCKETS)
        for b in BUCKETS:
            n = len(acc[w][b])
            print(f"  {w} {b}: {n}/{tot} ({100.0*n/max(1,tot):.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
