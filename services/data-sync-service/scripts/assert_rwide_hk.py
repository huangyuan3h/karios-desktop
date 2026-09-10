#!/usr/bin/env python3
"""OPT-147: R-wide breadth HK-pollution magnitude assertion (read-only).

Question: does the HK sleeve (~2800 names in `daily`) leak into the R-wide
breadth (close>MA20 share, gate 0.5)? The breadth loop in
state_bucket_track._day_features requires mv membership, and stock_dailybasic
carries 0 HK rows -- so HK should already be excluded. This script measures:

  breadth_cur   = engine as-is (mv-gated, via _cached_day_features)
  breadth_noHK  = explicit .HK drop on top (must equal cur if gate holds)
  breadth_wHK   = counterfactual with NO mv gate (what a pattern-copy
                  regression would compute; quantifies the guarded risk)

Read-only: no engine change, no file writes. Verdict per OPT-147:
  flips(cur vs noHK) == 0 -> close item (+ first-principles II.7 line).
  flips > 0 -> do NOT fix engine here; open pre-reg for clip4 impact.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/assert_rwide_hk.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service import state_bucket_track as sbt  # noqa: E402
from data_sync_service.service.state_bucket_track import load_sgap_context  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
GATE = 0.5


def _is_hk(ts: str) -> bool:
    return str(ts).upper().endswith(".HK")


def _breadth_variants(ctx: dict, day: str) -> tuple[float, float, float, int, int]:
    """(cur, noHK, withHK, n_mv_pool, n_hk_skipped_by_mv)."""
    _, cur = sbt._cached_day_features(ctx, day)
    per_ts = ctx["per_ts"]
    mv_map = ctx["mv_map"]
    date_idx = ctx["date_idx"]
    mv_day = mv_map.get(day, {})
    tot_nohk = above_nohk = 0
    tot_whk = above_whk = 0
    hk_skipped = 0
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 20:
            continue
        closes = [r["close"] for r in series[idx - 19: idx + 1] if r["close"]]
        if len(closes) < 20:
            continue
        is_above = series[idx]["close"] > sum(closes) / 20
        # counterfactual: whole pool, no mv gate
        tot_whk += 1
        if is_above:
            above_whk += 1
        # explicit-noHK on top of mv gate
        if ts not in mv_day:
            if _is_hk(ts):
                hk_skipped += 1
            continue
        if _is_hk(ts):
            continue  # belt: would be a leak if ever hit
        tot_nohk += 1
        if is_above:
            above_nohk += 1
    nohk = above_nohk / tot_nohk if tot_nohk else 0.0
    whk = above_whk / tot_whk if tot_whk else 0.0
    return cur, nohk, whk, tot_nohk, hk_skipped


def main() -> int:
    print("OPT-147 R-wide HK assertion (read-only)\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)

    per_ts = ctx["per_ts"]
    n_all = len(per_ts)
    n_hk_pool = sum(1 for ts in per_ts if _is_hk(ts))
    mv_hk = {ts for day in ctx["mv_map"].values() for ts in day if _is_hk(ts)}
    try:
        import psycopg  # noqa: E402
        from data_sync_service.config import get_settings  # noqa: E402

        conn = psycopg.connect(get_settings().database_url)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT ts_code) FROM bar_5min WHERE ts_code LIKE '%.HK'")
        n_bar5_hk = int(cur.fetchone()[0])
        conn.close()
    except Exception as exc:  # DB unreachable -> report, don't fail assertion
        n_bar5_hk = -1
        print(f"  (bar_5min HK count skipped: {exc})", flush=True)

    print(f"pool census: symbols={n_all} hk_in_daily={n_hk_pool} hk_in_mv={len(mv_hk)} hk_in_bar5min={n_bar5_hk}")
    assert n_hk_pool > 0, "expected HK names in daily pool (premise of OPT-147)"
    assert len(mv_hk) == 0, f"mv gate premise broken: {len(mv_hk)} HK names in stock_dailybasic"

    total_flips_nahk = 0
    for wname, (s, e) in WINDOWS.items():
        days = [d for d in ctx["cal"] if s <= d <= e]
        flips_nahk = flips_whk = gate_diff = 0
        max_abs = 0.0
        sum_abs = 0.0
        n = 0
        for day in days:
            cur, nohk, whk, pool_n, _ = _breadth_variants(ctx, day)
            n += 1
            if (cur > GATE) != (nohk > GATE):
                flips_nahk += 1
            if (cur > GATE) != (whk > GATE):
                flips_whk += 1
                gate_diff += 1
            diff = abs(cur - whk)
            sum_abs += diff
            max_abs = max(max_abs, diff)
        total_flips_nahk += flips_nahk
        print(
            f"{wname}: days={n} flips(cur-vs-noHK)={flips_nahk} "
            f"flips(cur-vs-withHK)={flips_whk} max|cur-withHK|={max_abs:.3f} mean|.|={sum_abs/max(n,1):.3f}"
        )

    print(f"\nverdict: flips(cur-vs-noHK) total={total_flips_nahk} -> ", end="")
    if total_flips_nahk == 0:
        print("CLOSE item (mv gate holds; add first-principles II.7 line). Engine untouched.")
    else:
        print("OPEN pre-reg for clip4 impact (do NOT fix engine in this OPT).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
