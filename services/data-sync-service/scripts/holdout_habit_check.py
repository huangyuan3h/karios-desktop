#!/usr/bin/env python3
"""S1: habit holdout audit — READ-ONLY validation, no tuning on the outcome.

Replays the frozen habit Live recipe (C1 3% same_1430, body=3, day-3 1430
exit, amp rank, top-1/3, R-wide 0.5, clip4 strict, opp_50) on sessions AFTER
the recipe freeze (2026-08-10~2026-09-03, 19 sessions with 1430 prints).

Rule: whatever this shows, no parameter changes. A weak holdout means the
valid cushion (+2.7) was thin, not that we get to re-tune. A strong holdout
does not license new variants either.
"""

from __future__ import annotations

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

START = "2026-08-10"
END = "2026-09-03"
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
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def main() -> int:
    print(f"holdout audit {START}~{END} (recipe frozen, read-only)\n")
    ctx = load_sgap_context("2026-08-01", END)
    sat = replay_sgap_from_context(
        ctx, start=START, end=END, skip_t1_limit=True, pool_mode="strict",
        max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430, fill_hhmm="1430",
        exit_hhmm="1430", max_open_to_1430_pct=0.03,
    )
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    sat_nav = [float(r["satNav"]) for r in rows]
    if sat_nav and sat_nav[0] > 0:
        sat_nav = [v / sat_nav[0] for v in sat_nav]
    active = [bool(r.get("satActive")) for r in rows]
    etf_close = fetch_etf_closes()
    cache = warm_window(START, END, etf_close)
    r = build_nav_from_cache(
        cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
        score="mom", top2=False, trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    core = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        core.append(last)
    n = min(len(core), len(sat_nav))
    twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
    core_m, twin_m, sat_m = _stats(core[:n]), _stats(twin), _stats(sat_nav[:n])
    summary = sat.get("summary") or {}
    print(f"sessions={len(dates)} fills={summary.get('fillCount')} "
          f"skipC1={summary.get('skipC1Count')}")
    print(f"core {core_m['total_pct']:+.1f}/{core_m['sharpe']:.2f}/{core_m['max_dd']:.1f}")
    print(f"twin {twin_m['total_pct']:+.1f}/{twin_m['sharpe']:.2f}/{twin_m['max_dd']:.1f}  "
          f"Δ {twin_m['total_pct'] - core_m['total_pct']:+.1f}")
    print(f"sat  {sat_m['total_pct']:+.1f}/{sat_m['sharpe']:.2f}/{sat_m['max_dd']:.1f}")
    payload = {
        "tag": "sat-holdout-2026-09-04",
        "window": [START, END],
        "rule": "read-only; no tuning on the outcome",
        "core": core_m, "twin": twin_m, "sat": sat_m,
        "fills": summary.get("fillCount"),
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / "sat_holdout_2026-09-04.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
