#!/usr/bin/env python3
"""Test S-3 gate variants impact on both standalone S-3 and pick-strong fused NAV.

Runs three windows (OOS2/train/valid) + past_year, comparing each variant vs baseline.

Variants tested (S-3 CN only, HK fixed):
  baseline      : gates=full (S3_CONFIG)
  gates_regime  : gates=regime
  gates_none    : gates=none
  rs0           : rs_rank_min=0 (no RS filter)
  rs03          : rs_rank_min=0.3 (looser)
  rs07          : rs_rank_min=0.7 (tighter)
  neutral_off   : neutral_block=False
  entry_score   : entry_style=score (disable auto)
  score60       : score_threshold=60
  no_exclude    : exclude_boards="" (include 300)

Both S-3 standalone (totalNetPnlPct/DD/sharpe) and pick-strong fused (trail8, window-local)
are reported per window with deltas vs baseline.

Usage:
  PYTHONPATH=src:scripts python3 scripts/test_s3_pickstrong_gates.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from run_walk_forward import S3_CONFIG, HK_S3_CONFIG, WINDOWS
import pick_strong_grid as pg

PAST_YEAR = ("2025-08-28", "2026-08-28")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR}
# only test OOS2/train/valid/past_year (long/holdout too heavy)
TEST_WINDOWS = ["OOS2", "train", "valid", "past_year"]

VARIANTS = [
    ("baseline", {}),
    ("gates_regime", {"gates": "regime"}),
    ("gates_none", {"gates": "none"}),
    ("rs0", {"rs_rank_min": 0.0}),
    ("rs03", {"rs_rank_min": 0.3}),
    ("rs07", {"rs_rank_min": 0.7}),
    ("neutral_off", {"neutral_block": False}),
    ("entry_score", {"entry_style": "score"}),
    ("score60", {"score_threshold": 60.0}),
    ("no_exclude300", {"exclude_boards": ""}),
]

def fmt(v):
    return f"{v:+.1f}"

def run():
    print("Fetching ETF closes ...", flush=True)
    etf_close = pg.fetch_etf_closes()
    # Pre-warm baseline caches for pick-strong comparison (will be reused for baseline)
    # For each variant we need to rebuild CN engine; HK stays baseline.
    # Cache HK per window once.
    hk_cache = {}
    for w in TEST_WINDOWS:
        s, e = ALL_WINDOWS[w]
        cfg_hk = BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
        data_hk = BacktestData(cfg_hk)
        run_hk = simulate(cfg_hk, data_hk)
        hk_cache[w] = {
            "data": data_hk,
            "run": run_hk,
            "snap": {str(r.get("date")): r for r in run_hk.positions_by_day},
        }
        print(f"  HK warm {w} done ({run_hk.summary.closed} trades)", flush=True)

    results = {}
    baseline_s3 = {}
    baseline_fused = {}

    for vid, overrides in VARIANTS:
        print(f"\n=== Variant {vid} {overrides} ===", flush=True)
        variant_s3 = {}
        variant_fused = {}
        for w in TEST_WINDOWS:
            s, e = ALL_WINDOWS[w]
            cfg_cn = BacktestConfig(start_date=s, end_date=e, **{**S3_CONFIG, **overrides})
            data_cn = BacktestData(cfg_cn)
            run_cn = simulate(cfg_cn, data_cn)
            s3_total = run_cn.summary.total_net_pnl_pct
            s3_dd = run_cn.summary.max_drawdown_pct
            s3_sharpe = run_cn.summary.sharpe or 0
            s3_closed = run_cn.summary.closed
            variant_s3[w] = (s3_total, s3_dd, s3_sharpe, s3_closed)
            # Build pick-strong fused NAV using this CN run + baseline HK run
            snap_cn = {str(r.get("date")): r for r in run_cn.positions_by_day}
            snap_hk = hk_cache[w]["snap"]
            # Build cache dict expected by build_nav_from_cache
            # Need close_by_ts, calendar etc. Use data_cn + HK data union
            hk_data = hk_cache[w]["data"]
            calendar = sorted(set(data_cn.calendar) | set(hk_data.calendar))
            close_by_ts = {**data_cn.close_by_ts_day, **hk_data.close_by_ts_day}
            # etf_ret derivation (same as warm_window)
            etf_ret = {}
            for k, mp in etf_close.items():
                days = sorted(mp.keys())
                ret = {}
                for i in range(1, len(days)):
                    d, prev = days[i], days[i-1]
                    if mp[prev] != 0:
                        ret[d] = mp[d]/mp[prev] - 1.0
                etf_ret[k] = ret
            cache = {
                "calendar": calendar,
                "close_by_ts": close_by_ts,
                "etf_close": etf_close,
                "etf_ret": etf_ret,
                "snap_cn": snap_cn,
                "snap_hk": snap_hk,
                "ts_days": {ts: sorted(mp.keys()) for ts, mp in close_by_ts.items()},
            }
            fused = pg.build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0, score="mom", top2=False, trail_pct=8.0)
            variant_fused[w] = (fused["fusedPct"], fused["maxDdFusedPct"], fused["switches"])
            print(f"  {w:10s} S3 {s3_total:+6.1f}% dd{s3_dd:4.1f} sr{s3_sharpe:4.2f} n{s3_closed:3d} | FUSED {fused['fusedPct']:+6.1f}% dd{fused['maxDdFusedPct']:4.1f} sw{fused['switches']:2d}", flush=True)
        results[vid] = {"s3": variant_s3, "fused": variant_fused}
        if vid == "baseline":
            baseline_s3 = variant_s3
            baseline_fused = variant_fused

    # Summary tables with deltas
    print("\n\n===== S-3 standalone Δ vs baseline (full) =====", flush=True)
    print("| variant | OOS2 Δ | train Δ | valid Δ | past_year Δ | note |", flush=True)
    print("|---------|--------|---------|---------|-------------|------|", flush=True)
    for vid, _ in VARIANTS:
        if vid == "baseline":
            continue
        row = results[vid]["s3"]
        deltas = []
        fails = []
        for w in TEST_WINDOWS:
            b = baseline_s3[w][0]
            v = row[w][0]
            d = v - b
            deltas.append(f"{d:+.1f}")
            if w in ("OOS2","train","valid") and d < -5:
                fails.append(w)
        note = "❌ " + ",".join(fails) if fails else "✅" if any(float(d) >= 2 for d in deltas[:3]) else "≈"
        print(f"| {vid:12s} | {deltas[0]:>6s} | {deltas[1]:>7s} | {deltas[2]:>7s} | {deltas[3]:>11s} | {note} |", flush=True)

    print("\n===== Pick-strong fused (trail8) Δ vs baseline =====", flush=True)
    print("| variant | OOS2 Δ | train Δ | valid Δ | past_year Δ | verdict |", flush=True)
    print("|---------|--------|---------|---------|-------------|---------|", flush=True)
    for vid, _ in VARIANTS:
        if vid == "baseline":
            continue
        row = results[vid]["fused"]
        deltas = []
        fails = []
        for w in TEST_WINDOWS:
            b = baseline_fused[w][0]
            v = row[w][0]
            d = v - b
            deltas.append(f"{d:+.1f}")
            if w in ("OOS2","train","valid") and d < -5:
                fails.append(w)
        verdict = "❌ " + ",".join(fails) if fails else "✅" if any(float(d) >= 5 for d in deltas[:3]) else ("≈" if not fails else "❌")
        # stricter: need no fails and at least one +2
        ups = sum(1 for d in deltas[:3] if float(d) >= 2)
        if not fails and ups >= 1:
            verdict = "✅ +" + ",".join([w for w,d in zip(TEST_WINDOWS, deltas) if float(d)>=2])
        elif fails:
            verdict = "❌ " + ",".join(fails)
        else:
            verdict = "≈"
        print(f"| {vid:12s} | {deltas[0]:>6s} | {deltas[1]:>7s} | {deltas[2]:>7s} | {deltas[3]:>11s} | {verdict} |", flush=True)

    print("\n===== Baseline fused absolute =====", flush=True)
    for w in TEST_WINDOWS:
        v, dd, sw = baseline_fused[w]
        print(f"  {w:10s} {v:+6.1f}% dd{dd:4.1f} sw{sw}", flush=True)
    print("\n===== Baseline S-3 absolute =====", flush=True)
    for w in TEST_WINDOWS:
        v, dd, sr, n = baseline_s3[w]
        print(f"  {w:10s} {v:+6.1f}% dd{dd:4.1f} sr{sr:.2f} n{n}", flush=True)

if __name__ == "__main__":
    run()
