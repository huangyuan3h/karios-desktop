"""Signal pool · P10 — 52-week-high proximity (ADDITIVE entry gate).

Hypothesis: candidates whose close sits close to their 250-session high are
in the continuation phase of an uptrend ("投资者锚定高点" — behavioral
anchoring); a continuous proximity gate is smoother than P1's discrete
Donchian breakout (a name can be very strong without printing a new high).

⚠️ Pre-check (signal_p10_p9_correlation.py, 2026-08-15): A2's falsified
trend score contained a "distance to 52w high" component, so RS overlap was
measured FIRST:
- candidate-pool |r| vs RS rank: OOS2 0.185 / train 0.260 / valid 0.252 — all < 0.5 ✅
- whole-market |r|: OOS2 0.366 / train 0.446 / valid 0.510 — valid borderline ⚠️
→ Gate operates on the candidate pool (where correlation is low), so the
experiment is justified; the borderline whole-market number is documented
in the verdict and any single-window gain is treated extra cautiously.

Variants (threshold % of 250d high):
- base  (off)
- H80   close >= 0.80 x 250d-high
- H85   close >= 0.85 x 250d-high
- H90   close >= 0.90 x 250d-high
- H95   close >= 0.95 x 250d-high

Verdict rule (todo §19 + planned-doc §3):
- three-window 0-degradation + single-window improvement (long window extra)
- valid >= 30 triggered trades or the verdict degrades to "direction only"
"""

from __future__ import annotations

import argparse
import json
import sys

from data_sync_service.service import backtest_engine as be

S3_CONFIG = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 2,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    "atr_stop_mult": 2.0,
    "atr_stop_strong_only": True,
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
    "neutral_block": True,
    "max_hold_env_shorten": 45,
    "env_position_scale": "uptrend:1.25,fan:0.75",
}

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
LONG_WINDOW = ("2021-08-01", "2026-08-07")

VARIANTS = {
    "base": 0.0,
    "H80": 80.0,
    "H85": 85.0,
    "H90": 90.0,
    "H95": 95.0,
}


def summarize(run: be.BacktestRun) -> dict:
    s = run.summary
    return {
        "pnl": round(float(s.total_net_pnl_pct or 0.0), 1),
        "dd": round(float(s.max_drawdown_pct or 0.0), 1),
        "sharpe": round(float(s.sharpe or 0.0), 2),
        "win": round(float(s.win_rate or 0.0), 4),
        "closed": int(s.closed or 0),
        "calmar": round(float(s.total_net_pnl_pct or 0.0) / abs(float(s.max_drawdown_pct or 1.0)), 2)
        if float(s.max_drawdown_pct or 0.0) > 0
        else 0.0,
        "h52wBlock": int((s.gated_blocks or {}).get("high52w", 0)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--variants", default="base,H80,H85,H90,H95")
    ap.add_argument("--long", action="store_true", help="also run the 2021-08 long window")
    args = ap.parse_args()

    windows = args.windows.split(",")
    variants = args.variants.split(",")
    for v in variants:
        if v not in VARIANTS:
            print(f"unknown variant {v}", file=sys.stderr)
            return 2

    results: dict[str, dict] = {}
    for name in windows:
        if name not in WINDOWS:
            print(f"unknown window {name}", file=sys.stderr)
            return 2
        s, e = WINDOWS[name]
        results[name] = {}
        print(f"\n=== {name} ({s}..{e}) ===")
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'h52wBlk':>7s}")
        for v in variants:
            th = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                high_52w_min_pct=th,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results[name][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['h52wBlock']:7d}"
            )

    if args.long:
        results["LONG"] = {}
        print("\n=== LONG (2021-08..2026-08) ===")
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'h52wBlk':>7s}")
        for v in variants:
            th = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                high_52w_min_pct=th,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['h52wBlock']:7d}"
            )

    json.dump(results, open("data/backtest_reports/signal_p10_high52w.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p10_high52w.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
