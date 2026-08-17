"""TIP-014 · D6 — profit-trail (A6) re-validation on the current baseline.

A6 (2026-08-12 defensive round) was rejected then: trailing -8% -> -6/-5 lost
money by cutting winning legs early. D6 re-runs it on TODAY's baseline (which
now has D2 max_hold_env_shorten, D3 env sizing, E2 panic=2, entry_style auto)
— the profit-trail may behave differently now that winning legs are already
trimmed at 45 days and sized up on uptrend days.

Mechanism (engine): once a leg's peak gain >= trigger, the allowed pullback
from peak tightens from trailing_stop (-8) to profit_trail_pct (e.g. -4).
Variants:
- base      (off)
- t10-4      trigger 10% -> trail -4
- t10-6      trigger 10% -> trail -6
- t15-6      trigger 15% -> trail -6
- t15-8      trigger 15% -> trail -8 (= trailing, only arms it)
- t20-6      trigger 20% -> trail -6

Verdict rule (todo §19): three-window 0-degradation + single-window
improvement; one-window fluke = overfit, reject.
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
    "base": (0.0, 0.0),
    "t10-4": (10.0, -4.0),
    "t10-6": (10.0, -6.0),
    "t15-6": (15.0, -6.0),
    "t15-8": (15.0, -8.0),
    "t20-6": (20.0, -6.0),
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
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--variants", default="base,t10-4,t10-6,t15-6,t15-8,t20-6")
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
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s}")
        for v in variants:
            trigger, trail = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                profit_trail_trigger_pct=trigger,
                profit_trail_pct=trail,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results[name][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d}"
            )

    if args.long:
        results["LONG"] = {}
        print("\n=== LONG (2021-08..2026-08) ===")
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s}")
        for v in variants:
            trigger, trail = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                profit_trail_trigger_pct=trigger,
                profit_trail_pct=trail,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d}"
            )

    json.dump(results, open("data/backtest_reports/tip014_d6_profit_trail.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_d6_profit_trail.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
