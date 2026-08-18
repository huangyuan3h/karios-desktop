"""Signal pool · P5/P6 — dual-MA golden cross + three-line alignment.

P5 (event): entry allowed within N sessions after MA5 crossed above MA20
(dual-MA golden cross). Event-driven, NOT the A2 alignment state.
P6 (state): MA5 > MA10 > MA20 required (三线多头排列) — expected to
repeat the A2 falsification; included to close the question.

Variants:
- base    (both off)
- C3      P5: entry within 3 sessions of the golden cross
- C5      P5: entry within 5 sessions
- C10     P5: entry within 10 sessions
- A1      P6: MA5 > MA10 > MA20 required

Verdict rule (todo §19 + planned-doc §3): three-window 0-degradation +
single-window improvement; valid >= 30 triggered trades.
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
    "base": (0, False),
    "C3": (3, False),
    "C5": (5, False),
    "C10": (10, False),
    "A1": (0, True),
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
    ap.add_argument("--variants", default="base,C3,C5,C10,A1")
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
            cross, aligned = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                ma_cross_days=cross,
                ma_aligned=aligned,
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
            cross, aligned = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                ma_cross_days=cross,
                ma_aligned=aligned,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d}"
            )

    json.dump(results, open("data/backtest_reports/signal_p56_ma_cross_aligned.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p56_ma_cross_aligned.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
