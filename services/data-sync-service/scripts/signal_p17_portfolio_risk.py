"""Signal pool · P17 — portfolio-level risk controls (each sub-item alone).

Hypothesis (planned-doc §2 P17): the strategy loses to drawdown / position
/ exposure, not to stock selection. Sub-items tested separately so any
effect is attributable:

- LIQ1/LIQ2/LIQ5 — liquidity floor: 60d avg daily turnover >= 1/2/5 亿元
- T10/T20/T30    — unprofitable time stop: cut a holding still underwater
                    after 10/20/30 days (frees capital tied in flat losers)
- IND2/IND3      — industry cap: at most 2/3 holdings per industry
                    (20-30% concentration guard; deliberately NOT the 8/12
                    of D5 which cut alpha — this only prevents extreme
                    concentration)

Already covered elsewhere (not re-tested here): ATR stop (OPT-105), max
hold 60d (D2: env-shortened to 45 on uptrend entries), volatility-target
sizing (atr_size_* rejected 2026-08-09).

Verdict rule (todo §19 + planned-doc §3): three-window 0-degradation +
single-window improvement; valid >= 30 trades; a risk-control sub-item may
pass with flat pnl if DD/Calmar/sharpe improve meaningfully, but pnl may
not degrade >5pt in any window.
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

# variant -> (min_avg_amount 亿, max_hold_unprofitable_days, max_per_industry)
VARIANTS = {
    "base": (0.0, 0, 0),
    "LIQ1": (1.0, 0, 0),
    "LIQ2": (2.0, 0, 0),
    "LIQ5": (5.0, 0, 0),
    "T10": (0.0, 10, 0),
    "T20": (0.0, 20, 0),
    "T30": (0.0, 30, 0),
    "IND2": (0.0, 0, 2),
    "IND3": (0.0, 0, 3),
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
        "liqBlock": int((s.gated_blocks or {}).get("liquidity", 0)),
        "indBlock": int((s.gated_blocks or {}).get("industry_cap", 0)),
        "timeStop": sum(1 for t in run.trades if t.close_reason == "time_stop"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--variants", default="base,LIQ1,LIQ2,LIQ5,T10,T20,T30,IND2,IND3")
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
        print(f"  {'variant':7s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'liqBlk':>6s} {'indBlk':>6s} {'tStop':>5s}")
        for v in variants:
            amt, tstop, indcap = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                min_avg_amount=amt,
                max_hold_unprofitable_days=tstop,
                max_per_industry=indcap,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results[name][v] = m
            print(
                f"  {v:7s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['liqBlock']:6d} {m['indBlock']:6d} {m['timeStop']:5d}"
            )

    if args.long:
        results["LONG"] = {}
        print("\n=== LONG (2021-08..2026-08) ===")
        print(f"  {'variant':7s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'liqBlk':>6s} {'indBlk':>6s} {'tStop':>5s}")
        for v in variants:
            amt, tstop, indcap = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                min_avg_amount=amt,
                max_hold_unprofitable_days=tstop,
                max_per_industry=indcap,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:7s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['liqBlock']:6d} {m['indBlock']:6d} {m['timeStop']:5d}"
            )

    json.dump(results, open("data/backtest_reports/signal_p17_portfolio_risk.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p17_portfolio_risk.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
