"""Signal pool · P11 — industry-neutral RS / industry momentum (ADDITIVE entry gates).

Two independent sub-filters, both computed per day from the FULL daily table
(equal-weighted industry window return; industry membership =
stock_eastmoney_industry, the same table the mainline gate uses):

- M{20,60,120}  industry momentum: the candidate's industry must rank in the
                top 1/3 of ALL industries by window average return.
                ("先选强势行业" — price-return dimension, distinct from the
                fund-flow Top3 mainline gate.)
- N{60,120}     within-industry RS: the candidate must rank in the top 30%
                of its own industry by window return. ("行业内选强" — the
                whole-market RS rank cannot see this: a weak industry's
                strongest name ranks low globally.)

Variant matrix (windows per planned-doc §2 P11):
- base  (off)
- M20   industry momentum 20d, top 1/3
- M60   industry momentum 60d, top 1/3
- N60   within-industry RS 60d, top 30%
- N120  within-industry RS 120d, top 30%
- M60N60 both, 60d

Verdict rule (todo §19 + planned-doc §3):
- three-window 0-degradation + single-window improvement (long window extra)
- valid >= 30 triggered trades or the verdict degrades to "direction only"
- hypothesis: fewer industry-bucket losses (higher concentration of strong
  names) — sharpe/DD improvement counts even at flat pnl, but no window may
  degrade >5pt.
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

# variant -> (ind_mom_days, ind_neutral_days)
VARIANTS = {
    "base": (0, 0),
    "M20": (20, 0),
    "M60": (60, 0),
    "N60": (0, 60),
    "N120": (0, 120),
    "M60N60": (60, 60),
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
        "indMomBlock": int((s.gated_blocks or {}).get("ind_mom", 0)),
        "indNeutralBlock": int((s.gated_blocks or {}).get("ind_neutral", 0)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--variants", default="base,M20,M60,N60,N120,M60N60")
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
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'momBlk':>6s} {'neuBlk':>6s}")
        for v in variants:
            mom, neu = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                ind_mom_days=mom,
                ind_neutral_days=neu,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results[name][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['indMomBlock']:6d} {m['indNeutralBlock']:6d}"
            )

    if args.long:
        results["LONG"] = {}
        print(f"\n=== LONG (2021-08..2026-08) ===")
        print(f"  {'variant':8s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'momBlk':>6s} {'neuBlk':>6s}")
        for v in variants:
            mom, neu = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                ind_mom_days=mom,
                ind_neutral_days=neu,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:8s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['indMomBlock']:6d} {m['indNeutralBlock']:6d}"
            )

    json.dump(results, open("data/backtest_reports/signal_p11_industry_neutral.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p11_industry_neutral.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
