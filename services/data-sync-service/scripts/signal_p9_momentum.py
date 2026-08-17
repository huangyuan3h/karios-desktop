"""Signal pool · P9 — 12-1 cross-sectional momentum (ADDITIVE entry gate).

Hypothesis: mid-horizon (120-250 session) momentum confirms the trend that
20d RS only samples near-term strength. A-share short-term reversal is well
documented, so the recent window is SKIPPED (skip 20d): mom = ret from
(day - skip - ret_days) to (day - skip). Ranked cross-sectionally against
the whole market (same pool as RS ranks); the candidate must be in the top
mom_rank_min.

Pre-check (signal_p10_p9_correlation.py, 2026-08-15) — REQUIRED by
planned-doc §3.3 (RS=20d short horizon vs P9=120-250d mid horizon):
- whole-market |r| vs RS: p9_mom120_skip20 OOS2 0.084 / train 0.055 / valid 0.098
- whole-market |r| vs RS: p9_mom250_skip20 OOS2 0.057 / train 0.044 / valid 0.071
→ all << 0.5 — horizons genuinely disjoint ✅ (vs P10's 0.37-0.51 which
failed at the trading level anyway)

Variants (ret_days / skip / rank-min):
- base   (off)
- M120S20P30  120d skip 20d, top 30%
- M120S20P50  120d skip 20d, top 50%
- M250S20P30  250d skip 20d, top 30%
- M250S20P50  250d skip 20d, top 50%

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

# variant -> (ret_days, skip_days, rank_min)
VARIANTS = {
    "base": (0, 20, 0.5),
    "M120S20P30": (120, 20, 0.3),
    "M120S20P50": (120, 20, 0.5),
    "M250S20P30": (250, 20, 0.3),
    "M250S20P50": (250, 20, 0.5),
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
        "momBlock": int((s.gated_blocks or {}).get("mom", 0)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--variants", default="base,M120S20P30,M120S20P50,M250S20P30,M250S20P50")
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
        print(f"  {'variant':12s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'momBlk':>6s}")
        for v in variants:
            ret, skip, rank = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=s, end_date=e, **S3_CONFIG,
                mom_ret_days=ret,
                mom_skip_days=skip,
                mom_rank_min=rank,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results[name][v] = m
            print(
                f"  {v:12s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['momBlock']:6d}"
            )

    if args.long:
        results["LONG"] = {}
        print("\n=== LONG (2021-08..2026-08) ===")
        print(f"  {'variant':12s} {'pnl%':>8s} {'dd%':>6s} {'calmar':>7s} {'sharpe':>7s} {'win':>6s} {'closed':>6s} {'momBlk':>6s}")
        for v in variants:
            ret, skip, rank = VARIANTS[v]
            cfg = be.BacktestConfig(
                start_date=LONG_WINDOW[0], end_date=LONG_WINDOW[1], **S3_CONFIG,
                mom_ret_days=ret,
                mom_skip_days=skip,
                mom_rank_min=rank,
            )
            run = be.simulate(cfg)
            m = summarize(run)
            results["LONG"][v] = m
            print(
                f"  {v:12s} {m['pnl']:+8.1f} {m['dd']:6.1f} {m['calmar']:7.2f} "
                f"{m['sharpe']:7.2f} {m['win']*100:5.1f}% {m['closed']:6d} {m['momBlock']:6d}"
            )

    json.dump(results, open("data/backtest_reports/signal_p9_momentum.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/signal_p9_momentum.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
