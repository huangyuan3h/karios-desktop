"""TIP-014 · 板块画像 — industry × environment × window attribution.

For each window, replay the S-3 baseline and bucket every CLOSED trade by:
- industry (data.industry_by_ts, Eastmoney EM industry)
- environment of its ENTRY day (env_label)
- window year

Answers: which industries actually paid in uptrend vs fan vs weak days?
Where is the alpha concentrated? (extends TIP-014's environment×style work —
user asked: 不同板块的特点，主升买什么板块、电风扇买什么板块、防御期买什么板块)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict

from data_sync_service.service import backtest_engine as be
from data_sync_service.service.env_label import (
    ENV_FAN,
    ENV_NEUTRAL,
    ENV_UPTREND,
    ENV_WEAK,
    load_env_by_day,
)
from data_sync_service.service.trendok import _symbol_to_ts_code

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

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
    "panic_cooldown_days": 3,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    "atr_stop_mult": 2.0,
    "atr_stop_strong_only": True,
    "neutral_block": True,
}


def _ts_of(symbol: str) -> str | None:
    parsed = _symbol_to_ts_code(symbol)
    return parsed[2] if parsed else None


def ret5_of(closes: list, day: str) -> float | None:
    idx = None
    for i, (d, _c) in enumerate(closes):
        if str(d) == day:
            idx = i
            break
    if idx is None or idx < 5:
        return None
    c_prev = closes[idx - 5][1]
    c_now = closes[idx][1]
    if c_prev <= 0:
        return None
    return (c_now / c_prev - 1.0) * 100.0


def classify_style(rs: float | None, ret5: float | None) -> str:
    if rs is None or rs < 0.8:
        return "low_rs"
    if ret5 is None:
        return "no_ret5"
    if ret5 <= -5.0:
        return "dip"
    if ret5 >= -3.0:
        return "momentum"
    return "between"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--min-trades", type=int, default=5, help="minimum trades to show an industry")
    args = ap.parse_args()

    results: dict[str, dict] = {}
    for name in args.windows.split(","):
        if name not in WINDOWS:
            print(f"unknown window {name}", file=sys.stderr)
            return 2

    for name in args.windows.split(","):
        s, e = WINDOWS[name]
        cfg = be.BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = be.BacktestData(cfg)
        env = load_env_by_day(s, e)
        run = be.simulate(cfg, data=data)

        # ind -> env -> [pnl]
        by_ind_env: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        ind_total: dict[str, list[float]] = defaultdict(list)
        # style attribution inside each industry (TIP-014 extension)
        by_ind_style: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        for t in run.trades:
            ts = _ts_of(t.symbol)
            ind = data.industry_by_ts.get(ts, "?") if ts else "?"
            e_name = env.get(t.entry_date, ENV_NEUTRAL)
            by_ind_env[ind][e_name].append(t.pnl_pct)
            ind_total[ind].append(t.pnl_pct)
            rs = data.rs_rank_by_day.get(t.entry_date, {}).get(ts) if ts else None
            closes = data.closes_by_ts.get(ts) if ts else None
            ret5 = ret5_of(closes, t.entry_date) if closes else None
            st = classify_style(rs, ret5)
            by_ind_style[ind][st].append(t.pnl_pct)

        results[name] = {}
        print(f"\n=== {name} ({s}..{e}) · trades={len(run.trades)} ===")
        ranked = sorted(
            ind_total.items(),
            key=lambda kv: (sum(kv[1]) / len(kv[1])),
            reverse=True,
        )
        print(f"{'行业':<12s} {'n':>4s} {'avg%':>7s} {'sum%':>9s}   uptrend/fan/weak/neutral avg")
        for ind, pnls in ranked:
            if len(pnls) < args.min_trades:
                continue
            avg = sum(pnls) / len(pnls)
            parts = []
            for e2 in (ENV_UPTREND, ENV_FAN, ENV_WEAK, ENV_NEUTRAL):
                v = by_ind_env[ind].get(e2)
                if v:
                    parts.append(f"{e2[:4]}={sum(v)/len(v):+.1f}%({len(v)})")
                else:
                    parts.append(f"{e2[:4]}=—")
            print(
                f"{ind:<12s} {len(pnls):>4d} {avg:>+7.2f}% {sum(pnls):>+9.1f}%   "
                + "  ".join(parts)
            )
            results[name][ind] = {
                "n": len(pnls),
                "avg": round(avg, 2),
                "sum": round(sum(pnls), 1),
                "env": {
                    e2: {"n": len(v), "avg": round(sum(v) / len(v), 2)}
                    for e2, v in by_ind_env[ind].items()
                    if v
                },
                "style": {
                    st: {"n": len(v), "avg": round(sum(v) / len(v), 2)}
                    for st, v in by_ind_style[ind].items()
                    if v
                },
            }

        # Style attribution inside the top industries (same window).
        top_inds = [ind for ind, pnls in ranked if len(pnls) >= args.min_trades][:5]
        if top_inds:
            print("  风格细分（top 行业内部 momentum/dip/low_rs）:")
            for ind in top_inds:
                parts = []
                for st in ("momentum", "dip", "low_rs", "between", "no_ret5"):
                    v = by_ind_style[ind].get(st)
                    if v:
                        parts.append(f"{st}={sum(v)/len(v):+.1f}%({len(v)})")
                print(f"    {ind:<10s} " + "  ".join(parts))

    json.dump(results, open("data/backtest_reports/tip014_industry_profile.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_industry_profile.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
