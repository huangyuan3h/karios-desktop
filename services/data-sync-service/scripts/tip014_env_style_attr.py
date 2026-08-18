"""TIP-014 · Phase 1c — environment × style attribution.

For a window, replay the baseline (no style filter) and bucket every CLOSED
trade by the environment of its ENTRY day, then show realised pnl per bucket
per "would-be style" (momentum vs dip) using the SAME threshold definitions
as the engine. This answers: in uptrend days did momentum names actually pay?
In fan days did dip names actually pay? — without re-running the engine 9×.
"""

from __future__ import annotations

import argparse
import json
import sys

from data_sync_service.service import backtest_engine as be
from data_sync_service.service.env_label import (
    ENV_FAN,
    ENV_NEUTRAL,
    ENV_UPTREND,
    ENV_WEAK,
    load_env_by_day,
)
from data_sync_service.service.trendok import _symbol_to_ts_code

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
}

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

STYLE_RS_MIN = 0.8
STYLE_DIP_MIN = -5.0
STYLE_DIP_MAX = -3.0


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
    if rs is None or rs < STYLE_RS_MIN:
        return "low_rs"
    if ret5 is None:
        return "no_ret5"
    if ret5 <= STYLE_DIP_MIN:
        return "dip"
    if ret5 >= STYLE_DIP_MAX:
        return "momentum"
    return "between"


def _ts_of(symbol: str) -> str | None:
    parsed = _symbol_to_ts_code(symbol)
    return parsed[2] if parsed else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid")
    args = ap.parse_args()

    for name in args.windows.split(","):
        if name not in WINDOWS:
            print(f"unknown window {name}", file=sys.stderr)
            return 2
    results: dict[str, dict] = {}
    for name in args.windows.split(","):
        s, e = WINDOWS[name]
        cfg = be.BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = be.BacktestData(cfg)
        env = load_env_by_day(s, e)
        run = be.simulate(cfg, data=data)

        buckets: dict[str, dict] = {
            k: {"dip": [], "momentum": [], "low_rs": [], "between": [], "no_ret5": [], "total": []}
            for k in (ENV_UPTREND, ENV_FAN, ENV_WEAK, ENV_NEUTRAL)
        }
        for t in run.trades:
            e_name = env.get(t.entry_date, ENV_NEUTRAL)
            b = buckets[e_name]
            b["total"].append(t.pnl_pct)
            ts = _ts_of(t.symbol)
            if ts is None:
                b["no_ret5"].append(t.pnl_pct)
                continue
            rs = data.rs_rank_by_day.get(t.entry_date, {}).get(ts)
            closes = data.closes_by_ts.get(ts)
            ret5 = ret5_of(closes, t.entry_date) if closes else None
            st = classify_style(rs, ret5)
            b.setdefault(st, []).append(t.pnl_pct)

        results[name] = {}
        print(f"\n=== {name} ({s}..{e}) ===  trades={len(run.trades)}")
        for e_name, b in buckets.items():
            if not b["total"]:
                continue
            total = sum(b["total"])
            n = len(b["total"])
            avg = total / n
            line = f"  {e_name:8s} n={n:3d}  avg={avg:+6.2f}%  sum={total:+8.1f}%"
            for st in ("dip", "momentum", "low_rs", "between", "no_ret5"):
                if b.get(st):
                    bt = sum(b[st])
                    bn = len(b[st])
                    line += f"   {st:9s} n={bn:3d} avg={bt / bn:+6.2f}% sum={bt:+7.1f}%"
            print(line)
            results[name][e_name] = {
                k: {"n": len(v), "sum": round(sum(v), 2), "avg": round(sum(v) / len(v), 2) if v else 0}
                for k, v in b.items()
                if v
            }
    json.dump(results, open("data/backtest_reports/tip014_env_style.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_env_style.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
