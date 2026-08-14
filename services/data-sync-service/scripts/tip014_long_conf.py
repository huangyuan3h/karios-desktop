"""TIP-014 · long-window confidence check — per-year PnL, old vs new config.

Runs the S-3 config twice over the full 2021-08..2026-08 window:
- ``old``: the pre-TIP-014 config (no neutral_block / no entry_style)
- ``new``: current S3_CONFIG (neutral_block=True + entry_style=auto RS0.7 dip3%)

and shows per-year totals so we can see exactly WHERE the new rules act
(sentiment data exists only from 2026-01 → only 2026 years can differ) and
whether they hold up in the bear years (2022 / 2023) that were untouched.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

from data_sync_service.service import backtest_engine as be

S3_NEW = {
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
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
}
S3_OLD = {k: v for k, v in S3_NEW.items() if k not in ("neutral_block", "entry_style", "entry_style_rs_min", "entry_style_dip_min")}

WINDOW = ("2021-08-01", "2026-08-07")


def per_year(run) -> dict[str, list[float]]:
    out: dict[str, list[float]] = defaultdict(list)
    for t in run.trades:
        out[t.entry_date[:4]].append(t.pnl_pct)
    return out


def main() -> int:
    out = {"window": WINDOW}
    print(f"window {WINDOW[0]} .. {WINDOW[1]}")
    print(f"{'年':<6s} {'old 笔':>6s} {'old%':>8s} {'new 笔':>6s} {'new%':>8s} {'Δpt':>7s}   {'old avg':>8s} {'new avg':>8s}")
    rows = []
    for tag, cfg in (("old", S3_OLD), ("new", S3_NEW)):
        c = be.BacktestConfig(start_date=WINDOW[0], end_date=WINDOW[1], **cfg)
        data = be.BacktestData(c)
        run = be.simulate(c, data=data)
        years = per_year(run)
        out[tag] = {
            "total": round(run.summary.total_net_pnl_pct, 1),
            "maxDd": round(run.summary.max_drawdown_pct, 1),
            "closed": run.summary.closed,
            "winRate": round(run.summary.win_rate, 3),
            "byYear": {y: {"n": len(v), "sum": round(sum(v), 1)} for y, v in sorted(years.items())},
        }
        rows.append((tag, run, years))

    old_run, new_run = rows[0][1], rows[1][1]
    old_years, new_years = rows[0][2], rows[1][2]
    for y in sorted(set(old_years) | set(new_years)):
        o = old_years.get(y, [])
        n = new_years.get(y, [])
        os_, ns = sum(o), sum(n)
        oavg = os_ / len(o) if o else 0.0
        navg = ns / len(n) if n else 0.0
        print(
            f"{y:<6s} {len(o):>6d} {os_:>+8.1f} {len(n):>6d} {ns:>+8.1f} {ns-os_:>+7.1f}   {oavg:>+8.2f} {navg:>+8.2f}"
        )
    print(f"\nold: total {out['old']['total']:+.1f}% dd {out['old']['maxDd']} win {out['old']['winRate']:.1%} ({out['old']['closed']}笔)")
    print(f"new: total {out['new']['total']:+.1f}% dd {out['new']['maxDd']} win {out['new']['winRate']:.1%} ({out['new']['closed']}笔)")
    print(f"Δ   {out['new']['total']-out['old']['total']:+.1f}pt · DD {out['old']['maxDd']}→{out['new']['maxDd']} · 胜率 {out['old']['winRate']:.1%}→{out['new']['winRate']:.1%}")
    json.dump(out, open("data/backtest_reports/tip014_long_conf.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_long_conf.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
