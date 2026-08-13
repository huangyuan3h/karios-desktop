"""Monte Carlo confidence analysis for the S-3 strategy (block bootstrap).

Runs ONE full-window simulate (same engine/caliber as the walk-forward),
builds the account-level daily PnL series from closed trades (close-date
bucketed, pnl x position_pct), then resamples it 5000x with a block
bootstrap (block=5 keeps short-term dependence clusters together). Outputs
the distribution of total net PnL / max drawdown / sharpe with 5/25/50/75/95
percentiles, plus where the single backtest result sits in that
distribution.

Caliber note: trade/close-date level, so DD/sharpe are directional
references (the engine has no intra-position mark-to-market). The total-PnL
distribution is the reliable output.

Usage:
  PYTHONPATH=src python3 scripts/run_monte_carlo.py --market CN
  PYTHONPATH=src python3 scripts/run_monte_carlo.py --market HK --iters 2000
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from pathlib import Path

REPORT_DIR = Path(__file__).resolve().parent.parent / "data" / "backtest_reports"

WINDOWS = {
    "CN": ("2021-08-01", "2026-08-11"),  # LONG_WINDOW_CN (strategy-params §1)
    "HK": ("2022-06-01", "2026-08-07"),  # HK qfq reseed start (2026-08-10)
}


def _load_config(market: str) -> dict[str, float | int | str]:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # type: ignore

    return dict(HK_S3_CONFIG if market == "HK" else S3_CONFIG)


def _daily_returns(run) -> list[float]:
    """Account-level daily PnL pct bucketed by close date (sorted by date)."""
    by_day: dict[str, float] = {}
    for t in run.trades:
        # Account-level contribution: trade net pnl pct x position weight
        # (e.g. +10.5% x 0.10 sleeve = +1.05% of the account that day).
        by_day[t.close_date] = by_day.get(t.close_date, 0.0) + (
            t.pnl_pct * t.position_pct
        )
    return [by_day[d] for d in sorted(by_day)]


def _block_bootstrap(seq: list[float], block: int, rng: random.Random) -> list[float]:
    """Resample ``seq`` to equal length using contiguous blocks (circular)."""
    if not seq:
        return []
    n = len(seq)
    out: list[float] = []
    while len(out) < n:
        start = rng.randrange(n)
        chunk = [seq[(start + i) % n] for i in range(block)]
        out.extend(chunk)
    return out[:n]


def _metrics(curve: list[float]) -> dict[str, float]:
    """Total net pnl pct, max drawdown pct, trade-level sharpe (annualized)."""
    total = sum(curve)
    # NAV curve from 100 (account % points) for a sane drawdown divisor.
    nav = 100.0
    peak_nav = 100.0
    dd = 0.0
    for x in curve:
        nav += x
        if nav > peak_nav:
            peak_nav = nav
        if peak_nav > 0:
            dd = min(dd, (nav - peak_nav) / peak_nav * 100.0)
    mean = statistics.mean(curve) if curve else 0.0
    std = statistics.stdev(curve) if len(curve) > 1 else 0.0
    sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
    return {"totalPct": round(total, 2), "maxDDpct": round(dd, 2), "sharpe": round(sharpe, 3)}


def _percentiles(values: list[float], pcts: list[float]) -> list[float]:
    s = sorted(values)
    n = len(s)
    return [s[min(n - 1, int(p / 100.0 * n))] for p in pcts]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--market", choices=["CN", "HK"], default="CN")
    ap.add_argument("--iters", type=int, default=5000)
    ap.add_argument("--block", type=int, default=5)
    ap.add_argument("--seed", type=int, default=20260813)
    ap.add_argument("--json", default="")
    args = ap.parse_args()

    start, end = WINDOWS[args.market]
    config = _load_config(args.market)
    from data_sync_service.service.backtest_engine import BacktestConfig, simulate

    cfg = BacktestConfig(start_date=start, end_date=end, **config)
    print(f"[{args.market}] simulate {start} .. {end} ...")
    run = simulate(cfg)
    base = _metrics(_daily_returns(run))
    print(
        f"  single run: closed={len(run.trades)} total={base['totalPct']:+.1f}% "
        f"dd={base['maxDDpct']:.1f}% sharpe={base['sharpe']}"
    )

    daily = _daily_returns(run)
    rng = random.Random(args.seed)
    totals: list[float] = []
    dds: list[float] = []
    sharpes: list[float] = []
    for _ in range(args.iters):
        m = _metrics(_block_bootstrap(daily, args.block, rng))
        totals.append(m["totalPct"])
        dds.append(m["maxDDpct"])
        sharpes.append(m["sharpe"])

    pct = [5, 25, 50, 75, 95]
    print()
    print(f"## Monte Carlo confidence — {args.market} · {args.iters} iters · block={args.block}")
    print(f"- window: {start} ~ {end} · 单次回测: 收益 {base['totalPct']:+.1f}% / DD {base['maxDDpct']:.1f}% / 夏普 {base['sharpe']}")
    print("| 指标 | 单次 | 5% | 25% | 50% | 75% | 95% |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    names = ["totalPct", "maxDDpct", "sharpe"]
    labels = ["总收益 %", "最大回撤 %", "夏普(年化)"]
    for label, key, vals in zip(labels, names, (totals, dds, sharpes), strict=True):
        q = _percentiles(vals, pct)
        b = base[key]
        if key == "totalPct":
            cell = f"{b:+.1f}%"
            qc = [f"{x:+.1f}%" for x in q]
        elif key == "maxDDpct":
            cell = f"{b:.1f}%"
            qc = [f"{x:.1f}%" for x in q]
        else:
            cell = f"{b:.2f}"
            qc = [f"{x:.2f}" for x in q]
        print(f"| {label} | {cell} | " + " | ".join(qc) + " |")

    below = sum(1 for v in totals if v < base["totalPct"])
    print()
    print(f"- 单次结果位于总收益分布的 {below / args.iters * 100:.1f}% 分位（{below}/{args.iters} 次低于单次）")
    print(f"- 最差 5% 情形（95% 置信下界）：收益 ≤ {_percentiles(totals, [5])[0]:+.1f}%")
    print(f"- DD 的 95% 分位（最坏回撤上限）: {_percentiles(dds, [95])[0]:.1f}%")

    if args.json:
        out = {
            "market": args.market,
            "window": [start, end],
            "iters": args.iters,
            "block": args.block,
            "single": base,
            "percentiles": {
                "totalPct": _percentiles(totals, pct),
                "maxDDpct": _percentiles(dds, pct),
                "sharpe": _percentiles(sharpes, pct),
            },
            "worst5": _percentiles(totals, [5])[0],
            "dd95": _percentiles(dds, [95])[0],
        }
        path = REPORT_DIR / args.json
        path.write_text(json.dumps(out, ensure_ascii=False, indent=2))
        print(f"\nreport -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
