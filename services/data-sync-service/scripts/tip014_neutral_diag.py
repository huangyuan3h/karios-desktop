"""TIP-014 · neutral_block 大环境诊断 — weak 日收益 vs 市场状态分桶.

问题: neutral_block (weak 日禁开仓) 在 valid +10.7pt 但 OOS2 -38.8pt。
OOS2 weak 日交易赚钱 (14笔 +1.78%)、valid weak 日 16/16 全亏。
假设: weak 日的含义随"大环境"变化 — 弱市年弱日=超跌反弹点,
强市年弱日=衰退信号。

本脚本: 对每个窗的每笔 weak 日入场交易, 计算其"大环境特征":
- weak_ratio_60d: 前 60 个交易日中 regime=Weak 的占比 (高=熊市环境)
- mkt_ret_60d: 沪深300 前 60 日收益 (负=熊市)
- 然后按这些特征分桶, 看 weak 日交易的收益分布。

结论用于设计 neutral_block 的"大环境条件化"规则 (实验 E1)。
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

from data_sync_service.service import backtest_engine as be
from data_sync_service.service.env_label import ENV_WEAK, load_env_by_day

S3 = dict(
    score_threshold=65.0, max_hold_days=60, stop_loss_pct=-5.0, target_pnl_pct=100.0,
    score_floor=0.0, market="CN", gates="full", trailing_stop_pct=-8.0, position_pct=0.10,
    max_positions=20, rs_rank_min=0.5, diverging_scale=1.0, panic_cooldown_days=3,
    drawdown_circuit_pct=-25.0, slippage_pct=0.05, pyramid_trigger_pct=2.5,
    pyramid_add_scale=0.5, pyramid_max_adds=1, exclude_boards="300",
    atr_stop_mult=2.0, atr_stop_strong_only=True,
    neutral_block=False, entry_style="score",
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}


def _weak_ratio_before(data: be.BacktestData, day: str, lookback: int = 60) -> float | None:
    """Fraction of regime=Weak trading days in the `lookback` sessions before `day`."""
    calendar = data.calendar
    idx = None
    for i, d in enumerate(calendar):
        if str(d) == day:
            idx = i
            break
    if idx is None or idx < 10:
        return None
    window = calendar[max(0, idx - lookback):idx]
    if not window:
        return None
    weak = sum(1 for d in window if data.regime_by_day.get(str(d)) == "Weak")
    return weak / len(window)


def _mkt_ret_before(data: be.BacktestData, day: str, lookback: int = 60) -> float | None:
    """沪深300 index return over the lookback sessions before `day` (%)."""
    closes = data.closes_by_ts.get("000300.SH") or data.closes_by_ts.get("000300.SZ") or None
    if not closes:
        return None
    idx = None
    for i, (d, _c) in enumerate(closes):
        if str(d) == day:
            idx = i
            break
    if idx is None or idx < lookback:
        return None
    c0 = closes[idx - lookback][1]
    c1 = closes[idx][1]
    if c0 <= 0:
        return None
    return (c1 / c0 - 1.0) * 100.0


def main() -> int:
    out: dict = {}
    for name, (s, e) in WINDOWS.items():
        cfg = be.BacktestConfig(start_date=s, end_date=e, **S3)
        data = be.BacktestData(cfg)
        env = load_env_by_day(s, e)
        run = be.simulate(cfg, data=data)

        weak_trades = [t for t in run.trades if env.get(t.entry_date) == ENV_WEAK]
        print(f"\n=== {name} · weak 日交易 {len(weak_trades)} 笔 ===")
        if not weak_trades:
            continue

        # 分桶 1: weak_ratio_60d (前60日 Weak 占比)
        buckets = defaultdict(list)
        for t in weak_trades:
            wr = _weak_ratio_before(data, t.entry_date)
            if wr is None:
                buckets["no_data"].append(t.pnl_pct)
            elif wr >= 0.6:
                buckets["熊市(weak>=60%)"].append(t.pnl_pct)
            elif wr >= 0.4:
                buckets["偏弱(40-60%)"].append(t.pnl_pct)
            else:
                buckets["偏强(<40%)"].append(t.pnl_pct)
        print("  按前60日Weak占比分桶:")
        for k, v in sorted(buckets.items()):
            print(f"    {k:16s} n={len(v):4d} avg={sum(v)/len(v):+6.2f}% sum={sum(v):+7.1f}%")

        # 分桶 2: 沪深300 前60日收益
        buckets2 = defaultdict(list)
        for t in weak_trades:
            mr = _mkt_ret_before(data, t.entry_date)
            if mr is None:
                buckets2["no_data"].append(t.pnl_pct)
            elif mr <= -5:
                buckets2["熊市(<-5%)"].append(t.pnl_pct)
            elif mr <= 3:
                buckets2["震荡(-5~3%)"].append(t.pnl_pct)
            else:
                buckets2["牛市(>3%)"].append(t.pnl_pct)
        print("  按沪深300前60日收益分桶:")
        for k, v in sorted(buckets2.items()):
            print(f"    {k:16s} n={len(v):4d} avg={sum(v)/len(v):+6.2f}% sum={sum(v):+7.1f}%")

        out[name] = {
            "weak_trades": len(weak_trades),
            "by_weak_ratio": {k: {"n": len(v), "avg": round(sum(v) / len(v), 2)} for k, v in buckets.items()},
            "by_mkt_ret": {k: {"n": len(v), "avg": round(sum(v) / len(v), 2)} for k, v in buckets2.items()},
        }

    json.dump(out, open("data/backtest_reports/tip014_neutral_diag.json", "w"), indent=1, ensure_ascii=False)
    print("\nreport -> data/backtest_reports/tip014_neutral_diag.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
