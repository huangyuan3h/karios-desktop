"""OPT-155 Phase-1 diagnostic — ADV participation at a stated AUM (READ ONLY).

For frozen CN core (S-3) entries, compute order participation
``u = order_notional / 60d_ADV`` at AUM = ¥1000万 (0.1亿) and the implied
market-impact drag under a standard square-root model. Zero PnL, stdout only.

Run:  PYTHONPATH=src:scripts python3 scripts/diag_adv_impact.py
"""

from __future__ import annotations

import math
import statistics as st

from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    _resolve_ts_code,
    simulate,
)

AUM_YI = 0.10  # ¥1000万 = 0.1亿元
WINDOW = ("2021-08-01", "2026-08-07")
# square-root impact per side: bps = C * sqrt(u / u_ref), u_ref = 1%.
U_REF = 0.01


def main() -> None:
    from run_walk_forward import S3_CONFIG

    s, e = WINDOW
    cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data=data)

    rows: list[tuple[float, float, str]] = []  # (u, adv_yi, ts)
    for t in run.trades:
        r = _resolve_ts_code(t.symbol)
        if r is None or r[0] != "CN":
            continue
        ts = r[1]
        adv = (data.avg_amount_by_day.get(t.entry_date) or {}).get(ts)
        if not adv or adv <= 0:
            continue
        order_yi = float(t.position_pct) * AUM_YI
        rows.append((order_yi / adv, adv, ts))

    n = len(rows)
    us = sorted(r[0] for r in rows)
    print("=" * 78)
    print(f"OPT-155 诊断 · 冻结 CN 核心 · AUM=¥{AUM_YI*1e4:.0f}万 · long 2021-08~2026-08")
    print(f"样本 {n} 笔 · 单票 10% = ¥{0.10*AUM_YI*1e4:.0f}万")
    print("=" * 78)
    if not n:
        print("无样本（avg_amount 未加载？min_avg_amount>0 才有）")
        return
    pct = lambda q: us[min(n - 1, int(q * n))]  # noqa: E731
    print("参与率 u = 订单名义/60日均额：")
    print(f"  mean {st.mean(us):.3%} · median {st.median(us):.3%} · p90 {pct(0.90):.3%} · p99 {pct(0.99):.3%} · max {us[-1]:.3%}")
    for thr in (0.005, 0.01, 0.02, 0.05, 0.10):
        c = sum(1 for u in us if u > thr)
        print(f"  u > {thr:.1%}: {c:3} 笔 ({c/n:.1%})")

    print("\n冲击估计（单边 bps = C·√(u/1%)；往返 = 2×）：")
    for c in (5.0, 10.0, 15.0):
        per_side = [c * math.sqrt(u / U_REF) for u in us]
        rt = [2 * x for x in per_side]
        total_pp = sum(rt) / 100.0  # bps summed over trades → percentage points (simple)
        print(f"  C={c:>4.0f}: 单边 mean {st.mean(per_side):5.1f}bps · p90 {sorted(per_side)[int(0.9*n)]:5.1f}bps "
              f"| 往返 Σ 拖累 ≈ {total_pp:5.2f}pt（{n} 笔简单加总）")

    print("\n被冲击最重的 8 笔（小 ADV）：")
    for u, adv, ts in sorted(rows, key=lambda x: -x[0])[:8]:
        print(f"  {ts}  ADV {adv:6.2f}亿  u {u:6.2%}  单边(C=10) {10*math.sqrt(u/U_REF):5.1f}bps")


if __name__ == "__main__":
    main()
