# 预注册：星舰停车资产配比前沿（H-SAT-PARK-FRONT · 2026-09-21）

> **价目表（不裁决）。** 目的：给用户一张 **星舰停车资产** 的收益-回撤曲线，量化
> 「每多买 1pt 回撤，能换多少收益」，用于选**痛苦点**。
> **关键词**：星舰 · 停车资产 · 配比 · 前沿 · 价目表 · 预注册

## 0. 机制（冻结）

停车资产 = **静态配比组合** `a×套筒 + b×B3 + c×REPO`（`a+b+c=1`，日频恒定权重、无再平衡成本）；
`port_ret_t = sat_ret_t + w_t × park_ret_t − 5bps × |w_t − w_{t−1}|`，`w_t = cashShare(T−1)` 因果。
卫星腿冻结（同 A4/A2 口径）。

## 1. 格点（冻结，非扫描寻优）

- 二维前沿：`a ∈ {0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0}`，`b = 1−a`，`c = 0`（纯 B3 ↔ 纯套筒）。
- REPO 参考：`(a,b,c) = (0,0.5,0.5)`、`(0.25,0.5,0.25)`、`(0.5,0.25,0.25)`。
- 四窗（OOS2/train/valid/long）+ holdout 描述。

## 2. 产出（不裁决）

每个格点报 total / CAGR / MDD / Sharpe / Calmar + 三窗 + Δ vs standalone；
并标注 **A4_true（a=0）/ A6_true（a=0.5）/ A2_true（a=1）** 三个已知候选点。
**无 PASS/REJECT**；仅价目。**Live 不动**。

## 3. 产出文件

- 脚本 `scripts/eval_sat_parking_frontier.py` → `data/backtest_reports/sat_parking_frontier_2026-09-21.json`。
- 结论落 `docs/backtests/stable/sat-parking-frontier-2026-09-21.md` + `SUMMARY.md`。

*冻结于 2026-09-21，跑数前。*

**结果（跑后补 · 2026-09-21）**：价目表已出，**拐点 a≈0.25**——**a25（25% 套筒 + 75% B3）
long +728.4% / CAGR 55.0% / MDD −6.9% / SR 3.49 / Calmar 7.98 支配纯 B3**（+654.3/−6.8）；
a>0.25 后 MDD 陡增（a50 −15.0 → 纯套筒 −29.9）。最低回撤 = `b3_repo_5050`（+556.2/−5.9/Calmar 8.09）。
结论落 [`../backtests/stable/sat-parking-frontier-2026-09-21.md`](../backtests/stable/sat-parking-frontier-2026-09-21.md)。
