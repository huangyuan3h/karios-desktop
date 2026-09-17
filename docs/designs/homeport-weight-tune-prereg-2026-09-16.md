# 预注册：母港权重向港湾侧扩展（H-MIX-TUNE · 2026-09-16）

> **状态**：用户授权执行（防守也要年化收益），跑前冻结。
> **动机**：M50（50/50）long +118.6/CAGR 17.6%，防守满分但收益垫底；
> 沿 M 带向港湾侧加两档（M30=70% 港湾、M20=80% 港湾），找"防守成立前提下的最高年化"。
> **只动权重**：B3 构成/月频再平衡/成本口径（单边 5bp，OPT-211 P2 钉死）全部不动；
> B13/B16（被动腿/上限）维持关闭。

## 1. 网格（S4 结构通道，一格一测）

M{N} = N% B3 + (100−N)% 港湾，N ∈ {60, 50, 40, **30, 20**}（粗体为新增）。
每档 + cost20 臂（`rp_hi` + COST_HIGH，与 m50_cost20 同口径）。

## 2. 冻结裁决（B15 K1–K5 + 年化目标）

- **约束（硬）**：K1 四窗 Sharpe ≥ 港湾；K2 四窗 MDD ≥ 港湾（即 B15 原门，一字不改）。
- **目标**：约束内 long CAGR 最大；tiebreak 用 dev 三窗 total 均值。
- **报告项**：K3 收益地板 / K4 2022–23 压测 / K5 cost20（新档同算，不满足则降级）。
- 死因预判：#2（向港湾靠近=向基线回归，增量被稀释）或 K1/K2 在 M20 破（B3  cushion 不足）。

## 3. L 门 tick

后处理 blend（冻结 harbor/rp 腿，无新数据源、无新成交语义、无新阈值）：
L1/L2/L4 n/a · L3 不动（成本口径钉死，双边复核见 OPT-211 P2）· L5 report 落盘 +
复用 B15 冻结数对照 · L6/L7 同 evalu 脚本惯例。

## 4. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_harbor_riskbudget.py --save-report  # 含新增臂
cp data/backtest_reports/harbor_riskbudget_2026-09-13.json \
   data/backtest_reports/harbor_riskbudget_tune_2026-09-16.json
```
