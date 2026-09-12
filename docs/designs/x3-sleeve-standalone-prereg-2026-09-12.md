# 预注册：X3 投资/应计长持 sleeve 独立化（只读 · 不进 Live）

> 状态：草稿 2026-09-12，跑之前冻结。任何结果都不改 Live / 双子星参数。
> **执行结果（2026-09-12）：`PARK`。** 超额真实且 OOS 稳健（+2.2%/年、OOS +2.6%/89% 胜），但 Sharpe 0.33 / maxDD −58%，
> **参数无关波动率层无效**（Sharpe/DD 不动）→ 无可用风险管理层，smart-beta 边而非独立引擎。
> 结果见 [`../backtests/factors/fund-sleeve-standalone-2026-09-12.md`](../backtests/factors/fund-sleeve-standalone-2026-09-12.md)。
> 前置：[`../backtests/factors/fund-investment-accruals-2026-09-11.md`](../backtests/factors/fund-investment-accruals-2026-09-11.md)（§5–§8）。
> 脚本：`services/data-sync-service/scripts/incubate/fund_sleeve_standalone.py`（只读）。
> 结果回写 `../backtests/factors/fund-sleeve-standalone-2026-09-12.md`。
> P0-12 纪律：新策略自有基线；单假设、零网格；不在 S-3 引擎加 gate。

## 0. 自查（最可能死在哪）

| 死因 | 风险 |
|------|------|
| 经济性 | 最高。原 sleeve 超额仅 +2%/年、绝对 Sharpe 0.24，本质是"long-only A 股 beta + 小超额"——很可能判 **PARK**（不是 alpha 引擎） |
| 共线 | 高。行业中性后增量已证明非 size（§7），但仍是"价值/低投资"风格 beta，与已知风格同源 |
| 样本 | 中。因子与中性化在 §5–§7 已在 2008–2024 上选定 → 需**时间 OOS**（2016–2024）当纪律闸门 |
| 前视 | 低。形成日 5 月、流动性用当年 2–4 月（as-of）；行业映射仍**当前快照**（非 PIT，已声明） |

## 1. 机制假设（一句话）

长期"低资产扩张 + 低应计"（保守经营）在流动池内、行业中性后**相对流动池等权有稳定正超额**；
但原 sleeve 的问题是**风险（beta/回撤）不是收益**——用一个**参数无关的波动率管理层**能否把它变成可独立持有的策略？

## 2. 冻结规则（不得调参）

- 形成：每年 5 月；流动性 = 当年 2–4 月日均成交额（as-of）；池 = 流动 ≥ 中位。
- 分：行业中性（`stock_eastmoney_industry`，缺失归 `UNK`）`score = 1 − (rank(asset_growth)+rank(accruals))/2`。
- 持：池内 `score` 头档（quintile）等权、12 个月；成本 = 换手 × 30bp（压力档 50bp）。
- 自有基线：**流动池等权（pool EW）**；次基线下 `中证500 (000905)`。

## 3. 唯一干预（单假设 · 零网格）

**波动率管理层（vol-managed）**：
- 月频；`vol_t` = 策略自身月收益的 trailing 24m 已实现波动（年化）。
- 目标 = 扩张窗口 `median(vol)` as-of（只用 ≤t−1 数据）→ `scale_t = clip(目标/vol_{t−1}, 0, 1)`（**不加杠杆**，未投部分现金 0%）。
- 成本：`|Δscale| × 30bp` 叠加在换手上。
- 无自由参数（24m/median 是固定规则，不扫）。

## 4. 判据与 kill 线（跑之前写死）

主指标 = 月频净 NAV：CAGR / vol / maxDD / Sharpe / Calmar / 相对 pool EW 超额 / 年胜率。

- **OOS 纪律**：2008–2015 = 形成样本，**2016–2024 = 时间外**。
- **判定**：
  1. 裸 sleeve **OOS 超额 vs pool EW ≥ +1.0%/年 且年胜率 ≥ 55%** → 至少有真实超额；否则 **PARK**。
  2. 波动率层：**OOS Sharpe ≥ 裸 sleeve Sharpe 且 OOS maxDD ≤ 裸 sleeve maxDD − 5pt**，且**不吞掉超额** → 记为 `DEPLOY-CANDIDATE`（进 paper 讨论）；否则 ⚠️。
  3. 全窗 Sharpe 不劣于 pool EW → 否则 **PARK**。
- 任一 kill 线不过 → `PARK`（不组合、不上 paper、不调参）。

## 5. 必报审计

- 容量：每年持仓名单 2–4 月日均成交额之和 × 10%（亿元）= 策略容量上限。
- 成本压力：30bp vs 50bp 的 NAV/超额差。
- 年换手、持仓数、行业分布（描述）。
- 诚实边界：行业映射非 PIT；A 股 long-only 高波动是结构性的；绝对收益不作发布依据。

## 6. 不做什么

- 不与 S-3 / 双子星组合（§8 已 REJECT）；不进 Live；不改任何参数。
- 不做参数网格（24m/median 之外的窗口、分位、阈值都不扫）。
- 不把 absolute CAGR 当 KPI——本策略定位是"相对自己的等权基线有没有稳定超额"。
