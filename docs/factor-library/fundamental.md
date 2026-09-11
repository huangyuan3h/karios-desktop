# 因子库 · 基本面（财务）

> 覆盖 P0-11 防守因子（F/G 系列）、P0-12 慢价值（V）、长持多倍股（L）、聚合盈利（A1）、
> 价值×动量（P18），以及 2008+ 长史单因子扫描。
> 原始长档：[`../backtests/factors/`](../backtests/factors/)
> **总基调**：**短期（60d）财务因子几乎全灭**（方向常反、且多是市值代理）；**长周期（12 月）的投资/应计与慢价值**才见真信号。

---

## ROE-TTM (F1/G3)
- **定义**：ROE-TTM > 行业中位数（防御闸候选）。
- **数据/方法**：三表 2018Q1–2026Q2，5461 股，PiT `ann_date`，非金融，`report_type='1'`；22 季；mean RankIC(roe, fwd60)、Q5−Q1 单调、与 mom60 不共线。
- **结果**：meanIC **−0.060**、Q-spread 正仅 27%、16/22 季为负；G3 市值中性后 pooledIC **−0.048**（几乎无改善，非市值混淆）。
- **判定**：❌ REJECT（方向反坐实）。源：[`fin-f1`](../backtests/factors/fin-f1-roe-ttm-2026-09-10.md) · [`fin-g3`](../backtests/factors/fin-g3-roe-neutral-2026-09-11.md)。

## 现金流含金量 CCR (F2/G1)
- **定义**：CFO-TTM / 净利-TTM（盈利质量）。
- **结果**：meanIC **+0.0156**（弱）、Q-spread 正 50%、max|corr(mom)| 0.150；G1 市值中性 pooledIC +0.026 但 Q-spread 仍 50%。闸预览 ccr>0.5 不稳。
- **判定**：❌ REJECT（弱方向，不够当闸）。源：[`fin-f2`](../backtests/factors/fin-f2-cashconv-2026-09-10.md) · [`fin-g1`](../backtests/factors/fin-g1-ccr-neutral-2026-09-11.md)。

## 杠杆安全 (F4/G2)
- **定义**：行业中位资产负债率 − 自身（越高越安全）。
- **结果**：meanIC **+0.0146**（噪音）、Q-spread 正 32%；G2 市值中性后 pooledIC **−0.018**（方向反）、Q-spread 20%；lev_safe 与市值 corr≈−0.2（低杠杆=大市值代理）。
- **判定**：❌ REJECT（方向反 + 市值代理）。源：[`fin-f4`](../backtests/factors/fin-f4-leverage-2026-09-10.md) · [`fin-g2`](../backtests/factors/fin-g2-lev-neutral-2026-09-11.md)。

## L1/L2 长持多倍股
- **定义**：GQ = mean(增长 rank, 质量 rank)，年度队列 5 月形成，top20 等权，持 2/3 年；L2 用连续两年增速下限 DQ。
- **数据**：年报 2018Q1+（5461 股），队列 2021–2024，基准=同队列 EW。
- **结果**：GQ-top20 fwd2 vs EW：2021 **−38.4** vs +12.2、2022 −38.6 vs +3.0、2023 −15.1 vs +2.1、2024 +57.2 vs +66.0；K1/K2 **0/4**。L2 减轻伤害（3/4 好于 naive）但 K2 仍 1/4。
- **机制**：1 年高增长 = **买在盈利脉冲/周期顶**（2021 COVID 检测耗材、2022/23 锂硅化工峰），高增长已 price in，后续估值杀。
- **判定**：❌ REJECT-frozen。源：[`fin-l1-tenbagger`](../backtests/factors/fin-l1-tenbagger-2026-09-11.md)。

## A1 聚合盈利 regime
- **定义**：全市场聚合 TTM 净利 YoY 预测沪深300 前瞻收益（配置层）。
- **数据/方法**：月度 PiT 快照 2021-01–2026-08（n=24），预冻结通过线 corr>0.2 且 Q4−Q1>0。
- **结果**：corr(yoy, fwd60) = **−0.135**（反）、Q4−Q1 = −1.62、corr(fwd120) = −0.47。高聚合增长→未来更差。
- **判定**：❌ REJECT（方向反）。源：[`fin-a1-earn-regime`](../backtests/factors/fin-a1-earn-regime-2026-09-11.md)。

## P18 价值×动量
- **定义**：composite = 0.5·价值 rank（EP/BP/SP/FCF）+ 0.5·mom60 rank，市值中性 pooled。
- **结果**：诊断 pooledIC **+0.118**、posQ 70% → 机械 PASS；但消融：mom-only +0.116、value-only +0.064（**价值增量≈0**）。回放 composite −8.5/−43.0/−43.3pt，≈ mom-only（value 腿是装饰）。
- **判定**：诊断 PASS → **replay REJECT**。源：[`fin-p18`](../backtests/factors/fin-p18-value-mom-2026-09-11.md)。

## V1 慢价值
- **定义**：同 P18 价值 composite，但用 **12 月** horizon（120/250d）测（价值是 12 月因子）。
- **数据/方法**：季频 value panel 2020Q4–2025Q2（17 季），市值 5 组中性 pooled。
- **结果**：mIC120 **+0.14**、mIC250 **+0.17**（vs 60d 仅 +0.016）；D-spread 正 82%（14/17）；top>EW 71%；唯深负季 2024-06。
- **判定**：✅ PASS → ⚠️ **INCUBATE**（唯一 PASS 的基本面因子）。源：[`fin-v1-slow-value`](../backtests/factors/fin-v1-slow-value-2026-09-11.md)。

## V2 慢价值套筒
- **定义**：5 月调仓、每个市值组取价值 composite top5（=25 股）等权、持 250 日、30bp 往返。
- **结果**：超额 +33.09 / −9.38 / +8.25 / +2.63 / **−24.13**（4/5 需胜，实 3/5）；均值 +2.09pt → fail。**风格税**：2025 动量/小盘年大亏 −24pt。
- **判定**：❌ REJECT（价值只在熊市/震荡年赢 = regime 腿，非独立套筒）。源：[`fin-v2-slow-sleeve`](../backtests/factors/fin-v2-slow-sleeve-2026-09-11.md)。

## 投资因子 asset_growth
- **定义**：`total_assets / prev_year_assets − 1`（Fama-French 投资因子；越高=扩张越猛）。
- **方向**：**负**（低资产扩张 → 跑赢）。
- **数据/方法**：年报 as-of 2008–2024（n=17），5 月形成，未来 12 月 rank-IC；控市值/反转/动量残差。
- **结果**：IC **−0.066**、t **−2.91**、%正 24%；控市值后 IC −0.039/t −1.9（部分存活）。
- **判定**：⚠️ **INCUBATE**（见下 composite）。源：[`fund-investment-accruals`](../backtests/factors/fund-investment-accruals-2026-09-11.md) §2。

## 应计 accruals
- **定义**：`(净利 − 经营现金流) / 总资产`（Sloan 应计异常；越高=盈利质量越差）。
- **方向**：**负**（低应计 → 跑赢）。
- **结果**：IC **−0.037**、t **−2.10**；控市值后 **−0.039/t −2.35（存活）**。
- **判定**：⚠️ **INCUBATE**。

## 投资/应计 composite（长持 sleeve 原型）
- **定义**：`−asset_growth − accruals`（行业中性化后取头档）。
- **结果**：IC +0.065/t+3.02；**行业中性后流动池 Q5−EW 从 +0.7%→+2.0%/年（88% 胜）**。
  Sleeve（年度、含成本）净 **+5.3%/年 vs 流动 EW +3.4%（超额 +2.0%/年、14/17 胜）vs 中证500 +2.5%**；DD−49%/sr0.24。
- **未决**：行业映射**非 PIT**、波动大、未与 S-3 组合验证。估值折（2020+）value+inv+accr IC+0.112（n=5）。
- **稳健性（2026-09-11）**：tushare **独立行业分类**复现（+2.08% ≈ 东财 +2.24%）；**安慰剂（打乱分组）无效**（+1.01 ≈ raw +1.16）；**市值中性不提升**（+0.61）；**两期都正**（08-15 +2.1 / 16-24 +2.3）。→ 增量来自真实行业结构，非快照/分组机制假象。
- **与 S-3 组合（2026-09-11）**：sleeve **只从 2024 起**评估时 w0.3 看似三窗变好；**延到 2021（=S-3 5 年长窗）后反转**——sleeve 自 DD **−40%**、组合收益/Sharpe↓、DD 几乎不变 → **组合 ❌ REJECT**（样本起点选择偏差，吃了 2024–25 牛市）。教训：**组合验证必须用与对腿等长的重叠样本**。
- **判定**：⚠️ **INCUBATE（sleeve 本体）/ 组合 REJECT**。源：[`fund-investment-accruals`](../backtests/factors/fund-investment-accruals-2026-09-11.md) §2–8。

## F-score
- **定义**：Piotroski 9 项财务健康打分。
- **结果**：IC **−0.005**（t−0.32，**A 股失效**）；控市值后 +0.013（弱）。
- **判定**：❌ REJECT（A 股不适用）。

## 营收 / 净利增长 / 毛利率
- **定义**：rev_growth、ni_growth、gross_margin（年报）。
- **结果**：rev_growth IC −0.042/t−2.37（被市值吸收）；ni_growth −0.026/t−1.34；gross_margin +0.001（无）；ROE（长史）−0.039/t−1.21（控市值后死）。
- **判定**：❌（增长类与 L1 同根：高增长→未来差，且多为市值代理）。
