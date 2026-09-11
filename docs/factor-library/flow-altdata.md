# 因子库 · 资金流 / 情绪 / 另类数据 / 事件

> 覆盖个股资金流、股东户数、陆股通、人气榜、研报覆盖、Alpha 卡事件、市场级情绪/资金流择时、两融与国家队。
> 原始长档：[`../backtests/factors/`](../backtests/factors/)
> **总基调**：**全灭**——要么是噪音，要么方向反，要么是市值代理。唯一"可用"的是国家队 ETF 份额（作**闸门**，非 alpha）。

---

## 股东户数变化
- **定义**：股东户数环比（户数↓=筹码集中，预期看多）。
- **数据/方法**：`cn_holder_number` 2020+，月末 as-of rank-IC → 下月收益；变体（1/2 期）、horizon（1/3 月）、流动池；市值/反转/动量正交化。
- **结果**：`holder_chg` IC −0.009/t−2.28（方向对）；`chg2` 1 月 IC +0.014/t3.05，但**控市值后 +0.002/t0.66 ≈ 0**；Q5−Q1 仅 0.5–0.8%/季。
- **判定**：❌ REJECT（**小市值代理**，非独立 alpha）。源：[`alt-alpha-screen`](../backtests/factors/alt-alpha-screen-2026-09-11.md)。

## 陆股通持股变化
- **定义**：`cn_hk_hold.ratio` 月内变化（北向增持，预期看多）。
- **数据**：2023-01+
- **结果**：rank-IC **+0.003/t+0.37**（噪音）。
- **判定**：❌ REJECT。

## 大单资金流
- **定义**：`cn_moneyflow` 大单+超大单净流入占比（主力买入）。
- **数据**：2023-01+
- **结果**：rank-IC **−0.009/t−1.26**（弱/反向）。
- **判定**：❌ REJECT。

## 研报覆盖
- **定义**：首次覆盖 / 覆盖强度（预期看多）。
- **数据**：`research_reports` 2026-08+；E1 首次覆盖 n=248（回填 300）；10/20d，次日开盘入场，相对市场。
- **结果**：E1 10d **−2.99%** hit31.5%；回填后 −2.28%；覆盖越密越差（≥2 篇 −3.27% vs 1 篇 −2.81%）。
- **判定**：❌ REJECT（方向反：关注度顶点）。源：[`research-coverage`](../backtests/factors/research-coverage-2026-09-07.md)。

## 人气榜 (D1)
- **定义**：东财个股人气排名（预期热股看多）。
- **数据**：`cn_hot_rank` 185.8 万行 / 2025-03–2026-09；周快照 forward 14 周。
- **结果**：mIC20 **−0.036**、mIC60 **−0.110**、Q-spread 正 50%；top100 −13~−26；与 mom60 corr −0.25~−0.48。
- **判定**：⏸️ **SHELVE**（追高税，方向反；"买冷门"未反向开发）。源：[`fin-d1-hot-rank`](../backtests/factors/fin-d1-hot-rank-2026-09-11.md)。

## Alpha 卡映射票
- **定义**：Alpha Incubator LLM 趋势卡映射的票（S/A 级）。
- **数据**：卡出生后映射，可评估 1041（S/A），20d；基准全 A EW / 000300。
- **结果**：S 10d **−3.63%**、A −2.64%；S 20d −7.43%、A −6.00%；hit 30–42%；无 `S>A>B` 梯度；重映射漂移 52.6%。
- **判定**：❌ REJECT。源：[`alpha-trend-forward`](../backtests/factors/alpha-trend-forward-2026-09-07.md)。

## 市场级情绪/资金流共振
- **定义**：两融、国家队 ETF 份额、北向、小单、换手、涨停、risk_mode 的市场级极值/共振。
- **数据/方法**：`cn_etf_share`(2018-07+) / `cn_margin_total.rzye`(2021-01+) / `cn_moneyflow_hsgt`(2021-01+) / `cn_flow_daily`(2023-01+) 等；时序条件前瞻 1/5/20d + 共线 + 三窗方向一致。
- **结果**：主假设（两融去杠杆低分位→前瞻负）**三窗翻转证伪**；共振覆盖≈0（全正 5/2/0 天）；北向被趋势共线（+0.76）杀；唯一三窗一致=国家队 20dΔ 低分位跑输（幅度 −0.1~−1.0%/20d，仅指数>MA200），降级显示。
- **判定**：❌ REJECT / 无候选进 Phase 2。源：[`candidate-c-flow-resonance`](../backtests/factors/candidate-c-flow-resonance-2026-09-10.md)。

## 两融 / 国家队
- **两融去杠杆**：作为**闸门/倾斜**曾见于早期（+0.09%/月，68% 胜）但未采纳；市场级择时证伪（上条）。
- **国家队 ETF 份额**（`cn_etf_share`）：**不是 alpha 是闸门**——沪深300<MA200 且 4 只宽基 ETF 20 日份额净增≤0 → 暂停新仓（fail-open）。预注册三窗 +0.0（现代惰性）、long +20.0、past_year +0.0 → **PASS，已进冻结**（TIP-017 B）。
- **行业资金流/主线**：作**闸门**（`gates=full` 的主线白名单），非独立 alpha。
- 源：[`strategy-params.md` §1](../modules/strategy-params.md) · [`risk-state-sensors`](../backtests/risk-state-sensors-2026-09-09.md)。
