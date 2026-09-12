# 因子档案总台账（Factor Experiments Ledger）

> **这是什么**：`docs/backtests/factors/` 的唯一索引。每份因子实验一行——**是什么 / 结论 / 一句话为什么**。
> 以后 agent 想开新因子/新形态，**先看本页 + [`../../factor-library/README.md`](../../factor-library/README.md)**，命中已判死的方向别重开。
> **判定标签**：❌ REJECT（证伪/无增量/不可交易）· ⚠️ INCUBATE（真实信号，未上 Live）· ⏸️ PARK/SHELVE（暂存/独立化失败）· 🟡 WEAK（有统计信号、经济弱）· 🧱 诊断（只描述）· 📝 笔记（无裁决）。
> **规则**：新档必须带 `结论 + 机制 + 数据/窗口 + 判定`；结果写进本表一行。预注册在 `../../designs/`。

---

## 0. 今日两组价量因子库（2026-09-12 · 重点）

| 组 | 档 | 判定 | 一句话结论 |
|----|----|------|-----------|
| **Alpha101（1–101）** | [alpha101-l0-screen-2026-09-12](alpha101-l0-screen-2026-09-12.md) | ❌ **REJECT（终结）** | L0：101 条仅 11 条统计活，全是**同一族**（价量协方差反向）；S1 正交化该族**独立**于 size/流动性/反转/动量/行业（残差三窗 ICIR ≥0.5）；**S2 用 30bp 成本全灭**（唯一三窗净为正的 h=20 IR +0.01/+0.24/+0.69，收益全在 valid 单 regime）。结论：**独立但不可交易**。 |
| **GTJA 191** | [gtja191-l0-screen-2026-09-12](gtja191-l0-screen-2026-09-12.md) | ❌ **REJECT** | 168 条实测：0 PASS / 16 CANDIDATE / 152 REJECT；候选全是**同一价量相关家族 + 量波动 + 中期反转**，净价差全负；与 Alpha101 逐数吻合（139=A6/105=A3/099=A13）。**无任何新独立轴**（"抗跌/独立行情/趋势显著性/DMI"全部证伪）。 |

**两组共同结论**：价量类短周期横截面因子在 A 股**统计上有微弱信号、成本后不可交易**，且高度同族；**不要期待它们给双子星加增量**。真正的正结果都在**长周期慢基本面**（慢价值/低投资，但只是 ~+2%/年 smart-beta，非引擎）。

---

## 1. 全部因子档（一眼判定）

### 技术 / 价量 / 动量 / 反转
| 档 | 是什么 | 判定 | 为什么 |
|----|--------|------|--------|
| [alpha101-l0-screen-2026-09-12](alpha101-l0-screen-2026-09-12.md) | WQ 101 全集 | ❌ | 仅价量协方差一族有统计信号，成本全负、S2 终结 |
| [gtja191-l0-screen-2026-09-12](gtja191-l0-screen-2026-09-12.md) | GTJA 191 全集 | ❌ | 同族 + 量波动 + 中期反转，无新轴 |
| [factor-ic-2026-08-22](factor-ic-2026-08-22.md) | TrendOK score/RS/amount | ❌ | 全 \|IC\|<0.04，无预测力 |
| [factor-ic-phaseB-2026-08-22](factor-ic-phaseB-2026-08-22.md) | mom20/vol20/dd60/flow5d | ❌ | 无 ICIR≥0.5；`vol20` 负 IC 但不稳 |
| [intraday-microstructure-2026-09-11](intraday-microstructure-2026-09-11.md) | 5min 日内尾盘/隔夜 | ❌ | 强反转（IC−0.11）但毛 0.21%/天 < 30bp |
| [amihud-illiquidity-2026-09-08](amihud-illiquidity-2026-09-08.md) | Amihud 非流动性 | ❌ | 溢价在 0.7 亿门外的不可交易区 |
| [macd-trend-study.md](macd-trend-study.md) | MACD | 📝 | 无统一 edge |
| [kdj-trend-study.md](kdj-trend-study.md) | KDJ | 📝 | 同上 |
| [bollinger-trend-study.md](bollinger-trend-study.md) | 布林 %b/带宽 | 📝 | 目标依赖，无统一 edge |
| [indicator-supertrend-fibonacci-priceaction-notes.md](indicator-supertrend-fibonacci-priceaction-notes.md) | SuperTrend/Fib/PA | 📝 | 已被 MA+ATR/trail 族覆盖 |
| [uptrend-pullback-study.md](uptrend-pullback-study.md) | 上升趋势回调 | 🟡 | 有信号、可交易性弱 |
| [support-resistance-box-study.md](support-resistance-box-study.md) | 箱体支撑阻力 | 🟡 | 同上 |
| [long-consolidation-breakout-study.md](long-consolidation-breakout-study.md) | 长调整突破 | 🟡 | 同上 |
| [scoop-exhaustion-oos-check-2026-09-04](scoop-exhaustion-oos-check-2026-09-04.md) | 强股勺型耗尽（做空） | 🧱 | 方法缺失，不下结论 |

### 基本面（财务）
| 档 | 是什么 | 判定 | 为什么 |
|----|--------|------|--------|
| [fin-f1-roe-ttm-2026-09-10](fin-f1-roe-ttm-2026-09-10.md) | ROE-TTM > 行业中位 | ❌ | 方向反；控市值后死 |
| [fin-f2-cashconv-2026-09-10](fin-f2-cashconv-2026-09-10.md) | 现金流含金量 CFO/NI | ❌ | 弱 |
| [fin-f4-leverage-2026-09-10](fin-f4-leverage-2026-09-10.md) | 杠杆安全 | ❌ | 弱/反 |
| [fin-g1-ccr-neutral-2026-09-11](fin-g1-ccr-neutral-2026-09-11.md) · [g2](fin-g2-lev-neutral-2026-09-11.md) · [g3](fin-g3-roe-neutral-2026-09-11.md) | F1/F2/F4 + 市值中性 | ❌ | 中性化后无残余 |
| [fin-l1-tenbagger-2026-09-11](fin-l1-tenbagger-2026-09-11.md) | 长持找几倍股（高增长） | ❌ | 高增长 = 盈利脉冲/周期顶，已 price in |
| [fin-p18-value-mom-2026-09-11](fin-p18-value-mom-2026-09-11.md) | 价值 × 动量 | 🧱/⚠️ | 诊断 PASS，回放未见 |
| [fin-v1-slow-value-2026-09-11](fin-v1-slow-value-2026-09-11.md) | 慢价值 V1（12 月） | ⚠️ **INCUBATE** | 唯一 PASS 的慢基本面因子 |
| [fin-v2-slow-sleeve-2026-09-11](fin-v2-slow-sleeve-2026-09-11.md) | 慢价值独立套筒 | ❌ | 风格腿，非独立套筒 |
| [fin-a1-earn-regime-2026-09-11](fin-a1-earn-regime-2026-09-11.md) | 全市场盈利 regime 定仓位 | ❌ | 方向反（好消息兑现即打折） |
| [fund-investment-accruals-2026-09-11](fund-investment-accruals-2026-09-11.md) | 投资/应计 composite | ⚠️ **INCUBATE** / 组合 ❌ | 流动池+行业中性 +2%/年真实；与 S-3 组合无增益 |
| [fund-sleeve-standalone-2026-09-12](fund-sleeve-standalone-2026-09-12.md) | X3 独立化（月频+波动率层） | ⏸️ **PARK** | +2.2%/年真实但满仓 beta、Sharpe0.33、波动率层无效 |
| [garp-sleeve-2026-09-12](garp-sleeve-2026-09-12.md) | GARP（质量+便宜） | ❌ **CLOSE** | GARP ≈ value（corr0.93），质量无增量 |

### 资金流 / 情绪 / 另类 / 事件
| 档 | 是什么 | 判定 | 为什么 |
|----|--------|------|--------|
| [alt-alpha-screen-2026-09-11](alt-alpha-screen-2026-09-11.md) | 股东户数/陆股通/大单 | ❌ | 陆股通/大单噪音；股东户数=小市值代理 |
| [candidate-c-flow-resonance-2026-09-10](candidate-c-flow-resonance-2026-09-10.md) | 市场级情绪/资金流共振 | ❌ | 预注册主假设三窗翻转证伪；共振覆盖≈0 |
| [research-coverage-2026-09-07](research-coverage-2026-09-07.md) | 研报首覆盖事件 | ❌ | 首覆盖后跑输 3%；评级无差异；分数前视 |
| [fin-d1-hot-rank-2026-09-11](fin-d1-hot-rank-2026-09-11.md) | 个股人气榜 | ⏸️ SHELVE | 追高税；反向不开 |
| [alpha-trend-forward-2026-09-07](alpha-trend-forward-2026-09-07.md) | 趋势卡映射票前瞻 | ❌ | 映射票跑输，分级无单调 |

### 风格 / 市况 / 结构
| 档 | 是什么 | 判定 | 为什么 |
|----|--------|------|--------|
| [style-regime-2026-09-11](style-regime-2026-09-11.md) | 风格×市值×市况 | 🧱 | 描述性砖，轮动打平等权，不作规则 |

---

## 2. 家族速查（开新因子前先对号）

| 家族/维度 | 判定 | 代表 | 死因 |
|-----------|------|------|------|
| 价量相关/协方差反向 | ❌ | Alpha101 A13/A16、GTJA099/062/… | 成本（换手 0.6–0.9）；S1 独立但 S2 死 |
| 短反转 / 日内 | ❌ | GTJA071、Alpha101 A33/A101、5min | 毛收益 <30bp 成本 |
| 均线 / 突破 / 趋势形态 | ❌ | P1–P8、MA/MACD/KDJ | 与 RS/主线共线 |
| 量/额波动（低波反向） | ❌ | GTJA070/097、`vol20` | 流动性/市值代理 |
| 基本面质量（ROE/现金流/杠杆/F-score） | ❌ | F1/F2/F4/G1–G3 | 被市值/价值吸收 |
| 基本面价值/低投资/应计（长周期） | ⚠️ smart-beta | 慢价值 V1、X3 投资/应计 | 真实 +2%/年，但满仓 beta、非引擎 |
| 另类数据（股东户数/北向/大单） | ❌ | X1 | 噪音或小市值代理 |
| 情绪/研报/热点事件 | ❌ | 研报覆盖、人气榜、趋势卡 | 注意力峰值 = 追高税 |

---

## 3. 怎么用 / 怎么写

- **想开新因子**：先过 [`first-principles-2026-09-05.md`](../first-principles-2026-09-05.md) 自查 + 本页家族速查；撞死区的不开。
- **新档模板**：一句话结论（判定）→ 机制 → 数据/窗口/宇宙 → 判定表（IC/ICIR/单调/换手/净）→ 诚实边界。
- **预注册**：`../../designs/`；**结果回写本页一行 + [`../../factor-library/`](../../factor-library/) 对应分类档**。
- **工具链**：`scripts/alpha101_screen.py`（L0 引擎）· `scripts/gtja191_screen.py` · `scripts/alpha101_candidates_diag.py`（S1）· `scripts/alpha101_candidates_s2.py`（S2）——新因子集可复用。
