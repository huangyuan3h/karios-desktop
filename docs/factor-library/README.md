# Karios 因子库（Factor Library）

> **用途**：把 Karios 至今验证过的**所有因子**集中登记——**无论有用没用**——每个因子给
> 定义、数据、测试方法、结果、判定、与 S-3/产品的关系，并链到原始实验档。
> **与别处的关系**：
> - 原始实验长档在 [`../backtests/factors/`](../backtests/factors/)；本库是**登记 + 摘要 + 索引**，不复制长档。
> - 拒收总表在 [`../backtests/SUMMARY.md`](../backtests/SUMMARY.md)；本库是**因子维度**的重排。
> - 新想法先查 [`../backtests/first-principles-2026-09-05.md`](../backtests/first-principles-2026-09-05.md)。
> **怎么用**：找因子 → 看本 README 索引 → 进分类详档 → 需要原始数据/方法再进 backtests/factors。

## 判定标签

| 标签 | 含义 |
|------|------|
| ✅ PASS | 通过预注册/三窗，可进下一阶段 |
| ⚠️ INCUBATE | 有真实信号、尚在孵化（未上 Live） |
| 🟡 WEAK | 有统计信号但经济/可交易性弱（多为 size/流动性代理） |
| 🧱 诊断 | 只作描述性参考，不作规则 |
| ⏸️ SHELVE | 暂存、方向反但未反向开发 |
| ❌ REJECT | 证伪/无增量/不可交易 |
| 📝 笔记 | 研究笔记，无正式裁决 |

## 分类详档

| 文件 | 覆盖 |
|------|------|
| [technical.md](technical.md) | 价量 / 技术指标 / 动量 / 反转 |
| [fundamental.md](fundamental.md) | 财务：盈利、质量、价值、投资、应计 |
| [flow-altdata.md](flow-altdata.md) | 资金流 / 情绪 / 另类数据 / 事件 |
| [microstructure-liquidity.md](microstructure-liquidity.md) | 日内微结构 / 流动性 / 波动率 |
| [style-regime-allocation.md](style-regime-allocation.md) | 风格 / 市值 / 市况 / 配置 / 指数 beta |
| [pattern-morphology.md](pattern-morphology.md) | 形态识别（箱体/突破/回调/耗尽） |

---

## 总索引（一表看全）

### 技术 / 价量 / 动量 / 反转

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| 海龟/唐奇安突破 (P1) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p1-海龟唐奇安突破) |
| 放量突破 (P2) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p2-放量突破) |
| MA200 过滤 (P3) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p3-ma200) |
| 均线斜率 (P4) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p4-均线斜率) |
| 双均线金叉 (P5) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p5-双均线金叉) |
| 三线多头 (P6) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p6-三线多头) |
| 短线超卖反转 (P7) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p7-短线超卖反转) |
| 长阴次日反转 (P8) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p8-长阴次日反转) |
| 风险调整动量 (P12) | 多 | 日线 · 三窗 | ❌ | [technical](technical.md#p12-风险调整动量) |
| ST 剔除 (P16-ST) | 过滤 | 日线 · 三窗 | ❌ | [technical](technical.md#p16-st-剔除) |
| TrendOK Score/RS/amount | 多 | 全市场 · 30d/90d/1y | ❌ | [technical](technical.md#trendok-scorersamount) |
| mom20/vol20/dd60/flow5d | 多/负 | 全市场 · 90d/1y | ❌ | [technical](technical.md#mom20vol20dd60flow5d) |
| MACD | 多 | ETF + CN1500 · 2023–26 | 📝噪音 | [technical](technical.md#macd) |
| KDJ | 多 | ETF + CN1500 · 2023–26 | 📝噪音 | [technical](technical.md#kdj) |
| Bollinger %b/带宽 | 多 | ETF + CN1500 · 2023–26 | 📝无统一edge | [technical](technical.md#bollinger) |
| SuperTrend/Fib/PA | 多 | 笔记 | 📝笔记 | [technical](technical.md#supertrend-fibonacci-price-action) |

### 基本面（财务）

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| ROE-TTM > 行业中位 (F1/G3) | 多 | 财报 2018+ · 22 季 | ❌ 方向反 | [fundamental](fundamental.md#roe-ttm-f1g3) |
| 现金流含金量 CFO/NI (F2/G1) | 多 | 财报 2018+ · 22 季 | ❌ 弱 | [fundamental](fundamental.md#现金流含金量-ccr-f2g1) |
| 杠杆安全 (F4/G2) | 多 | 财报 2018+ · 22 季 | ❌ 弱/反 | [fundamental](fundamental.md#杠杆安全-f4g2) |
| 长持增长+质量找几倍股 (L1/L2) | 多 | 年度队列 2021–24 | ❌ | [fundamental](fundamental.md#l1l2-长持多倍股) |
| 聚合盈利 regime (A1) | 仓位 | 月度 2021+ | ❌ 方向反 | [fundamental](fundamental.md#a1-聚合盈利-regime) |
| 价值×动量复合 (P18) | 多 | 季频 · 20 季 | ❌ replay | [fundamental](fundamental.md#p18-价值动量) |
| 慢价值 V1（12 月） | 多 | 季频 2020Q4+ | ⚠️ INCUBATE | [fundamental](fundamental.md#v1-慢价值) |
| 慢价值独立套筒 V2 | 多 | 5 vintage 2021–25 | ❌ | [fundamental](fundamental.md#v2-慢价值套筒) |
| **投资因子 asset_growth** | 负 | 年报 2008–24 | ⚠️ INCUBATE | [fundamental](fundamental.md#投资因子-asset_growth) |
| **应计 accruals** | 负 | 年报 2008–24 | ⚠️ INCUBATE | [fundamental](fundamental.md#应计-accruals) |
| Piotroski F-score | 多 | 年报 2008–24 | ❌ A股失效 | [fundamental](fundamental.md#f-score) |
| 营收/净利增长、毛利率 | 多 | 年报 2008–24 | ❌ | [fundamental](fundamental.md#营收--净利增长--毛利率) |

### 资金流 / 情绪 / 另类数据 / 事件

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| 股东户数变化 | 多(集中) | 2020+ 月度 | ❌ size 代理 | [flow](flow-altdata.md#股东户数变化) |
| 陆股通持股变化 | 多 | 2023+ | ❌ 噪音 | [flow](flow-altdata.md#陆股通持股变化) |
| 大单/超大单资金流 | 多 | 2023+ | ❌ 噪音 | [flow](flow-altdata.md#大单资金流) |
| 研报覆盖事件 | 多 | 2026-08+ | ❌ 方向反 | [flow](flow-altdata.md#研报覆盖) |
| 个股人气榜 (D1) | 多 | 2025-03+ | ⏸️ SHELVE | [flow](flow-altdata.md#人气榜-d1) |
| Alpha 卡映射票 | 多 | 2026 卡 | ❌ | [flow](flow-altdata.md#alpha-卡映射票) |
| 市场级情绪/资金流共振 (候选C) | 择时 | 2021+ | ❌ | [flow](flow-altdata.md#市场级情绪资金流共振) |
| 两融去杠杆 / 国家队份额 | 择时 | 2021+/2018+ | ❌ / 闸PASS | [flow](flow-altdata.md#两融--国家队) |

### 微结构 / 流动性 / 波动率

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| Amihud 非流动性 | 多 | 日线 · OOS2/train | ❌ 不可交易 | [micro](microstructure-liquidity.md#amihud-非流动性) |
| 日内 last30 / overnight | 反转/动量 | bar_5min 2021+ | ❌ 成本 | [micro](microstructure-liquidity.md#日内微结构) |
| 市值/流动性因子 | 负 | 日线 2007+ | ❌（A股负因子） | [micro](microstructure-liquidity.md#市值流动性因子) |
| 逆波动加权 | — | 日线 2007+ | ❌ | [micro](microstructure-liquidity.md#逆波动加权) |
| 低波动选股 | 多 | 日线 2007+ | ❌ 反向失效 | [micro](microstructure-liquidity.md#低波动选股) |

### 风格 / 市况 / 配置 / 指数 beta

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| 风格×市值×市况 (C1) | 轮动 | 2021–26 | 🧱 描述性 | [style](style-regime-allocation.md#c1-风格市值市况) |
| regime 配置 R1（趋势×慢价值） | 切换 | 2021–26 | ❌ 方向证伪 | [style](style-regime-allocation.md#r1-regime-配置腿) |
| 指数趋势延续 | 择时 | 三窗 | ❌ | [style](style-regime-allocation.md#指数趋势延续) |
| 指数 + 波动率目标（定海 DH-2） | beta | 2005+/2007+ | ⚠️ KEEP(对照) | [style](style-regime-allocation.md#定海-dh) |

### 形态

| 因子 | 方向 | 数据/区间 | 判定 | 详情 |
|------|------|-----------|------|------|
| 强股勺型耗尽（做空） | 空 | factor_signals | ❌ 成交语义证伪 | [pattern](pattern-morphology.md#强股勺型耗尽) |
| 长调整+突破 | 多 | CN1500 2022–26 | 🟡 WEAK | [pattern](pattern-morphology.md#长调整突破) |
| 上升趋势回调 | 多 | CN1500 2021–26 | 🟡 WEAK | [pattern](pattern-morphology.md#上升趋势回调) |
| 箱体支撑/阻力 | 多 | CN1500 2023–26 | 🟡 WEAK | [pattern](pattern-morphology.md#箱体支撑阻力) |

---

## 跨因子教训（每次开新因子前必看）

1. **多半"新因子"是小市值/流动性代理**：控制市值后 IC 塌掉（股东户数、ROE、杠杆、多数基本面）。
   新因子**必须**做市值 / 反转 / 动量正交化后再谈。
2. **绝对量技术形态对 S-3 无增量**（P1–P8、MA、MACD/KDJ/Bollinger）：S-3 的 alpha 全在
   「RS + 环境感知 + 纪律空仓」，形态与之共线或更差。
3. **防守收紧截断右尾**（trail、止损收紧、条件单、水位节流）：回撤端也常没赚到。
4. **单窗好看 = 过拟合**：必须三窗/多窗一致；valid 好 + 选参窗崩 = 陷阱。
5. **成本先算**：日内/高频信号先比"毛收益 vs 往返成本"，IC 再高也可能被竞价价差吃掉。
6. **前视**：任何选池/信号必须用调仓日**可得**的 as-of 值（DH-1 用本月末成交额选本月票 = +20pt/yr 假象）。
7. **仅有的正结果**：慢价值 V1（12 月）、指数 beta DH-2、投资/应计 composite——都是**长周期、低换手**。

## 新增因子流程（模板）

1. 预注册：因子定义/公式、方向、数据、universe、horizon、**通过线**（先写死不事后改）。
2. 先做 **IC 速筛**（rank-IC + IR + 分位单调）→ 再 **市值/反转/动量正交化**。
3. 再做**可交易性**（流动池、成本、换手）。
4. 再做 **walk-forward / 多窗**（S-3 类走三窗；长持类走年度多窗）。
5. 结果（PASS/REJECT）写进本库对应分类档 + `backtests/factors/` 长档 + `SUMMARY.md` 一行。
