# 因子库 · 技术 / 价量 / 动量 / 反转

> 覆盖信号池 P1–P12/P16、TIP-013 IC 验证、以及 MACD/KDJ/Bollinger 等指标研究。
> 原始长档：[`../backtests/factors/`](../backtests/factors/) · 信号池归档 [`../archive/2026-08-15-signal-pool-p1-p26.md`](../archive/2026-08-15-signal-pool-p1-p26.md)
> **总基调**：**全部 REJECT / 噪音**。S-3 的 alpha 只来自「RS 相对强弱 + 环境感知 + 纪律空仓」，
> 绝对量技术形态与之共线或更差（[死因 #1/#3](../backtests/SUMMARY.md)）。

---

## P1 海龟/唐奇安突破
- **定义**：突破 N 日最高价（唐奇安通道上沿）入场。
- **数据/方法**：日线，S-3 三窗（OOS2/train/valid），引擎 `breakout_days` 参数。
- **结果**：OOS2 单窗 +31，但 train **−37.5**、长窗 DD 45→90.8。
- **判定**：❌ REJECT（单窗好看=过拟合）。引擎参数默认关。

## P2 放量突破
- **定义**：价格突破且成交量 > 均量×倍数。
- **结果**：OOS2 单窗 +76，但 train **−97** / valid **−124**。
- **判定**：❌ REJECT。

## P3 MA200
- **定义**：价 > MA200（或带缓冲）。
- **结果**：拦截≈0 笔（信息已被 RS 占用）；缓冲版全劣化。引擎 `ma200_min_pct` 默认 −1（关）。
- **判定**：❌ REJECT。

## P4 均线斜率
- **定义**：MA 斜率 > 阈值。
- **结果**：与 RS 共线，全窗劣化 + 长窗 DD 恶化。引擎 `ma_slope_min_pct` 默认关。
- **判定**：❌ REJECT（死因 #3）。

## P5 双均线金叉
- **定义**：短均线上穿长均线。
- **结果**：金叉=滞后确认，train **−76~−110**。引擎 `ma_cross_days` 默认关。
- **判定**：❌ REJECT。

## P6 三线多头
- **定义**：MA5>MA20>MA60 多头排列。
- **结果**：重复 A2（OOS2 −5.5 / train −6.3）。引擎 `ma_aligned` 默认关。
- **判定**：❌ REJECT。

## P7 短线超卖反转
- **定义**：RSI 等超卖后买反转。
- **结果**：与候选集互斥（valid/train 零样本）。引擎 `rsi_reversal_max` 默认关。
- **判定**：❌ REJECT。

## P8 长阴次日反转
- **定义**：单日大跌后次日买反弹。
- **结果**：valid 2 笔 + 长窗 **−38.9pt**。引擎 `down_day_reversal_pct` 默认关。
- **判定**：❌ REJECT。

## P12 风险调整动量
- **定义**：动量/波动（夏普动量）。
- **结果**：train 全变体 −23~−24 + 长窗 DD 45→63~84。引擎 `risk_adj_mom_*` 默认关。
- **判定**：❌ REJECT（= 砍 beta）。

## P16-ST 剔除
- **定义**：剔除 ST 股。
- **结果**：拦截 3145 次仅少 1 笔；**ST 摘帽才是真 alpha**（准入链已过滤尾部）。引擎 `exclude_st` 默认关。
- **判定**：❌ REJECT。

## TrendOK Score/RS/amount
- **定义**：现有 watchlist 的 score、对沪深300 20 日相对强弱 RS、60 日均额。
- **数据/方法**：全市场 N=5210，30d(2026-07-08)/90d(2026-05-08)/1y(2025-08-01)，Spearman RankIC h=1/3/5/10。
- **结果**：全部 |IC|<0.04、ICIR<0.5、hit≈50%；score h5 30d meanIC **−0.12**；高分组 −3.23 vs +0.12，无区分度。
- **判定**：❌ REJECT（现有横截面信号无预测力）。源：[`factor-ic-2026-08-22.md`](../backtests/factors/factor-ic-2026-08-22.md)。

## mom20/vol20/dd60/flow5d
- **定义**：20 日动量、20 日波动、60 日回撤、行业 5 日净流入。
- **数据/方法**：全市场 N=5210，90d + 1y，h=5/10/20（TIP-013 Phase B）。
- **结果**：全部 |IC|<0.06、ICIR<0.5、hit≈44%；`vol20` 90d h10 −0.267/ICIR −0.81 但不稳（30d hit 29%）；dd60/flow5d≈0。
- **判定**：❌ REJECT（无 ICIR≥0.5 + 单调）。源：[`factor-ic-phaseB-2026-08-22.md`](../backtests/factors/factor-ic-phaseB-2026-08-22.md)。

## MACD
- **定义**：MACD(12,26,9) 金叉 / DIF / Hist。
- **数据/方法**：ETF 518880/513350/513100/511260 + CN 流动1500，2023-01~2026-08，forward 10d 分层，n≥500。
- **结果**：CN 金叉 +1.23%（+0.47% 超额）最好但 win 49.2%、中位 0；down+金叉(+1.30%) > up+金叉(+0.85%)；无统一 edge。
- **判定**：📝 噪音（不入系统）。源：[`macd-trend-study.md`](../backtests/factors/macd-trend-study.md)。

## KDJ
- **定义**：KDJ(9,3,3) 金叉/死叉、J 超买超卖。
- **数据/方法**：同上。
- **结果**：CN `J>80` +1.25%（+0.49% 超额）最强但 win 49.5%、中位 0；all <0.7% 超额。
- **判定**：📝 噪音。源：[`kdj-trend-study.md`](../backtests/factors/kdj-trend-study.md)。

## Bollinger
- **定义**：布林带位置 %b 与带宽 bandwidth。
- **数据/方法**：ETF + CN1500，2023–26，forward 10d。
- **结果**：无统一 edge；CN `expand_all` +1.88% win51.9% 最强；ETF 油 `up+%b>0.9` −2.53% 反转；中位为负。
- **判定**：📝 无统一 edge（目标依赖）。源：[`bollinger-trend-study.md`](../backtests/factors/bollinger-trend-study.md)。

## SuperTrend / Fibonacci / Price Action
- **定义**：SuperTrend（ATR 跟踪）、Fibonacci 回撤、K 线 price action。
- **结果**：无新回测。SuperTrend ≈ MA+波动（MA200+−8% trail 同族，天花板 +1~2%）；Fibonacci 主观、已测 edge <1%、不单测；PA 已覆盖到顶（pin 1.6% / box +1.4% / 长突破 +3.5%，均未到 win70%）。
- **判定**：📝 笔记（SuperTrend 低优可试，其余不扩）。源：[`indicator-supertrend-fibonacci-priceaction-notes.md`](../backtests/factors/indicator-supertrend-fibonacci-priceaction-notes.md)。
