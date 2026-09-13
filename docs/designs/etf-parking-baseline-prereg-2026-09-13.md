# 预注册：ETF 层作为 S-3 之上的新基线（H-PARK · 2026-09-13）

> **单假设 · 门槛跑前冻结。** 把 ETF 层退回**原始定位**——**S-3 闲置现金的停车场**（替代 0/逆回购），并把它确立为**新基线**（后续实验从 S-3+parking 出发）。**闲置即停**：任一交易日只要 `idle > 0` 就按 mom60+MA200 择强停进 ETF，不设 floor、不设"强过股票篮"门槛。
> **关键词**：停车场 闲置即停 14:30 S-3 新基线 预注册

## 0. 一句话机制（§四 S1）
S-3 常留 ~40–60% 闲置现金；把闲置部分按 **mom60+MA200 择强**停进 ETF（金/油/纳指/国债），替代收益≈0 的现金 → 不改变股票仓的前提下增厚收益。**只在闲置上动手，天然保 S-3 alpha**。

## 1. 为什么重开（背景）
- ETF 层**原本**就是"闲置现金的 ETF 增强"（`pick-strong-track.md:39`）；后"择强单轨"把它并成 **100% argmax 同池** → OPT-177 前视修正后实测**打崩 S-3**（long ≈ +0.8% / MDD −58%），定位错误已确认。
- H-SLEEVE（2026-09-12）回头验证停车场：V2/V4 三窗 +11~13pt、V4 最平滑；但**未含成本、无 long 窗**，且 Live 口径带两个 overlay（`MIN_IDLE_PCT=20`、ETF mom60 > STOCK mom60）。
- Live 前视审计（2026-09-13）：trail8 触发用 T 收盘、账面按 T-1 收盘平仓 → 同类前视；另有 `_pnl_for` 字段名错、`SELL_TO_A_SHARE` 0 元平仓、测试污染真实账本三个 bug（详见 §7，判定通过后一并修）。
- 本稿目的：把"闲置即停"跑成 **三窗+long 全过**的正式口径，**冻结为新基线**，并量化 Live overlay 的代价。

## 2. 口径（冻结）
- 基线：**S-3 CN 引擎**（`S3_CONFIG`，window-local，逐窗独立），`engine_nav_by_day`。
- 闲置比例：每日 `idle = 1 − min(1, Σ position_pct)`（用 **t-1** 快照持仓）。
- 时钟=**统一 14:30（卫星口径）**：信号与成交同一 print（T 14:30，mom60/MA200/trail 全用可得数据）；回测用 **T 收盘代理**（与 14:30 差 0.5h，零前视）；Live 目标 = 14:28 出信号 → 14:30 执行（替换现有 18:20 job + T+1 开盘）。
- 成本：**0.05%/边**（`COST`），rotate=2 边、单边进出=1 边，计在 `idle` 名义上；闲置每日跟随 `idle` 增减视为账户内划转（与 H-SLEEVE 同简化）。
- 无 pick（全候选破 MA200 / trail 触发）→ repo=**0**（保守）。
- 候选：{518880 金, 513350 油, NASDAQ=best(513110,513100), 511260 国债}，**全部须 close>MA200** 才入池（= Live `_pick` 规则）。
- 数据：`data/etf/etf_daily.csv`（**close_adj** 复权价；513350 自 2023-11-28 上市）。

## 3. 变体（预声明）
| # | 规则 |
|---|------|
| V0 | 基线：闲置收益 0（现状） |
| **P1** | **闲置即停**：idle>0 → 停 argmax mom60（MA200 上）；因果 trail8；rotate；成本 |
| R1 | P1 + Live floor：仅 `idle ≥ 20%` 才停（量化 Live overlay 代价） |
| R2 | P1 + Live gate：`ETF mom60 > 股票篮 mom60` 才停（同上） |
| R3 | P1 但 BOND10 恒入池（V3 旧口径对照） |

## 4. 判定（冻结）
- **K1**：P1 三窗（OOS2/train/valid）增量 ≥ −0.05pt（vs V0）。
- **K2**：三窗合计增量 **> 0**。
- **K3**：long（2021-08-01~2026-08-07）增量 **> 0**。
- K1&K2&K3 → **新基线冻结 = S-3 + 停车场**；R1/R2/R3 只作 robustness/代价记录，**不作选参**。
- 不扫：候选清单 / lookback / MA / trail 阈值（60/200/8 沿用）。
- 报告：三窗+long 的 total/delta，并补组合 NAV 的 CAGR/MDD/Sharpe（新基线档案）。

## 5. 死因预判（跑前写死）
- 主：`#2 共线`（闲置期=弱市，ETF 同跌）；`#4 regime`。
- 次：成本吃掉（rotate 频繁）；long 窗 gold/oil 单边行情放大样本偏差。

## 6. 产出
- 脚本：`scripts/eval_etf_parking_baseline.py` → `data/backtest_reports/etf_parking_baseline_2026-09-13.json`
- 结论落 `docs/backtests/stable/etf-parking-baseline-2026-09-13.md` + SUMMARY + todo。

## 7. Live 修复清单（判定通过后执行；不通过则只修 bug 不改口径）
1. **时钟**：18:20 job（t-1 信号 + t+1 开盘）→ **14:28 出信号 / 14:30 执行**（与卫星统一）；trail/破 MA/rotate 同。
2. **真前视**：`_etf_trail_exit` 触发用 T 收盘但账面按 T-1 收盘平仓 → 按 14:30 print 平仓。
3. **`_pnl_for` 字段名**（读 snake、行是 camelCase）→ 恒 0 的 pnl/days。
4. **`SELL_TO_A_SHARE` 0 元平仓**：`pick` 无 `close` 时的成交价来源。
5. **测试污染**：`tests/test_sleeve_paper_auto.py` 会关真实候选腿 → 测试只准动带前缀 symbol。
6. **overlay 对齐**：`MIN_IDLE_PCT=20`、ETF>STOCK gate 的去留按 P1/R1/R2 结果决定。

**结果（跑后补）**：**P1 PASS（K1/K2/K3 全过）→ 新基线 = S-3 + 停车场**。三窗增量 **+9.6/+13.7/+9.0**、long **+119.6**（幻影日修正后）；V0 三窗 = 官方基线 ✅。Live overlay：R1（20% 地板）≈0（long −1.7）；**R2（ETF>STOCK）long +20.8 vs +90.0 → 应去掉**；R3 ≈ P1。风险：valid MDD −9.4→**−23.1**、Sharpe 2.44→1.91（空仓期满仓 ETF + trail 无冷却再进）；long MDD −23.6/Sharpe 0.83（均优于 V0）。见 [`etf-parking-baseline-2026-09-13.md`](../backtests/stable/etf-parking-baseline-2026-09-13.md)。

*冻结于 2026-09-13，跑数前。*
