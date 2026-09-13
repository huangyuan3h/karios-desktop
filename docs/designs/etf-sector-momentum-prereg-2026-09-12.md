# 预注册：板块 ETF 相对强度作为信号（H-ETF-B · 2026-09-12）

> 跑前冻结。只读诊断（不改 Live/引擎）。先证伪"信号本身"，再谈要不要接主线闸。

## 假设

**H-ETF-B**：板块 ETF 相对强度（mom20 高的行业）**预测未来行业收益** → 可作为/补充主线闸的行业 allowlist。

**一句话机制**：资金流入某板块 → 该板块 ETF 相对走强 → 相对强度延续（板块级动量）。若真，Top3 强势板块 ETF 的**远期收益**应高于全体板块 ETF 等权。

**为什么先测信号本身**：`first-principles §五 维5 行业` = **全吸收**（P11-N −30.3 / P17-IND −124 / D5 −47.1 / 三簇全拒），主线闸已吃行业维度。故先做**必要条件**（信号有远期 edge 吗），无 edge 直接关，不进闸门设计；有 edge 才谈与 flow 闸的重合。

**预判死因**：**#2 共线（主线闸吸收）+ #4 单窗/regime**（指数/ETF 动量线已三次死于 regime 不一致，`index-trend §7`）。

## 冻结口径

| 项 | 值 |
|----|----|
| 宇宙 | `data/etf/etf_daily.csv` 中 **kind=sector** 的 29 只（复权 `close_adj`） |
| 信号 | 按 `mom20 = close_adj(t)/close_adj(t−20)−1` 排名；取 **Top3**（次档 mom60 仅形态检查） |
| 远期 | Top3 的 H 日远期收益均值 vs **全板块 ETF 等权**远期，H = 5/10/20 |
| 日频 edge | `edge_H(t) = mean_fwd_top3 − ew_fwd_all`；分窗 OOS2/train/valid/**long(2021-08~2026-08-07)** + 分年 |
| 轮动 NAV（背景） | 每 5 交易日按 mom20 Top3 等权，日收益 close-to-close，20bp 换手；对照 EW 板块 |
| 对齐 | 只用信号日**收盘**（无前视）；缺 t 或 t+H 的 ETF 剔除 |

## 判定线（跑前冻结）

- **K1**（信号有 edge）：long `mean edge_20` > 0 **且** OOS2/train/valid ≥2 窗 >0。
- **K2**（H=20 单调背景）：edge_5/edge_10/edge_20 不全 ≤0（防只单档偶然）。
- **K3**（轮动 NAV）：mom20 Top3 周轮动 long NAV 扣 20bp **过** EW 板块；且 ≥2 WF 窗过。
- **结论**：三项全过 → 开"ETF 强度 vs/补主线闸"预注册；任一不过 → **REJECT 关线**（不补网格/不调 TopK/不改 H）。
- 分窗/分年只读；mom60、TopK 邻域不扫（避免网格）。

## 结果（跑后补）

**REJECT**（K1 ✅ 微弱：edge_20 long +0.15%、WF [0.11,+2.34,+1.58]；K2 ✅；**K3 ❌：轮动 long −44.1% vs EW +4.0%、DD 69%、0/3 窗**）。信号有微弱正 edge 但不可交易，regime 翻转。见 [`etf-sector-momentum-2026-09-12.md`](../backtests/etf/etf-sector-momentum-2026-09-12.md)。
