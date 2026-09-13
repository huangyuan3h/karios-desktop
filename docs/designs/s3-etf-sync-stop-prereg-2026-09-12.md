# 预注册：板块 ETF 同步破位当个股退出（H-ETF-C · 2026-09-12）

> 跑前冻结。**只读诊断**（不碰 Live、不改引擎、不开回放）。按 `first-principles §一.10 退出紧度律` 纪律：先印**覆盖 + 鞭打/救损占比 + 退出分布**，只有诊断方向对才配开引擎回放。

## 假设

**H-ETF-C**：S-3 持仓股**同板块 ETF** 同步破位（跌破 MA20 或自持仓峰值回撤 ≥8%）领先于个股自身止损 → 提前退出能避免后续下跌（`Delta>0`）。

**一句话机制**：板块是"集合信息"，可能比单票更早反映资金撤离（`§一.3`：**外生截面**信号，非自指涉持仓门）；若成立，ETF 破位是趋势反转的领先指标。

**最可能死因**：**#1 截断右尾 + §一.10 鞭打税**——ETF 破位多为噪音，个股随后收复，提前卖=把赢家砍在半路；次选 #2（与已有 stock trailing/ATR 止损共线，覆盖率低）。

## 冻结口径

| 项 | 值 |
|----|----|
| S-3 | CN 冻结 `S3_CONFIG`，窗口 OOS2/train/valid/**long(2021-08-01~2026-08-07)** |
| 逐笔 | `run.trades`；持有路径 = `close_by_ts_day[symbol]` 在 [entry_date, close_date] 的日收盘 |
| ETF 数据 | `data/etf/etf_daily.csv`（复权 `close_adj`） |
| **主规则** | **sector_trail8**：同板块 ETF 自**持仓期峰值**回撤 ≥8%（与个股 trail8 对称）→ 当日收盘卖 |
| 形态对照（非选参） | sector_ma20（ETF 收盘 < ETF MA20）· board_trail8 · board_ma20（板块无对口 ETF 时 board 兜底） |
| 行业对齐 | `watchlist_score_daily.industry` 关键词映射（同 ①），无对口 → 记未覆盖 |
| 逐笔度量 | `R_nat`=股 entry→close；`R_stop`=股 entry→stop日；`Delta=R_stop−R_nat`；停火仅当 stop 日 < close_date |
| 统计 | 覆盖=停火笔/总笔；鞭打=停火笔中 `Delta<0` 占比；救损=停火笔中 `Delta>0`；另报 mean/median Delta（停火笔 & 全样本） |

## 判定线（跑前冻结）

- **K1**：long 主规则**停火笔** mean Delta > 0 **且** OOS2/train/valid ≥2 窗 >0。
- **K2**：覆盖 ∈ [10%, 70%]（<10% 无增量／>70% 砍太多=截右尾）。
- **K3**：鞭打率 < 50%（多数停火是真避损，不是噪音）。
- **结论**：K1&K2&K3 → 开引擎三窗回放（另预注册 NAV）；否则 **REJECT 关线**（不补网格）。
- 形态对照仅作平台/共线检查，**不挑最优**；分窗/分年只读。

## 结果（跑后补）

**REJECT**（K1 ❌：long meanΔ +3.55% 但 WF [−1.50, +1.69, 0] 仅 1/3；K2 ✅ cov 12.4%；K3 ✅ 鞭打 9.8%）。放宽 sector_ma20 覆盖升但 OOS2 meanΔ −4.76%（中位 +1.42）= 截右尾。见 [`s3-etf-sync-stop-2026-09-12.md`](../backtests/etf/s3-etf-sync-stop-2026-09-12.md)。
