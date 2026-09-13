# 预注册：S-3 股票腿是 alpha 还是 beta？（同窗同板块 ETF 归因 · H-ETF-A · 2026-09-12）

> 跑前冻结。只读诊断（不改 Live、不开回放）。回答用户问题①「ETF 能不能代替 S-3 股票腿」的**前置尺子**：先量 S-3 每笔持仓相对同窗同板块 ETF 有没有超额。

## 假设

**H-ETF-A**：S-3 选股/择时有 alpha → 每笔持仓窗内个股收益**跑赢同窗同板块 ETF**（同板块宽基 + 行业 ETF）；若超额 ≤0，则股票腿≈板块 beta，ETF 替代值得 A/B。

**一句话机制**：S-3 吃趋势**早期**（`first-principles §一.2`），若这层择时+选股是真的，它应在同样的持有日历里打败"直接买那个板块的 ETF"；若打不过，说明收益全来自板块 beta，选股层不增值。

**预判**：趋势窗（train/valid）超额为正、弱市窗（OOS2）可能≤0；`#6 成分效应`（弱/牛年不是同一个池）是最大威胁。

## 冻结口径

| 项 | 值 |
|----|----|
| S-3 | CN 冻结 `S3_CONFIG`（score65/hold60/trail8+ATR/熔断/exclude300/next_open），窗口 **2021-08-01~2026-08-07** |
| 逐笔 | `run.trades`（已平仓）；窗口 = entry_date→close_date |
| 个股收益 | `close_by_ts_day[ts][close_date] / [entry_date] − 1`（引擎同源、close-to-close） |
| 板块基准 | 行业关键词→行业 ETF（`data/etf/etf_daily.csv`，复权 `close_adj`）；映射覆盖不足的笔单列 |
| 宽基基准 | `board`：688→588000.SH；主板(60/00)→510500.SH；创业板(30)→159915.SZ；另报 `broad300`=510300.SH |
| 超额 | `excess = 个股收益 − 基准收益`（同 entry/close 日历，ETF 复权 close-to-close） |
| 窗口 | OOS2/train/valid/**long** + 分年；分板块/宽基两张表 |
| 行业映射 | `watchlist_score_daily.industry`（as-of entry_date，PIT）；关键词表见脚本，覆盖 <50% 时板块结论标 underpowered |

## 判定线（跑前冻结 · 归因结论，非 Live 采纳）

- **K1**（打不打得过板块宽基）：long 均值 `excess_board` > 0 **且** OOS2/train/valid 至少 2 窗 >0。
- **K2**（板块内选股）：long 均值 `excess_sector` > 0 **且** ≥2 窗 >0（覆盖 ≥50% 笔）。
- **K3**（防右尾）：`median(excess_board)` > 0（防少数大赢家撑均值）。
- **结论规则**：
  - K1&K2&K3 → S-3 选股是真 alpha，**ETF 替代会毁价值**（记录，关闭"ETF 代替股票"）。
  - K1 过、K2 不过 → alpha 来自择时/宽度而非板块内选股 → 下探"ETF 打底 + S-3 选股"。
  - K1 不过 → 股票腿≈板块 beta → **ETF 替代值得开 A/B 回放**。
- `excess` 分布记 mean/median/胜率，另给基准自身同窗收益做背景；不扫参、不选窗。

## 结果（跑后补）

**K1 ✅（long board +1.22%，3/3 窗）/ K2 ✅（sector +1.55%，3/3 窗，cov 78%）/ K3 ❌（median −2.65%）→ 右尾型选股 alpha，非纯 beta；不采纳被动 ETF 替代。** 见 [`s3-alpha-vs-etf-2026-09-12.md`](../backtests/etf/s3-alpha-vs-etf-2026-09-12.md)。
