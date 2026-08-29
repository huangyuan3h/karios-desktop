# Karios · 路线图（todo · 2026-08-27 精简版）

> 唯一入口，只留活的 P0。完成即标 `[done]` 并迁 `archive/`。历史全量见 `archive/2026-08-27-todo-full-snapshot.md`。
> 对应：怎么做→`optimization-checklist.md` / 怎么投→`trading-improvement-checklist.md`。

## 0. 优先级（不可漂移）

| 1 收益 | §22 多资产 §8 回测 |
| 2 API/AI | §3 |
| 3 工程部署 | §4 |
| 4 数据源 | §5 |

## 1. 状态看板

| 域 | 状态 |
|----|------|
| §19 S-3 | ✅ 封闭（股票腿参数冻结；服务于择强单轨） |
| §22 / 择强单轨 | 🟡 **P0 主线** 产品策略 = [择强单轨](./modules/pick-strong-track.md)；过去一年定案 **+93.6%/DD28.3%** |
| §8 回测 | ✅ 实验归档指向择强单轨；CN NAV 三窗 47.3/34.1/38.7；C4 等 20 笔 |
| §4/5/6 | 按需 OPT，不占 P0 |

## 当前方向（验证+维护期）

- **P0：择强单轨**（全资产同权 100% 硬切）——只优化择强打分/切换；S-3 不扫参。真值 `modules/pick-strong-track.md`
- 验证闭环：`user_trades` / paper ≥20笔 → C4；Timeline/`mom_compare` 对齐 **[done] 2026-08-29**
- 系统健康：季度复核 + rolling OOS
- 脉冲天平（金/油）仅观察层，不升格为独立策略

---

## P0-1 脉冲天平周更（§22.7 · 2026-08-27 安排）

**大白话**：找“金/油/纳指谁更强”的高置信天平，赢率>70%才敢加杠杆。

- 已发现（662d, ahead10d spread）：`油RSI>70 → 金-油 +3.77% win73% n74` / `>80 +4.00% win82.9% n35` / `nas mom20<-5% 金-油+4.66% win71.4% n63` / `油低波 金-纳指+2.46% win65.1% n129`。仅 `>80%` 达杠杆线但 `past_year -1.06%` 回撤，不进杠杆。
- **节奏**：每周一 8:15 `scripts/commodity_pattern_scan.py` 跑分层表 → 追加一行到 `docs/backtests/gold-oil-nasdaq-balance.md`（条件→n→win→mean→可杠杆）→ `win>70% n>50` 才提 `paper 10%→20%`。
- **纪律**：不写新策略代码，只加行；三窗纪律不变。

## P0-2 套筒观察→实盘（§8 T6 · 2026-08-27 安排）

**大白话**：闲钱别趴 GC001，让它跟着最强ETF跑；观察期后搬进 watchlist 实盘提示。

- 已固化：`multi_asset_sleeve.py:52` “纳指优先（>MA200 且 mom60>0 且 rank1 则纳指，否则 max mom60）” 三窗 `+19.3/+17.9/+14.4 past_year+38.1`，`portfolio_health` 返回 `multiAssetSleeve OIL 7.11%`，`sleeve_paper_auto 18:20 ROTATE` 自动换仓。
- **待做**：
  1. `20d -10%` 硬切 GC001 变体三窗验证（尾险）
  2. `R3 risk-adj` 已证不如 `mom60`，标记废弃
  3. 观察期后进实盘 watchlist（`ThirdAssetSleeveBanner` 已可消费 `multiAssetSleeve.pick`）
  4. 联合 `R5CS`（CN/HK内部闲置吃套筒）三窗 `+10.8/+17.0/+30.9` 已验，需接 live `allocation.py`
- **不做**：期货杠杆/外盘直连/高频。

## P0-3 C4 paper对照（跳过·等20笔）

≥20笔平仓后 `scripts/paper_vs_backtest_report.py` 跑，现在 3/20 跳过。

---

## 实施清单（剩余 P0/P1 各一行）

| # | 动作 | 预期 |
|---|------|------|
| 9 | 付费API矩阵 | 上云选型 P1 |
| 2b | Tunnel 端到端 `brew install cloudflared` | 远程前提 P1 |

## 沉淀（近 5 条，余见 archive/README）

| 2026-08-27 | 形态三噪音+回踩MA20弱edge归档 | `backtests/bollinger|macd|kdj|uptrend-pullback` |
| 2026-08-27 | SuperTrend/Fib/PA 分流结论 | `backtests/indicator-supertrend-fibonacci-priceaction-notes.md` |
| 2026-08-24 | 多资产轮动固化 `mom60+MA200` | `service/multi_asset_sleeve.py` |
| 2026-08-14 | neutral_block/entry_style/env_scale 固化 | `strategy-params.md` |
| 2026-08-12 | 回撤熔断 -25 | `strategy-params §6` |

## 注意力预算

每天：读本页 P0-1/2 + `modules/watchlist.md` Gate；每周一：跑天平；改 schema 前读 `AGENTS.md`。

---
*压缩规则：完成段只留外链，不回写 archive；新想法先 `designs/` 草稿。*
