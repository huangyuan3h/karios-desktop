# Karios · 路线图（todo · 2026-08-27 精简版）

> 唯一入口，只留活的 P0。完成即标 `[done]` 并迁 `archive/`。历史全量见 `archive/2026-08-27-todo-full-snapshot.md`。
> 对应：怎么做→`optimization-checklist.md` / 怎么投→`trading-improvement-checklist.md`。

## 0. 优先级（不可漂移）

| 1 收益 | §22 机会双子星（核心+卫星）§8 回测可分析 |
| 2 API/AI | §3 |
| 3 工程部署 | §4 |
| 4 数据源 | §5 |

## 1. 状态看板

| 域 | 状态 |
|----|------|
| §19 S-3 | ✅ 封闭（STOCK 腿生成器；pick=STOCK 才进篮） |
| §22 / 机会双子星 | 🟢 **P0 主线** 实盘默认 = [机会双子星 v3.1 clip4](./backtests/state-bucket-algo-2026-08-31.md)（4×12.5%）；核心腿 [择强单轨](./modules/pick-strong-track.md) past_year **+190.6%** · clip4 **+194.9%** / DD12.6 |
| §8 回测 | 🟡 下一刀 = action Zod（OPT-009）+ 剩余 B2/B3/B5 / A5 C4；快照失败 + ETF/dailybasic 健康已上 |
| §4/5/6 | 按需 OPT（124–127 稳定性），不改策略 |

## 当前方向（默认 clip4 之后）

- **P0：把机会双子星跑成产品**——不扫新卫星参。三线并行：[工程 / 业务对齐 / 回测可分析](./designs/twin-star-ops-phase-2026-09-02.md)
- 冻结：`skip_t1`+strict、4×12.5%、body=3、回测=T 开盘、保护止损不进腿、past_year 不当拒收闸
- 单轨择强 = 核心腿 + Settings 对照，不再是实盘默认
- 脉冲天平仍观察层；C4 要改成双子星 paper vs 回测（停用 S-3「缺 19 只」当交易铃）

---

## P0-0 机会双子星运营阶段（2026-09-02 拍板）

**大白话**：默认已经是最好的那套（clip4）。下一阶段不找更猛的卫星，把「今天该买谁、买不到谁、核心该持什么」在 Watchlist 和回测页对齐。

详细表：[`designs/twin-star-ops-phase-2026-09-02.md`](./designs/twin-star-ops-phase-2026-09-02.md)

| 刀 | 线 | 验收 |
|----|----|------|
| 1 [done] 2026-09-02 | 业务 | Watchlist 仓位/文案/QuickBuy=12.5%；归因对照说双子星不是单轨 100% |
| 2 [done] 2026-09-02 | 可分析 | Timeline 叠 twin/核心/卫星 + 窗口标签（三窗 / 产品过去一年 / trailing） |
| 3 [done] 2026-09-02 | 工程 | 占用真值=Watchlist 4 槽；卫星 paper 簿 `source=twin_star` |
| 4 [done] 2026-09-02 | 可分析 | 每日跳过数 + 卫星 blotter |
| 5 [done] 2026-09-02 | 工程 | 12:30 快照失败可见；核心 ETF / dailybasic 新鲜度当双子星健康 |

**不做**：涨停顺位补、金字塔折进卫星、14:30 价当回测开盘、自动下单。

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

每天：读本页 P0-0 + `modules/watchlist.md` Gate；每周一：跑天平；改 schema 前读 `AGENTS.md`。

---
*压缩规则：完成段只留外链，不回写 archive；新想法先 `designs/` 草稿。*
