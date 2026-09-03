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
| §8 回测 | 🟡 运营阶段 B2–B5/A5 日流程已落地（OPT-135）；S-3 C4 统计仍等 20 笔 |
| §4/5/6 | 按需 OPT（124–127 稳定性），不改策略 |

## 当前方向（默认 clip4 之后）

- **P0：把机会双子星跑成产品**——不扫新卫星参。三线并行：[工程 / 业务对齐 / 回测可分析](./designs/twin-star-ops-phase-2026-09-02.md)
- 卫星 **成交日历要对齐习惯，不能拿冻结 9:30 当 14:30**（P0-4）——优化目标是「我 14:30 买还能不能赚」，不是贴近 9:30 回测
- 卫星 **14:30 入场过滤**：C1 3% 已三窗（tot/sr/dd）[sat-entry-c1](./backtests/sat-entry-c1-2026-09-03.md) — 相对无过滤 PASS+；vs 核心 valid tot −3.3，**不进 Live**
- 冻结：`skip_t1`+strict、4×12.5%、body=3 收盘卖（无 −5%）、S-3 篮 10×10%、回测=T 开盘、past_year 不当拒收闸
- 改策略前先读 [回测 SUMMARY](./backtests/SUMMARY.md) 与 [2026-09-03 讨论](./backtests/clip4-ops-decisions-2026-09-03.md)（Agent 规则在 `AGENTS.md`）
- 单轨择强 = 核心腿 + Settings 对照，不再是实盘默认
- 脉冲天平仍观察层；Watchlist 占用对照已是双子星 C4-lite（你卫星仓 vs 引擎模拟）；S-3 统计 C4 仍等 20 笔平仓

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
| 6 [done] 2026-09-02 | 工程 | `GET /api/backtest/twin-star/action` Zod + clip4 字面量（4×12.5% 再漂会拒收） |
| 7 [done] 2026-09-02 | 业务+可分析 | 日流程写进 Watchlist；pick≠STOCK 全 CN 卫星；STOCK 日 sat/S-3 拆开；S-3 缺票不当交易铃 |

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

≥20笔平仓后 `scripts/paper_vs_backtest_report.py` 跑，现在 3/20 跳过。双子星占用对照（你卫星仓 vs 引擎模拟）已在 Watchlist，不当交易铃。

## P0-4 卫星成交日历：14:30 ≠ 冻结 9:30（2026-09-03）

**大白话**：优化目标 = 在**你真实的买法**（当天缺口、约 14:30 买、第 3 日收盘卖）上，三窗不过拟合地找还能不能比纯核心赚。冻结 clip4 的 9:30 边是另一套策略，当对照，不当你的成绩单。

已测（收盘代理）：[sat-fill-same-close-2026-09-03.md](./backtests/sat-fill-same-close-2026-09-03.md) — 相对冻结 T 开盘 valid −17.7，**拒收当改写 9:30 引擎**。习惯日历要另开实验室，主判据 twin vs **核心**（任一窗 >5pt 差于核心或明显过拟合 → 不进 Live）。

### 导入什么（机器 36GB / 盘余 136GB / 库已 5.6GB）

| 要 | 不要 |
|----|------|
| **5 分钟 · 按年汇总 · 2024+2025+2026** | 1 分钟（体积大、3 日持有用不上） |
| **只入库 14:30–15:00 七根**（现成 `bar_5min`） | 30/60 分钟（14:30 对不齐） |
| 全 A 尾盘即可（约 5000×500 日×7 ≈ 1700 万行、约 3–4GB） | 按月归档（和按年重复） |
| | 全天 5 分钟（约 8× 行数，回测用不到早盘 K） |

三窗是 2024-08～2026-08，少一年就不够。解压到 `data/2024_5min`、`data/2025_5min`、`data/2026_5min`，然后：

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/import_ext_minute_csv.py
```

CSV **留在磁盘当档案**（zip 约 2.7GB 即可，解压的 13GB 目录导完可删）。库只查引擎会扫的表。C1「14:30/今开」用日线开盘 + 已入库 14:30 价就够；只有要「14:30 之前振幅」才从 CSV **提炼一行/天**（9:30–14:30 OHLC），仍不要灌全天 48 根。

**导入 [done] 2026-09-03**：三年尾盘 5 分钟入库（2320 万行，表 6.5GB）。`same_1430` 已跑：vs 核心 OOS2 +47.1、train −3.3、valid −10.9。

### 回测顺序

1. `same_1430` **[done] 2026-09-03**：当天缺口 + 14:30 成交。相对核心 train/valid 亏，不进 Live。
2. **C1/C2 [done] 2026-09-03**：[sat-entry-c1](./backtests/sat-entry-c1-2026-09-03.md)。C1 3% 相对无过滤 tot/sr/dd 全过；vs 核心夏普、回撤已好，**valid 总收益 −3.3**。不进 Live。不重开 −5% / trail / 砍 4 槽。
3. **3 天 vs 4 天 / 下午买点 [done] 2026-09-03**：[sat-habit-clock](./backtests/sat-habit-clock-2026-09-03.md)。计数仍 body=3；body=4 占槽；13:30–15:00 无更佳分钟。
4. **C1 + 第 3 日卖点 [done] 2026-09-03**：[sat-exit-hhmm](./backtests/sat-exit-hhmm-2026-09-03.md)。14:30 卖三窗 tot/sr/dd 过核心；10:00 不如它。未改 Live。

**不做**：1/30/60 分钟入库；全天 K；把 14:30 写进冻结 T 开盘；停等 baostock job；单窗好看就改 Live。

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

每天：读本页 P0-0 / P0-4 + `modules/watchlist.md` Gate；每周一：跑天平；改 schema 前读 `AGENTS.md`。

---
*压缩规则：完成段只留外链，不回写 archive；新想法先 `designs/` 草稿。*
