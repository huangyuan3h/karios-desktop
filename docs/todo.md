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
| §22 / 机会双子星 | 🟢 **P0 主线** 实盘默认 = [机会双子星 v3.1 clip4](./backtests/state-bucket-algo-2026-08-31.md)（4×12.5%）+ **习惯 Live：C1 3% + 第3日14:30卖**；核心腿 [择强单轨](./modules/pick-strong-track.md) past_year **+190.6%** · clip4 **+194.9%** / DD12.6 |
| §8 回测 | 🟡 运营阶段 B2–B5/A5 日流程已落地（OPT-135）；S-3 C4 统计仍等 20 笔 |
| §4/5/6 | 按需 OPT（124–127 稳定性），不改策略 |

## 当前方向（默认 clip4 之后）

- **P0：把机会双子星跑成产品**——不扫新卫星参。三线并行：[工程 / 业务对齐 / 回测可分析](./designs/twin-star-ops-phase-2026-09-02.md)
- 卫星 **成交日历要对齐习惯，不能拿冻结 9:30 当 14:30**（P0-4）——优化目标是「我 14:30 买还能不能赚」，不是贴近 9:30 回测
- 卫星 **14:30 入场过滤**：C1 3% 已三窗（tot/sr/dd）[sat-entry-c1](./backtests/sat-entry-c1-2026-09-03.md) — 相对无过滤 PASS+；单配收盘卖时 vs 核心 valid tot −3.3，**配第3日14:30卖后三窗全过核心，已进 Live（习惯）**
- 冻结对照：`skip_t1`+strict、4×12.5%、body=3 收盘卖（无 −5%）、S-3 篮 10×10%、回测=T 开盘、past_year 不当拒收闸；**Live 习惯：C1 3% + 第3日14:30卖**
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

**大白话**：优化目标 = 在**你真实的买法**（当天缺口、约 14:30 买、C1 过滤、第 3 日 14:30 卖）上，三窗不过拟合地找还能不能比纯核心赚。冻结 clip4 的 9:30 边是另一套策略，当对照，不当你的成绩单。

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
2. **C1/C2 [done] 2026-09-03**：[sat-entry-c1](./backtests/sat-entry-c1-2026-09-03.md)。C1 3% 相对无过滤 tot/sr/dd 全过；单配收盘卖时 vs 核心 valid 总收益 −3.3，**已随 habit（C1+14:30卖）进 Live**。不重开 −5% / trail / 砍 4 槽。
3. **3 天 vs 4 天 / 下午买点 [done] 2026-09-03**：[sat-habit-clock](./backtests/sat-habit-clock-2026-09-03.md)。计数仍 body=3；body=4 占槽；13:30–15:00 无更佳分钟。
4. **C1 + 第 3 日卖点 [done] 2026-09-03**：[sat-exit-hhmm](./backtests/sat-exit-hhmm-2026-09-03.md)。14:30 卖三窗 tot/sr/dd 过核心；10:00 不如它。**Live 已切 habit（2026-09-03 拍板全量跟进）**：Watchlist/paper `C1 3% + 第3日14:30卖`，冻结 T 开盘 Timeline 默认不动，习惯对照走 `sat_fill=same_1430&c1_pct=0.03&sat_exit=1430`。
5. **习惯排名 H1 [done] 2026-09-04**：[sat-rank-hhmm](./backtests/sat-rank-hhmm-2026-09-04.md)。无前视键（gap升序 / |14:30/今开−1|升序）2 变体全拒：gap OOS2 −96pt 永不重开；|runup| valid +14.4 但 OOS2 −21.5（过拟合陷阱）。Live 排名不动；R-wide 全天收盘闸记 H1-followup。
6. **习惯 C1 网格 H2 [done] 2026-09-04**：[sat-c1-grid](./backtests/sat-c1-grid-2026-09-04.md)。C1=3% 站在平顶上：2% 打平但 train 降（不换）；4% 走弱；5% train −5.8 拒收。**Live 保持 3%**。
7. **习惯 bucket_q H3 [done] 2026-09-04**：[sat-bucketq](./backtests/sat-bucketq-2026-09-04.md)。top-1/2 选参窗 tot/sr 全弱于 1/3（train −2.3/sr−0.41），valid 无差。**Live 保持 1/3**，4 槽不动。
8. **习惯 R-wide 闸 H4 [done] 2026-09-04**：[sat-rwide](./backtests/sat-rwide-2026-09-04.md)。0.5 三窗一致最优；0.4 valid −17.9；0.6 valid +13.4 但选参窗崩（拒）。**Live 保持 0.5，打磨收工**——等 paper 20 笔 C4 实证。
9. **习惯 C3 下跌过滤 S2 [done] 2026-09-04**：[sat-c3-fade](./backtests/sat-c3-fade-2026-09-04.md)。诊断两窗同向（<−3% 档最差），组合层面冗余（跳 564/fills−1）。**不进 Live**；周二/C2 方向死在诊断。
10. **习惯 holdout 审计 S1 [done] 2026-09-04**：[sat-holdout](./backtests/sat-holdout-2026-09-04.md)。19 sessions Δ −5.1 ≈ 第 7 百分位坏月份（p5 −5.53），分布内。**不调参**；真风险是方差（~7% 月份 −5pt），不对月考核卫星。
11. **习惯 CHURN 过滤 S4 [done] 2026-09-04**：[sat-churn](./backtests/sat-churn-2026-09-04.md)。六维诊断（换手/板块/市值/年限/大盘高开/breadth）只活一个：T-1 放量>4x 不追，train +2.4/valid +1.5 但 OOS2 −1.0 → PASS/worse。**不进 Live，记候选**（待 holdout 满 60 sessions 或 paper 20 笔重验）。
12. **大盘风格 vs 卫星 G1 [done] 2026-09-04**：[sat-regime](./backtests/sat-regime-2026-09-04.md)。up 三窗全赚最稳，choppy 次之，down 被 R-wide 拦；波动率非稳定因子。**无新规则**。
13. **第 3 日条件单 D3（用户规则）[done] 2026-09-04**：[sat-exit-d3trail](./backtests/sat-exit-d3trail-2026-09-04.md)。盘中高点回落 2% 卖否则 14:30，三窗 REJECT/total（OOS2 −6.4/train −2.8/valid −5.8，夏普全差，触发率 63%）。**回吐≠反转，不进 Live**；1%/3% 网格不补（机制证伪）。

**不做**：1/30/60 分钟入库；全天 K；把 14:30 写进冻结 T 开盘；停等 baostock job；单窗好看就改 Live。

## P0-5 补短板（系统评估 2026-09-04，低于 8 分项）

**大白话**：回测纪律是 9 分水平，短板全在"实盘靠人盯"。先把靠文档兜底的三件事变成机器保证。

| 分 | 项 | OPT | 验收 |
|----|----|-----|------|
| 工程 6 | 红套件清零（lastfailed 47 + tsc + 过期测试）[done] 2026-09-04 | OPT-138 P0 | pytest 全绿 + tsc 零报错 |
| 工程 6 | Scheduler 上报统一（交易链先行）[done] 2026-09-04 | OPT-139 P0 | 失败必留 record，提醒 job 不再静默挂 |
| 业务 5 | 模式口径（audit 不再报卫星"买了不该买"）[done] 2026-09-04 | OPT-140 P0 | 卫星票 extra 为空 + hub 横幅修复 |
| 业务 6 | Live 习惯口径进冻结引擎重跑三窗[done] 2026-09-04 | OPT-141 P0 | 三窗表 + PASS/REJECT 留档，不过就改 Live |
| 工程 6-7 | 日历收敛 + API 校验 + Alembic 重号[done] 2026-09-04 | OPT-142 P1 | weekday 只剩真值处 + history 线性 |
| 复盘 5 | 历史可重放（覆盖率表 + fail-open 审计 + ST 5%）[done] 2026-09-04 | OPT-143 P1 | 覆盖率表 + 清单进 strategy-params |
| — | 外围任务抖动（option_iv/news）[done] 2026-09-04 | OPT-144 P2 | 外围失败不即时推 Bark |

顺序：138 → 139 → 140 → 141 → 142/143 → 144。一次一 OPT，不扩 scope。

---

## P0-6 今日可跑实验队列（2026-09-04）

> 按重要程度排序，脚本+数据就绪、不碰 Live、不过预注册红线。做完一条标 `[done] YYYY-MM-DD`。
> 不在 `docs/` 根单开文件，此段即唯一队列。CHURN 重验 / C4 paper（3/20）/ V7.0-03 进 Live / 砍篮 / trail / gap 升序不在列（预注册锁死或已 REJECT）。

| # | 实验 | 脚本/数据 | 验收 |
|---|------|-----------|------|
| 1 | [done] 2026-09-04 卫星 holdout 只读重跑（冻结窗复现一致：19 sessions/32 fills，core −1.5/twin −6.6/Δ−5.1；外推至 09-04：20 sessions/36 fills，Δ−4.8，仍在分布内，不调参） | `scripts/holdout_habit_check.py` | Δ 是否在 p5（-5.53）内；弱不重开已拒变体，强也不开新变体 |
| 2 | [done] 2026-09-04 TIP-014 Phase3 名单漂移（Jaccard 0.43 但 fwd 差 0.14pp/笔，无超额；Live 本来就是 proxy 排名，不改；档 `sat-list-drift-2026-09-04.md`） | `bar_5min` 已入库 + `scripts/diag_sat_list_drift.py`（新建只读诊断） | 漂移率表，只写结论，不改 clip4 Live |
| 3 | [done] 2026-09-04 P0-2 套筒 20d -10% 硬切 GC001 三窗（OOS2/train 零触发无信息，valid +7.0 单窗亮，证据不足不进 Live；档 `sleeve-exit-hard20-2026-09-04.md`） | `scripts/multi_sleeve_grid.py` + `sleeve_exit_variants.py`，走 `run_walk_forward.py` | 三窗 tot/dd/sharpe vs 现状，>5pt 劣化拒收 |
| 4 | [done] 2026-09-04 P0-1 脉冲天平周更（全历史重算：RSI>80 n35 逐数复现，RSI>70 n84/win72.6%，mom<-5% n63 无新增；今日无触发，不进杠杆） | `scripts/commodity_pattern_scan.py` → 追加一行到 `gold-oil-nasdaq-balance.md` | 条件→n→win→mean→可杠杆，不写策略代码 |
| 5 | [done] 2026-09-04 TIP-013 结论落回（有效因子清单：空，无新增；S-3择时为唯一超额源） | 纯文档：`factor-ic-2026-08-22` + `factor-ic-phaseB` | 条目勾选“空，无新增，S-3择时为唯一超额源” |
| 6 | [done] 2026-09-04 形态独立验证首跑（目录核查：8形态7拒，唯一≥80%已落库；OOS快照因破位规则实现缺失判underpowered；生产表仅1天backfill无日调度；档 `scoop-exhaustion-oos-check-2026-09-04.md`） | `scripts/pin_bar_scan.py` + `designs/pattern-factor-validation.md` | 胜率/盈亏比 vs base rate，不进 S-3 |
| 7 | [blocked] TIP-010 宽宇宙 Jaccard 对照（需用户从东财手动导 ≥5 个交易日“均线多头排列”名单；收到后跑同口径回撤+TrendOK 算 Jaccard，不上线替换） | 人工导 5 日东财多头列表 + TrendOK 同口径 | 表格落 checklist，不上线替换 |

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
