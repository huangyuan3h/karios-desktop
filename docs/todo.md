# Karios · 路线图（todo · 2026-08-27 精简版）

> 唯一入口，只留活的 P0。完成即标 `[done]` 并迁 `archive/`。历史全量见 `archive/2026-08-27-todo-full-snapshot.md`。
> 对应：怎么做→`optimization-checklist.md` / 怎么投→`trading-improvement-checklist.md`。

## 0. 优先级（不可漂移）

| 1 收益 | 港湾 Harbor（S-3 核心 + 闲置现金 ETF 停车场）+ 回测可分析 |
| 2 API/AI | 外部打通 |
| 3 工程部署 | 稳定性 |
| 4 数据源 | 质量与覆盖 |

> 给普通人的一页纸（2026-09-13 · 2 分钟版）：
> Karios 管的是家里钱中**博收益的那部分（卫星仓）**，每天告诉你买什么、卖什么、买多少。
> 现在用的基线叫**港湾（Harbor）**：**S-3 股票核心**（选强势股，按纪律持有/退出）+
> **闲置现金停车场**（没买股票的钱，按 `mom60+MA200` 停在金/油/纳指/国债四选一，无候选就留现金）。
> 回测（三窗 + 长窗）：OOS2/train/valid **+55.2% / +52.2% / +50.3%**、长窗 **+201.5%**；相对纯 S-3 的增量 **+17.2 / +13.0 / +11.6pt**、长窗 **+118.8pt**（2026-09-14 日历修正 + 决策单源 clean 口径）。
> 风险实话：停车场在 S-3 空仓期会满仓 ETF，valid 回撤由 −6.1% 加深到 **−21.8%**（换收益的提升）；长窗回撤/夏普反而更好（−22.8 / 1.00）。**"trail 后冷却"候选已测并 REJECT**（B14：valid −6.8~−13.4pt，方向关闭）。
> 卫星腿（旧机会双子星，只拿 3 天）已被 2026-09-13 重拟合 **REJECT**（clean 口径：long 相对核心 +90.4pt 但 valid −21.2pt 单窗不过），不再使用；B12 旧「long −55.6/回撤 −48.3」为污染口径已作废。
> 实盘 paper 验证攒到 20 笔平仓才算数（现在 3/20），验证完之前**不调任何策略参数**；Live 已切港湾（前视/账本修复 + 双子星退役 = **OPT-178 ✅**）。
> 下一步：把港湾在 Watchlist 跑顺（P0-2；OPT-178 已落地）；想深挖看 [回测 SUMMARY](./backtests/SUMMARY.md)，想动手先读仓库根 `AGENTS.md`。

## 1. 状态看板

| 域 | 状态 |
|----|------|
| S-3 选股 | ✅ 封闭（**港湾的股票核心**；参数冻结见 [strategy-params](./modules/strategy-params.md)） |
| 港湾 Harbor | 🟢 **唯一产品基线**（2026-09-13 冻结 `harbor-p1-20260913`）= [S-3 核心 + 闲置现金 ETF 停车场](./modules/pick-strong-track.md)；三窗增量 +9.4/+6.0/+9.1、long +90.0（[B11](backtests/stable/etf-parking-baseline-2026-09-13.md)） |
| 旧双子星/择强单轨 | ⚪ 历史：择强被 OPT-177 证伪；卫星重拟合 [B12 REJECT](backtests/stable/twin-star-parking-refit-2026-09-13.md)（long −55.6/回撤 −48.3），不再使用 |
| 回测对照 | 🟡 运营阶段 B2–B5/A5 日流程已落地（OPT-135）；S-3 C4 统计仍等 20 笔 |
| Live 口径 | ✅ 港湾已上线（2026-09-13）——双子星退役 + 前视/账本修复（OPT-178）；T6 展示收敛 OPT-179 |
| 工程/数据源 | 按需 OPT（124–126 稳定性，127 已冬眠），不改策略 |

## 当前方向（港湾冻结之后）

- **P0：把港湾跑成产品**——S-3 核心 + 闲置现金停车场；先做完 **OPT-178**（Live 前视/账本修复 + 双子星下线），再谈新增强
- 停车场落地口径 = **14:30 出信号/执行、闲置即停、因果 trail8、去掉 `MIN_IDLE_PCT` 与 `ETF>STOCK` 门槛**（B11 已验；[B11 档](backtests/stable/etf-parking-baseline-2026-09-13.md)）
- 卫星腿（旧双子星：14:30 名单 / C1 3% / 第 3 日 14:30 卖 / 4×12.5%）**已 REJECT 下线**，不再扫参、不再排日历；[B12 档](backtests/stable/twin-star-parking-refit-2026-09-13.md) + [first-principles](backtests/first-principles-2026-09-05.md)
- 组合增强候选：**「母港」（Homeport = 港湾 × 风险预算 50/50）** **[B15 · done 2026-09-13 · PASS → 产品候选；后续 B16 上限 / B17 市况开关均 REJECT，静态 50/50 维持]**：修正 NAV + 月初现实再平衡下 K1–K5 全过（2026-09-14 clean 口径：四窗 Sharpe 1.90/3.52/2.26/1.18、MDD −9.0/−5.0/−11.4/−11.6、long +116.3 vs +201.5；2022–23 压测 + 20bp 敏感性均过）。**不进 Live（paper 3/20 + 资本结构需拍板）**；档 [harbor-riskbudget](backtests/stable/harbor-riskbudget-2026-09-13.md)
- 改策略走仓库根 `AGENTS.md` → Strategy / parameter changes（主流程；拒收总表见 SUMMARY）
- 脉冲天平仍观察层；S-3 统计 C4 仍等 20 笔平仓

### 产品化缺口盘点（2026-09-09 · 用户逐条拍板）

> 背景：全局代码盘点后，"双子星 → 完整产品"的缺口分五层。结论如下：

| 层 | 拍板 | 落点 |
|----|------|------|
| 执行桥（真实下单） | **暂缓**——QMT 之外很难接，不硬啃；维持 paper + 手动 QuickBuy + OCR 对账 | 无 OPT；将来接券商再立项 |
| 对账闭环缺一条腿（核心腿无 expected-vs-actual recon） | **提上来做**——纯代码层面问题 | **OPT-151** [done] 2026-09-09（checklist 归档） |
| 部署与可用性 | **就本机跑**——不赚钱没必要部署任何地方；tunnel/上云/付费 API 全 park | todo #9 / #2b 维持 P1 挂起 |
| 风控产品层（kill-switch / 偏离降级） | 任何风控机制**必须过回测 + 三窗**，按 Strategy / parameter changes 流程走 | 不立工程 OPT，先过 AGENTS.md 策略流程 |
| UI/UX 产品化 | 丑但能用；**先规划极简化美化** | [`designs/ui-minimal-redesign.md`](./designs/ui-minimal-redesign.md)（拍板后转 OPT） |

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

## P0-2 闲置停车场落地（港湾核心件 · 2026-09-13 重立）

**大白话**：闲钱别趴 GC001，让它停在四只 ETF 里最强的那个（金/油/纳指/国债）；这就是港湾的停车场，B11 已定案，剩下把 Live 代码改对。

- **冻结口径（P1）**：S-3 闲置现金 → 14:30 口径 `mom60+MA200` argmax 停泊，因果 trail8，含 0.05%/边成本；三窗 **+9.4/+6.0/+9.1pt**、long **+90.0pt**（[B11](backtests/stable/etf-parking-baseline-2026-09-13.md)）。
- **待做**（全部落 **OPT-178**，一会话一次改）：① 18:20 job → 14:28 出信号 / 14:30 执行；② trail 触发/记账同 print；③ `_pnl_for` 字段 bug；④ `SELL_TO_A_SHARE` 0 元平仓；⑤ 测试污染账本；⑥ 去掉 `MIN_IDLE_PCT` 与 `ETF>STOCK` 门槛；⑦ `third_asset_sleeve` / `multi_asset_sleeve` 双实现收敛。
- **已拒**：`20d -10%` 硬切 GC001（单窗无信息，不进 Live，档 `sleeve-exit-hard20-2026-09-04.md`）；`R3 risk-adj` 不如 `mom60`。
- **不做**：期货杠杆/外盘直连/高频；重开卫星腿。

## P0-3 C4 paper对照（跳过·等20笔）

≥20笔平仓后 `scripts/paper_vs_backtest_report.py` 跑，现在 3/20 跳过。卫星占用对照（旧双子星）随 B12 下线；港湾对账以核心 S-3 + 停车场为准。

## P0-4 旧卫星日历线（**已收口 · REJECT 归档** · 2026-09-13）

**大白话**：卫星腿（14:30 买、C1 3%、第 3 日 14:30 卖）整条线已随 B12 重拟合 **REJECT** 关闭——它在 2024–25 黄金段之外的 2022–23 熊市崩掉（standalone 2022 −34.8% / 2023 −48.0%、long MDD −80.6%），停车场核心上不再有增量（long −55.6pt、回撤 −48.3）。**不再调参、不再排日历、不再进 Live**；要救须带全周期门槛另起预注册。

历史档（只读）：[B12 重拟合](backtests/stable/twin-star-parking-refit-2026-09-13.md) · [sat-fill-same-close-2026-09-03.md](backtests/sat/sat-fill-same-close-2026-09-03.md) · 下方「导入什么 / 回测顺序」为已完成的实验记录。

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
2. **C1/C2 [done] 2026-09-03**：[sat-entry-c1](backtests/sat/sat-entry-c1-2026-09-03.md)。C1 3% 相对无过滤 tot/sr/dd 全过；单配收盘卖时 vs 核心 valid 总收益 −3.3，**已随 habit（C1+14:30卖）进 Live**。不重开 −5% / trail / 砍 4 槽。
3. **3 天 vs 4 天 / 下午买点 [done] 2026-09-03**：[sat-habit-clock](backtests/sat/sat-habit-clock-2026-09-03.md)。计数仍 body=3；body=4 占槽；13:30–15:00 无更佳分钟。
4. **C1 + 第 3 日卖点 [done] 2026-09-03**：[sat-exit-hhmm](backtests/sat/sat-exit-hhmm-2026-09-03.md)。14:30 卖三窗 tot/sr/dd 过核心；10:00 不如它。**Live 已切 habit（2026-09-03 拍板全量跟进）**：Watchlist/paper `C1 3% + 第3日14:30卖`，冻结 T 开盘 Timeline 默认不动，习惯对照走 `sat_fill=same_1430&c1_pct=0.03&sat_exit=1430`。
5. **习惯排名 H1 [done] 2026-09-04**：[sat-rank-hhmm](backtests/sat/sat-rank-hhmm-2026-09-04.md)。无前视键（gap升序 / |14:30/今开−1|升序）2 变体全拒：gap OOS2 −96pt 永不重开；|runup| valid +14.4 但 OOS2 −21.5（过拟合陷阱）。Live 排名不动；R-wide 全天收盘闸记 H1-followup。
6. **习惯 C1 网格 H2 [done] 2026-09-04**：[sat-c1-grid](backtests/sat/sat-c1-grid-2026-09-04.md)。C1=3% 站在平顶上：2% 打平但 train 降（不换）；4% 走弱；5% train −5.8 拒收。**Live 保持 3%**。
7. **习惯 bucket_q H3 [done] 2026-09-04**：[sat-bucketq](backtests/sat/sat-bucketq-2026-09-04.md)。top-1/2 选参窗 tot/sr 全弱于 1/3（train −2.3/sr−0.41），valid 无差。**Live 保持 1/3**，4 槽不动。
8. **习惯 R-wide 闸 H4 [done] 2026-09-04**：[sat-rwide](backtests/sat/sat-rwide-2026-09-04.md)。0.5 三窗一致最优；0.4 valid −17.9；0.6 valid +13.4 但选参窗崩（拒）。**Live 保持 0.5，打磨收工**——等 paper 20 笔 C4 实证。
9. **习惯 C3 下跌过滤 S2 [done] 2026-09-04**：[sat-c3-fade](backtests/sat/sat-c3-fade-2026-09-04.md)。诊断两窗同向（<−3% 档最差），组合层面冗余（跳 564/fills−1）。**不进 Live**；周二/C2 方向死在诊断。
10. **习惯 holdout 审计 S1 [done] 2026-09-04**：[sat-holdout](backtests/sat/sat-holdout-2026-09-04.md)。19 sessions Δ −5.1 ≈ 第 7 百分位坏月份（p5 −5.53），分布内。**不调参**；真风险是方差（~7% 月份 −5pt），不对月考核卫星。
11. **习惯 CHURN 过滤 S4 [done] 2026-09-04**：[sat-churn](backtests/sat/sat-churn-2026-09-04.md)。六维诊断（换手/板块/市值/年限/大盘高开/breadth）只活一个：T-1 放量>4x 不追，train +2.4/valid +1.5 但 OOS2 −1.0 → PASS/worse。**不进 Live，记候选**（待 holdout 满 60 sessions 或 paper 20 笔重验）。
12. **大盘风格 vs 卫星 G1 [done] 2026-09-04**：[sat-regime](backtests/sat/sat-regime-2026-09-04.md)。up 三窗全赚最稳，choppy 次之，down 被 R-wide 拦；波动率非稳定因子。**无新规则**。
13. **第 3 日条件单 D3（用户规则）[done] 2026-09-04**：[sat-exit-d3trail](backtests/sat/sat-exit-d3trail-2026-09-04.md)。盘中高点回落 2% 卖否则 14:30，三窗 REJECT/total（OOS2 −6.4/train −2.8/valid −5.8，夏普全差，触发率 63%）。**回吐≠反转，不进 Live**；1%/3% 网格不补（机制证伪）。

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

## P0-7 外购历史数据计划（2026-09-04 拍板）

> 用户外购分钟/复权包。定位：**数据≠新策略**，只干三件实事——熊市回放验稳健、复权对拍保质量、指数/ETF 存盘备用。工程部分见 [OPT-145](./optimization-checklist.md)。
> 交接规则：一次一阶段，前一阶段验收不过就停；C 阶段是诊断（无论结果不调 Live）。

| 阶段 | 做什么 | 在哪 | 验收 | 状态 |
|------|--------|------|------|------|
| A | 2024 vendor 5 分钟 vs 独立源抽样对拍 | OPT-145 | 七根90.59%（原≥99.5%字面没过；O/H/L一致、close抖中位0.056%，1500达98%）→ 有条件过，C加滑点base10bps/stress30bps覆盖 | [done] 2026-09-05 有条件过（见 `backtests/vendor-minute-compare-2026-09-05.md`） |
| B | 下 2021–2023 三年股票 5 分钟，切片入库尾盘七根 | OPT-145 | 2021 7153741行/4452名/243天 + 2022 7819588/4798/242 + 2023 8335124/5034/242；库总56493163行；解压删留zip，磁盘余88G | [done] 2026-09-05 |
| C | 习惯配方熊市回放（重点 2022） | 本段交接块 → 结论进 `docs/backtests/` | 2021/22/23 Δ+25.9/+18.5/+21.1，stress后仍正（22年+1.2）；PASS不调Live | [done] 2026-09-05（`backtests/sat-bear-replay-2026-09-05.md`） |
| D | 复权双份 vs 库内抽样对拍 | OPT-145 | vendor后复权实为价格序列、前复权远古89k负数、日收益中位差149.6bps→判不可直接用；只报不修，零影响 | [done] 2026-09-05（`backtests/vendor-adj-compare-2026-09-05.md`） |
| E | 指数/ETF 分钟存盘不导入 | — | 无消费者（习惯只吃股票14:30，核心ETF只吃日线），网盘留档、本地不存 | [done] 2026-09-05 存盘即完 |

### 交接块 A（对拍验质量）

```text
做 docs/optimization-checklist.md OPT-145 的 A 阶段。
- 新建 services/data-sync-service/scripts/compare_vendor_minute.py（只读）：
  读 repo data/ 下 2024 年 vendor 5 分钟 CSV，与库内 bar_5min 同（ts, 日, 时点）
  逐点对 close 价，输出总行数/吻合行数/价差>1分钱占比/缺失对照。
- vendor CSV 先由用户放到 services/data-sync-service/data/2024_5min_vendor/（没有就停下来要）。
- 验收：吻合率 ≥99.5% 才算过；附 1 个单测；pytest 过；不写库不改引擎。
```

### 交接块 B（切片入库）

```text
做 docs/optimization-checklist.md OPT-145 的 B 阶段（前置：A 已过）。
- 用户已下好 2021/2022/2023 三年股票 5 分钟按年包放 data/2021_5min 等目录。
- 复用 scripts/import_ext_minute_csv.py --times 只留尾盘七根导入 bar_5min；
  导完删解压目录留 zip，磁盘余量保持 >20G。
- 验收：三年七根行数 + skipNoPrint1430 覆盖率（含熊市年）；pytest 过。
```

### 交接块 C（熊市回放 · 诊断）

```text
习惯配方（C1 3% + same_1430 + body=3 + 第3日14:30卖，冻结口径）在 2021–2023（含 2022 熊市）
的只读回放，回答“配方在真熊市活不活”。
- 前置：B 已入库。只读，不碰 Live、不重选参（valid 不参与选择，沿用 S1/H1 诊断纪律）。
- 输出 twin vs 核心对照表（tot/sr/dd/Δ）+ fills/skip 口径说明，结论写新档
  docs/backtests/sat-bear-replay-2026-0X.md（PASS 或“熊市水土不服”都留档）。
- 验收：表 + 档；任何情况下不改 Live 参数。
```

### 交接块 D（复权对拍）

```text
做 docs/optimization-checklist.md OPT-145 的 D 阶段。
- 新建 scripts/compare_vendor_adj.py（只读）：vendor 东财/tushare 双份因子 vs 库内
  adj_factor，抽样最活 200 只 × 除权除息日，输出差异表。
- 验收：差异表短档进 docs/backtests/；只报不修；pytest 过。
```

---

## P0-8 砖块结论（2026-09-06 立 · 实验楼规律提取）[done] 2026-09-06

**大白话**：50+ 实验盖了一幢楼，现在从楼里拆砖块——跨实验成立的规律，沉进 first-principles。
真值档：[`backtests/first-principles-2026-09-05.md`](./backtests/first-principles-2026-09-05.md)（只增不改）。
**纪律**：纯读档 + 只读脚本，不跑新回测（M5/M6 只读已有 `backtest_reports`）；任何新参数想法仍走三窗 walk-forward。一次一 milestone。

| # | 里程碑 | 产出 | 验收 | 状态 |
|---|--------|------|------|------|
| M1 | [done] 2026-09-06 死因分类学：~90 拒收变体标七类（截右尾 20%/共线 19%/方向 19%/单窗 17%/砍宽 11%/样本 8%/覆盖 7%）+ 死因×时代交叉表 + 5 条元结论 | first-principles §三 | 表 + 交叉表 + 用法（预注册前先答"最可能死在哪类"） | [done] 2026-09-06 |
| M2 | 正例归纳：18 存活案例 × 5 特征（F5 18/18 > F2/F4 17/18 > F1 14/18 > F3 12/18）；产出 S1–S5：机制先行律 / 回测采纳必要条件 / 慢变量门豁免（修正 §一.3）/ 结构参数走平台 / 二值开关走诊断 | first-principles §四 | 特征矩阵 + 频率计数 + 反例（熔断/V7.0-02）有交代 | [done] 2026-09-06 |
| M3 | 结构覆盖矩阵：8 维 × 7 结构逐格取证；完备度分级（全吸收：动量/资金流/事件已测/行业/持仓；部分吸收：波动·形态·市场状态·候选级 CHURN；子项级未验证 P13/P15/P16/P18-P26）；4 个作用域反例（跨域套用必死）；查矩阵三问 | first-principles §五"结构吸收律"转正 | 矩阵表 + 分级 + 反例 + 用法 | [done] 2026-09-06 |
| M4 | 参数平台表：20 定案参数邻域汇编；四形状（宽平台 6 / 单峰 5 / 悬崖 5 / 结构选择 5+1）+ 五纪律：尖峰=拒收、悬崖只站安全侧（防守类全是悬崖=死因#1）、单峰高stakes重点防腐 | first-principles §六"参数平台律" | 证据表 + 形状分类 + 纪律 | [done] 2026-09-06 |
| M5+M6 | 指纹表：报告只有窗口聚合、无逐笔，改从已发表文档/报告汇编。卫星 +2.95/+1.81/+0.96%/笔·67/56/59%·~1.1笔/天（验算）；S-3 48/44/67%；对冲反面指纹 65.3%/+0.92%/−236%尾；右尾top-x%与月度绝对序列[待补]（冻结配置只读重跑+--dump-fills，不选参） | `backtests/leg-fingerprints-2026-09-06.md` + first-principles §一.4 引用 | 表 + 待补清单（方法已定） | [done] 2026-09-06 |

**不做**：新回测、新参数、重开已 REJECT。

---

## P0-9 文档轨（2026-09-06 立 · agent 友好 + 每文件一职责）[done] 2026-09-06

**大白话**：文档现在"找得到但不好读"——两大 checklist 3844+1024 行只增不减，同一数字 5 处互抄。
目标：每文件一职责、都不太长；agent 按任务 3–5 个文件读完开工；普通人看一页纸能 follow。
**纪律**：不动 modules 正文（另行批准）；不动 archive/；一次一条；每条验收 = 行数 + 引用收敛。

| # | 动作 | 验收 | 状态 |
|---|------|------|------|
| D0 | backtests 按策略分文件夹（core/sat/s3/factors/vendor/hedge，索引层留根）+ 全库链接重定向 | 死链 0 + 孤儿 0 + git 历史保留 | [done] 2026-09-06 |
| D1 | 拆 optimization-checklist：已完成 OPT 按天归档，正文只留 5 条未完成 + 索引 | 归档 13 新档 + 045/075/127 冬眠档 + 覆盖率 100% + 死链孤儿 0 | [done] 2026-09-06（3844→299 行；A 方案两次：045/075 + 127 冬眠） |
| D2 | 拆 trading-improvement-checklist：8 个主题归档 + TIP-014 正名压缩 | 1024→115 行 + 死链 0 | [done] 2026-09-06 |
| D3 | 数字单一源：clip4→state-bucket-algo，S-3 基线→strategy-params，三窗→backtests/README；其余只引用 | [done] 2026-09-06（190.6 定数：报告 product 窗 core=190.6，Δ4.3 自洽；190.7 只剩冻结实验档+注释。规则定为"一致+出处"而非掏空表格：定义单点，引用处数字保留但必带指针） |
| D4 | 改策略路由清单统一：AGENTS 为主源（+自查步骤+调参查找），其余四处改指 | 四处收录一致 + 死链 0 | [done] 2026-09-06 |
| D5 | todo §1 顶部加普通人一页纸（策略/收益/风险/下一步）+ §0/§1 去旧§号 | 普通人 2 分钟能复述 | [done] 2026-09-06 |

**不做**：watchlist 拆分（modules 正文，另批）；新写规划类 md。

---

## P0-10 工程坚实计划（2026-09-06 立 · [done] 2026-09-06，H1–H6 全收工）

**大白话**：代码让 agent 一读就懂、一下手就不踩坑；测试盖住核心策略链；用户一眼确信线上跑的就是冻结配方，2 分钟知道今天跟什么。
**基线（今日实测）**：覆盖率 86.35%/门 85；核心链偏薄 backtest_engine 73、paper_twin_star 67（job 46）、twin_star_daily 73、twin_star_intraday 64（job 49）、multi_asset_sleeve 80、state_bucket_track 81；未完成 OPT 剩 125/126/145/146；CI 有（lint+typecheck+test+build+PG16）缺死链门；`coverage.json` 跟踪中常脏；教训：OPT-124 连带修 8 个旧测试文件（mock 直连 `ts.pro_api`）。

| # | 动作 | 范围 | 验收 |
|---|------|------|------|
| H1 | 核心链覆盖率补齐 | backtest_engine / twin_star_daily+intraday(+jobs) / paper_twin_star(+job) / multi_asset_sleeve / state_bucket_track 各→≥90；门 85→88 | [done] 2026-09-06：engine 99.6 / sleeve 98 / state 99 / paper 100 / jobs 100+98 / daily 100 / intraday 98；全量 4131 passed + 门 89.31%≥88；余下均为证伪不可达防御分支（engine 6 行死代码候选 OPT 清理） |
| H2 | 稳定性收尾 | OPT-125 DB 池 / OPT-126 东财探针 / OPT-146 港股符号 / OPT-145 形式化脚本回填 | [done] 2026-09-06：池 2/20+超时+1×重试+compose 健康门；探针 10min+streak3 告警+横幅；54 行港股重标（总数 113 不变）+ 结算语义 §1.7；分钟对拍脚本固化（600000.SH 复现 95.7%/1500 97.5%）；checklist 未完成清零（108 归档）。OPT-127（P2 轮询）125 后已解锁，待唤醒 |
| H3 | Agent 读写公约 | outbound 收敛（`get_pool` 先例→DB 池同构；范围外 15+ `ts.pro_api` 分批收）；新模块模板（guard 文案 / ok-error 契约 / 单例+reset 可测性）沉淀进 AGENTS 一节 | [done] 2026-09-06：13 文件 17 站点→pool；裸 `ts.pro_api` 只剩 realtime_quote（tk.csv）+ bar_5min（全局 token）两处有据豁免；11 个旧测试文件换 `get_pool` patch；AGENTS +1 节；全量 4164 passed |
| H4 | 文档↔代码漂移护栏 | linkcheck 脚本 + 进 CI；shared Zod 补 datasources/`tushare_quota`（OPT-009 workflow，OPT-124 加的字段）+ 前端横幅消费 + test_api shape 断言 | [done] 2026-09-06：linkcheck（archive 豁免，118 文件干净，真抓 1 处 tv.py 死链）；CI +1 门；shared health.ts+4 用例；横幅 quota/熔断行+1 用例；test_health shape 断言；前端 879 passed |
| H5 | 用户信任：一致性 + 跟随 | live 配方哈希 == 冻结基线（单测 + UI 展示"线上跑的就是 clip4 v3.1"）；paper 每日对账（预期 vs 实际）；跟随页 today-plan + reason + evidence 链 | [done] 2026-09-06：后端跨层 attestation（TS 字面量=引擎常量）+ shared 版本常量 + 计划面板徽；paper_twin_star_recon（应买/已买/应卖/已卖 + 高事件）进 action brief；跟随页既有 reason（TodayActionCard）+ recipe 行；全量 4178 passed |
| H6 | CI 锁门 | linkcheck 进 ci.yml；`coverage.json` 去跟踪（`git rm --cached` + ignore，消灭常脏） | [done] 2026-09-06：CI=lint+typecheck+test+linkcheck+build；coverage.json 去跟踪（早有 ignore，误跟踪） |

**顺序**：H1 → H2 → H3 → H4 → H5 → H6（H5 依赖 H1 的 paper 覆盖；H6 最后锁门）。
**纪律**：一 H 一会话；只改范围文件；修测试不改行为（行为要变另起 OPT）。
**不做**：重写 engine；微服务拆分；任何策略参数改动（冻结）。

---

## P0-11 财务质量计划（2026-09-10 立 · 用户拍板：财务看长期）[done] 2026-09-11

**大白话**：三张原始报表（tushare `balancesheet/income/cashflow`）已入库，目标是给 S-3 加一层**防守型质量门**（只剔除、不预测涨跌）。财务是季度慢变量——只看 20–60 天归因，不碰 3 天卫星腿（尺度匹配律；P15 未验证子项，可以开）。

| # | 动作 | 范围/验收 |
|---|------|-----------|
| D1 | 数据补齐 | 表 `cn_balance_sheet/cn_income_stmt/cn_cashflow_stmt`（主键含 report_type，热字段 typed + 全行 JSONB；migration 0044）。4 年 8.6 万行/表 **[done] 2026-09-10**；扩展到 cutoff 2018-01-01 **[done] 2026-09-10**（2018Q1–2026Q2，5461 只，~16.5 万行/表，已验收） |
| D2 | TTM + 行业中位数管线 | 单季拆分、经营现金流 TTM / 归母 TTM、逐期截面中位数；金融股 `comp_type` 单独处理或剔除；增量接收盘链 |
| F1 | ROE-TTM > 行业中位数 **[done] 2026-09-10：REJECT/方向证伪**（22 季 meanIC −0.06、Q-spread 仅 27% 为正；档 `backtests/factors/fin-f1-roe-ttm-2026-09-10.md`；反号不开，需独立预注册） |
| G1 | F2 + 市值中性 **[done] 2026-09-11：REJECT**（20 季 pooledIC +0.026、Q-spread 仍 50%；规模不是主因；档 `backtests/factors/fin-g1-ccr-neutral-2026-09-11.md`） |
| G2 | F4 + 市值中性 **[done] 2026-09-11：REJECT/方向反**（pooledIC −0.018、Q-spread 仅 20%；原始弱正向疑为大盘代理；档 `backtests/factors/fin-g2-lev-neutral-2026-09-11.md`） |
| G3 | F1 + 市值中性 **[done] 2026-09-11：REJECT/方向反坐实**（pooledIC −0.048、Q-spread 仅 20%；roe–size 相关 0.2+ 但非主因；档 `backtests/factors/fin-g3-roe-neutral-2026-09-11.md`） |
| F2–F4 | CFO/净利 **[done] 2026-09-10：REJECT/弱方向**（档 `backtests/factors/fin-f2-cashconv-2026-09-10.md`）、应计（与 F2 同源跳过）、杠杆 **[done] 2026-09-10：REJECT/弱方向**（meanIC +0.015、Q-spread 仅 32%；档 `backtests/factors/fin-f4-leverage-2026-09-10.md`）。**P15 方向诊断层整体关闭，不进回放** |
| F5 | 价值复合（P18） **[done] 2026-09-11：回放 REJECT**（三臂：composite −8.5/−43/−43pt、mom_only +2/−44/−36pt；归因 83% 拦截是缺数 fail-closed → #7 覆盖 + #5 砍宽度；value 腿组合零增量；报告 `walk_forward_p18_*.json`；档 `backtests/factors/fin-p18-value-mom-2026-09-11.md` §5） |
| S1 | 合成决策 | 单因子 PASS 才谈复合；阶段一只做负面剔除 |

**顺序**：D1 → D2 → F1 → F2–F4 → F5 → S1。
**纪律**：PiT 只用 `ann_date`；valid ≥30 笔；看 DD/胜率/踩雷率不看总数；死因预判写进预注册（#2 共线 / #7 覆盖 / #4 单窗）。
**不做**：财务因子进卫星腿；一次全叠；用报告期不用公告日；重开已拒方向。

---

## P0-12 新策略孵化（2026-09-11 立 · 用户拍板：S-3 已到局域最优，不在它身上加 gate）

**大白话**：S-3 + 卫星是调到顶点的体系，结构吸收律决定新维度进去也是死（P0-11 七轮全关就是证据）。财务数据换赛道：孵化指纹完全不同的新策略——持有期、换手、宇宙、基线全不同。

| # | 方向 | 状态/验收 |
|---|------|-----------|
| V1 | 慢价值诊断 **[done] 2026-09-11：PASS，四线全过**（17 季 mIC120 +0.14/mIC250 +0.17、D-spread 82%、top>EW 71%；唯一深负 2024-06 小盘季；近期 n 薄系 FCF 滞后；档 `backtests/factors/fin-v1-slow-value-2026-09-11.md`） |
| V2 | 慢价值独立套筒 **[done] 2026-09-11：REJECT**（5 vintages 3/5 胜、mean +2.1pt；2021 +33 熊市胜、2025 −24 牛市税；风格腿非独立套筒；附混合日历/dailybasic 空洞三坑；档 `backtests/factors/fin-v2-slow-sleeve-2026-09-11.md`） |
| S1 | 特殊情形（ST 摘帽/重组小套筒） | 缺事件数据，待投入，parked |
| H1 | 做空输入（财务恶化进对冲楼） | 执行前提重（券源/交收），排后 |
| A1 | 聚合配置（全市场盈利周期定仓位） **[done] 2026-09-11：REJECT/方向反**（n=24 月，corr60 −0.14、corr120 −0.47；与 F1/P14 跨层同号：好消息兑现即打折；档 `backtests/factors/fin-a1-earn-regime-2026-09-11.md`） |
| D1 | 个股人气重叠检验 **[done] 2026-09-11：SHELVE/追高税**（14 周 mIC20 −0.04/mIC60 −0.11，top100 多周 −13~−26；反向买冷门不开；表保留；档 `backtests/factors/fin-d1-hot-rank-2026-09-11.md`） |
| R1 | regime 配置腿（换发动机：趋势腿 × 慢价值腿按慢市况切换，非 S-3 加 gate） | **[done] 2026-09-11：Phase 0 REJECT/方向证伪**（信号月 as-of + OPT-156 补洞后：R=OFF spread +1.66%/月、R=ON +0.15%/月，**两态同为正、regime 不翻转 V−T**；valid 反号；corr 0.391 ✓）。档 `designs/regime-allocation-incubator-prereg-2026-09-11.md` §6 |
| L1 | 长持集中基本面找几倍股（用户倾向方向） | **[done] 2026-09-11：L1+L2 均 REJECT**（2021–24 四队列 top20 4/4 输 EW；L2 连续两年增速下限减轻伤害但不转正——高增长已 price in）；下一步 GARP 或 财务回填 2007。档 `backtests/factors/fin-l1-tenbagger-2026-09-11.md` |
| C1 | 风格 × 市值 × 市况分类诊断 | **[done] 2026-09-11：描述性砖**（up 科技/资源、down 金融抗跌、跌后医药/科技；大盘 5 年 +243% vs 小盘 +34%；轮动打平等权，幸存者偏差）。档 `backtests/factors/style-regime-2026-09-11.md` |
| W1 | **定海 DH**：风险管理 beta（波动率目标；工作名 A1/W1） | **[done] 2026-09-11：DH-1 WITHDRAWN（前视）/ DH-2 KEEP**——DH-1 用本月末成交额选本月票（前视 ~+20pt/yr），修正后 top200 **−8.5%/yr**（A 股市值/流动性负因子）；**DH-2=真实指数 beta**（指数回补 2005+）**中证500+vt20% = +5.4%/yr、DD−51%、sr0.36**。**与 S-3 结合全 REJECT**：配比四窗不一致；**DH 作方向指引（指数趋势→收紧止损）OOS2 劣化**（钩子 `trend_guide_*` 默认关闭保留）。档 `backtests/a1-voltarget-beta-2026-09-11.md` / `dh1-s3-hybrid-2026-09-11.md`；脚本 `scripts/incubate/{dh_v2_honest,dh_v2_index,dh_combine}.py` |
| X1 | 另类数据 alpha 速筛（股东户数/陆股通/大单资金流） | **[done] 2026-09-11：REJECT/无增量**——陆股通/大单=噪音（t<1.5）；股东户数变化有信号（`chg2` 1月 IC+0.014/t3.05）**但控制市值后塌到 t0.66**=小市值代理，非独立 alpha。档 `backtests/factors/alt-alpha-screen-2026-09-11.md`；脚本 `scripts/incubate/alt_{alpha_ic,holder_refine,holder_ortho}.py` |
| X2 | 5 分钟日内微结构 | **[done] 2026-09-11：REJECT/不可交易**——`bar_5min` 稀疏（尾盘 2021+、1000/1330/1400 仅 2024+）；`last30`→次日开盘 IC−0.112/t−53.8 但**非单调仅 Q5 尾部、毛 0.21%/天<30bp 成本**（净 x0.30）。不做完整 5min 回填。档 `backtests/factors/intraday-microstructure-2026-09-11.md`；脚本 `scripts/incubate/intraday_{probe,reversal}.py` |
| X3 | 长史基本面 投资/应计 composite | **[done] 2026-09-11：INCUBATE（sleeve 本体）/ 组合 REJECT**——asset_growth IC−0.066/t−2.91、accruals −0.037/t−2.10；行业中性后流动池边 +2.0%/年(88%胜)；长持 sleeve 净 +5.3%/年 vs 流动 EW +3.4%；行业非PIT稳健性四重通过。**与 S-3 组合**：2024 起 sleeve 看似变好（选择偏差），**延到 2021 长窗后反转**——sleeve 自 DD−40%、组合收益/Sharpe↓、DD 几乎不变 → **REJECT**。档 `backtests/factors/fund-investment-accruals-2026-09-11.md` §2–8；脚本 `scripts/incubate/fundamental_{ic,combo,portfolio,neutral}.py` + `fund_sleeve{,_robust}.py` + `fund_s3_combine.py` |
| T1 | **打板 / 涨停连板买进**（超短动量·事件驱动；用户方向） | **[done] 2026-09-12：已测形态全部关闭**（假设可成交的 S-limit 收益 80% 来自买不进的一字板；次日追板 v0 三窗近零）。真打板（日内封板排队）需 level-2/封单，parked | **全形态死因汇总见 [limitup-do-not-redo](backtests/limitup-do-not-redo-2026-09-12.md)，不再在现有数据上重开** |
| B1 | **ETF 组合基准对照**（上证50 / 科创50 / 中证500 / 红利 / 港股科技 + 等权组合）vs 机会双子星 | **[done] 2026-09-12**（P0-13 A1）：双子星四窗全碾压所有 ETF 买持，相关 0~0.5；股票型 ETF 进**核心菜单**是负优化（`compare_core_menu`）。缺口：上证50/科创50 本地无数据。档 [newtracks-a](backtests/newtracks-a-2026-09-12.md) |

**顺序**：V1 →（转正）V2；**R1（regime 配置腿）**；S1/H1/T1/A1 parked，不并行；**B1（ETF 基准对照）只读可并跑**。
**纪律**：新策略自有基线（等权买持），不用 S-3 三窗基线判 1 年持有；回放单开脚手架，不在 S-3 引擎加 gate；单假设预注册，零网格。
**不做**：再给 S-3 加任何财务 gate；拿 60 天 horizon 测价值；调权重凑数；把打板逻辑塞进卫星/S-3。

---

## P0-13 新赛道实验队列（2026-09-12 立 · 用户拍板：继续做实验/出结果/落档）

**大白话**：A 股横截面 alpha 已挖尽（P0-12 全关）。换赛道按「工具/收益结构不同」排，不按「再挖一个因子」。分三阶段：A=现有数据即可跑；B=要先补数据；C=收益层（非 alpha）。一次一刀，只读诊断先行，落地走预注册 + 自有基线。

| 阶段 | # | 方向 | 数据 | 第一刀（只读诊断） | 死因预判 / 验收 |
|------|---|------|------|-------------------|-----------------|
| A1 | **B1 ETF 买持基准** | `daily` ETF（现有 300/500/创业板/黄金/十年债/纳指/恒科；上证50/科创50 缺） | **[done] 2026-09-12**：双子星四窗全碾压所有 ETF 买持（OOS2 +82.4 vs 最强恒科 +54.8；past_year +141 vs 创业板 +55.7），相关仅 0~0.5 → 标尺成立 | 不改 Live；档 `backtests/newtracks-a-2026-09-12.md` |
| A2 | **T1 打板 v0** | `daily` 涨跌停（现有） | **[done] 2026-09-12：方向关闭**——「涨停次日开盘追、T+1 卖」三窗 mixed 近零（ALL +0.14/+0.07/−0.20%），开盘溢价 0.8~3.9% 全付掉、无跟涨；3 板+ 买不进 9~17%。真打板（封板排队/开板回封）需封单/龙虎榜/level-2，**parked 等数据** | 不补网格；**打板全形态死因汇总见 [limitup-do-not-redo](backtests/limitup-do-not-redo-2026-09-12.md)** |
| A3 | **ETF 深挖（板块/宽基）· 线收口** | tushare `fund_daily`+`fund_adj`（**本轮已建面板** `data/etf/etf_daily.csv`，35 只=6 宽基+29 板块/46,885 行，2021+） | **[done 2026-09-12 · 线收口]** ① 归因：S-3 逐笔 vs 同板块 ETF 均值超额 long +1.22%（board，3/3）/ +1.55%（sector，3/3，cov 78%）→ **不是纯 beta、被动 ETF 替代丢右尾**；中位负 → **右尾/option 型 alpha，不采纳替代**。② 同步止损：**REJECT**（long meanΔ +3.55% 但 OOS2 −1.50%/valid 零停火，放宽 MA20 OOS2 −4.76% = 截右尾/鞭打）。③ 板块动量信号：**REJECT**（H20 edge 微弱正 long +0.15%/WF 3/3，但轮动 long **−44.1% vs EW +4.0%**、DD 69%、0/3 窗，regime 翻转） | 档 [s3-alpha-vs-etf](backtests/etf/s3-alpha-vs-etf-2026-09-12.md) · [s3-etf-sync-stop](backtests/etf/s3-etf-sync-stop-2026-09-12.md) · [etf-sector-momentum](backtests/etf/etf-sector-momentum-2026-09-12.md)；**ETF 线收口** |
| B1 | **CB 条款/估值线** | 数据已在 `~/Downloads/` + akshare PIT 溢价 | **[done 2026-09-12 · 线收口]** 日线 2021+（356,656 CB-day）+ PIT 溢价（449 只/382,643 行）；**全部 REJECT**：低价/低波（电池）、**mom20（长窗预注册 −24.2 vs EW+51.8、6/6 年负）**、**双低（长窗 Δ+18.3 但 WF 1/3、40bp 翻负 −12.7）** | 档 `backtests/cb/cb-{daily-factors,mom20,doublelow}-2026-09-12.md`；预注册 `designs/cb-{mom20,doublelow}-prereg-2026-09-12.md`。**CB 日线横截面全关，不再补因子**；仅剩条款事件（需 `cb_call`/`cb_price_chg` 权限） |
| B2 | **事件驱动 / 深层次信号**（指数调样 / 利率 / 关注度 / 主力脚印） | 指数成分史**缺**；利率 akshare 全；关注度快照/1 年；龙虎榜/股东户数/大宗 **2021+（已落 `data/{lhb,gdhs,block}/`）** | **指数调样**暂缓；利率 [done·REJECT]；深层次三源全跑完：龙虎榜机构 [done·REJECT]、股东户数 [done·REJECT]、**大宗溢价 [done·交易级 PASS → 组合级 park]**（溢价>折价在组合层成立但**多头不可实现**：价值在空头腿；多头扣成本 long 超额 −1.33%）。**大宗线关闭**；残余：大折价=避配过滤器（待覆盖检验）。**散户关注度快照 job [done 2026-09-12 · OPT-176：`cn_xq_follow` + 工作日 15:40 job，向前积累，攒够 12 个月再开诊断]** | 利率 [fomc](backtests/event/fomc-rate-2026-09-12.md)·[rate-exp](backtests/event/rate-expectation-2026-09-12.md)；[lhb](backtests/inst/lhb-inst-netbuy-2026-09-12.md)·[gdhs](backtests/inst/gdhs-concentration-2026-09-12.md)·[block](backtests/inst/block-premium-2026-09-12.md)·[momneutral](backtests/inst/block-momneutral-2026-09-12.md)·[replay](backtests/inst/block-premium-replay-2026-09-12.md) |
| C1 | **打新/新股 + 现金管理** | tushare `new_share`（**已落** `data/ipo/`） | **[done 2026-09-12]** 打新是正 EV 收益层（M=20万年化 ~3-7%）；与双子星结合顺手 +0.5~2%/年。**次要，有空再实现**；不专门建底仓 | 档 [ipo-ev](backtests/ipo/ipo-ev-2026-09-12.md) |
| B3 | **产业链推理链（上下游传导）** | 同花顺概念指数（`data/chain/`，苹果 2019+/英伟达 2023-05+）+ 美股 anchor（`stock_us_daily`）✅；ths 产业链 tushare ❌ | **[done 2026-09-12 · REJECT（§一.14 确认）]** 12 配对：anchor→概念**隔夜 gap corr +0.318**、**日内 corr −0.01**、mom20→fwd20 high−low −0.11% → **传导全在隔夜跳空、无慢漂移，不可交易**。产业链信息 = A 股消费的海外信息。深挖需 PIT 供应链映射 → parked | 档 [chain-anchor](backtests/chain/chain-anchor-2026-09-12.md) · 预注册 [prereg](designs/chain-anchor-prereg-2026-09-12.md) |
| B4 | **超跌反弹 / 国家队救市（事件择时）** | 库内即全：`cn_etf_share`（2018+）+ `index_daily`（2005+）；汇金公告 ⚠️ 可选 | **[done 2026-09-12 · REJECT]** K1 ✅/覆盖 ✅558，**K2 ❌**（现代"破 MA200 国家队必在买"→ 隔离不出增量；valid 反号）。**深度超跌&国家队买** 有生命体征（5d +1.07/10d +1.91，均值+中位双正、N 单调）但属均值回归家族。**"救市→反弹"机制不成立** | 档 [bounce-rescue](backtests/event/bounce-rescue-2026-09-12.md) · 预注册 [prereg](designs/bounce-rescue-prereg-2026-09-12.md) |
| B5 | **现金流折现模型（DCF 估值）** | 财报三表（`cn_cashflow/income/balance`，2007+ 已回填）+ 无风险利率（`us_yields`/中债）；WACC/永续增长假设 | **未落地（用户 2026-09-12 提出，仅记录）**：用 FCFF/FCFE 或 DDM 折现估内在价值 → 选市价 < 内在价值。**预判死因**：#2 共线（可能=优质/低估值换皮，价值因子 P18/V1/V2 已测：价值增量≈0、慢价值仅 regime 腿）、假设敏感（折现率/增长率/永续 = 陷阱网格）、A 股周期股/财报质量/幸存者偏差。**关联**：[P18 价值×动量](factor-library/fundamental.md#p18-价值动量)、[V1/V2 慢价值](factor-library/fundamental.md#v1-慢价值)、[F1 ROE](backtests/factors/fin-f1-roe-ttm-2026-09-10.md)（REJECT） | 待开预注册；数据齐（无新同步） |
| B6 | **稀有极端触发·高确信择时（一年数次）** | 库内：`index_daily`/`daily`/涨停/广度/成交量/`cn_etf_share`；政策事件（印花税/汇金公告）需另接 | **[done 2026-09-12 · NONE passed]** 5 预声明恐慌触发（崩盘/急跌/千股跌停/无差别抛售/放量），长窗 2015–26、门槛制（N5 胜率≥75%&均值≥+3%&中位>0&较基线+2pt）。**无一人选**；最接近 **C3 千股跌停**（n=35、胜率 65.7%/均值 +1.04%/中位 +1.95%，三子段稳）——**温和真边缘，非稳稳赚**（均值<中位=左尾巨亏）。**"明显恐慌稀有稳赚"证伪**；不调阈值重扫。复活只能换机制族（政策底/恐慌+企稳/个股错杀）另起预注册 | 档 [rare-triggers](backtests/event/rare-triggers-2026-09-12.md) · 预注册 [prereg](designs/rare-triggers-prereg-2026-09-12.md) |
| B7 | **多资产稳健核心（独立结构）** | `data/etf/etf_daily.csv`（300/500/黄金/纳指/国债，2021+） | **[done 2026-09-12 · 找到可用结构]** **B3 = 5 资产波动率倒数（风险预算）月度再平衡**：long **Sharpe 1.47 / CAGR +7.3% / MDD −7.3%**（碾压 300 买持 0.09/−42.2、60/40、等权 0.91/−13.0）；三窗 Sharpe 2.64/4.15/0.82。**独立结论（未结合双子星）**；边界=样本 5.6 年、regime 依赖、经典 ARP | 档 [stable-core](backtests/stable/stable-core-2026-09-12.md) · 预注册 [prereg](designs/stable-core-prereg-2026-09-12.md) |
| B8 | **双子星 × B3 稳健核心 · 组合层** | 组合层（现有两腿 NAV，无新数据） | **[done 2026-09-12 · 找到可用稳健组合 {T2,T3,T4}]** ⚠️ **OPT-177 修正**：初版 T0 含 trail8 前视；因果重跑后 **T0 long CAGR 4.04/Sharpe 0.28/MDD −41.4**，**T3「稳健双子星」long 6.24/0.52/−17.9、T4 7.63/1.21/−5.76——连收益都反超 T0**（原：T3 8.95/0.72/−16.9 vs T0 9.42/0.49/−39.9，作废）。两腿相关 0.273。**未落 Live**（若要落地需产品层预注册 + 证不被现有核心/套筒吸收） | 档 [twin-stable-combo](backtests/stable/twin-stable-combo-2026-09-12.md) · [audit-trail8](backtests/audit-trail8-2026-09-12.md) · 预注册 [prereg](designs/twin-stable-combo-prereg-2026-09-12.md) |
| B9 | **稳健双子星 · 不对称择时版** | 组合层 + 沪深300/策略NAV（无新数据） | **[done 2026-09-12 · 修正后 A2/A3 PASS]** ⚠️ **OPT-177 修正**：初版对着含前视 T0 比较→NONE；因果重跑后 **A2（沪深300×MA200）/ A3（软切）由 out 变 PASS**（A2 long 8.79/0.52/−26.0、valid 49.45；A3 6.66/−31.4/44.86）；**A1 仍失败**（valid −24.4）。注：A2 的 long MDD 仍深于 T3。**教训：首版 A1 当日 NAV 前视假飙 35.4%→t-1 8.23%** | 档 [twin-stable-adaptive](backtests/stable/twin-stable-adaptive-2026-09-12.md) · 预注册 [prereg](designs/twin-stable-adaptive-prereg-2026-09-12.md) |
| B10 | **闲置现金 ETF 停车场（回归原始定位）** | S-3 引擎 + ETF（518880/513350/513110/511260，2023+） | **[done 2026-09-12 · 可用 {V2,V3,V4}]** 只在 S-3 闲置现金上做多资产 mom60+MA200 轮动（**因果**）：**V4 +因果trail8 三窗 +11.2/+11.8/+11.4pt 最平滑**；V2/V3 +11.2/+13.2/+2.7；**V1 单纳指（旧 T6）valid −3.7 出局**。基线三窗与官方一致（口径正确）。**结论：ETF 层正确定位=闲置停车场，不是 100% argmax 同池（后者把 S-3 打崩 long ~0%/MDD −58%）** | 档 [idle-sleeve](backtests/stable/idle-sleeve-2026-09-12.md) · 预注册 [prereg](designs/idle-sleeve-prereg-2026-09-12.md) |
| B11 | **ETF 停车场确立为 S-3 之上新基线「港湾」（Harbor）** | 无新数据（`data/etf/etf_daily.csv` 复权） | **[done 2026-09-13 · P1 PASS → 新基线「港湾」冻结（tag `harbor-p1-20260913`）]** 闲置即停（14:30 卫星口径/收盘代理、0.05%/边、三窗+long）：三窗 **+15.5/+11.0/+11.6**、long **+125.4**（K1/K2/K3 全过；2026-09-13 幻影日 + 决策单源修复，对账 100%）；V0 三窗=官方基线 ✅。**Live overlay 代价**：20% 地板≈0；**ETF>STOCK 门槛 long +20.8 vs +90.0（−69pt）→ 应去掉**。**风险**：valid MDD −9.4→−23.1、Sharpe 2.44→1.91（空仓期停车场=满仓 ETF + trail 无冷却再进）；long MDD −23.6/Sharpe 0.83（均优于 V0）。**Live 修复清单待做**：14:30 job、trail 触发/记账同 print、`_pnl_for` 字段 bug（恒 0）、`SELL_TO_A_SHARE` 0 元平仓、测试污染真实账本、去 overlay、`third_asset_sleeve`/`multi_asset_sleeve` 双实现收敛、**`paper_twin_star` intake 15:00 快照前视**、卫星 replay breadth gate；**代码暂不改，攒完一次性修（OPT-178）**。**⚠️ 2026-09-14 日历修正（OPT-183）**：clean 口径 = +55.2/+52.2/+50.3/+201.5（Δ +17.2/+13.0/+11.6/+118.8），K1–K3 仍全过 | 档 [etf-parking-baseline](backtests/stable/etf-parking-baseline-2026-09-13.md) · 预注册 [prereg](designs/etf-parking-baseline-prereg-2026-09-13.md) |
| B12 | **习惯双子星重拟合（停车场核心）× Live 卫星** | 组合层（停车场 NAV + 卫星 replay，无新数据） | **[done 2026-09-13 · REJECT · ⚠️2026-09-14 口径作废，修正后仍 REJECT 只挂 valid]** 50/50 双子星 vs 停车场核心：OOS2 +36.5/+1.14sr/回撤改善，train −11.8/−0.16，valid −28.8/−0.84，**long −55.6/−0.13 且回撤 −23.6→−48.3**；40/60、60/40 全不过。**根因**：卫星 standalone **2022 −34.8% / 2023 −48.0%、long MDD −80.6%**（boom-bust，active 52%/3.48 槽/1139 笔）；旧 clip4 三窗恰是其 2024+164.7%/2025+77.4% 黄金段 + 含前视核心腿 → **旧"50/50 用很少收益换 Sharpe/回撤"双失真**。**卫星现行配方不进新基线、禁止调参重扫**；要救须带全周期门槛另起预注册。下一步 B1 改对停车场基线（不含卫星）跑 | 档 [twin-star-parking-refit](backtests/stable/twin-star-parking-refit-2026-09-13.md) · 预注册 [prereg](designs/twin-star-parking-refit-prereg-2026-09-13.md) |
| B13 | **ETF 买持基准 × 多方法拟合（停车场基线）** | `data/etf/etf_daily.csv`（41 只；510050/588000 已含） | **[done 2026-09-13 · 基线收益胜出]** 基线四窗 total 全高于所有单只 ETF/等权/风险预算/60-40/择时（事后最强单只仅 train 黄金 +49.7 反超；valid/long 输）。**风险上被动组合更强**：风险预算 long Sharpe 1.71/MDD −5.0 vs 基线 0.83/−23.6。**最佳拟合 = 基线×风险预算 50/50**：四窗 Sharpe 全升（1.53/2.92/2.07/1.01）、MDD 全面减半（−9.2/−5.3/−11.8/−12.2），代价=收益（long +114 vs +184）；基线×等权 50/50 略弱。套筒单独 long +63.4/0.54（S-3 才是收益主体）。**50/50 落地需另起预注册**（含 2022-23 压测与成本敏感性） | 档 [etf-benchmark-parking](backtests/stable/etf-benchmark-parking-2026-09-13.md) |
| B14 | **停车场再入场冷却（H-PARK-C · 港湾自审）** | 无新数据（同 B11 P1 口径 + `cooldown_days`） | **[done 2026-09-13 · REJECT]** trail8 出场后同 key 冷却 k∈{1,2,3,5}：**四档全挂 K1/K2/K4/K5**（k0 与 B11 逐数一致 +62.0/+45.3/+50.3/+219.9；冷却档 valid **−6.8~−13.4pt**、long k1 **−22.9pt**；valid MDD 最好 +2.4pt <3pt 门槛、Sharpe 四档全降、换手反增 21→26/27）。**主死因 #1 截右尾**（挡住急跌后 V 反抽）+ #4 regime；k5 long +5.0 为单窗假象。**方向关闭，港湾 P1（k=0）维持**；风险仍走 B13 的 50/50（产品层，未落地）。**⚠️ 2026-09-14 clean 重跑（OPT-183）：k0=+201.5，四档仍全挂，结论不变** | 档 [parking-cooldown](backtests/stable/parking-cooldown-2026-09-13.md) · 预注册 [prereg](designs/parking-cooldown-prereg-2026-09-13.md) |
| B15 | **「母港」（Homeport）＝港湾 × 风险预算 50/50（H-MIX · 产品层组合）** | 两腿：港湾 P1 + B3（300/500/金/纳/债 逆波动率月度）；`eval_harbor_riskbudget` | **[done 2026-09-13 · PASS → 产品候选（不进 Live）]** 月初再平衡 50/50（5bp/边）修正 NAV 下 **K1–K5 全过**：四窗 Sharpe **1.99/3.32/2.26/1.23**（vs 港湾 1.84/2.76/2.14/1.05）、MDD **−9.0/−5.2/−11.4/−12.2**（近腰斩）、long **+122.5**（vs +219.9）；2022–23 压测 **0.62/−8.3 vs 0.57/−15.9**；20bp 成本敏感性仍全过；B13 日频零成本复刻 ≈ 月频含成本（long +124.4 vs +122.5）。**代价=收益近半（买保险）**；落地需用户拍板资本结构 + paper 3/20 纪律。**⚠️ 2026-09-14 clean 重跑（OPT-183）：+36.5/+35.0/+25.6/+116.3，K1–K5 仍全过** | 档 [harbor-riskbudget](backtests/stable/harbor-riskbudget-2026-09-13.md) · 预注册 [prereg](designs/harbor-riskbudget-mix-prereg-2026-09-13.md) |
| B16 | **B3 单资产权重上限（H-B3-CAP）** | 无新数据（B3 权重水填 cap∈{50%,60%}） | **[done 2026-09-13 · REJECT]** cap50 独立 long CAGR 8.2→9.7 但 Sharpe **1.76→1.41**、MDD −4.7→−7.1；母港层近中性（+129.6/1.23/−12.6）；**#4 regime 命中**（债是本区间最高 Sharpe 资产，capsize=正解）。incumbent 维持，债权重上限方向关闭。**⚠️ 2026-09-14 clean 重跑（OPT-183）：仍 REJECT** | 档 [b3-cap](backtests/stable/b3-cap-2026-09-13.md) · 预注册 [prereg](designs/b3-cap-prereg-2026-09-13.md) |
| B17 | **母港市况开关（H-MIX-DYN）** | 无新数据（沪深300×MA200 切换两腿配比） | **[done 2026-09-13 · REJECT]** long CAGR 24.7（>M50 18.0）但 Sharpe 1.08 < 1.23、MDD −20.6；压测 0.20/−10.5 vs M50 0.62/−8.3；valid +42.8/MDD −21.7。**进攻 100/0 能实现、防守不比静态好**；同族不再重开，静态 M50 维持 | 档 [homeport-regime](backtests/stable/homeport-regime-2026-09-13.md) · 预注册 [prereg](designs/homeport-regime-prereg-2026-09-13.md) |

**顺序**：A1/A2/A3 **[done]** → B1 CB **[done 线收口]** → **C1 打新 [done · 次要，有空再实现]**；**B2 利率 [done · REJECT（含预期差）]**；**B2 深层次三源 [done · 全 REJECT，大宗溢价最强生命体征]**；B2 指数调样（本地样本不足）暂缓。
**计划（结合未做项 · 2026-09-12 定）**：
- **P1 数据就绪、先跑只读诊断**：① ~~**B4 超跌/国家队救市**~~ **[done 2026-09-12 · REJECT]** ② ~~**大宗溢价动量中性化**~~ **[done · PASS]** ③ ~~**大宗溢价组合级回放**~~ **[done · REJECT/park — 多头不可实现]**。大宗线关闭。**下一步：有潜力项深挖（B3 产业链 / 打新实现 / 散户关注度 job）**。
- **P2 数据先行**：③ ~~**散户关注度每日快照 job**~~ **[done 2026-09-12 · OPT-176 · `cn_xq_follow` 15:40 job 已上线，向前积累，12 个月后开诊断]** ④ ~~**B3 产业链**~~ **[done 2026-09-12 · REJECT（§一.14 确认）]**。深挖需 PIT 供应链映射 → parked。
- **P3 次要/parked**：⑤ 打新实现（有空做）⑥ 指数调样（等外部成分史）⑦ ~~补 510050/588000 落库~~ **[done 2026-09-13：两只早已在 `data/etf/etf_daily.csv`（2021 起全），B13 基准已用；DB `daily` 缺口无影响]**。
- 纪律不变：自有基线、预注册、零网格、不在 S-3/卫星引擎加 gate；一次一刀；结果落 `docs/backtests/` + SUMMARY 一行。
**纪律**：同 P0-12（自有基线、预注册、零网格、不在 S-3/卫星引擎加 gate）；结果一律落 `docs/backtests/` + SUMMARY 一行。
**不做**：再挖 A 股价量/财务因子；做空（hedge 已实现证伪）；期货/杠杆；小盘/微盘。

---

## 实施清单（剩余 P0/P1 各一行）

| # | 动作 | 预期 |
|---|------|------|
| 9 | 付费API矩阵 | 上云选型 P1 |
| 2b | Tunnel 端到端 `brew install cloudflared` | 远程前提 P1 |

## 沉淀（近 5 条，余见 archive/README）

| 2026-09-12 | P0-13 A1/A2/A3 + B1（ETF 买持/打板/ETF 深挖三刀 + CB 线） | `archive/2026-09-12-p013-newtracks-cb-etf.md` |
| 2026-09-12 | P0-13 C1 打新收益层（EV 预注册 PASS，M=20万年化 3–7%） | `archive/2026-09-12-ipo-line.md` |
| 2026-09-12 | P0-13 B2 利率线（FOMC 离散 REJECT + 预期差 ΔUS2Y「关系真不可交易」REJECT） | `archive/2026-09-12-event-rate.md` |
| 2026-09-12 | P0-13 深层次信号三源（龙虎榜机构/股东户数/大宗溢价，全 REJECT） | `archive/2026-09-12-deeplayer-inst.md` |
| 2026-09-12 | P0-13 B4 超跌×国家队净买→反弹（REJECT，"救市→反弹"机制不成立） | `archive/2026-09-12-bounce-rescue.md` |
| 2026-09-12 | P0-13 大宗溢价动量中性化复验（H-BLK-B **PASS/REVIVE**，本轮唯一 PASS → 溢价腿待引擎回放） | `archive/2026-09-12-deeplayer-inst.md` |
| 2026-09-12 | P0-13 大宗溢价组合级回放（H-BLK-C **REJECT/park**：K2 价差成立但多头不可实现 → 大宗线关闭） | `archive/2026-09-12-deeplayer-inst.md` |
| 2026-09-12 | P0-13 B3 产业链 anchor→概念传导（H-CHAIN-A **REJECT**：隔夜 gap 消费、无慢漂移，§一.14 增补证据） | `archive/2026-09-12-chain-anchor.md` |
| 2026-09-12 | P0-13 B6 稀有极端触发枚举（H-RARE **NONE passed**：5 预声明恐慌触发无一人选，C3 千股跌停最接近但仍非稳稳赚） | `archive/2026-09-12-rare-triggers.md` |
| 2026-09-12 | P0-13 B7 多资产稳健核心（H-STABLE **找到可用独立结构**：B3 波动率倒数，Sharpe 1.47/CAGR 7.3%/MDD −7.3%） | `archive/2026-09-12-stable-core.md` |
| 2026-09-12 | P0-13 B8 双子星×B3 组合层（H-COMBO **找到可用稳健组合 {T2,T3,T4}**：T3 50/50 MDD 腰斩、T4 风险预算 MDD −5.8%/Sharpe 1.34） | `archive/2026-09-12-stable-core.md` |
| 2026-09-12 | P0-13 B9 稳健双子星不对称择时版（H-ADAPT **修正后 A2/A3 PASS**；含 1 日前视 bug 纪律教训） | `archive/2026-09-12-stable-core.md` |
| 2026-09-12 | **OPT-177 ETF trail8 前视审计**（双子星核心回测污染；因果修复；推翻旧 trail8 证据） | `backtests/audit-trail8-2026-09-12.md` |
| 2026-09-12 | P0-13 B10 闲置现金 ETF 停车场（H-SLEEVE **可用**：多资产轮动因果三窗 +11.2/+13.2/+2.7，V4 最平滑） | `archive/2026-09-12-stable-core.md` |
| 2026-09-13 | P0-13 B11 ETF 停车场确立为 S-3 新基线（P1 PASS：+9.4/+6.0/+9.1/long +90.0；Live 前视+账本 bug 审计） | `archive/2026-09-13-etf-parking-baseline.md` |
| 2026-09-13 | P0-13 B12 习惯双子星重拟合（停车场核心 × 卫星 **REJECT**：long −55.6/回撤 −48.3；卫星 2022 −34.8%/2023 −48.0%/MDD −80.6% boom-bust）**⚠️ 2026-09-14 口径作废（旧前视键+缺 C1/14:30 卖+旧核心）；修正后仍 REJECT，但只挂 valid 单窗** | `archive/2026-09-13-etf-parking-baseline.md` |
| 2026-09-13 | P0-13 B13 ETF 买持基准×多方法拟合（基线收益全胜；**最佳拟合=基线×风险预算 50/50**：Sharpe 全升、MDD 减半；510050/588000 面板已含） | `archive/2026-09-13-etf-parking-baseline.md` |
| 2026-09-13 | **港湾上线 · 双子星退役**（后端/前端/paper/watchlist/回测/调度/DB 全切；Live 前视与账本修复 OPT-178；后端 4171 绿/前端 833 绿） | `archive/2026-09-13-etf-parking-baseline.md` |
| 2026-09-14 | **三策略前视审计与修正（OPT-182 + OPT-183 日历）**：双子星评估口径（`amp_1430`/C1/14:30 卖/单源核心）、B3 负索引、卫星 `gate_1430`、**卫星 qfq×raw 基期混用**、Live trail/_pick as-of、平仓 T+1 回填、ROTATE `parkPct`、recon 日期门槛、**双市场日历（HK-only 日期）过滤**；最终 clean：双子星 **+149.7/+52.2/+29.1/+291.9** 仍 REJECT（只挂 valid）、港湾 **+55.2/+52.2/+50.3/+201.5**、母港 PASS；CN/HK 官方基线重固化；**卫星 valid 短板诊断 = regime 依赖**（有仓日核心同日 3.2×；闸门 +12.4pt 正贡献）；**港湾×卫星曝露曲线（H-SAT-W）7 档全 REJECT**（valid 稀释 ∝ w、K1/K2 无交集）→ Live=港湾、不引入卫星曝露；**三腿「母港×卫星」（H-B3-SAT）PASS（chosen=1/3、稳健 0.15–0.25）**——换基座后 valid 斜率 −42.4×w→**−14.6×w**（B3 吸收 2/3），long +198.1/sr 1.84 → **产品候选增量、不进 Live** | [`archive/2026-09-14-three-strategy-audit.md`](archive/2026-09-14-three-strategy-audit.md) · 诊断 [`backtests/sat/sat-valid-shortfall-diagnosis-2026-09-14.md`](backtests/sat/sat-valid-shortfall-diagnosis-2026-09-14.md) · 曲线 [`backtests/stable/harbor-sat-weight-2026-09-14.md`](backtests/stable/harbor-sat-weight-2026-09-14.md) · 三腿 [`backtests/stable/harbor-b3-sat-2026-09-14.md`](backtests/stable/harbor-b3-sat-2026-09-14.md) |
| 2026-09-12 | P0-12 价量公式因子速筛（Alpha101/GTJA191/HK；含 X3 sleeve PARK、GARP CLOSE） | `archive/2026-09-12-factor-library-screens.md` |
| 2026-09-11 | P0-11 财务质量（三表 16.5 万行/表 + 7 诊断 + P18 三臂回放，全关） | `archive/2026-09-11-p11-fin-quality.md` |
| 2026-09-06 | P0-9 文档轨 D0–D5（backtests 分夹/checklist 双拆/数字单一源/路由统一/一页纸） | `archive/2026-09-06-docs-p09-agent-friendly.md` |
| 2026-09-06 | P0-8 砖块结论 M1–M6（死因分类/存活律/吸收矩阵/平台表/指纹表） | `archive/2026-09-06-brick-laws-m1-m6.md` |

| 2026-08-27 | 形态三噪音+回踩MA20弱edge归档 | `backtests/bollinger|macd|kdj|uptrend-pullback` |
| 2026-08-27 | SuperTrend/Fib/PA 分流结论 | `backtests/indicator-supertrend-fibonacci-priceaction-notes.md` |
| 2026-08-24 | 多资产轮动固化 `mom60+MA200` | `service/multi_asset_sleeve.py` |
| 2026-08-14 | neutral_block/entry_style/env_scale 固化 | `strategy-params.md` |
| 2026-08-12 | 回撤熔断 -25 | `strategy-params §6` |

## 注意力预算

每天：读本页 P0-0 / P0-4 + `modules/watchlist.md` Gate；每周一：跑天平；改 schema 前读 `AGENTS.md`。

---
*压缩规则：完成段只留外链，不回写 archive；新想法先 `designs/` 草稿。*
