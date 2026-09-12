# Karios Desktop 优化 Checklist

> 工程执行栈（OPT-xxx）：架构 / 性能 / 兼容 / 工程债怎么做。编号 `OPT-001 ~ OPT-173`（重号见文末）。
> **正文 9 条未完成 + 3 条冬眠**；已完成 117 条按天归档 [`archive/`](archive/)（见文末索引表）。

---

## 如何使用

1. 未完成队列已清空（2026-09-06 H2 收官）；新 OPT 直接在"未完成"下开节，唤醒冬眠同理。
2. 每个任务开独立 Agent 会话，把对应章节整段粘贴给 Agent 作为 scope。
3. 完成后将 `[ ]` 改为 `[x]`，填写 **完成日期**，并按天归档：新档 `archive/YYYY-MM-DD-opt-NNN-slug.md`（仿 git 历史）+ 正文该节替换为索引行。
4. 若实施过程中方案有变，在本文件更新，不要另起文档。

### Agent 任务模板（见仓库根 AGENTS.md → Scoped optimization tasks；本文件只补一条）

> 开独立会话，把对应章节整段粘贴为 scope；只改列出的文件范围；更新状态；补测试；不扩 scope。

---

## 未完成（9 条 · 2026-09-11 工程盘点立）

> 来源：2026-09-11 工程健康盘点。**一 OPT 一会话，只改列出的范围，补测试，不扩 scope。**
> 顺序建议：A → B →（按需）C/D；E 策略实现准确度建议与 B 同期。

### A 仓库卫生（高杠杆低风险）✅ 全完成（OPT-160/161/162）

### B 质量护栏（棘轮，防回退）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-163 | [ ] | **pyright 警告棘轮**：`pyproject` 现把 `report{ArgumentType,OptionalSubscript,OptionalIterable,OptionalMemberAccess,GeneralTypeIssues,AttributeAccessIssue}` 降为 warning。先统计基线数，逐模块转 error（engine/routes/paper 优先）。验收：warning 数单向下行，CI 保持绿 |
| OPT-175 | [ ] | **coverage 余量修复**（OPT-165 发现）：门 88% 现仅 **88.05%**，太薄。给 P0-11 未测的 `service/fin_panel.py`（~200 stmt，0 测）补单测 / 或核减死码。验收：覆盖率回到 ≥89%，门 88 恢复安全垫 |

### C 性能 / 数据（按需）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-167 | [ ] | **热表索引 + 慢查询审计**：`bar_5min`(6.5GB)/`daily` EXPLAIN 关键查询、补缺失索引；前端 bundle 体积分析。验收：慢查询清单 + 索引前后耗时对比 |
| OPT-168 | [ ] | **依赖刷新**：`pnpm outdated` + `uv lock`，分批升级 + 回归。验收：锁文件更新、全量测试绿、无 breaking |

### D 产品 / 可观测（最低优先）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-169 | [ ] | **UI 极简化美化**：按 `designs/ui-minimal-redesign.md`（拍板后从 designs 转 OPT）。验收：设计稿条目落地、前端测试绿 |
| OPT-170 | [ ] | **健康聚合 / 告警巡检补强**：聚合 job 失败/数据新鲜度/熔断为单一 health 视图，巡检缺口。验收：缺口表 + 告警用例 |

### E 策略实现准确度（2026-09-11 新增）

> 把"前视/时钟/成本"那类事故（OPT-159 前视、14:30 时钟统一、OPT-154 滑点双计、OPT-157 qfq 混基准）从**人工审计**变成**机器护栏**。

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-171 | [ ] | **PiT / 前视回归护栏**：为所有选池/因子入口（`strategy_a1_voltarget.build_panel`、各 `incubate/*` 筛选、S-3 选股）加"调仓日可得性"单测（禁止本月值选本月票 / `.shift(-n)`）。验收：新增回归测试；伪造前视会红 |
| OPT-172 | [ ] | **配方一致性 attestation 扩展**：在 H5（clip4 字面量跨层）基础上，把**卫星时钟（`same_1430`+C1 3%+第3日14:30）、成本（CN 30bps/HK 90bps）、fill 模式**做成单一源 + 漂移门，声明 Live==冻结。验收：改任一字面量测试红 + UI 展示 |
| OPT-173 | [ ] | **回放黄金测试**：冻结小窗口 fixtures，断言回测引擎输出 bit-stable（重放保真）。验收：golden 文件对比测试进 CI |

---

### 刚完成（待归档）

| OPT-165 | [x] 2026-09-12 · **schema parity 护栏**：`tests/test_schema_parity.py`（纯 migration-coverage + PG 列超集）；**抓到真 drift**——`system_events` 无 migration → 补 `0048_system_events`（head 0048）；全量 4298 passed | — |
| OPT-166 | [x] 2026-09-12 · **死代码/omit 收敛**：`state_bucket_slice.py` 移出 `src/`→`scripts/`（无 src 引用，文档已更新），删 coverage omit；engine 6 候选行复核为防御 nil 守卫、保留 | — |
| OPT-174 | [x] 2026-09-12 · **main 变绿 hotfix**：修 ruff 7 + pyright 7（`ruff check`/`pyright` 均 0 error；全量 4295 passed） | — |
| OPT-164 | [x] 2026-09-12 · **CI 格式门**：一次性 `ruff format`（432 文件）+ `prettier --write`（251 源码）；`lint` 加 `ruff format --check`、root `format:check` + CI 步骤 | — |
| OPT-162 | [x] 2026-09-12 · 工作区 WIP 按主题切分提交（5 个 commit：OPT-160/161 工程 · sat 时钟统一 · trend_guide 钩子 · 数据一致性 · P0-12 研究+因子库；`git status` 干净） | — |

| OPT-160 | [x] 2026-09-12 · 运行产物去跟踪分类治理（111 文件 `git rm --cached`；`.gitignore` +4 规则 +`coverage_tmp.json`；README 再生来源表；报告测试 57 passed） | [2026-09-12-opt-160-artifact-untrack.md](archive/2026-09-12-opt-160-artifact-untrack.md) |
| OPT-161 | [x] 2026-09-12 · 文档死链清零（根因：`opt-151/152` 归档被误放 `docs/optimization-checklist/archive/` → `git mv` 到 `docs/archive/`；`linkcheck` clean 173 文件） | — |

| OPT-156 | [x] 2026-09-11 · stock_dailybasic 2023-12-29→2024-08-01 空洞回填（**实缺 2024-01~07**，140 个 A 股交易日/74.8 万行）+ trade_calendar SSE 补齐到 2005（+6575 行，4374 open days） | — |
| OPT-157 | [x] 2026-09-11 · CN `daily` 复权统一重建 + A 股日线回填 2007 + 财报回填 2007–2017 + 两融空洞补全（原 close 是标准 qfq/陈旧基准/未复权混合，跨除权收益跳变；重建后 2007–2026 单一基准，全市场一致性 99.8%→100%，脚本 `backfill_cn_daily_history`/`rebuild_cn_daily_qfq`/`fix_daily_outliers`/`backfill_cn_financials_history`，备份 `daily_backup_20260911`） | [data-consistency-2026-09-11.md](backtests/data-consistency-2026-09-11.md) |
| OPT-158 | [x] 2026-09-11 · 指数历史回补：`index_daily` 000300/000905/000001 等 5 指数经 `index_hist_extend.py --since 2005-01-01` 补 2005–2020（+20870 行），支撑真实市值加权 beta（DH-2） | [a1-voltarget-beta-2026-09-11.md](backtests/a1-voltarget-beta-2026-09-11.md) |
| OPT-159 | [x] 2026-09-11 · P0-12 pilot 前视审计：`strategy_a1_voltarget.build_panel` 用本月末成交额选本月票（前视 ~+20pt/yr）→ 改上月末 as-of；沉淀纪律"选池必须调仓日可得"；同批 `diag_tenbagger` 审计为 as-of 正确 | [a1-voltarget-beta-2026-09-11.md](backtests/a1-voltarget-beta-2026-09-11.md) |

| OPT-155 | [x] 2026-09-10 · ADV 冲击诊断（AUM ¥1000万 → 不 material，关闭；复活条件 ¥3000万+） | [2026-09-10-opt-155-adv-impact.md](archive/2026-09-10-opt-155-adv-impact.md) |

| OPT-154 | [x] 2026-09-10 · E5 滑点双计修正（回测成本 = live 成本模型，CN 30bps / HK 90bps） | [2026-09-10-opt-154-e5-slippage.md](archive/2026-09-10-opt-154-e5-slippage.md) |

| OPT-153 | [x] 2026-09-10 · 卫星腿判定修正（自选卫星单不再误报偏离 + 腿推断去 `===12.5`） | [2026-09-10-opt-153-sat-leg-audit.md](archive/2026-09-10-opt-153-sat-leg-audit.md) |

| OPT-152 | [x] 2026-09-09 · Timeline 实盘口径主曲线（A 方案：S-3 实际权益主曲线 + 100% 押注基准降虚线） | [2026-09-09-opt-152-timeline-product-curve.md](archive/2026-09-09-opt-152-timeline-product-curve.md) |

| OPT-151 | [x] 2026-09-09 · 核心腿 recon 对账闭环（sleeve_paper_recon + brief/job/API/UI） | [2026-09-09-opt-151-sleeve-core-recon.md](archive/2026-09-09-opt-151-sleeve-core-recon.md) |

| OPT-150 | [x] 2026-09-09 · 账本修正机制 PATCH 改腿 + 审计一键挪链 | [2026-09-09-opt-150-trade-correct.md](archive/2026-09-09-opt-150-trade-correct.md) |

| OPT-149 | [x] 2026-09-09 · user_trades 腿拆分 sat/s3 + 审计按腿分支（卫星对 paper 账） | [2026-09-09-opt-149-trade-legs.md](archive/2026-09-09-opt-149-trade-legs.md) |

| OPT-147 | [x] 2026-09-09 · R-wide breadth 池 HK 污染量级断言（flips 0/0/0 关项，引擎零改动） | [2026-09-09-opt-147-rwide-hk.md](archive/2026-09-09-opt-147-rwide-hk.md) |

| OPT-148 | [x] 2026-09-08 · paper HK T+2 settled 账本 + 平安最高佣金 90bps + swap 费用归位 | [2026-09-08-opt-148-hk-settle-ledger.md](archive/2026-09-08-opt-148-hk-settle-ledger.md) |

| OPT-124 | [x] 2026-09-06 · 多 token 轮换 + 配额看门狗 | [2026-09-06-opt-124-tushare-pool.md](archive/2026-09-06-opt-124-tushare-pool.md) |

---

---

| OPT-125 | [x] 2026-09-06 · DB 连接池 + 重试 + 慢查询护栏 | [2026-09-06-opt-125-db-pool.md](archive/2026-09-06-opt-125-db-pool.md) |
| OPT-126 | [x] 2026-09-06 · 东财出口探针 + 熔断可视化 | [2026-09-06-opt-126-em-probe.md](archive/2026-09-06-opt-126-em-probe.md) |
| OPT-145 | [x] 2026-09-06 · 外购分钟对拍 A 形式化 | [2026-09-06-opt-145-vendor-minute.md](archive/2026-09-06-opt-145-vendor-minute.md) |
| OPT-146 | [x] 2026-09-06 · 港股符号误标 + 结算语义 | [2026-09-06-opt-146-hk-symbol.md](archive/2026-09-06-opt-146-hk-symbol.md) |

---

---

## 冬眠（3 条 · 未关闭 · 可唤醒）

> 陈年 open / P2 储备，移出未完成队列减负。唤醒 = 移回"未完成"并排优先级。

| OPT | 状态 | 全文 |
|-----|------|------|
| OPT-045 | [ ] Phase A done · B/C 待开 | [2026-09-06-opt-dormant-open.md](archive/2026-09-06-opt-dormant-open.md) |
| OPT-075 | [ ] 2026-08-12 登记未处理 | 同上 |
| OPT-127 | [ ] P2 储备（用户判断暂无用；OPT-125 后重估） | 同上 |

---

## 已完成索引（108 条 · 按天归档 · 只读）

| 日期 | OPT | 标题 | 归档 |
|------|-----|------|------|
| 2026-06-18 | 031–040 | Sentiment 降载 / ensure_table / 指数批量 / 信号去重 / 退役串行 / Screener / Dashboard / RSS / Query 迁移 | [2026-06-18-opt-031-040-perf-queries.md](archive/2026-06-18-opt-031-040-perf-queries.md) |
| 2026-07-29 | 041–044 | HK 闸门 / ETF 通用化 / HK 日线 cron / watchlist 显示 | [2026-07-29-opt-041-044-hk-etf.md](archive/2026-07-29-opt-041-044-hk-etf.md) |
| 2026-08-01 | 045 Phase A | /v1 API 整圈（045+046+047 Phase A；正文 045 留 B/C） | [2026-08-01-opt-045-v1-api-surface.md](archive/2026-08-01-opt-045-v1-api-surface.md) |
| 2026-08-01 | 046–048 | /v1 业务 endpoint / explain 文档 / Tunnel 部署 | [2026-08-01-opt-046-048-api-v1.md](archive/2026-08-01-opt-046-048-api-v1.md) |
| 2026-08-01 | 049 | Paper-trading 启动 | [2026-08-01-opt-049-paper-trading.md](archive/2026-08-01-opt-049-paper-trading.md) |
| 2026-08-01 | 050 | 数据源质量审计 | [2026-08-01-opt-050-data-source-audit.md](archive/2026-08-01-opt-050-data-source-audit.md) |
| 2026-08-01 | 051 | API key 配额 openapi | [2026-08-01-opt-051-api-key-quota-openapi.md](archive/2026-08-01-opt-051-api-key-quota-openapi.md) |
| 2026-08-01 | 052 | Alpha Radar HK | [2026-08-01-opt-052-alpha-radar-hk.md](archive/2026-08-01-opt-052-alpha-radar-hk.md) |
| 2026-08-01 | 053 | DB 走向决策 | [2026-08-01-opt-053-db-direction.md](archive/2026-08-01-opt-053-db-direction.md) |
| 2026-08-01 | 055 | AI agent cookbook | [2026-08-01-opt-055-ai-agent-cookbook.md](archive/2026-08-01-opt-055-ai-agent-cookbook.md) |
| 2026-08-01 | 056 | Docker 一键起 | [2026-08-01-opt-056-docker-one-click.md](archive/2026-08-01-opt-056-docker-one-click.md) |
| 2026-08-01 | 057 | TV Capture 三轨（另一条 057 见 08-31 档） | [2026-08-01-opt-057-tv-capture-three-track.md](archive/2026-08-01-opt-057-tv-capture-three-track.md) |
| 2026-08-02 | 058 | 漏斗 N 日表格（另一条 058 见 08-31 档） | [2026-08-02-opt-058-funnel-history-paper-v0.1.md](archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md) |
| 2026-08-03 | 059 | legacy 清理（与 09-01 档的 059 涨停审计不同条） | [2026-08-03-opt-059-legacy-cleanup.md](archive/2026-08-03-opt-059-legacy-cleanup.md) |
| 2026-08-04 | 060 | Tauri 降级（另一条 060 见 09-01 档） | [2026-08-04-opt-060-tauri-deprecation.md](archive/2026-08-04-opt-060-tauri-deprecation.md) |
| 2026-08-04 | 061 | DB 备份迁移 | [2026-08-04-opt-061-db-backup-migrate.md](archive/2026-08-04-opt-061-db-backup-migrate.md) |
| 2026-08-07 | 062 | Paper v0.2 | [2026-08-07-opt-062-paper-v02.md](archive/2026-08-07-opt-062-paper-v02.md) |
| 2026-08-07 | 063 | 回测引擎 v0 | [2026-08-07-opt-063-backtest-engine.md](archive/2026-08-07-opt-063-backtest-engine.md) |
| 2026-08-07 | 064 | 卖出归因 + 回测页 | [2026-08-07-opt-064-exit-attribution-backtest-page.md](archive/2026-08-07-opt-064-exit-attribution-backtest-page.md) |
| 2026-08-07 | 065 | 周度复盘 | [2026-08-07-opt-065-weekly-review.md](archive/2026-08-07-opt-065-weekly-review.md) |
| 2026-08-07 | 066 | journal 防御 | [2026-08-07-opt-066-journal-symbol-defense.md](archive/2026-08-07-opt-066-journal-symbol-defense.md) |
| 2026-08-07 | 067 | 相关性防火墙 | [2026-08-07-opt-067-correlation-firewall.md](archive/2026-08-07-opt-067-correlation-firewall.md) |
| 2026-08-09 | 068–073 | 真实交易看板 / K 线源 / 入池闸门 / score 回填 / RS 过滤 / 持仓体检 | [2026-08-09-opt-068-073-engine-batch.md](archive/2026-08-09-opt-068-073-engine-batch.md) |
| 2026-08-12 | 074–087（无 075、077；075 未完成在正文，077 从未立项） | 健壮性审查 / 执行闸统一 / 体检提醒 / 对账 / BacktestPage / 通知 / 周报 / 条件单 / 信息层 / C4 框架 | [2026-08-12-opt-074-087-robust-batch.md](archive/2026-08-12-opt-074-087-robust-batch.md) |
| 2026-08-12 | 085–089 | S-3 信息层 + C4 对照框架 + 体检卡（当时总结） | [2026-08-12-opt-085-089-s3-info-layers.md](archive/2026-08-12-opt-085-089-s3-info-layers.md) |
| 2026-08-12 | 090–094 | webhook + 一键启动 + 红绿灯 | [2026-08-12-opt-090-094-webhook-lights.md](archive/2026-08-12-opt-090-094-webhook-lights.md) |
| 2026-08-13 | 095–110（无 101，从未立项） | 退出对齐 / HK 信号 / 蒙特卡洛 / 涨跌停 / ATR / 对账 / LLM 移峰 / Alpha 初验 | [2026-08-13-opt-095-110-align-batch.md](archive/2026-08-13-opt-095-110-align-batch.md) |
| 2026-08-14 | 111–118 | 对账横幅 / cron / 执行卡 / Webhook 目录 / Bark / Tunnel / 手机 UI / 网关 | [2026-08-14-opt-111-118-mobile-webhook.md](archive/2026-08-14-opt-111-118-mobile-webhook.md) |
| 2026-08-21 | 119–121 | 套筒模拟器 / 核心仓核对 / dual 联合验收 | [2026-08-21-opt-119-121-sleeve-dual.md](archive/2026-08-21-opt-119-121-sleeve-dual.md) |
| 2026-08-31 | 057–058 | dailybasic 停更修复 / 双子星 14:30 决策审计 | [2026-08-31-opt-057-058-dailybasic-twin-audit.md](archive/2026-08-31-opt-057-058-dailybasic-twin-audit.md) |
| 2026-09-01 | 059–060 | 涨停审计 / 双子星 v2 | [2026-09-01-opt-059-060-limitup-twin-v2.md](archive/2026-09-01-opt-059-060-limitup-twin-v2.md) |
| 2026-09-01 | 122–123 | 盘中近似信号 / v3 固化 | [2026-09-01-opt-122-123-twin-signal.md](archive/2026-09-01-opt-122-123-twin-signal.md) |
| 2026-09-02 | 128–135 | clip4 / 对齐 / Timeline / 占用 / blotter / 健康 / Zod / 日流程 | [2026-09-02-opt-128-135-twin-live.md](archive/2026-09-02-opt-128-135-twin-live.md) |
| 2026-09-03 | 136–137 | body=3 对齐 / 5 分钟线 | [2026-09-03-opt-136-137-sat-live.md](archive/2026-09-03-opt-136-137-sat-live.md) |
| 2026-09-04 | 138 | 红套件清零 | [2026-09-04-opt-138-red-suite-green.md](archive/2026-09-04-opt-138-red-suite-green.md) |
| 2026-09-04 | 139 | Scheduler 上报统一 | [2026-09-04-opt-139-job-guard.md](archive/2026-09-04-opt-139-job-guard.md) |
| 2026-09-04 | 140 | 模式口径统一 | [2026-09-04-opt-140-leg-split.md](archive/2026-09-04-opt-140-leg-split.md) |
| 2026-09-04 | 141 | 习惯口径引擎化 | [2026-09-04-opt-141-live-caliber.md](archive/2026-09-04-opt-141-live-caliber.md) |
| 2026-09-04 | 142 | 日历收敛 + Alembic | [2026-09-04-opt-142-calendar-api-migration.md](archive/2026-09-04-opt-142-calendar-api-migration.md) |
| 2026-09-04 | 143–144 | 可重放补强 / 外围抖动 | [2026-09-04-opt-143-144-replay-quiet.md](archive/2026-09-04-opt-143-144-replay-quiet.md) |
| 2026-09-06 | 124 | 多 token 轮换 + 配额看门狗 | [2026-09-06-opt-124-tushare-pool.md](archive/2026-09-06-opt-124-tushare-pool.md) |
| 2026-09-06 | 125 | DB 连接池 + 重试 + 慢查询护栏 | [2026-09-06-opt-125-db-pool.md](archive/2026-09-06-opt-125-db-pool.md) |
| 2026-09-06 | 126 | 东财出口探针 + 熔断可视化 | [2026-09-06-opt-126-em-probe.md](archive/2026-09-06-opt-126-em-probe.md) |
| 2026-09-06 | 145 | 外购分钟对拍 A 形式化 | [2026-09-06-opt-145-vendor-minute.md](archive/2026-09-06-opt-145-vendor-minute.md) |
| 2026-09-06 | 146 | 港股符号误标 + 结算语义 | [2026-09-06-opt-146-hk-symbol.md](archive/2026-09-06-opt-146-hk-symbol.md) |
| 2026-09-09 | 147 | R-wide breadth 池 HK 污染量级断言（关项） | [2026-09-09-opt-147-rwide-hk.md](archive/2026-09-09-opt-147-rwide-hk.md) |
| 2026-09-09 | 149 | user_trades 腿拆分 sat/s3 + 审计按腿分支 | [2026-09-09-opt-149-trade-legs.md](archive/2026-09-09-opt-149-trade-legs.md) |
| 2026-09-09 | 150 | 账本修正机制 PATCH 改腿 + 审计一键挪链 | [2026-09-09-opt-150-trade-correct.md](archive/2026-09-09-opt-150-trade-correct.md) |
| 2026-09-09 | 151 | 核心腿 recon 对账闭环（sleeve_paper_recon + brief/job/API/UI） | [2026-09-09-opt-151-sleeve-core-recon.md](archive/2026-09-09-opt-151-sleeve-core-recon.md) |
| 2026-09-09 | 152 | Timeline 实盘口径主曲线（A 方案：S-3 实际权益主曲线 + 基准降虚线） | [2026-09-09-opt-152-timeline-product-curve.md](archive/2026-09-09-opt-152-timeline-product-curve.md) |

> **历史重号说明**：OPT-057/058/060 在正文出现过两次（不同日期不同内容），OPT-059 的归档文件与正文条目主题不同。
> 一律不重编号，以"日期+标题"区分：057 = 08-01 TV Capture / 08-31 dailybasic 停更；058 = 08-02 漏斗 / 08-31 双子星 14:30 审计；
> 060 = 08-04 Tauri / 09-01 双子星 v2；059 归档 = 08-03 legacy 清理，正文 059（09-01 涨停审计）在 09-01 档。
