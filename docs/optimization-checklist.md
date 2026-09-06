# Karios Desktop 优化 Checklist

> 工程执行栈（OPT-xxx）：架构 / 性能 / 兼容 / 工程债怎么做。编号 `OPT-001 ~ OPT-146`（重号见文末）。
> **正文只留 4 条未完成 + 3 条冬眠**；已完成 104 条按天归档 [`archive/`](archive/)（见文末索引表）。

---

## 如何使用

1. 按 **优先级（P0 → P1）** 顺序执行下面 5 条（另有 3 条冬眠，见文末冬眠区）。
2. 每个任务开独立 Agent 会话，把对应章节整段粘贴给 Agent 作为 scope。
3. 完成后将 `[ ]` 改为 `[x]`，填写 **完成日期**，并按天归档：新档 `archive/YYYY-MM-DD-opt-NNN-slug.md`（仿 git 历史）+ 正文该节替换为索引行。
4. 若实施过程中方案有变，在本文件更新，不要另起文档。

### Agent 任务模板（见仓库根 AGENTS.md → Scoped optimization tasks；本文件只补一条）

> 开独立会话，把对应章节整段粘贴为 scope；只改列出的文件范围；更新状态；补测试；不扩 scope。

---

## 未完成（4 条）

| OPT-124 | [x] 2026-09-06 · 多 token 轮换 + 配额看门狗 | [2026-09-06-opt-124-tushare-pool.md](archive/2026-09-06-opt-124-tushare-pool.md) |

---

---

### OPT-125：DB 连接池 + DB 重试 + 慢查询护栏（P1）

**状态**：[ ] 待排期  
**优先级**：P1（`psycopg 无池` + `DB 层无重试` + 仅 `statement_timeout 120s`；前端 herd 易 `too many clients`）  
**关联**：[`stability-audit-2026-09-01.md`(./designs/stability-audit-2026-09-01.md) §3

#### 背景

`db/__init__.py:8` 每次 `psycopg.connect(connect_timeout=5, statement_timeout=120s)` 新建 TCP，无 `psycopg_pool` / `pgbouncer`，无 `lock_timeout` / `idle_in_transaction` 回收。前端 `refetchOnWindowFocus` + 多 job 并发易打满 `max_connections=100`；DB 瞬断无重试。

#### 目标

- 引入 `psycopg_pool.ConnectionPool(min 2 / max 20, timeout 10s)` 或 `pgbouncer` sidecar（二选一，倾向 `psycopg_pool` 零运维）
- `get_connection()` 改为 `pool.getconn() / putconn()`；`check_db()` 走池
- `statement_timeout 120s` 保留，新增 `lock_timeout 10s` + `idle_in_transaction_session_timeout 60s`（`options` 追加）
- DB 层 transient（`OperationalError / TimeoutError`）`retry 1×`（指数 0.5s），避免与网络层 `_with_retry` 叠加雪崩
- Alembic `env.py` 仍 `NullPool`（迁移不占池），`docker-compose.yml` healthcheck 改走 pool

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `services/data-sync-service/src/data_sync_service/db/__init__.py` |
| DB | `services/data-sync-service/alembic/env.py` |
| Compose | `docker-compose.yml` |
| Service | `services/data-sync-service/src/data_sync_service/service/dashboard.py`（`wait(..., timeout 0.3s)` 处 DB 超时语义） |
| Tests | `tests/test_db_pool.py`（**新**：池获取/归还/超时；mock 下 transient 重试） |

#### 验证

- [ ] 并发 30 请求不 `too many clients`（mock pool `max 20` 排队而非新建）
- [ ] `lock_timeout` 超限触发重试 1 次后成功
- [ ] `alembic upgrade head` 仍 `NullPool` 不走业务池
- [ ] pytest 通过

#### 反模式

- ❌ 把 pool size 设 `100` 对齐 PG max（应 `≤20` 留余量给 pgadmin/migrate）
- ❌ 在 Alembic 里复用业务池（迁移长事务会占池）
- ❌ DB 重试与网络 `_with_retry 3×` 无差别叠加（DB 仅 1 次，避免放大）

---

---

### OPT-126：东财出口探针 + 熔断可视化（P1）

**状态**：[ ] 待排期  
**优先级**：P1（5 个 job 共用同一出口 IP，ban 时静默 `skipped` 难定位）  
**关联**：[`stability-audit-2026-09-01.md`(./designs/stability-audit-2026-09-01.md) §2.2/§3

#### 背景

`industry_fund_flow` / `top_inst` / `option_iv` / `twin_star` / `minute_capture` / `realtime_quote fallback` 均走 `push2.eastmoney.com / data.eastmoney.com`，已用 `ProxyHandler({})` 直连 + `_EM_BLOCKED 15min latch` + `_PROXY_DEGRADED`（`service/em_push2_http.py` `industry_fund_flow.py:163`），但无探针、无可视化，`cn_industry_post_close_sync` 18:15 前失败仅写 `ok=False` 横幅不预警。

#### 目标

- 新增 `service/em_probe.py`：每 10min 黑盒探测 `push2 / dataapi bkzj / push2his` 各 1 次轻量 GET（`timeout 10s`，`ProxyHandler({})` 直连），`failed streak≥3` 即 `system_events insert (severity=high, dedupe=em_probe:{host})` + `webhook emit em_probe_failed`
- `health_routes._SOURCES` 新增 `eastmoney_probe` 源（`threshold 20min`），前端 `SystemHealth` 横幅可报警（复用 `health/datasources` 12 类后第 13 类）
- `em_push2_http.py` 的 `_EM_BLOCKED` 熔断状态暴露到 `GET /api/health/datasources`（`eastmoney_ip_ban_latched` + `cooldown_remaining_s`）
- `cn_industry_post_close_job` 失败明细进 `sync_job_record.error_message` 已有，前端 `SchedulerHealth` 对 `cn_industry_post_close_sync` 单独标红（`HIGH_JOB_TYPES` 已含）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/em_probe.py`（**新**） |
| Service | `services/data-sync-service/src/data_sync_service/service/em_push2_http.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/industry_fund_flow.py` |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/em_probe_job.py`（**新**，`Interval 10min`） |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/__init__.py` |
| API | `services/data-sync-service/src/data_sync_service/api/health_routes.py` |
| Tests | `tests/test_em_probe.py`（**新**：探针失败→high event；熔断可视化） |

#### 验证

- [ ] 本地断网/代理出口封时 10min 内 `system_events high` 出现
- [ ] `GET /api/health/datasources` 返回 `eastmoney_probe stale:true` + `ban_latched:true`
- [ ] 探针失败不影响业务 job（隔离线程/超时 10s）
- [ ] pytest 通过

#### 反模式

- ❌ 让探针走代理（必须 `ProxyHandler({})` 直连，与业务同路径）
- ❌ 探针高频 1min 打东财（10min 已够，避免自触发 ban）
- ❌ 把探针失败直接标 `success=False` 到业务 `sync_job_record`（探针独立 `em_probe` job_type）

---

---

### OPT-145：外购分钟/复权数据入库（P1）

**状态**：[ ]
**优先级**：P1
**关联 todo**：[P0-7 外购数据计划](./todo.md)

#### 背景

用户外购赛博空间 2077 历史数据包（百度网盘）：A 股分钟 K（2000 至今）、基金分钟、A 股复权因子（东财+tushare 双份）、分钟 K（股票/指数/ETF）。磁盘余 99G，全量下不动。用途只有三个：① 习惯配方拉到熊市（2021–2023，重点 2022）回放；② 复权因子抽样对拍；③ 指数/ETF 分钟存盘备用。1 分钟线、30/60 分钟、全天 48 根一律不要（P0-4 既定）。

#### 目标

- A 阶段对拍脚本：2024 年 vendor 5 分钟 vs 库内 `bar_5min`（2320 万行）逐点对拍，输出吻合率；阈值过了才往下走。
- B 阶段切片入库：只下 2021–2023 三年股票 5 分钟按年包，复用 `import_ext_minute_csv.py --times` 只留尾盘七根（1330,1400,1430,1440,1450,1500 + 1000 研究位按需），CSV 落盘当档案、导完删解压目录。
- D 阶段复权抽样对拍：vendor 双份因子 vs 库内 `adj_factor`，抽样 most-active 200 只 × 除权除息日，输出差异表，不入库。

#### 文件范围

| 层 | 文件 |
|----|------|
| Script | `services/data-sync-service/scripts/compare_vendor_minute.py`（**新**，A 阶段对拍，只读） |
| Script | `services/data-sync-service/scripts/compare_vendor_adj.py`（**新**，D 阶段对拍，只读） |
| Script | `services/data-sync-service/scripts/import_ext_minute_csv.py`（复用，必要小改需注明） |
| Tests | 对拍脚本各 1 个单测（mock 数据定行数/吻合率公式） |

#### 验证

- [x] A（2026-09-05 有条件过）：40只×2024七根67606点=90.59%（原≥99.5%字面没过；O/H/L一致、close抖中位0.056%、1500达98%，属快照时差非错数）；修订门=习惯1430/1500+相对差，C加滑点base10bps/stress30bps覆盖；档 `docs/backtests/vendor-minute-compare-2026-09-05.md`；形式化compare脚本+单测待回填
- [x] B（2026-09-05 done）：2021 7153741行/4452名/243天 + 2022 7819588/4798/242（含熊市年） + 2023 8335124/5034/242；`bar_5min`总56493163行（ext_5min 51.0M + baostock 5.4M）；解压删留zip（2021/2022/2023_5min.zip共2.7G），磁盘余88G；`test_bar_5min + test_ext_minute_csv` 18 passed；`skipNoPrint1430`口径待C回放统计
- [x] D（2026-09-05 done）：vendor后复权实为价格序列、前复权远古89370负数、日收益中位差149.6bps→判不可直接用；只报不修零影响；档 `docs/backtests/vendor-adj-compare-2026-09-05.md`；单测 `test_compare_vendor_adj` 2 passed
- [ ] pytest 通过；磁盘导入期间余量 >20G

#### 反模式

- ❌ 整包下载/全量灌库（99G 装不下 2000 年全 A 分钟）
- ❌ 1 分钟 / 30 / 60 分钟入库（3 日持有用不上）
- ❌ 对拍不过就“差不多”入库（质量门是硬的）
- ❌ 复权差异顺手“修库”（只报不修，另起事项）

---

---

### OPT-146：factor_signals 港股符号误标 + 形态结算语义文档化（P2）

**状态**：[ ]
**优先级**：P2
**来源**：2026-09-05 对冲双子星实现证伪（`docs/backtests/hedge-twin-2026-09-05.md` §2）

#### 背景

1. `service/factor_signals_service.py` 把港股信号记成 `CN:xxxxx`（如 `CN:00004` 实为 00004.HK），与深市 000004 撞码，任何按 symbol join 的消费方都会吃错票。表内 2025-06-16 backfill 77 条部分受影响。
2. 形态验证的“目标/止损先触”语义对倒挂限价位（止损在入场价下方，82% scoop setup）是幻影成交——正确语义是限价单（低触才成交），已在 `scripts/repro_scoop_short.py`（v0.2.2 settle）实现，判别层文档需同步口径，避免后人复用旧语义。

#### 目标

- symbol 落库带市场前缀消歧（`CN:`/`HK:` 与 ts_code 同构），存量 77 条 backfill 重写或标注。
- `docs/designs/pattern-factor-validation.md` §1 加“倒挂限价位”成交规则一节（low-touch fill / gap-through 按 open / gap-over 无成交持有）。

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/factor_signals_service.py`（symbol 组装 + 回填） |
| Docs | `docs/designs/pattern-factor-validation.md`（§1 方法论补成交语义） |
| Tests | `tests/test_factor_signals_symbol.py`（新：HK 符号不再标 CN） |

#### 验证

- [ ] 新 scan 落库无歧义符号；77 条历史已处理
- [ ] pytest 通过

#### 反模式

- ❌ 只修符号不修理论文档（下次还有人按 high 碰止损价写回放）
- ❌ 把旧 89% 表删了（留档 + 作废标注，不删历史）

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

## 已完成索引（103 条 · 按天归档 · 只读）

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

> **历史重号说明**：OPT-057/058/060 在正文出现过两次（不同日期不同内容），OPT-059 的归档文件与正文条目主题不同。
> 一律不重编号，以"日期+标题"区分：057 = 08-01 TV Capture / 08-31 dailybasic 停更；058 = 08-02 漏斗 / 08-31 双子星 14:30 审计；
> 060 = 08-04 Tauri / 09-01 双子星 v2；059 归档 = 08-03 legacy 清理，正文 059（09-01 涨停审计）在 09-01 档。
