# Karios Desktop 优化 Checklist

> 工程执行栈（OPT-xxx）：架构 / 性能 / 兼容 / 工程债怎么做。编号 `OPT-001 ~ OPT-147`（重号见文末）。
> **正文 1 条未完成（OPT-147） + 3 条冬眠**；已完成 108 条按天归档 [`archive/`](archive/)（见文末索引表）。

---

## 如何使用

1. 未完成队列已清空（2026-09-06 H2 收官）；新 OPT 直接在"未完成"下开节，唤醒冬眠同理。
2. 每个任务开独立 Agent 会话，把对应章节整段粘贴给 Agent 作为 scope。
3. 完成后将 `[ ]` 改为 `[x]`，填写 **完成日期**，并按天归档：新档 `archive/YYYY-MM-DD-opt-NNN-slug.md`（仿 git 历史）+ 正文该节替换为索引行。
4. 若实施过程中方案有变，在本文件更新，不要另起文档。

### Agent 任务模板（见仓库根 AGENTS.md → Scoped optimization tasks；本文件只补一条）

> 开独立会话，把对应章节整段粘贴为 scope；只改列出的文件范围；更新状态；补测试；不扩 scope。

---

## 未完成（1 条 · OPT-147 open）

### OPT-147：R-wide breadth 池 HK 污染量级断言（P2 · 只读不断言不修引擎）

**状态**：[ ] open（2026-09-08 登记 · 来源 CPA/Amihud 两连撞宇宙门复查）

**背景**：`state_bucket_track._load_rows` 无 `sb.market` 过滤，`daily` 池含 ~2500 HK 行。
habit 引擎已验证两道 containment（`_day_features` mv 门排除 HK，`bar_5min` 0 HK symbol），
但 R-wide breadth（close>MA20 占比）在含 HK 池上计算，~1/3 权重为 HK。见
`first-principles-2026-09-05.md §二.7`。

**范围（只读，不改冻结引擎）**：
- `services/data-sync-service/scripts/` 新只读断言脚本（仿 `diag_amihud.py` 口径）
- 读 `services/data-sync-service/src/data_sync_service/service/state_bucket_track.py:113` `_day_features` breadth 段

**验收**：
1. 输出 OOS2/train/valid 每日 breadth（含 HK）vs 去 HK breadth 的差序列：翻转天数（跨 0.5 门限的天）、R-wide 开闸日差异数。
2. 若翻转天数为 0 → 关项，first-principles §二.7 补一行结论。
3. 若 >0 → 不修引擎，另起预注册评估对 clip4 三窗的影响（诊断→回放 discipline），本 OPT 只负责量级数字。
4. 加回归测试：`bar_5min` 0 HK 断言 + mv 门 HK 排除断言（防 pattern 复制退化）。

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

> **历史重号说明**：OPT-057/058/060 在正文出现过两次（不同日期不同内容），OPT-059 的归档文件与正文条目主题不同。
> 一律不重编号，以"日期+标题"区分：057 = 08-01 TV Capture / 08-31 dailybasic 停更；058 = 08-02 漏斗 / 08-31 双子星 14:30 审计；
> 060 = 08-04 Tauri / 09-01 双子星 v2；059 归档 = 08-03 legacy 清理，正文 059（09-01 涨停审计）在 09-01 档。
