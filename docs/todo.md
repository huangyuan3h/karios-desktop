# Karios · 路线图（todo）

> 产品级路线图，**按领域分章节 + 优先级标记**。完成时把条目标 `[done]`，详情迁到 [`docs/archive/`](./archive/)。
>
> **对应关系**：
> - 本文件管「**做什么、为什么**」（产品/战略层）。
> - 怎么做（架构/工程债）→ [`docs/optimization-checklist.md`](./optimization-checklist.md) 的 `OPT-xxx`。
> - 怎么投（交易规则）→ [`docs/trading-improvement-checklist.md`](./trading-improvement-checklist.md) 的 `TIP-xxx` / `V6.x`。

---

## 0. 我的优先级（用户口径，不可漂移）

| 序 | 维度 | 为什么 | 当前 todo 域 |
|----|------|--------|--------------|
| 1 | **收益** | 立命之本 | §2 交易策略、§8 回测 |
| 2 | **API 开放 / AI 打通** | 让外部 AI 助手能调我的数据 | §3 API 开放 |
| 3 | **工程架构 / 部署** | 长期可持续（含云，但 DB 大概率不上云） | §4 工程与部署 |
| 4 | **浏览器 / 数据源优化** | 上云的硬约束 | §5 数据源 |

> 任何新需求先对一下表：放错了域就回正。

---

## 1. 状态看板

> 每行"完成归档"列 = 已在代码 / doc 中沉淀的位置。新完成的 todo 摘要迁到 `docs/archive/` 后**同时**在这里补一行。

| 领域 | 在做（P0） | 待办（P1-P4） | 完成归档 |
|------|------------|---------------|----------|
| §1 定位与形态 | — | — | ✅ Tauri 降级 done 2026-08-04（OPT-060 / §12 #11）|
| §2 收益 / 交易 | TIP-011 开火归因 | — | TIP-001~009 + V6.2/3 已沉淀；hover tooltip + Dashboard 精简 done 2026-08-01（§15）；漏斗 N 日表格 done 2026-08-02（OPT-058）；**TIP-009 Alpha 映射自动 QA done 2026-08-04**（数据驱动 5 信号 · 用户零操作）；**TIP-011 开火来源归因 done 2026-08-04**（TV/Alpha/手动 胜率分桶 + Copy section）；**V7.0-02 风险平价开仓尺寸 done 2026-08-05**（0.5% 风险预算/止损距离 · FE+shared 零 BE 改动）；**TIP-012 研报→Alpha 通道 done 2026-08-05**（东财研报 API · 确定性评分 · 复用 TIP-004 闸门 · 每轮 cap 10 · 评分回写 + camelCase API 对齐 2026-08-06）；**TIP-013 Copy 新鲜度可见 done 2026-08-06**（`/api/health/datasources` · Copy All 头部 per-source 时间戳 + STALE 警告）；**TIP-014 Copy 强制刷新 done 2026-08-06**（forceFresh 绕过 react-query 缓存重拉行情/screener/评分）；**TIP-015 决策 Agent 闭环 M1 done 2026-08-06**（设计稿 `docs/designs/tip-015-decision-agent-loop.md` · 三层 context：活跃/窗口/10天归档 · 会话持久化 decision_sessions/messages/snapshots · SidebarNav「决策 Agent」页）|
| §3 API 开放 | — | API Key 配额 + 限流 | ✅ 已归档 → `archive/2026-08-01-opt-045-v1-api-surface.md`（OPT-045/046/047 整圈）|
| §4 工程与部署 | — | — | ✅ Tunnel 脚本骨架 OPT-048；DB 决策 OPT-053；Docker 一键 OPT-056；隐藏页 & legacy 清理 done 2026-08-03（OPT-059）；**DB 本地备份 + 跨机迁移包 done 2026-08-04（OPT-061 / §12 #18）** |
| §5 数据源 / 浏览器 | — | 付费 API 矩阵 | ✅ TV Scanner API 作为唯一池子（2026-08-01）；ego-lite/Chrome CDP 仅作 fallback；数据源审计 done 2026-08-01 |
| §6 新闻 / 研报 | — | News 质量评估（老婆反馈不如财经新闻准） | `OPT-037/038/039` News Query 并行化 |
| §7 多市场 | — | 美股 / 加拿大时区 | `OPT-041/042/043/044` HK + ETF 已通 |
| §8 回测 | — | BacktestPage 重写（等 paper 数据） | ✅ paper-trading v0 → [`archive/2026-08-01-opt-049-paper-trading.md`](./archive/2026-08-01-opt-049-paper-trading.md)；v0.1 关闭条件 done 2026-08-02（OPT-058）；历史 BacktestPage 已隐藏 |
| **升级方向 L3→L4** | L4-P1 券商研究 | L4 全项（见 §16） | **§16 已立 2026-08-07** → [`designs/l3-l4-evolution-roadmap.md`](./designs/l3-l4-evolution-roadmap.md)（L3=验证闭环，L4=执行闭环）；**L3 全部完成 2026-08-07**：P1 度量基座（OPT-062）、P2 回测引擎（OPT-063）、P3 归因（OPT-064）、P4 周度复盘（OPT-065）、**P5 组合风控 done**（OPT-067：相关性防火墙，tech_hk 34.2% 超限实拦）；AGENTS.md 加 DB 测试清理纪律 |
| **doc 大扫除** | — | — | `archive/modules-legacy/`（2026-08-01：industry-flow / market-sentiment / news-brief 3 旧版模块文档） |

---

## 2. 产品定位与形态

> 一边用一边改的现状不可持续，必须先把"长期形态"定下来。

- **[P0] Tauri 桌面 vs 固定 URL**：✅ **[done] 2026-08-04** → [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)（OPT-060）。决策：Web 形态为唯一交付形态；Tauri 保留 `src-tauri/` 源码 + `scripts/build-sidecars-macos.sh`，未来真要重启时 ≤ 0.5 天接入。
- **[P0] 形态迁移路线**：✅ **[done] 2026-08-04** → 同上（OPT-060 / §12 #11）。`pnpm dev` + Docker compose 为唯一活跃路径；dev 流程不再接入 Tauri。
- **[P1] 可分享 / 可订阅 URL**：每张关键页面（Watchlist、Dashboard、特定 symbol）有 stable URL，方便 AI 助手 query、复盘自包含。
- **[P1] 基础 AI 能力保留**：内置 Chat Panel + 摘要生成（不依赖外部 AI 调用本地数据时也能用）。
- **[P4] 完整产品定位文档**：把"卫星仓纪律化操作工具"等核心定位写成一页式宣言。

---

## 3. 收益 / 交易策略（最高优先级 · 优先级 1）

> 架构层与技术债的工程任务 → `OPT-xxx`；交易规则调整 → `TIP-xxx` / `V6.x`。本节只放**产品/战略层**的收益相关决策。

- **[P0] 数据源质量审计**：现有源"非常杂，质量不高"。评估是否替换/补强：
  - Tushare（200/年，国内）：保留作为基线
  - HK：akshare（Sina）已验证最稳（`OPT-043`），不要再切
  - 评估：聚宽 / Wind mini / Choice / iFinD / 自建爬虫 中是否有 ROI 为正的补强
- **[P0] 重启回测系统**：原 BacktestPage 效果差已隐藏，待重做 → 见 §8
- **[P0] 漏斗转化率度量闭环**：[done] 2026-08-02 —— `TIP-002` 埋点 + N 日表格已闭环（[`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)）；周报形态待 §3 P1 排期。
- **[P1] 研报/新闻 α 来源**：见 §6。
- **[P1] 卫星仓上限 / 仓位管理复核**：当前 15% 单票 + 30% 板块 + 袖子上限体系是否仍合理（参考 §13 `positionPct` 复杂度）。
- **[P2] 开火来源归因（TV/Alpha/手动）**：✅ **[done] 2026-08-04** → [`archive/2026-08-04-tip-011-execution-source.md`](./archive/2026-08-04-tip-011-execution-source.md)（TIP-011 · source 贯穿 write-path：前端 deriveActionCard 写 `source`（TV/ALPHA/MANUAL）→ diff_snapshots 透传 → `execution_decision_changes.source` + `paper_trades.source`；`GET /v1/execution/source-stats` 按来源出 BUY 信号量 + 平仓胜率；Copy markdown 新 section「Execution · Source attribution (30d)」；alembic 0018）。
- **[P2] Alpha 映射质量抽检**：✅ **[done] 2026-08-04** → [`archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md`](./archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md)（TIP-009 · 5 信号自动 QA · 用户零操作；theme_industry_map 从历史 alpha_radar_trends 数据驱动学，penalty 应用到 compute_alpha_additions，Copy markdown 末尾暴露给外部 AI agent）。
- **[P2] 风险平价开仓尺寸（V7.0-02）**：✅ **[done] 2026-08-05** → [`archive/2026-08-05-v7-02-risk-parity-sizing.md`](./archive/2026-08-05-v7-02-risk-parity-sizing.md)（Suggest% = min(5% clip, 0.5%风险预算/止损距离%, 单票/行业/Sleeve room)；实际止损位优先、2×ATR 兜底；<2.5% 放弃；绑定约束 note=`risk` + 面板显示止损距离；BE 零改动）。关联 §3 P1「卫星仓上限/仓位管理复核」。
- **[P2] 研报 → α 通道（TIP-012）**：✅ **[done] 2026-08-05** → [`archive/2026-08-05-tip-012-research-alpha-channel.md`](./archive/2026-08-05-tip-012-research-alpha-channel.md)（东财研报 API 免费源 · 确定性评分（评级×80+目标价×20 权重，14天半衰期）· 复用 TIP-004 闸门 score_min=70 · 每轮 cap 10 · registry source='research' 可归因 · alembic 0019 + 每2h job；关联 §7 P2「研报→α通道」与 §7 P1「研报源评估」的可行性验证）。

---

## 4. API 开放与外部 AI 打通（优先级 2）

> 让个人 AI 助手等外部系统能调用本系统的数据 / 信号。

- **[P0] OpenAI 兼容 API**（**这是 Karios 与外部 AI 助手的唯一桥梁**）：
  - 业务 endpoint（只读为主；写操作留给前端 + 后端当前 API，AI 助手不应改仓）：
    - `GET /v1/market/snapshot?symbols=...` → 实时价/分时/技术指标
    - `GET /v1/watchlist/items`
    - `GET /v1/decision-journal/query`
    - `POST /v1/explain/{symbol}` → 复用本地 Chat Panel 的 retrieval + LLM
  - **稳定发现性 endpoint**（AI 助手"一眼看到当前 API"）：
    - `GET /v1/version` → 版本号；major 跳变时 AI 助手主动告警
    - `GET /v1/schema` → OpenAPI 3.1 JSON（自动从 Zod 生成）
    - `GET /v1/errors` → 错误码字典（含 `recovery_hint` 给 LLM 修错用）
    - `GET /v1/changelog?since=X` → 接口变更 diff
  - 字段 `description` 必须写人话（不能"内部代号 / 待补充"），给 LLM 看的
  - 版本号规则：MAJOR 删字段 / MINOR 加 endpoint / PATCH 修描述（CI 校验）
  - 详细规范见 [`docs/designs/api-contract.md`](./designs/api-contract.md)
- **[P1] API Key 配额管理**：多 Key、scope（read-only / write-journal）、限流、审计日志。
- **[P1] 人类可读 API 文档**：`docs/api/` Markdown + FastAPI Swagger UI（**自动从 OpenAPI 生成，不手写**）
- **[P2] MCP server 暴露**：❌ **[cancelled] 2026-08-04** —— 用户确认 AI 助手为自写 Python/Node agent（`docs/integrations/ai-agent-cookbook.md §7` 风格），`/v1/*` + cookbook 已 100% 覆盖；MCP 主要价值（Claude Desktop / Cursor UI 内工具自动发现）不命中。MCP 是另一层抽象，对自写 agent 零增量价值。后续如需启用 Claude Desktop / Cursor 内置 AI 直调，按本节原描述单独起 OPT。
- ❌ **不在 Karios 范围**（明确分工）：
  - Telegram Bot / 推送 / 主动日报代理 → **外部 AI 助手做**，通过 `/v1/*` 拉数据
  - 自然语言决策代理 / 自动下单 → **外部 AI 助手做**
  - 监控 / 状态巡检 / 异常报警 → **外部 AI 助手做**
  - Karios 的 Chat Panel 仅服务于"看 + 问"的**局部交互**，不替代外部 AI 助手

---

## 5. 工程架构与部署（优先级 3）

> 大概率不上云（DB 贵），但**单 Docker 一键起 + 可内网穿透**是底线。

- **[P0] DB 走向决策**：写一份 `designs/db-direction-YYYY-MM.md`（拍板后迁到 module 或 archive 视情况），明确：
  - 主体仍然本地 Postgres（`OPT-032` 后 schema 已干净）
  - 云上只放 **只读副本** 给 API 用，本地仍是权威源
  - 或：放弃云，全部本地 + frp/zerotier 对外暴露
- **[P0] Docker compose 一键起**：已有 `docker-compose.yml`，要保证：
  - 包含 data-sync + desktop-ui 静态构建 + ai-service
  - 包含 Postgres（本地持久卷）
  - `pnpm dev` 仍可用作开发模式
- **[P1] 频繁改代码 vs 部署**：开发模式用热重载；发布模式用 Docker 镜像。**不**做自动 CI/CD（手工 build 已够用，省钱）。
- **[P1] 内网穿透/反向代理**：研究 Tailscale / Cloudflare Tunnel / FRP 中最适合"经常改代码"的方案。
- **[P2] Tauri 构建降级**：✅ **[done] 2026-08-04** → [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)（OPT-060）。`src-tauri/` 保留不动，但 `package.json` 活跃路径已无 Tauri scripts/deps，未来 desktop bug 不再修。
- **[P2] Alembic 迁移纪律**：见 `AGENTS.md`，所有 schema 改必须经过 Alembic（已建立 baseline）。
- **[P0] DB 本地备份自动化**（2026-08-02 审查新发现）：`OPT-053` 已拍板"备份 3 副本策略"，但仓库里**没有任何 backup 脚本 / cron**——"换电脑也能跑"痛点（§13）的数据侧还是空的。落地 pg_dump 日备份 + 本地/异地双副本 + 恢复演练（0.5-1 天）。
- **[P1] 隐藏页面与 legacy 清理**：✅ **[done] 2026-08-03** → [`archive/2026-08-03-opt-059-legacy-cleanup.md`](./archive/2026-08-03-opt-059-legacy-cleanup.md)（OPT-059）：`SimTradePage`（1017 行）+ `/simtrade` API、`BacktestPage`（664 行）+ `testback/` 旧回测框架全部删除（代码/路由/测试/Alembic 0017 删表）；§8 重启回测时不复用旧框架。

---

## 6. 数据源 / 浏览器替代（优先级 4）

> chrome 后台抓 TV 池子 → 不能上云 + 重。需要更轻量的方案。

- **[P1] ego-lite 调研**：✅ **done 2026-08-01** → [`designs/ego-lite-spike-2026-08.md`](./designs/ego-lite-spike-2026-08.md)（结论：用 TV Scanner API 替代 Chrome，无需浏览器；Chrome 作为 fallback 6 个月后 deprecate）
- **[P1] TV Capture 数据源决策**：A股 3 screener 用 Tushare Pro / 港美股用 TV Scanner API / 落地时机 → **待拍板**（A股已有 Tushare 200/年全覆盖；港股需 Tushare HK 另订或 TV API；需验证 Tushare 能否 1:1 复刻 3 个 screener 逻辑）
- **[P1] 付费 API 矩阵**：对比候选源 → 写 `archive/YYYY-MM-datasource-matrix.md`：
  - Tushare Pro / Tushare HK / 聚宽 / 通达信 L2 / iFinD / Wind mini / Choice
  - 维度：覆盖范围（CN/HK/US）、价格、限频、数据质量、稳定性
- **[P2] 自建爬虫兜底**：仅在上述都不可行时启动，最低优先级。
- **[P2] 资讯 RSS 源扩张**：参考现有 `OPT-038` 已并行化，源不够时再加；不要超过 20 个源（噪音 vs 收益边际递减）。

---

## 7. 新闻 / 研报

> 当前 News Brief 主要是 RSS + 摘要，研报是另一个量级的信息。

- **[P1] 研报源评估**：可用性 / 合规 / 价格 —— ✅ **东财研报中心 API 免费可用已实测**（TIP-012，2026-08-05）：单日 40-60 份个股研报、评级/目标价/EPS/行业全结构化、无鉴权；巨潮/慧博/Wind 无需再评估（除非需要深度研报全文）
- **[P2] 是否独立**：决定是否单独抽一个 `karios-research` 子项目，避免污染主仓的卫星仓交易逻辑。
- **[P2] 研报 → α 通道**：✅ **[done] 2026-08-05** → TIP-012（见 §3 完成记录）；评级/目标价 → 复用 Alpha Radar 旁路进 Watchlist，registry source='research' 可归因

### News Substrate 2.0（老婆反馈 #2 "没有财经新闻准"）

> 问题根源：RSS 源有 BBC/NYT 等通用新闻混入；无 tier 分级；无关联个股/板块提取。
>
> 三轨并行：
> - **Track 1**：RSS 源分级（Tier A/B/C/D）+ 投资级替换 → **done 2026-08-02**
> - **Track 2**：LLM enrichment（tickers / sectors / event_type / importance / relevance_score / ai_summary）→ 进行中
> - **Track 3**：Morning Brief（08:30 + 12:30 定时 top 5-7 条推送到 Dashboard）

**Track 1 · done 2026-08-02**：

| 内容 | 结果 |
|------|------|
| Tier A (6 sources) | 财联社·电报, 华尔街见闻·全球, 金十数据·快讯, 格隆汇·快讯, 财联社·深度, 财新网 |
| Tier B (5 sources) | 36氪·资讯, 华尔街见闻·美股, 第一财经, 虎嗅·财经, 36氪 |
| Tier C (3 sources) | 国家统计局, 证监会, 金十数据·数据 |
| Disabled (8 sources) | BBC, NYT, HN, Reddit + Playwright-only 6 sources (xueqiu/10jqka/eastmoney/stcn/36kr-flash/Reuters) |
| DB columns | `tier TEXT NOT NULL DEFAULT 'D'`, `category TEXT` on `news_sources` |
| Seed script | `scripts/seed_news_sources.py` (idempotent, `--dry-run` / `--legacy-disable`) |
| Tests | `tests/test_news_sources_seed.py` (6 tests: tier budget, legacy exclusion, tier-A required, duplicate check, RSSHub reachability) |

**Track 2 · done 2026-08-02**：

| 内容 | 结果 |
|------|------|
| DB columns | `tickers TEXT[]`, `sectors TEXT[]`, `event_type TEXT`, `importance SMALLINT`, `relevance_score SMALLINT`, `ai_summary TEXT`, `enrichment_status TEXT`, `enriched_at TIMESTAMPTZ`, `enrichment_model TEXT` on `news_items` |
| Alembic | `0014_news_items_enrichment.py` |
| DB helpers | `fetch_pending_enrichment()`, `update_item_enrichment()`, `count_by_enrichment_status()` |
| LLM worker | `service/news_enrich.py` — batch 10 items, JSON schema, watchlist-aware scoring (+30 watchlist, +50 held), 0–5 importance |
| Scheduler | `scheduler/news_enrich_job.py` (every 2h, after news_fetch_job) |
| API | `GET /api/news/enrichment/status`, `POST /api/news/enrichment/run` |
| Frontend | `NewsPage` shows enrichment status bar + per-item event_type/importance/relevance/tickers/aiSummary badges |
| Shared | `SCHEDULER_JOB_CATALOG` + `SYNC_JOB_TYPES` updated with `news_enrich_job` |
| Tests | 1220 backend + 391 frontend + 55 shared all green (1 pre-existing flaky test excluded) |

**Track 3 · done 2026-08-02**：

| 内容 | 结果 |
|------|------|
| DB table | `morning_briefs` (id, brief_date, brief_type, items JSONB, macro_overview, model_version, source_item_ids, created_at) |
| Alembic | `0015_morning_briefs.py` |
| Selection | `service/morning_brief.py` — score = importance × 0.4 + relevance × 0.4 + freshness × 0.2; top 7 items; only enriched items |
| Cron | `scheduler/morning_brief_job.py` — AM 08:30 + PM 12:30 Asia/Shanghai weekdays |
| API | `GET /api/news/brief/latest`, `GET /api/news/brief/recent`, `POST /api/news/brief/generate` |
| Shared | `SCHEDULER_JOB_CATALOG` + `SYNC_JOB_TYPES` updated with `morning_brief_am` / `morning_brief_pm` |
| Tests | `tests/test_morning_brief.py` (9 tests: freshness bonus, scoring, filtering, sorting, max items) |

---

## 8. 回测（重启）

> 原 BacktestPage 效果差已隐藏。重启前先定假设，否则又是一次无效投入。

- **[P0] 重启前置条件**：
  1. 必须与 live 的 Execution Gate 同口径（不要重写一份规则）
  2. 必须能拉历史 bars（HK ≥5y 已通过 `OPT-043`；CN 5y+ 已有 `daily` 表）
  3. 纸面交易（paper-trading）先于纯回测——回测容易过拟合，paper 不会
- **[P1] Paper-trading daily 跑**：把当前 BUY/ADD 信号在收盘后假买入，跟踪 N 日后的实际表现
- **[P1] Paper-trading v0.1 关闭条件补齐**：[done] 2026-08-02 —— +10% `target_hit` / score 跌穿 `score_floor` / 离开 watchlist `pool_exit` 已落地（[`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)）；胜率 / 持有天数统计口径已完整。
- **[P1] 单策略回测能力**：保留但**不作为发布决策依据**，只为理解参数敏感度
- **[P2] BacktestPage 重写**：产品形态（不写到本 todo 的 P0，因为要先有数字）

---

## 9. 多市场 / 远景（最低优先级）

> 加拿大生活规划会推高美股优先级，但当下数据不足决策。

- **[P3] 美股 symbol 闸门**：参考 HK 闸门（`OPT-041`）的 8 处 `symbol→ts_code` 改造范式
- **[P3] 时区 / 交易日适配**：美股 vs CN vs HK 时区不同，scheduler (`scheduler/*.job.py`) 需要按市场分别触发
- **[P3] 美股数据源**：yfinance 已被 rate-limit 实测（`OPT-043` 注）；评估 Polygon / Alpaca / IEX Cloud / Tiingo
- **[P4] 加拿大税务/账户模型**：完全 P4，先保持基础 symbol 闸门就够

---

## 相关执行清单（不在本文件更新范围内）

| 清单 | 命名 | 用途 | 状态 |
|------|------|------|------|
| 架构优化 | `OPT-001` ~ `OPT-044+` | 工程实现债 / 性能 / 兼容 | 滚动维护 |
| 交易改进 | `TIP-001` ~ `TIP-011` | 业务规则校准 | 大部分完成 |
| 交易中枢 | `V6.2-*` / `V6.3-*` | Execution Gate 子规则 | 完成 |

> **规则**：新任务先在 `todo.md` 起条；明确属于工程债 → 转 OPT-xxx；属于交易规则 → 转 TIP-xxx。todo 本体不写实现细节。

---

## 数据源 / 系统现状速览（备忘）

- **CN 行情**：Tushare（200/年）为主基线，akshare 多源兜底；港股走 tushare.hk_basic + akshare(stock_hk_daily) + yfinance(^HSI 指数)
- **ETF**：fund_basic 已同步（2102+），fund_daily 已启用，`OPT-042` 完成
- **新闻**：RSS + AI 摘要（`OPT-038` 并行化）
- **指数**：index_daily + index_dailybasic（`OPT-033` 批量读 + `OPT-034` 去重）
- **回测**：BacktestPage 隐藏，效果差；paper-trading 缺位
- **执行**：Execution Gate + Action Card + Decision Journal 三件套已上线（V6.x）

> 本节只用来"对账"，变更频繁请改 `optimization-checklist.md` / `trading-improvement-checklist.md`，本文件保持指向。

---

## 维护规则

1. **新增条目** 用 `[Px] {一句话动词 + 名词}`，必要时给 2-3 行补充。
2. **完成判定**：标 `[done] YYYY-MM-DD`，同时把摘要迁到 `archive/YYYY-MM-{slug}.md`，todo 上保留一行完成链接。
3. **优先级漂移**：如果某条用户口径变了，**先动 §0，再动该节**，避免局部拧。
4. **每 30 天回顾一次 §1 状态看板**，把长期 P0 但未动的项目显式降级或归档。

---

## 10. 已沉淀到 archive/

> "沉淀"指"重大判断 + 不希望被忘记"的事件，不是所有实现细节。一个事件通常一条独立归档（见 `archive/README.md`）。

| 日期 | 事件 | 归档位置 |
|------|------|----------|
| 2026-08-08 | **L4-Gate 全清（H1~H10 + K1/K4）**：4 个 live bug 根因修复（intake key 错位 / camelCase×2 / journal 校验）、测试隔离纪律化（233 假账户+141 假 session 清理、db_rows_baseline 27 表验收）、fail-open 清单（修 2 激进项）、时区/数值健壮性、API 契约对照（删前端 okBook 死字段）、调度幂等（ingest heartbeat 测试锁定）、安全扫描（本地 CSRF Origin 守卫 11 测试） | [`archive/2026-08-08-l4-gate-audit.md`](./archive/2026-08-08-l4-gate-audit.md)（后端 1435 passed + 前端 515 passed + tsc 干净；L4 准入 Gate 6/7 项达标，剩归档动作已完成——§17 全部勾选） |
| 2026-08-07 | **L3-P5 / OPT-067**：组合相关性防火墙（V7.0-01 转正）——9 个语义因子簇（ETF 前缀 + 东财行业 + HK 科技清单）+ 20 日经验相关性（日历对齐 fail-open）；簇 >30% 拦簇内新开仓（CORRELATION_CAP_BLOCK）+ Suggest% roomCorrelation min 链；回测页「组合相关性防火墙」面板；实测 tech_hk 34.2%（腾讯+恒生科技 ETF）超限实拦，00700×513180 r=0.926 | [`archive/2026-08-07-opt-067-correlation-firewall.md`](./archive/2026-08-07-opt-067-correlation-firewall.md)（1388 后端 + 500 前端全绿；**L3 五里程碑全部完成**） |
| 2026-08-07 | **OPT-066**：journal 上游 symbol 防御层——`is_valid_watchlist_symbol`（CN/HK/ETF 格式校验）+ diff/ingest 双层过滤（坏卡 `rejectedCards` 可观测）+ 前端提交前过滤；坏 symbol 永远进不了决策日志 | [`archive/2026-08-07-opt-066-journal-symbol-defense.md`](./archive/2026-08-07-opt-066-journal-symbol-defense.md)（1379 后端 + 495 前端全绿） |
| 2026-08-07 | **L3-P4 / OPT-065**：周度决策质量复盘——决策量 / paper 净口径 / 卖出归因 / 漏斗健康度 → 中文 markdown 报告；决策 Agent「分析」tab 新增周报卡（复制喂 AI agent）；首次实测：本周 38 条信号 97% 来自 ALPHA（自动提示供给单一化） | [`archive/2026-08-07-opt-065-weekly-review.md`](./archive/2026-08-07-opt-065-weekly-review.md)（1376 后端 + 494 前端全绿） |
| 2026-08-07 | **L3-P3 / OPT-064**：卖出归因（前向收益分桶 by close_reason + 组合暴露）+ 回测页（SidebarNav「回测」）；**期间修复 2 个 live bug**：(1) intake 读 journal 的 key 错位 → paper 自上线从未有真实数据；(2) service 层 snake_case 读 db camelCase → run_update 永不更新（修复后首笔真实闭环 CN:600000 pool_exit）；测试基建加 teardown 防 DB 污染 | [`archive/2026-08-07-opt-064-exit-attribution-backtest-page.md`](./archive/2026-08-07-opt-064-exit-attribution-backtest-page.md)（1370 后端 + 494 前端全绿；已知问题：journal 上游 hash symbol 待修） |
| 2026-08-07 | **L3-P2 / OPT-063**：回测引擎 v0——信号回放（watchlist_score_daily 历史实际分）+ `_pick_close_reason` 同码复用（as-of score 注入防前视）；36 组敏感度网格（score×hold×stop）+ CLI/API；实测近 7 周全组合净期望为负（敏感度价值；不作发布依据） | [`archive/2026-08-07-opt-063-backtest-engine.md`](./archive/2026-08-07-opt-063-backtest-engine.md)（1365 后端全绿；v0.2：TV 池回撤窗口 / 月度滚动 / BacktestPage） |
| 2026-08-07 | **L3-P1 / OPT-062**：Paper v0.2——HK 接入 + 分市场成本模型（CN 30bps / HK 60bps 往返）；pnl_pct 重定义为净口径，stop/target 按净值触发；`/v1/paper-trades` 加 market 过滤 + stats byMarket；决策 Agent 页分市场展示；db 层切 dict_row 退役位置索引 hack；Alembic 0022（legacy 回填 CN/0） | [`archive/2026-08-07-opt-062-paper-v02.md`](./archive/2026-08-07-opt-062-paper-v02.md)（1352 后端 + 494 前端全绿；汇率/ETF 记入 L3-P3） |
| 2026-08-03 | **OPT-059 / §12 #19**：隐藏页 / legacy 清理——SimTradePage + `/simtrade` API、BacktestPage + `/backtest/*`、`testback/` 框架整体退役删除；Alembic `0017_drop_backtest_tables` 删表（2+132 行旧数据）；baseline/测试/文档同步 | [`archive/2026-08-03-opt-059-legacy-cleanup.md`](./archive/2026-08-03-opt-059-legacy-cleanup.md)（1247 后端 + 429 前端测试绿；唯一失败为既有 trendok flaky，stash 验证与本次无关）|
| 2026-08-04 | **OPT-061 / §12 #18**：DB 本地备份 + 跨机迁移包——`db_backup.sh`（pg_dump -Fc + iCloud mirror + 25h last-age 跳过）+ `db_restore.sh`（docker cp + pg_restore --jobs=4 + alembic + manifest cross-check）+ `karios_migrate_export.sh`（tarball bundle）+ `install-db-backup-launchd.sh`（plist 03:00 + RunAtLoad + Wake + DATABASE_URL env）；设计稿 `designs/db-backup-and-migrate-2026-08.md` 解决"电脑休眠 → 唤醒后 launchd 不补跑错过的 job"问题（3 trigger 叠加 + last-age 检查兜底）| [`archive/2026-08-04-opt-061-db-backup-migrate.md`](./archive/2026-08-04-opt-061-db-backup-migrate.md)（端到端 2 次演练：round-trip drop+restore 21s + 新 Mac 模拟全新容器 restore 44 表 + 00700.HK 2026-08-04 487.6 数据完整）|
| 2026-08-04 | **OPT-060 / §12 #11**：形态迁移 · Tauri 降级——根 + apps/desktop-ui 的 tauri scripts/deps/concurrently 全删；`src-tauri/` Rust 源码 + `scripts/build-sidecars-macos.sh` 按 §2 P0 "保留 build 配置" 不动；顶层 docs（README / AGENTS / docs/README / docker-one-click / next.config / Dockerfile）同步；6 新单测全绿 | [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)（决策真值：Web = 唯一交付形态；Tauri 复活需 ≤ 0.5 天接入）|
| 2026-08-01 | doc 大扫除：3 个旧模块文档迁移至 `archive/modules-legacy/`（与 V6.x 规则脱节） | `archive/modules-legacy/README.md` |
| 2026-08-01 | OPT-045 Phase A：4 个稳定发现性 endpoint + API Key 鉴权 + 17 单测全绿 | 见 `optimization-checklist.md` OPT-045 |
| 2026-08-01 | OPT-046：3 个只读业务 endpoint（/v1/market/snapshot + /v1/watchlist/items + /v1/decision-journal/query）+ 18 单测全绿 | 见 `optimization-checklist.md` OPT-046 |
| 2026-08-01 | OPT-047：/v1/explain/{symbol} + docs/api/ 6 份人类可读 + scripts/bump-api-version.sh + 14 单测 | 见 `optimization-checklist.md` OPT-047 |
| 2026-08-01 | **OPT-045 整圈归档**（OPT-045/046/047 合并视角）：/v1/* 端到端 8 个 endpoint 落地 + 6 份人类可读文档 + 49 v1/* 单测 | [`archive/2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md) |
| 2026-08-01 | OPT-048 脚本骨架：Tunnel 一行起 + 生产模式 + setup 文档 + 12 单测 | 见 `optimization-checklist.md` OPT-048 |
| 2026-08-01 | OPT-049：paper_trades 表 + 2 cron + 2 /v1 endpoint + 19 单测；Alembic 0011 | [`archive/2026-08-01-opt-049-paper-trading.md`](./archive/2026-08-01-opt-049-paper-trading.md) |
| 2026-08-01 | OPT-050：数据源审计（5 候选对比 + 决策 = 续 Tushare 不引 Wind）+ healthcheck 脚本 | [`archive/2026-08-01-opt-050-data-source-audit.md`](./archive/2026-08-01-opt-050-data-source-audit.md) |
| 2026-08-01 | OPT-051 / §12 #5：API Key 多 Key + 三窗口滑动配额 + /v1/quota + Swagger/Redoc + docs/api/openapi.md | [`archive/2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md) |
| 2026-08-01 | OPT-052 / §12 #6：Alpha Radar 扩展 HK 标的识别（hk_mapping prompt + resolve_hk_mapping + trend_json.hkSymbols + aggregate 合并 + watchlist HK 跳过 EM industry 闸门）| [`archive/2026-08-01-opt-052-alpha-radar-hk.md`](./archive/2026-08-01-opt-052-alpha-radar-hk.md) |
| 2026-08-01 | OPT-053 / §12 #10：DB 走向决策（5 选项对比 + 备份 3 副本 + 6 触发条件 + 半年期复审）| [`archive/2026-08-01-opt-053-db-direction.md`](./archive/2026-08-01-opt-053-db-direction.md)（决策真值在 `designs/db-direction-2026-08.md`）|
| 2026-08-01 | §12 #8 ego-lite spike：Chrome capture 替代方案调研（TV Scanner API 发现 + spike 验证）| [`designs/ego-lite-spike-2026-08.md`](./designs/ego-lite-spike-2026-08.md) |
| 2026-07-27 | V6.3 极端资金流豁免 `INTRADAY_OVERFLOW_OVERRIDE` + Alpha S TrendOK recovering | 见 `trading-improvement-checklist.md` V6.3 节 |
| 2026-07-24 | V6.2 14:30 尾盘时间锁 + 防守双轨袖子 + Zero-Pos 归零清场 | 见 `trading-improvement-checklist.md` V6.2 节 |
| 2026-07-22 | 漏斗转化率 / Pullback 主宇宙校准 / Alpha 进池闸 / Alpha GC 对称化 | 见 `trading-improvement-checklist.md` TIP-001~006 |
| 2026-07-29 | HK + ETF 闸门全打通（OPT-041~044） | 见 `optimization-checklist.md` |
| 2026-08-01 | OPT-056 / §12 #7：Docker 一键起 + UPS 自动恢复（3 Dockerfile + 4 compose service + 6 脚本 + setup doc + 57 tests）| [`archive/2026-08-01-opt-056-docker-one-click.md`](./archive/2026-08-01-opt-056-docker-one-click.md)（脚本骨架完整，端到端实跑需用户跑 `scripts/docker-up.sh --migrate`）|
| 2026-08-02 | **OPT-058 / §12 #20+#21**：漏斗 N 日表格（TIP-002 收尾：`GET /watchlist/automation/runs` + FunnelHistoryTable）+ Paper-trading v0.1 关闭条件（target_hit / score_floor / pool_exit，fail-open 纪律）| [`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)（后端 50 相关测试 + 前端 5 新测试全绿）|
| 2026-08-04 | **TIP-009 / §3 P2**：Alpha 映射质量抽检 + 错映射惩罚（数据驱动 · 用户零操作版本）——5 信号自动 QA（行业不匹配 / 历史胜率低 / 名称歧义 / 板块资金流背离 / 个股资金流背离）；theme→industry 映射从历史 alpha_radar_trends 自动聚类（90d 数据 → 11 主题 / 季度跑脚本更新）；penalty 应用到 `compute_alpha_additions` 的 catalystScore；Dashboard Copy markdown 末尾新 2 section（⚠ Mapping warnings + Theme historical win-rate）喂外部 AI agent 决策；新增 `GET /api/alpha-radar/auto-qa-stats` | [`archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md`](./archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md)（1274 后端 + 440 前端全绿；用户日常仍是 Sync + Copy，0 增量操作）|
| 2026-08-04 | **TIP-011 / §2 P2**：开火来源归因（TV/Alpha/手动）——`source` 贯穿 write-path：前端 `deriveActionCard` 按 TV screener 快照 + Alpha catalyst 集合写 `source`（closed enum TV/ALPHA/MANUAL）→ `diff_snapshots` 透传到 `execution_decision_changes.source` → paper_trades intake 镜像到 `paper_trades.source`；新增 `GET /v1/execution/source-stats`（按来源出 BUY 信号量 + 平仓胜率 + 持仓数）；Copy markdown 新 section「Execution · Source attribution (30d)」；alembic 0018 + 全量 1295 后端 + 456 前端测试全绿 | [`archive/2026-08-04-tip-011-execution-source.md`](./archive/2026-08-04-tip-011-execution-source.md) |
| 2026-08-01 | OPT-057 / §12 #8.5：TV Capture 三轨架构（Scanner API + ego-lite + Chrome fallback）+ 新建 screener 模板化 UI + 5 模板 live API 验证通过 + capture 流程端到端走通 | [`archive/2026-08-01-opt-057-tv-capture-three-track.md`](./archive/2026-08-01-opt-057-tv-capture-three-track.md)（47 新单测 + 1055 全绿；Scanner API filter 必须用数组格式 `[{left,op,right}]`；HK `exchange=HKEX`，US `exchange∈[NASDAQ,NYSE,AMEX]`；**最终决策**：TV Scanner API 池子基本够用，ego-lite/Chrome CDP 仅作 fallback）|

---

## 11. 注意力预算（自用）

> 散点信息太多时容易自乱。这节规定每天 / 每周的"读哪里 / 改哪里"。

| 周期 | 必读 | 可选 |
|------|------|------|
| 每天开工前 | 本 todo §1 状态看板 + **§12 当前 # 编号** | — |
| 每天开工前 | `modules/watchlist.md` Execution Gate 节（确认 live 闸与纸面一致） | `modules/screener.md` 若今天改了 screener |
| 每周一次（周末） | §0 优先级表 → 是不是要漂移 | §10 沉淀表 → 是不是有重大事件该归档 |
| **每周一** | **§12 这周要打的 # 编号** → 在 freelancer-arch / cloud-deployment / data-source-audit 找上下文 | — |
| 改动 schema / 新依赖前 | `AGENTS.md` + `optimization-checklist.md`（OPT-xxx 进行中列表） | — |
| 修改交易规则前 | `trading-improvement-checklist.md` 最新一条 → 沿革 | — |
| 想做 idea 但排不上 P0 | 起一份草稿到 `designs/`（不要污染 todo） | — |

**反模式**：

- ❌ 把 §1 看板改满 ✓ 之后没有任何 archive 落地 —— todo 不能"假装完成"。
- ❌ 没有拍板就长期留在 todo P0；要么降级要么归档。
- ❌ 新建散点 markdown 文档（"会议纪要" / "杂记"）—— docs/ 只允许本 todo + 真值模块 + 设计草稿 + 归档。

**每日 / 每周加 1 条**：
- 跑 `bash services/data-sync-service/scripts/data-source-healthcheck.sh` → 失败立即处理（不囤）

---

## 12. 实施清单（按 ROI 排序 · 凑时间一个个实现）

> **本节是 todo 唯一**"**做哪个先**"的可执行序列。各领域的 P0/P1 仍在 §2-§9 维护；§12 只**按 ROI 重排**后给一份跨领域工作流。
>
> **关联设计稿**：
> - 上云 / Tunnel / 形态决策 → [`docs/designs/cloud-deployment-options.md`](./designs/cloud-deployment-options.md)
> - 自由人架构 / AI 日报代理 / 移动端职责 → [`docs/designs/freelancer-architecture.md`](./designs/freelancer-architecture.md)

### 排好的清单

| # | 动作 | 域 | 预估工时 | 依赖 | 预期收益 |
|---|------|----|----------|------|----------|
| 1 | **OpenAI 兼容 `/v1/*` + AI 助手可发现性** | §3 API | 4-5 天 | BE schema 已有 | ✅ **done 2026-08-01** → 摘要 [`archive/2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md)；OPT-045/046/047 整圈闭合，49 v1/* 单测全绿 |
| 2 | **Cloudflare Tunnel 部署** | §4 工程 | 0.5 天 | 域名已在 Route53 | ✅ **done 2026-08-01（脚本骨架）** → `scripts/start-quick-tunnel.sh` + `setup-named-tunnel.sh` + `docs/designs/cloudflare-tunnel-setup.md`；真实端到端验证 pending（需用户装 cloudflared，brew install cloudflared） |
| 3 | **paper-trading daily 启动** | §8 回测 | 2-3 天 | bars 数据已全 | ✅ **done 2026-08-01（v0 CN only）** → [`archive/2026-08-01-opt-049-paper-trading.md`](./archive/2026-08-01-opt-049-paper-trading.md)；paper_trades 表 + 2 cron + 2 /v1 endpoint + 19 单测全绿 |
| 4 | **数据源质量审计**（出决策文档） | §3 收益 + §6 数据源 | 1 天 | — | ✅ **done 2026-08-01** → [`archive/2026-08-01-opt-050-data-source-audit.md`](./archive/2026-08-01-opt-050-data-source-audit.md)；续 Tushare 200/年；不引 Wind/Choice/iFinD/聚宽；healthcheck 脚本就绪 |
| 5 | **API Key 配额 + 人类可读 OpenAPI 文档** | §3 API | 1-2 天 | #1 完成 | ✅ **done 2026-08-01** → [`archive/2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md)；多 Key + label:secret:rpm:rph:rpd 格式（向后兼容旧）；三窗口滑动配额 in-mem；/v1/quota 自查；FastAPI metadata + openapi_tags + Swagger UI + ReDoc；23+11 单测全绿 |
| 6 | **Alpha Radar 扩展 HK 标的识别**（原 "HK Alpha S 自动归类"，已改名更精确）| §3 收益 | 2-3 天（实际 1）| `OPT-044` 已通 | ✅ **done 2026-08-01** → [`archive/2026-08-01-opt-052-alpha-radar-hk.md`](./archive/2026-08-01-opt-052-alpha-radar-hk.md)；ai-service hk_mapping prompt 字段 + python resolve_hk_mapping + trend_json.hkSymbols + aggregate_catalyst_stocks 合并 CN+HK + compute_alpha_additions 跳过 HK EM industry 闸门；13+1 单测全绿 |
| 7 | **Docker 一键起 + UPS 自动恢复** | §4 工程 + §13 longevity | 1-2 天 | docker-compose 已有 | ✅ **done 2026-08-01** → [`archive/2026-08-01-opt-056-docker-one-click.md`](./archive/2026-08-01-opt-056-docker-one-click.md)；3 Dockerfile（data-sync / ai-service / desktop-ui+nginx）+ 4 compose service（migrate / data-sync / ai-service / desktop-ui）+ 6 脚本（docker-up/down/status + install-launchd/uninstall-launchd/ups-shutdown）+ 1 setup doc + 57 smoke tests 全绿；旧用户首次实跑必加 `--migrate`；UPS hook 由 nut/apcupsd 外挂触发（macOS 无原生电池 API）|
| 8 | **ego-lite 调研结论** | §6 数据源 | 0.5 天（实际 spike） | — | ✅ **done 2026-08-01** → [`designs/ego-lite-spike-2026-08.md`](./designs/ego-lite-spike-2026-08.md)；结论：用 TV Scanner API（`scanner.tradingview.com/global/scan`）替代 Chrome capture，无需浏览器/Playwright/login，30+ 字段，CN stocks 支持；详见 spike 文档 |
| 8.5 | **TV Capture 数据源决策**：Scanner API 作为唯一池子，ego-lite/Chrome CDP 降级为 fallback | §3 + §6 | 3-4 天 | #1 完成 | ✅ **done 2026-08-01** → [`archive/2026-08-01-opt-057-tv-capture-three-track.md`](./archive/2026-08-01-opt-057-tv-capture-three-track.md)；Alembic `0012_tv_screeners_api_mode.py`；5 模板 live API 验证通过 + capture 流程端到端走通；关键发现：Scanner API filter 必须用**数组格式** `[{left,op,right}]`；HK `exchange=HKEX`，US `exchange∈[NASDAQ,NYSE,AMEX]`；47 新单测 + 1055 全绿；`docs/modules/screener.md` 已更新为三轨架构；**最终决策**：TV Scanner API 池子基本够用，ego-lite/Chrome CDP 仅作 fallback，不作为主要数据源 |
| 9 | **付费 API 矩阵评估** | §6 数据源 | 1-2 天 | — | 影响未来上云选型 |
| 10 | **DB 走向决策文档** | §4 工程 | 0.5 天 | — | ✅ **done 2026-08-01** → [`designs/db-direction-2026-08.md`](./designs/db-direction-2026-08.md)（不进 archive——是未拍板后的真值）；5 选项横向对比 + 备份 cron 策略 + 6 触发条件（半年期复审）+ 已知风险；`freelancer-arch.md` + `cloud-deployment-options.md` 链到本文档 |
| 11 | **形态迁移（Tauri 降级）** | §2 定位 | 1 天 | — | 长期减少维护面 | ✅ **done 2026-08-04** → [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)（OPT-060）；根 + apps/desktop-ui 的 tauri scripts/deps/concurrently 全删；`src-tauri/` + sidecar build 脚本按 "保留 build 配置" 不动；6 新单测全绿 |
| 12 | **BacktestPage 重写**（基于 paper 数据） | §8 回测 | 3-5 天 | paper-trading 有 N 日数据 | 仅作参数敏感度工具，不作发布依据 |
| 13 | **MCP server 暴露** | §3 API | 1-2 天 | #1 完成 | Cursor / Claude Desktop 直接调（另一种标准化形式） | ❌ **cancelled 2026-08-04** —— AI 助手为自写 agent，`/v1/*` + cookbook 已够；§3 P2 同条标 cancelled |
| 14 | **美股 symbol 闸门** | §7 多市场 | 3-5 天 | 加拿大规划启动 | 远期触发 |
| 15 | **加拿大税务/账户模型** | §7 多市场 | 远期 | — | 远景 |
| 16 | **Watchlist table hover tooltip** | §2 收益 | 0.5 天 | — | ✅ **done 2026-08-01** → `lib/watchlist-column-help.tsx` + `ColumnHeader`（§15 反馈 #1）|
| 17 | **Dashboard 精简 + 参数说明** | §2 收益 | 1 天 | — | ✅ **done 2026-08-01** → `lib/dashboard-card-help.tsx` + `DashboardHeader`（§15 反馈 #3）|
| 18 | **DB 本地备份自动化** | §4 工程 | 0.5-1 天 | OPT-053 决策已立 | ✅ **done 2026-08-04** → [`archive/2026-08-04-opt-061-db-backup-migrate.md`](./archive/2026-08-04-opt-061-db-backup-migrate.md)（OPT-061 · §13 Longevity "换电脑也能跑" 数据侧补完 · 用户"电脑就休眠"约束 · 3 脚本 + launchd plist + tarball migrate + 端到端 2 次演练）；与 §12 #7 Docker 一键起互补 |
| 19 | **隐藏页面 / legacy 清理** | §4 工程 | 0.5-1 天（实际 2h） | — | ✅ **done 2026-08-03** → [`archive/2026-08-03-opt-059-legacy-cleanup.md`](./archive/2026-08-03-opt-059-legacy-cleanup.md)（OPT-059）；SimTradePage + /simtrade、BacktestPage + testback/ 全删（含 Alembic 0017 删表）；1247 后端 + 429 前端测试绿，唯一失败为既有 trendok flaky |
| 20 | **漏斗 N 日转化率表格** | §3 收益 | 0.5 天 | TIP-002 埋点已就绪 | ✅ **done 2026-08-02** → [`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)；`GET /watchlist/automation/runs` + FunnelHistoryTable 挂 WatchlistPage |
| 21 | **Paper-trading v0.1 关闭条件** | §8 回测 | 1 天 | OPT-049 | ✅ **done 2026-08-02** → 同上；target_hit / score_floor / pool_exit 补齐，fail-open 纪律 |

### 怎么"凑时间一个个实现"

**周节奏建议**（按你"自由人 + 功能不重合"目标倒推）：

- **本周先打 #1**（OpenAI 兼容 endpoint）：哪怕只完成 `GET /v1/market/snapshot` 一个 endpoint，**就让你的 AI 助手能独立做事**（写日报 / 推 Telegram / 监控异常），从此不必在 Karios 内做推送
- **下周打 #2**（Tunnel）：让 AI 助手能远程调 `/v1/`，这是"出门"前提
- **再下周打 #3**（paper-trading）：2-3 天集中投入，跑一周就有数据
- **月底复盘**：用 paper 数据 + 真实日记，写 §12 #4 的数据源审计文档

**何时跳过 / 降级**：

- 如果**没买 Mac mini / UPS** → 把 #7 暂缓，先做 #1 + #2 + #3（不依赖硬件的）
- 如果**加拿大时间表后移** → #14/#15 自动降级
- 如果**数据源不想换** → #8 #9 可以合并成一个调研，跳过付费 API 评估

**实现动作模板**（每个 # 都照这个来）：

```text
1. 在 docs/optimization-checklist.md（或 trading-improvement-checklist.md）起一条 OPT-xxx / TIP-xxx
2. AGENTS.md 已规定：完成 → 勾选 + 写测试
3. 完成 → todo §10 加一行 + docs/archive/ 起摘要
4. §12 这行末尾加 ✅ + 完成日期
```

### 反模式

- ❌ **跳着做**：先做 #5 再做 #1 → 外部 AI 没有数据源接口，配额和文档没意义
- ❌ **贪多**：一周打 5 个 → 每个都半成品，半年后什么都没真正可用
- ❌ **先做 #11 #12**：Tauri 降级 + BacktestPage 是"做完才有用"的任务，没有 paper 数据 #12 没意义
- ❌ **被数据源卡住**：tushare 现在 200/年够用，先别花 1 天做 #4，做 #1 #2 立刻出杠杆
- ❌ **在 Karios 内做 Telegram Bot / 推送**：违反"功能不重合"原则，这部分归 AI 助手
- ❌ **API 字段 description 写"待补充"或内部代号**：AI 助手无法判断含义，违反 `api-contract.md` 约束
- ❌ **业务 endpoint 改路径名而不 bump MAJOR**：破坏 AI 助手缓存的 schema
- ❌ **在外部仓库（AI 助手那边）手写 Karios API 文档**：永远从 `/v1/schema` 自动生成

**每日 / 每周加 1 条**：
- 跑 `bash services/data-sync-service/scripts/data-source-healthcheck.sh` → 失败立即处理（不囤）

---

## 13. Longevity · 系统长期生命力（用户 2026-08-01 真痛点）

> **用户原话**："我关心的无非换电脑也能正常跑这个系统，让这个系统长期有生命力，远程也能访问这几个痛点。"
>
> **决策真值**：[`docs/designs/karios-longevity-2026-08.md`](./designs/karios-longevity-2026-08.md)（2026-08-01 立）
>
> **未来方案**：[`docs/designs/mac-mini-deployment.md`](./designs/mac-mini-deployment.md)（2026-08-01 立 · 用户拿到 Mac mini 那天的整体部署方案）

| 痛点 | 现状 | §13 行动项 |
|------|------|-----------|
| **换电脑也能跑** | 当前 1-3 天 | ✅ §12 #7 Docker 一键起 **done 2026-08-01**（OPT-056）→ ~2 小时（含首次 build）；脚本骨架 + 82 tests + setup doc 就绪 |
| **换电脑也能恢复数据** | 1.7 GB DB 无法迁移 | ✅ §12 #18 DB 本地备份 + 跨机迁移包 **done 2026-08-04**（OPT-061）→ 新 Mac 5 分钟恢复（详见 [`designs/db-backup-and-migrate-2026-08.md`](./designs/db-backup-and-migrate-2026-08.md)）；iCloud 异地副本兜底 Mac 整机丢失场景 |
| **数据独立于 Mac** | 本地 PG 单点 | §13 #1 Neon 只读副本 + 定时 sync（1 天）🟡 暂缓（OPT-061 iCloud 已部分覆盖）|
| **远程访问兜底** | 仅 Cloudflare Tunnel | §13 #2 Tailscale Funnel fallback（0.5 天）🟡 暂缓 |
| **Mac 长期关机 fallback** | 不支持 | §13 #3 临时 VM Hetzner €4/月按月开（0.5 天）🟡 暂缓 |

### §13.1 Mac mini 时代整体部署（用户 2026-08-01 review）

> **用户原话**（review §13 时）："我倾向于系统整体稳定部署 docker，然后自动启动，db 用现在的，不用两个，保证系统长期开机，可能是未来很久以后还 mac mini 长期开机部署的方案，对这个项目真的能养活我之后的事情吧。"

**核心决策（2026-08-01）**：

| 维度 | 决策 | 理由 |
|------|------|------|
| **部署形态** | Docker compose | 自动启动 + UPS 保护 + 多端访问 |
| **数据源** | **本地 Postgres（brew services）单一数据源** | "db 用现在的" + 不切换数据所有权 |
| **自动启动** | macOS LaunchAgent → `docker compose up -d` | 开机即用，断电恢复不干预 |
| **触发时机** | 用户拿到 Mac mini（或同等 7×24 设备）那天 | 不是现在 |

**不**要做的事（避免重复建设）：
- ❌ 不在 docker compose 里跑自己的 postgres service（跟本地 PG 抢 5432）
- ❌ 不复制数据到第二个 PG（违反"单一数据源"）
- ❌ 不现在就 full migration（用户没 Mac mini）

**完整方案 + 时序 + 反例**：[`docs/designs/mac-mini-deployment.md`](./designs/mac-mini-deployment.md)（§3 架构 / §4 与现状差异 / §5 实施时序 / §6 日常维护 / §7 触发条件 / §8 失败模式）

> **核心约束**：**Mac 永远在线 / 你永远想维护** = 信仰，不是契约。Longevity 的核心是**不依赖单一维护者 + 单机**。详见 longevity 文档。

---

## 14. AI Agent 打通 + TV 数据源决策（用户 2026-08-01 优先级修正）

> **用户原话**（review §13 时）："远程部署的部分先优先级下降一些，我们先保证这个系统和 我个人的 ai agent 打通以及 chrome 替代方案吧，暂时云还有一段路。"
>
> **结果**：§13 远程部署项（Neon 副本 / Tailscale / 临时 VM）**暂缓**；§12 #8 ego-lite 调研**升级为立即做**；新增 §14 AI agent 打通 cookbook。
>
> **最终决策**（2026-08-01）：TV Scanner API 作为唯一池子基本够用，ego-lite/Chrome CDP 仅作 fallback。Screener 模板化 + Pullback 过滤（-5% ~ -15%）已足够。

### §14 行动项

| # | 动作 | 工时 | 状态 |
|---|------|------|------|
| **§14 #1** | **AI agent 集成 cookbook**（启动 4 步 + 4 场景 + 错误处理 + 配额监控 + Python/Node client）| 1-2 天 | ✅ **done 2026-08-01** → [`integrations/ai-agent-cookbook.md`](./integrations/ai-agent-cookbook.md)（10 节 + Python/Node client）|
| §14 #2 | `/v1/*` 持续稳定保证（含 rate limit retry cookbook）| 1 天 | 等 §14 #1 跑通 |
| §14 #3 | 决策/告警 webhook（AI agent 订阅 Karios 事件）| 1-2 天 | 设计稿 |
| §12 #8 | **ego-lite 调研 spike**（TV 数据不依赖 Chrome）| 0.5 天（实际） | ✅ done 2026-08-01 → `designs/ego-lite-spike-2026-08.md`：TV Scanner API 替代 Chrome |
| §12 #8.5 | TV Capture 三轨架构（Scanner API + ego-lite + Chrome fallback）| 3-4 天 | ✅ done 2026-08-01 → `archive/2026-08-01-opt-057-tv-capture-three-track.md`；**最终决策**：TV Scanner API 池子基本够用，ego-lite/Chrome CDP 仅作 fallback |

### §13 远程部署暂缓（用户 review 后）

| 项 | 状态 | 暂缓触发 |
|----|------|----------|
| §13 #1 Neon 只读副本 | 🟡 暂缓 | 用户："暂时云还有一段路" |
| §13 #2 Tailscale Funnel | 🟡 暂缓 | 同上 |
| §13 #3 临时 VM fallback | 🟡 暂缓 | 同上 |
| §12 #7 Docker 一键起 | 🟢 仍建议 | **换电脑 + 长期生命力痛点不依赖云**——可独立做 |

> **保留项**：§12 #7 Docker 一键起——痛点 1（换电脑）**不依赖云**，仍可独立推进。

---

## 15. 老婆使用 watchlist 的反馈（2026-08-01 已收到 · 不污染 P0）

> **状态**：已收到具体反馈，需落实
> **来源**：老婆亲自使用 Karios 后给的具体建议
> **行动**：反馈汇总后落 `docs/modules/watchlist.md` 末尾"用户使用笔记"段；衍生需求列为 §3 / §12 的 P1 子条目

### 已收到反馈

| # | 反馈 | 影响范围 | 优先级 |
|---|------|----------|--------|
| 1 | Watchlist table header 参数看不懂，hover 上去能明白每一个参数干什么的 | WatchlistPage table columns | P1 |
| 2 | 新闻模块特别是 dashboard 这里的部分没有她财经新闻准 | Dashboard news + modules/news.md | P2 |
| 3 | Dashboard 里面有些内容重复，参数看不懂不知道干什么 | DashboardPage cards | P1 |

### 衍生需求（待落实）

- [x] Watchlist table columns 加 hover tooltip（P1 · 2026-08-01 完成 → `lib/watchlist-column-help.tsx` + `ColumnHeader`）
- [x] Dashboard 精简重复内容 + 参数说明（P1 · 2026-08-01 完成 → `lib/dashboard-card-help.tsx` + `DashboardHeader`；Last sync table → 单行；Index rule 块 → hover）
- [x] News 模块质量评估（是否需要替换/补强）（P2）→ ✅ **done 2026-08-02** → News Substrate 2.0 全三轨完成（Track 1: 13 investment-grade sources；Track 2: LLM enrichment；Track 3: Morning Brief cron + API）。详见 §7 下方。
- [ ] 反馈落到 `docs/modules/watchlist.md` 末尾"用户使用笔记"段
- [ ] 衍生需求（P1）列入 todo §3 或 §12

---

## 16. 升级方向：L3 → L4（2026-08-07 立 · 系统演进真值）

> **决策**：当前系统评估为 **L2.5（纪律化决策完成态，缺验证闭环）**。以 **L3（验证闭环）** 为当前目标，**L4（执行闭环）** 为长期愿景。
> **真值文档**：[`designs/l3-l4-evolution-roadmap.md`](./designs/l3-l4-evolution-roadmap.md)（分级定义 / 完成判定 / 里程碑 / 红线）。
> **本节的条**：拍板前在 §16 起条；落地时转 OPT-xxx（工程）或 TIP-xxx（规则），完成后按 §10 归档。

### 分级（简版）

| 级别 | 定义 |
|------|------|
| L2 | 纪律化决策（信号 + 闸门 + 仓位 + 日志）—— 已达成 |
| **L3（目标）** | **验证闭环**：回测 / paper / 成本滑点建模 / 归因复盘 / 参数敏感度，信号价值可度量 |
| L4（远期） | **执行闭环**：券商 API + 半自动下单 + 回执对账 + 组合级实时风控（人始终在环） |

### L3 里程碑（当前目标 · 预计 8-12 周）

| # | 里程碑 | 内容 | 依赖 | 状态 |
|---|--------|------|------|------|
| **L3-P1** | 度量基座 | paper v0.2：HK 接入 + 滑点/佣金/印花税建模 + 成交假设统一 | §8 paper v0.1 已有 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-062-paper-v02.md`](./archive/2026-08-07-opt-062-paper-v02.md)（OPT-062：CN+HK 净口径成本模型 + byMarket 统计 + decision 分析分市场；T+1 由盘后 cron 节奏天然满足；FX 汇率/涨跌停/停牌/ETF 记入 L3-P3 精化） |
| **L3-P2** | 回测引擎 | 与 live Execution Gate 同口径回测（同一份规则代码）+ ≥5y 历史 + 参数敏感度视图 | L3-P1 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-063-backtest-engine.md`](./archive/2026-08-07-opt-063-backtest-engine.md)（OPT-063：信号回放 + `_pick_close_reason` 同码复用 + 净成本；36 组网格 CLI/API；**实测 2026-06-18 起全组合净期望为负——为阈值再校准提供依据**；v0.2：TV 池回撤窗口 / 月度滚动 / BacktestPage UI） |
| **L3-P3** | 归因与敏感度 | 卖出归因分桶（卖早/卖晚/卖对）；参数敏感性报告；卫星仓上限复核（15%/30%/sleeve） | L3-P2 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-064-exit-attribution-backtest-page.md`](./archive/2026-08-07-opt-064-exit-attribution-backtest-page.md)（OPT-064：卖出归因分桶 + 组合暴露 + **回测页（用户可见位置）**；过程中修复 2 个 live bug：intake journal key 错位（paper 从未有真实数据）、service/db camelCase 错位（run_update 永不更新）；journal 上游 hash symbol **已修** OPT-066 双层防御） |
| **L3-P4** | 决策 Agent M2 | 周度复盘：喂 paper 实绩 + 归因 + 漏斗数据，输出「本周决策质量报告」 | L3-P1/P3 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-065-weekly-review.md`](./archive/2026-08-07-opt-065-weekly-review.md)（OPT-065 v0：数据驱动周报 + 决策 Agent「分析」tab 展示；M2 v1：LLM 深度解读 / 自动推送归外部 agent） |
| **L3-P5** | 组合风控 | V7.0-01 相关性热力网转正落地（Correlation Cap + 共振预警） | L3-P2 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-067-correlation-firewall.md`](./archive/2026-08-07-opt-067-correlation-firewall.md)（OPT-067：9 语义簇 + 日历对齐相关性 + >30% 拦新开仓 + roomCorrelation min 链；实测 tech_hk 34.2% 超限实拦） |

### L4 里程碑（长期愿景 · 6-12 个月 +）

| # | 里程碑 | 内容 | 依赖 | 状态 |
|---|--------|------|------|------|
| **L4-P1** | 券商研究 | 券商 API 矩阵（可用性/合规/费率/沙箱），拍板试点 | — | [ ] 未拍板不动代码 |
| **L4-P2** | 半自动下单 | 人工确认 → broker API → 回执入库；幂等重试；先小额实盘 | L4-P1 | [ ] |
| **L4-P3** | 组合风控实时化 | 相关性 cap + 共振熔断 + 盘中风险预算（L3-P5 盘中化） | L3-P5 | [ ] |
| **L4-P4** | 自动对账 | 券商持仓 ↔ 本地 Watchlist 每日对账 + 异常告警 | L4-P2 | [ ] |
| **L4-P5** | 多市场执行 | US/CA 数据 + 时区调度 + 执行 | §7 P3 数据源先行 | [ ] |

### 红线（不可漂移）

1. **人始终在环**——L4 自动下单必须人工确认，不做无人值守
2. **先验证后执行**——L3 验证闭环是 L4 执行闭环的前置，不允许跳过
3. **同口径是铁律**——回测 / paper / live 共享同一份规则代码
4. **卫星仓定位不变**——核心仓在系统外；信号再强也不等于全家 all-in
5. 每个里程碑交付可勾选的「证据」（测试/报告/归档链接），todo 不假装完成

---

## 17. L4 准入 Gate：全模块排查与加固（2026-08-07 立）

> **决策**：进入 L4（券商对接/执行闭环）之前，全模块过一遍，消灭 P0/P1 级「影响判断和逻辑」的问题。
> **背景**：2026-08-07 完成 L3-P1~P5 过程中暴露 3 个 live bug + 1 个数据污染事件，证明「测试全绿 ≠ 逻辑正确」（mock shape 与 db 不一致掩盖了数周）。必须系统性排查。
> **详细计划**：[`archive/2026-08-08-l4-gate-audit.md`](./archive/2026-08-08-l4-gate-audit.md)（9 项横切检查 + A/B/C 模块分级 + P0/P1/P2 加固项 + 退出标准）。

### Gate 状态

| 项 | 状态 |
|----|------|
| K1：`decision.py` 读 camelCase 错位（paper 进不了决策快照） | **[x] 2026-08-08**：4 处错位 + exit_attribution 1 处漏网 + 附带发现 `import json` 缺失（extract_pending_actions 从未工作） |
| H1 数据口径审计 | **[x] 2026-08-08**：全量对照表完成（详见 archive/l4-gate-audit §4）；decision.py 覆盖率 43% → 99% |
| H3 测试隔离复查（26 个 requires_postgres 文件） | **[x] 2026-08-08**：7 处污染源修复（含 flaky 根因：UTC/上海跨天窗口）；清 233 测试账户 + 141 测试 session + 48 假 changes；`scripts/db_rows_baseline.py` 验收 OK |
| K4 correlation 簇回归 | **[x] 2026-08-08**：持仓全保护；补 8 条簇规则（电子/元件/PCB/小金属/化学制药等）；correlation.py 57%→95%；遗留：stock_basic CN=0 致行业缺失（B7）+ fail-open 激进语义（入 H5 清单） |
| H2 盘后链路端到端冒烟 | **[x] 2026-08-08**：`test_postclose_smoke.py` 五步链路全绿；**抓到生产 bug**：run_intake side 变量泄漏（最后一条 action 污染所有 insert，解释 paper_trades 长期 1 行）已修 + 回归测试；基线验收零变化 |
| H4 前端决策链边界矩阵 | **[x] 2026-08-08**：13 个边界用例（满仓/负 room/ETF 豁免/correlation 绑定/ratchet）；前端 515 passed + tsc 干净 |
| H5 fail-open 语义清单 | **[x] 2026-08-08**：扫描 11 文件 75 处 except；**修 2 个激进项**（宏观死锁读取失败→fail-closed 锁激活且不缓存；registry 读取失败→不再批量删止损）；2 个高危项记录设计权衡（日内风控 bar 陈旧、breadth panic 依赖 sentiment 兜底）；其余 14 项中低危记录 |
| H6 时区/日历一致性 | **[x] 2026-08-08**：调度全 Asia/Shanghai ✓；**修 `_messages_on` UTC→上海日界**（凌晨消息漏出快照）+ `_holding_days_for` None 崩溃；跨周末/跨月测试；HK 日历差异文档化 |
| H7 数值健壮性扫描 | **[x] 2026-08-08**：评分函数 None 守卫 ×7（返回 0 不崩）+ 测试锁定；扫描全服务层 float/int/除零路径无其他崩溃 |
| H8/H9/H10 契约/调度/安全 | **[x] 2026-08-08 H8**：v1 三端点模型全匹配 + docs/api ✓；删前端 `okBook` 死字段（tsc 干净）。**[x] 2026-08-08 H9**：26 模块 ON CONFLICT + ingest heartbeat 幂等测试锁定；无幂等缺陷。**[x] 2026-08-08 H10**：gitignore/硬编码密钥 0 命中；.env.example 补 `GEMINI_API_KEY`；/v1 鉴权面确认（business/explain 挂 require_api_key、quota 挂 enforce_quota、discovery 无鉴权设计）；新增 `LocalOriginGuardMiddleware` 拒非本机 Origin 写请求（11 测试锁定）；全量 1435 passed 零变化。 |

### 剩余风险处置（2026-08-08 Gate 后 · 用户拍板 ①→②→③ 全做）

| 项 | 状态 |
|----|------|
| ① B7：stock_basic/EM 行业缺口 | **[x] 2026-08-08**：根因 3 层——① missing 正则把 ETF 当 CN 股票（15 开头）→ 增量 sync 永远 0 resolved、表卡死 1630 行；② push2 主域名死亡无 fallback；③ 空批次记 success=True 假绿。修复：market 字段过滤（CN 总数 7389→5543 修正）+ fallback 链 `push2→push2delay→emweb F10`（EM2016 二级）+ 空批次记 failure；**回填 5543/5543=100%**，16 个缺失 symbol 全恢复；baseline 脚本补 2 张表；全量 1440 passed 零变化 |
| ② 数据源健康告警 | **[x] 2026-08-08**：`services/data-sync-service/scripts/data_healthcheck.py` 6 项检查（28 job 健康/daily 新鲜/TV 快照/EM 覆盖/分数新鲜/备份年龄）+ `scripts/install-healthcheck-launchd.sh`（每日 08:30 + 登录 + FAIL 桌面通知）；**上线即抓 4 真实问题**：adj_factor 连续 4 周五失败=17:00 与 daily_sync 并发抢 tushare 配额→**错峰 18:30**；stock_daily_full 6-13 起失败=已重定向 close_sync 降噪处理；etf/news 入观察 |
| ③ 阈值再校准实验 | **[x] 2026-08-08**：18 组合聚焦网格（score 60/70/80 × hold 5/10 × stop -6/-8/-10，2026-06-18~08-07）**全组合仍负**；相对最优 score 70+stop -6（-0.76%，72 笔，win 38.9%）；hold 5/10 无差异；**结论：不调 live 参数**（-0.76% 仍负期望，调参只挖浅 1%）；收益归因在信号供给单一化（97% ALPHA）→ 立为 L4 后业务课题 |
| **观察清单**（healthcheck 暴露，非紧急） | **[w]** `etf_daily_full`：8-01（每月 1 日 19:00 回填）限流失败，9-01 复查；`news_enrich_job`：8-07 20:59 一次失败（enriched 24/49 部分成功），前面多次成功——每天 2 次跑，若复发深挖 |

### 覆盖率波 1（§8 计划 · 目标 BE ≥75% + 11 核心模块 ≥85%）

| 项 | 状态 |
|----|------|
| `scripts/coverage_gate.py` | **[x] 2026-08-08**：读 coverage.json，按核心模块清单（11 个 ≥85%）+ 整体阈值（--overall）fail；豁免清单显式（tv/capture.py 退役路径）；输出 模块×覆盖率×缺口 可直接定位 |
| 11 核心模块 ≥85% | **[x] 2026-08-08**：decision 99.5 / correlation 94.9 / exit_attr 91.5 / weekly_review 90 / trendok 87.3 / research 88.6 / execution_journal 92.6 / execution_source 84.9+ / paper_trading 87.5 / backtest_engine 77.2→85 / watchlist_automation 70.5→85 全达标（补 ~40 测试） |
| **顺带修复 3 个真 bug** | **[x]** ① `get_top_5d_industry_names` 未 strip 行业名（GC 精确匹配失效）→ 加 strip；② `backtest_engine` end_of_window 平仓后未 del positions → `open_at_end` 虚高（迭代中 del 修复 + list() 快照）；③ `_with_retry` 全失败 raise 语义确认（原测试预期错误） |
| 整体覆盖率 | **[x] 2026-08-08：75.0% GATE PASSED**（基线 65.9%，三会话累计 +9.1pp / ~290 新测试，1759 全绿）——第 3 会话覆盖：trendok（alpha-S recovering/risk buy blocks/quote merge/industry flow）、alpha_radar_process（extract/save/batch/pending）、tv_chrome（profile copy/start/stop 全路径）、macro_daily（paging/合约解析/full sync 驱动）、broker（decode/ai-extract/images/条件单删除）、etf_fund_flow 剩余（universe/frame merge/classify）、macro_snapshot_on_demand（metrics/on-demand 系列/enrich） |
| 验收 | **[x] 2026-08-08**：**波 1 整体验收**：后端 1759 passed / 2 skipped 全绿 + 27 张表零变化 + `coverage_gate.py` GATE PASSED（OVERALL 75.0%，11 核心模块最低 87.9% 全 ≥85%，tv/capture.py EXEMPT） |

### 覆盖率波 2（波 1 后续 · 继续推整体覆盖率）

| 项 | 状态 |
|----|------|
| 会话 4（75.0% → 79.7%） | **[x] 2026-08-08**：1978 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 79.7%，-1023 missed）——top_inst_flow 68→95.1%（网络层/retry/tushare/provider 编排 53 测试）、query_routes 46.1→98%（resolve/quotes/全部小 endpoint 27）、option_iv 68.5→97%（sync driver/akshare fallback/paging 27）、db/index_daily 46→88%、db/news 49→97%、db/tv 38→99%、db/index_basic 14→93%、news_enrich 58→88%（_call_llm/enrich_batch/cycle 22）、db/macro_daily 58→91%、db/watchlist_automation 50→96%、mainline 70→98%（metrics 计算层 13） |
| 会话 5（79.7% → 83.2%） | **[x] 2026-08-08**：2229 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 83.2%，-1038 missed）——etf_daily 20→99、db/broker 40→97、alpha_radar_mapping 38→100、market_detail 56→96、close_sync 56→97（trade calendar 全分支/分页/断点续传）、adj_factor 24→100、db/stock_eastmoney_industry 50→97、em_push2_http 61→100、sina_http 46→97、fund_basic 51→99、service/daily 45→96、db/stoploss 34→100、db/research 43→94、db/stock_basic 66→97（fetch_market_stocks 分页+quote 聚合） |
| 会话 6（83.2% → 83.9%） | **[x] 2026-08-08**：2295 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 83.9%，-155 missed）——watchlist_routes 64→90（registry CRUD/backfill/automation 全 endpoint 21）、alpha_radar_routes 51→98（sources/trends/sync/process/remap/risk 21）、sync_routes 67→98（全部 sync 端点 + /sync/jobs 聚合含降级分支 24） |
| 会话 7（83.9% → 84.6%） | **[x] 2026-08-08**：2494 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 84.6%，-141 missed）——trendok 92→99（`_trendok_one` 全分支：exit_now/momentum exhaustion/ETF fallback/sector divergence/T1 sniper/intraday distribution/RS leader/Alpha-S recovering + compute 集成含 registry 持仓/宏观死锁 73）、market_regime 84→96（指数信号全信号分支/realtime merge/HK on-demand/breadth/liquidity/缓存 42）、market_sentiment 89→96（panic 规则/capitulation/FTD/breadth 分页+intraday 并发/risk_mode 全分支/sync 日期解析 84） |
| 会话 8（84.6% → 86.1%） | **[x] 2026-08-08**：2656 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 86.1%，-337 missed）——service/tv 81→98（screener CRUD/template/sqlite 迁移/capture 三轨 dispatch 全分支/job 队列 wait 全状态 72）、db/trade_review 43→99（全 CRUD + fetch 序列 cursor mock 19）、service/morning_brief 37→94（freshness 分档/watchlist boost 分级/分类规则/select 过滤链/brief 生成 25）、api/news_routes 37→100（17 endpoint 全绿）、service/watchlist_momentum_alerts 69→91（breakout/exit/hold 三态 + tranche 阶梯 16）、db/top_inst 62→96（daily/summary upsert + description 列映射查询 13） |
| 会话 9（86.1% → 87.5%） | **[x] 2026-08-08**：2870 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 87.5%，-307 missed）——service/etf_fund_flow 87→99（sync 全流程/skip 分支/spot 估算/tushare 历史 merge/分页回退 52）、service/macro_daily 85→99（分页抓取 380 天/HS 三源 fallback/SGX/INE 解析器/sync 恢复矩阵 36）、service/macro_snapshot_on_demand 81→100（yf/akshare/tushare 三源链全分支/df→metrics 全转换 41）、service/alpha_radar_qa 81→100（五类 penalty 信号/catalyst 批量/stats 聚合/名称歧义 18）、service/dashboard 87→98（summary 全开关矩阵/盘前 clamp/screener 状态机/三 sync 流程 39）、service/industry_fund_flow 78→98（东财数据源/日线 kline/akshare 回退/SW L1 分类过滤/交易日跳过逻辑 28） |
| 会话 10（87.5% → 89.3%） | **[x] 2026-08-08**：3034 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 89.3%，-381 missed）——service/macro_snapshot 72→100（realtime overlay 全分支/PUT IV 卡/backfill pct/警告矩阵 22）、service/alpha_radar_pipeline 76→100（cooldown/rounds 计算/ingest/process/sync 全状态机 26）、service/alpha_radar_symbol_resolve 72→100（CN/HK 双前缀解析/三 lookup 链/hybrid fallback 13）、service/eastmoney_industry 79→100（push2/push2delay/emweb 三源链/增量 sync 全分支 29）、service/hk_daily 66→100（tencent→ak→yf→tushare 四源链/darwin 禁用/full sync 恢复 22）、service/hk_basic 60→100（月度 skip/映射器/全错误路径 11）、api/broker_routes 54→100（账户 CRUD/快照/图片/import/sync/条件单 14）、api/backtest_routes 53→100（run/sensitivity/report/exit-attribution/weekly-review/correlation 13）、api/system_prompts_routes 52→100（9 endpoint 全分支 + HTTPException 透传 8） |
| 会话 11（89.3% → 90.4%） | **[x] 2026-08-08**：3125 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 90.4%，-2101 missed）——api/journal_routes 52→100（CRUD 全分支/404 矩阵）、api/execution_journal_routes 54→100（快照校验矩阵/列表/变化/journal.md 默认日期）、api/industry_flow_routes 44→100（fund-flow/mainline 4 端点成功+错误+默认值）、service/index_daily 40→100（skip/resume/无 key/全流程/空 df/未来日期跳过/失败记录）、service/index_basic 32→100（同上结构）、service/stock_basic 68→100（skip/无 key 记录/空 df/success/异常/状态查询）、db/trade_calendar 57→99（upsert 全转换/查询矩阵/summary）、service/trade_calendar 67→97（分页/空页/默认日期）、service/market_bars 71→92（ETF 分支/ts_code 后缀校验/_lookup_name 三态/force 四源 sync 链） |
| 会话 12（90.4% → 91.8%） | **[x] 2026-08-08**：3222 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 91.8%，-1797 missed）——scheduler 全目录 26→94%（create_scheduler 注册矩阵 31 job id + trigger 类型/简单 cron job 三分支参数化 ×9/close_sync 3 分支+post/close_catchup 5 分支记录矩阵/daily_sync 4 分支/watchlist_automation 4 分支/decision 三 job/paper_trading 双 job/morning_brief 双触发器 5 分支/news_fetch-enrich 各 3 分支/research_report 3 分支/alpha_radar 三 job+env 解析）；**修复产品缺陷**：create_scheduler 原只注册 tv_screener_capture_am，PM 快照任务从未调度，补注册 JOB_ID_PM |
| 会话 13（FE：queries 层测试） | **[x] 2026-08-08**：desktop-ui 608 tests 全绿（63 文件，1 skipped），All files 69.4%→74.6%，**lib/queries 43.96%→79.92%**、lib/api 69.38%→92.51%——backtest 0→100（run/sensitivity/exit-attribution/correlation 路径+clusterExposure 4 分支）、broker 0→100（keys/fetch/options/hooks/invalidate）、execution-journal 0→100（默认上海日期/refetch 60s/120s/limit）、weekly-review 0→100（enabled/staleTime）、tvCapture 0→100（轮询/失败抛错/超时 fake timers）、decision 6.77→92（fetch 系列/create/update/rename/append/delete/markdown 渲染 6 分支）、sentiment 30→100、alphaRadar 20→92（4 fetch+4 options+hooks+3 mutation+invalidate）、news 23→85、research 24→100；**技术要点**：mock useQuery 捕获 options 直接调 hook（无 jsdom 免渲染）、tvCapture fake timers 先 attach assertion 防 unhandled rejection；note：apps/desktop-ui eslint 坏为预先存在（eslint-plugin-import 解析失败，既有文件同样失败） |
| 剩余大块 | db/industry_fund_flow（67%）、hk_*（hk_daily 66%/hk_basic 60%/hk_daily_tx 81%）、db/paper_trading（80%）、db/decision（84%）、market_bars（71%）、tv/scanner_api（86%）、db/morning_brief（23%）、db/industry_mainline_metrics（22%）、db/journal（17%）、service/etf_fund_flow（87%）、service/macro_daily（85%）、service/macro_snapshot_on_demand（81%）、service/alpha_radar_qa（81%）、service/dashboard（87%）、service/industry_fund_flow（78%）、service/macro_snapshot（72%）、service/alpha_radar_pipeline（76%）、api/broker_routes（54%）、service/alpha_radar_symbol_resolve（72%）、service/eastmoney_industry（79%）、api/backtest_routes（53%）、api/system_prompts_routes（52%） |

### 铁律

1. **P0 未清之前不碰 L4-P1**（券商研究）
2. 每个 H 项一个会话，不混 scope（OPT-068+ 落地）
3. 每个修复 = 测试 + 验收证据（勾选本文件）
4. 全量测试跑完 dev DB 表行数不变 = 测试隔离合格
