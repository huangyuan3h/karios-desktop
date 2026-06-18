# Karios Desktop 优化 Checklist

> 记录架构审查结论与优化方案，供后续逐个 Agent 任务执行。  
> **第六轮审查**：2026-06-18  
> **背景**：OPT-001 ~ OPT-030 已全部完成；本轮聚焦 **Sentiment Sync 全市场扫股**、**ensure_table 热路径**、**指数信号 batch/dedup**、**遗留 sync 退役**、**前端 Query 全覆盖收尾**。

---

## 如何使用

1. 按 **优先级（P0 → P1）** 顺序执行。
2. 每个任务开独立 Agent 会话，把对应章节整段粘贴给 Agent 作为 scope。
3. 完成后将 `[ ]` 改为 `[x]`，填写 **完成日期** 和 **PR/Commit**。
4. 若实施过程中方案有变，在本文件更新，不要另起文档。

### Agent 任务模板

```text
请实现 docs/optimization-checklist.md 中的 OPT-XXX。
要求：
- 只改 checklist 列出的文件范围
- 完成后更新 checklist 状态
- 补充/更新相关测试
- 不要扩大 scope
```

---

## 优先级总览

| ID | 标题 | 优先级 | 预估工时 | 状态 |
|----|------|--------|----------|------|
| OPT-031 | Market Sentiment Sync 全市场 intraday breadth 降载 | P0 | 1–2 天 | [x] |
| OPT-032 | ensure_table() 热路径一次性 guard | P0 | 0.5–1 天 | [x] |
| OPT-033 | market_regime 指数 K 线批量读 | P0 | 1 天 | [x] |
| OPT-034 | Index ↔ Dashboard 指数信号跨端去重 | P0 | 1–2 天 | [x] |
| OPT-035 | 废弃 sync_daily_full 逐股串行兜底 | P0 | 1–2 天 | [x] |
| OPT-036 | Screener 快照 2N 请求与 import 串行 | P1 | 1–2 天 | [x] |
| OPT-037 | Dashboard 阶段三拆分（Sentiment/News 子 Query） | P1 | 2–3 天 | [x] |
| OPT-038 | News / Alpha Radar RSS ingest 并行化 | P1 | 1 天 | [x] |
| OPT-039 | AlphaIncubator / NewsPage React Query 迁移 | P1 | 1–2 天 | [x] |
| OPT-040 | ChatPanel 残留 trendok 与 Export fallback 统一 Query | P1 | 0.5–1 天 | [x] |

---

## P0 — 最高收益

### OPT-031：Market Sentiment Sync 全市场 intraday breadth 降载

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

> **注**：原候选 OPT-031（SimTradePage 全市场预加载）已忽略——`SidebarNav.tsx` 中 simtrade 入口已注释，用户无法进入该页。

#### 背景

`market_sentiment.py` 的 `compute_cn_sentiment_for_date()` 在当日 EOD breadth 缺失时，会调用 `fetch_cn_market_breadth_intraday()`：对 `fetch_ts_codes()` 返回的 **全 A 股**（~5000+）按 50 只一批 **串行** 调 Tushare realtime quote。该路径在 Dashboard **Sync All** 的 `marketSentiment` step（`sync_cn_sentiment`）触发，交易日上午一次 Sync 即可产生 **100+ 次** 外部 API 调用，是真实用户路径上的重瓶颈。

#### 目标

- intraday breadth 结果加 **进程内 TTL 缓存**（如 5–10min），Sync All 与重复 sync 不重复扫全市场
- quote batch 请求改为 **并行**（`ThreadPoolExecutor`，concurrency 4–6），单 batch 失败 isolation
- 可选：EOD breadth 已足够时跳过 intraday fallback（可配置）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/market_sentiment.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/dashboard.py`（`_sync_sentiment_step`） |
| 测试 | `tests/test_market_sentiment_breadth_cache.py` |

#### 验证

- [ ] 10min 内第二次 `sync_cn_sentiment` 不重复调用 `fetch_cn_market_breadth_intraday`
- [ ] mock 下 intraday breadth quote batch 并行 inflight > 1
- [ ] pytest 通过；Sync All sentiment step 耗时显著下降

---

### OPT-032：ensure_table() 热路径一次性 guard

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

Alembic 已接管 schema 后，`ensure_table()` 仍挂在几乎所有 DB 读路径。典型：`db/daily.py` 的 `fetch_last_ohlcv_batch()` 每次 TrendOK 计算都执行 `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ... ADD COLUMN`；`index_daily.py`、`news.py`、`macro_daily.py` 等同理。TrendOK / Dashboard / Watchlist 高频路径每次都付 DDL 检查成本。

#### 目标

- 模块级 `_TABLE_ENSURED: set[str]` 或 startup 一次性 ensure
- Alembic 环境下读路径默认 skip DDL；`ensure_table()` 保留供 local dev 空库 bootstrap
- 同步更新 `CREATE_SQL` 与 migration parity（不新增 ad-hoc runtime ALTER）

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `services/data-sync-service/src/data_sync_service/db/daily.py`、`index_daily.py`、`news.py`、`macro_daily.py` 等高频 `db/*.py` |
| 测试 | `tests/test_ensure_table_guard.py`（mock 下第二次读不执行 DDL） |

#### 验证

- [ ] mock 下同一进程第二次 `fetch_last_ohlcv_batch` 不调用 `ensure_table`
- [ ] fresh empty DB + `alembic upgrade head` 仍可用
- [ ] pytest 通过

---

### OPT-033：market_regime 指数 K 线批量读

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

`market_regime.py` 的 `_compute_index_signals()`（L371–381）对 `INDEX_SIGNALS`（上证 + 创业板）逐个调用 `fetch_last_closes_vol()` / `fetch_last_closes_vol_upto()`（各 80 日）。该函数经 `get_index_signals()` 被 Dashboard summary（60s 轮询）和 Macro snapshot 高频触发；虽有 60s TTL，仍是重复 SQL round-trip。

#### 目标

- `index_daily.py` 新增 `fetch_last_closes_vol_batch(ts_codes, days, as_of_date?)`
- `_compute_index_signals` 改为 **一次 batch 读** + 内存组装
- JSON shape 不变

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `services/data-sync-service/src/data_sync_service/db/index_daily.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/market_regime.py` |
| 测试 | `tests/test_market_regime_signals.py` 或 `test_index_daily_batch.py` |

#### 验证

- [ ] mock 下 `_compute_index_signals` 指数 DB 读 = 1（非 N 次单码）
- [ ] `test_dashboard_summary_endpoint_shape` 通过
- [ ] pytest 通过

---

### OPT-034：Index ↔ Dashboard 指数信号跨端去重

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

**前端**：`IndexPage.tsx` 用 `useMacroSnapshotQuery()`（45s 轮询）；`DashboardPage` 用 `useDashboardSummaryQuery()`（60s 轮询）——两者都含 `cnIndexSignals`/`indexSignals`，无 Query 共享。

**后端**：`dashboard.py` 的 `dashboard_summary()` 在非 realtime 窗口对 `get_index_signals()` 调 **两次**（`as_of_date=as_of` 与 `as_of_date=None`）；`test_dashboard_summary_calls_get_index_signals_twice_when_historical_as_of` 已 documenting 此行为。

#### 目标

- 后端统一 as_of 语义，sentiment 与 macro **共用同一 signals 列表**
- 前端 macro query 从 dashboard summary cache seed（类似 OPT-025/030），或合并为单一 `indexSignals` query key
- 消除盘后 historical as_of 路径双倍 `_compute_index_signals`

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/dashboard.py` |
| Query | `apps/desktop-ui/src/lib/queries/macro.ts`、`dashboard.ts` |
| Page | `apps/desktop-ui/src/components/pages/IndexPage.tsx` |
| 测试 | `test_dashboard_index_signal.py`、前端 macro query 单测 |

#### 验证

- [ ] mock 下 historical as_of summary 仅 1 次 `get_index_signals`
- [ ] Index 页与 Dashboard 切换不重复打 macro/summary 指数块
- [ ] pytest + vitest 通过

---

### OPT-035：废弃 sync_daily_full 逐股串行兜底

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

`service/daily.py` 的 `sync_daily_full()` 对 `fetch_ts_codes()` 全量列表 **逐股** 调用 `pro.daily()`；仍被 `sync_routes.py` 和 `scheduler/daily_sync_job.py` 暴露。Meanwhile `close_sync.py` 已用 `_fetch_paged_daily(pro, trade_date)` 按交易日 market-wide 分页同步，效率差几个数量级。

#### 目标

- `/sync/daily` endpoint 与 cron job **重定向至** `sync_close()` 或等价 close_sync 路径
- `sync_daily_full` 标记 deprecated 或仅作 manual recovery CLI
- 更新 README / scheduler 文档，避免 ops 误触发逐股 job

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `service/daily.py`、`service/close_sync.py` |
| API | `api/sync_routes.py` |
| Scheduler | `scheduler/daily_sync_job.py`、`scheduler/close_sync_job.py` |
| 测试 | sync route 重定向 / deprecated 警告测试 |

#### 验证

- [ ] 默认 daily sync job 不再逐股 loop
- [ ] close_sync 路径仍覆盖全市场增量
- [ ] pytest 通过

---

## P1 — 高价值

### OPT-036：Screener 快照 2N 请求与 import 串行

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

- `lib/queries/screener.ts` 的 `fetchLatestSnapshotDetail()`：每个 screener **list + detail** 两次请求；N 个 screener = 2N
- `lib/watchlist-screener-import.ts`：`for` 循环 **串行** 拉 snapshot；TrendOK 阶段 chunk 200 但 **串行** await 各 chunk
- Dashboard screener 状态已 batch 化（OPT-019），前端读/import 路径仍 2N 不对称

#### 目标

- 后端新增「latest snapshot + rows」批量 endpoint（或扩展 `list_latest_snapshots_for_screeners` 含 row payload）
- 前端 screener query 改为单次 batch；import 用 `Promise.all` + 复用 `fetchTrendOkMap`

#### 文件范围

| 层 | 文件 |
|----|------|
| DB/API | `db/tv.py`、TV integration routes |
| Query | `apps/desktop-ui/src/lib/queries/screener.ts` |
| Import | `apps/desktop-ui/src/lib/watchlist-screener-import.ts` |
| 测试 | screener batch + import parallel 单测 |

#### 验证

- [ ] N screener 快照加载 ≤ 1 batch API（非 2N）
- [ ] import 并行 inflight > 1（mock 断言）
- [ ] vitest 通过

---

### OPT-037：Dashboard 阶段三拆分（Sentiment/News 子 Query）

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

`DashboardPage.tsx` 仍 **~1060 行** God Page；`useDashboardSummary()` 每 60s 拉取 **整包** summary（industry + sentiment + news + macro + screeners meta），即使用户只关心部分卡片。News AI brief 另走 `regenerateNewsSummary()` → raw fetch + AI service，与 Query 缓存未统一。

#### 目标

- 按卡片拆分子 Query（sentiment、news、macro 可选 include）；主 query 仅 asOfDate + industry + screeners meta
- News brief 独立 staleTime；组件文件拆分（延续 OPT-013 阶段三）
- Copy/PDF 只 refetch 所需块

#### 文件范围

| 层 | 文件 |
|----|------|
| Page | `apps/desktop-ui/src/components/pages/DashboardPage.tsx` |
| Hook | `apps/desktop-ui/src/hooks/useDashboardSummary.ts` |
| Query | 新建 `lib/queries/sentiment.ts`、`lib/queries/news.ts` |
| 测试 | dashboard query key + partial refetch 单测 |

#### 验证

- [ ] summary lite query 不含 macro/news 重块（或 `include_*` 参数化）
- [ ] News regenerate 不重复打 full summary
- [ ] vitest 通过

---

### OPT-038：News / Alpha Radar RSS ingest 并行化

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

- `service/news.py` 的 `fetch_all_sources()`：**串行** `for source in sources` 拉 RSS + upsert
- `service/alpha_radar_ingest.py` 的 `fetch_all_sources()`：同样串行；priority source 的 Jina fulltext 亦串行
- 两者均在 `dashboard_sync_parallel()` 的 news step 和 Alpha pipeline ingest 中调用，Sync All 被最慢 source 拖住

#### 目标

- `ThreadPoolExecutor(max_workers=4–6)` 并行 fetch RSS
- Jina fulltext 限并发；单 source 失败 isolation（与 OPT-027 模式一致）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `service/news.py`、`service/alpha_radar_ingest.py` |
| Service | `service/dashboard.py`（news sync step，若需调整聚合） |
| 测试 | `tests/test_news_ingest_parallel.py` |

#### 验证

- [ ] mock 下多 source 并行启动（max inflight > 1）
- [ ] 单 source raise 不阻塞其余
- [ ] pytest 通过

---

### OPT-039：AlphaIncubator / NewsPage React Query 迁移

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

OPT-012 阶段 B 仍有 major page 未覆盖：

- `AlphaIncubatorPage.tsx`（~835 行）：`refresh()` 手写 fetch；tab/scope 切换时 **重复** 拉 trends/documents/catalyst
- `NewsPage.tsx`：`useEffect` + `refresh()` 无 cache/dedup/staleTime
- 同类：`BacktestPage.tsx`（手写 loadRuns/index）、`BrokerPage.tsx`（无 Query）

#### 目标

- 新建 `lib/queries/alphaRadar.ts`、`news.ts`、`backtest.ts`、`broker.ts`
- 统一 staleTime / refetchOnWindowFocus；Alpha 页 tab 切换读 cache 而非全量 refresh

#### 文件范围

| 层 | 文件 |
|----|------|
| Query | 新建 `apps/desktop-ui/src/lib/queries/alphaRadar.ts`、`news.ts`、`backtest.ts`、`broker.ts` |
| Page | `AlphaIncubatorPage.tsx`、`NewsPage.tsx`、`BacktestPage.tsx`、`BrokerPage.tsx` |
| 测试 | 各 query module 单测 |

#### 验证

- [ ] 上述页面无 mount-only raw fetch loop（grep `useEffect.*refresh` 收敛）
- [ ] tab 切换命中 cache（mock 断言 fetch 次数）
- [ ] vitest 通过

---

### OPT-040：ChatPanel 残留 trendok 与 Export fallback 统一 Query

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：round6 (local)

#### 背景

OPT-030 未完全收口：

- `ChatPanel.tsx`（L729）dashboard context 构建时仍 **raw `fetch` POST `/market/stocks/trendok`**，绕过 `watchlistMarketQueryOptions` 缓存
- `dashboard-export.ts` 无 `queryClient` 时仍 chunk 多次 trendok + quote
- `watchlist-screener-import.ts` 串行 chunk trendok，未复用 `fetchTrendOkMap`（`lib/api/trendok.ts` 有 inflight dedupe）

#### 目标

- ChatPanel 注入 `queryClient`，watchlist 块用 `fetchQuery(watchlistMarketQueryOptions)`
- export/import 统一走 `fetchTrendOkMap`；消除 POST trendok 特殊路径

#### 文件范围

| 层 | 文件 |
|----|------|
| Chat | `apps/desktop-ui/src/components/chat/ChatPanel.tsx` |
| Export | `apps/desktop-ui/src/lib/dashboard-export.ts` |
| Import | `apps/desktop-ui/src/lib/watchlist-screener-import.ts` |
| API | `apps/desktop-ui/src/lib/api/trendok.ts` |
| 测试 | 扩展 `dashboard-export.test.ts`、ChatPanel trendok cache 单测 |

#### 验证

- [ ] Chat dashboard context 不 POST trendok（mock 断言）
- [ ] export fallback 走 `fetchTrendOkMap`
- [ ] vitest 通过

---

## 推荐执行顺序

```
Week 1: OPT-032（ensure_table guard）→ OPT-033（index batch）→ OPT-034（信号 dedup）
Week 2: OPT-031（Sentiment breadth）→ OPT-035（sync_daily_full 退役）
Week 3: OPT-036（Screener 2N）→ OPT-038（RSS 并行）
Week 4: OPT-037（Dashboard 拆分）→ OPT-039 + OPT-040（Query 收尾）
```

**建议 PR 拆分**：

- **PR-A（backend P0）**：OPT-032 + OPT-033 + OPT-034 + OPT-035 + OPT-038
- **PR-B（frontend P1）**：OPT-036 + OPT-037 + OPT-039 + OPT-040

---

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | 第六轮审查：OPT-001 ~ OPT-030 全部完成；新增 OPT-031 ~ OPT-040 |
| 2026-06-18 | 第六轮实施完成：OPT-031 ~ OPT-040（backend P0 + frontend P1 Query 收尾） |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
