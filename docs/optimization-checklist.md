# Karios Desktop 优化 Checklist

> 记录架构审查结论与优化方案，供后续逐个 Agent 任务执行。  
> 创建日期：2026-06-18（第二轮）  
> 第三轮审查：2026-06-18  
> 背景：OPT-001 ~ OPT-015 已完成；功能稳定，第三轮聚焦 **热路径 DB/计算去重** 与 **前端 Query 缓存打通**。

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
| OPT-011 | Watchlist 手动刷新并行化 + TrendOK 统一 fetch | P0 | 1–2 天 | [x] |
| OPT-012 | React Query 替换手写轮询 | P1 | 2–4 天 | [x] |
| OPT-013 | Dashboard / Watchlist God Page 拆分（阶段二） | P1 | 3–5 天 | [x] |
| OPT-014 | Industry Fund Flow 读路径 N+1 消除 | P1 | 1–2 天 | [x] |
| OPT-015 | Watchlist Automation 去重 TrendOK 计算 | P1 | 0.5–1 天 | [x] |
| OPT-016 | TrendOK 行业资金流上下文批量化 | P0 | 1 天 | [x] |
| OPT-017 | Dashboard 去重 `get_index_signals` + TTL | P0 | 0.5–1 天 | [x] |
| OPT-018 | Watchlist Risk 复用 `watchlist-market` Query 缓存 | P1 | 0.5–1 天 | [x] |
| OPT-019 | TV Screener 最新快照 N+1 → 批量查询 | P1 | 0.5–1 天 | [x] |
| OPT-020 | ScreenerPage React Query + 并行 snapshot 加载 | P1 | 1–1.5 天 | [x] |

---

## P0 — 最高收益

### OPT-011：Watchlist 手动刷新并行化 + TrendOK 统一 fetch

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **后端**：`sync_daily_for_ts_code()` 单股 tushare 增量 sync；`GET /bars?force=true` 接通 force 语义
- **前端**：新建 `lib/watchlist-market.ts`（`forceRefreshWatchlistBars` concurrency=4、`fetchWatchlistQuotes`、`fetchWatchlistMarketSnapshot` + `fetchTrendOkMap`）；`lib/concurrency.ts`、`lib/symbols.ts`
- `WatchlistPage` 删除串行 force bars + 120ms sleep；`refreshTrend` / Copy Markdown 共用 snapshot helper
- 测试：`test_daily_symbol_sync.py`（4）、`test_api.py` force 用例、`watchlist-market.test.ts`（4）、`concurrency.test.ts`（2）

#### 验证

- [x] vitest 通过（105 passed；含并发/去重/chunk 用例）
- [x] pytest `test_daily_symbol_sync.py` + bars force API 用例通过
- [ ] 50 股 manual refresh 耗时 < 5s（需 UI 手动 benchmark）
- [x] 100+ 股 watchlist TrendOK chunk 生效（单测 250 symbols → 2 batch）
- [x] 单股 force 失败不阻塞其余（单测 failures 计数）

---

## P1 — 高价值

### OPT-012：React Query 替换手写轮询

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 添加 `@tanstack/react-query` + [`query-client.ts`](apps/desktop-ui/src/lib/query-client.ts) + `AppShell` `QueryClientProvider`
- 新建 `lib/queries/{intervals,dashboard,watchlist,macro}.ts`
- **Dashboard**：`useDashboardSummaryQuery` + `useWatchlistRiskQuery`（60s 交易时段 gating）；SSE done / sentiment sync 写 query cache
- **Watchlist**：`useWatchlistMarketQuery`（10 min）；manual force / copy / automation 走 `refetchWatchlistMarket` / `invalidateQueries`
- **Index**：`useMacroSnapshotQuery`（45s）
- 未迁移：`useWatchlistAutomation` 60s tick（阶段 B）
- 测试：`intervals.test.ts`、`dashboard.test.ts`、`watchlist.test.ts`

#### 验证

- [x] vitest 通过（query key / interval 单测）
- [ ] Dashboard / Watchlist 切换 tab 无重复 inflight（需 Network 手动确认）
- [x] `refetchIntervalInBackground: false`（隐藏 tab 暂停 interval 轮询）
- [ ] 行为与改造前一致（需 UI smoke：Sync All、Manual Refresh、Index Refresh）

---

### OPT-013：Dashboard / Watchlist God Page 拆分（阶段二）

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **共享 lib**：`trendok-display.ts`、`dashboard-format.ts`、`dashboard-export.ts`、`watchlist-export.ts`；`StockPage` 改用已有 `lib/symbols.ts`
- **Dashboard hooks**：`useDashboardSummary`（query + news brief cache）、`useWatchlistRisk`（薄封装）、`useDashboardSync`（SSE Sync All）
- **Dashboard 组件**：`IndustryFundFlowCard.tsx`；Sentiment / News / WatchlistRisk / Screeners 卡片仍留 Page
- **Watchlist hooks**：`useWatchlistItems`（hydrate/persist/CRUD/name resolve）、`useWatchlistTrend`（`useWatchlistMarketQuery` + maxPrice 同步）
- **Watchlist 组件**：`WatchlistToolbar`、`WatchlistImportDebug`、`WatchlistTable` + `watchlist-table-cells.ts`
- **行数**：`DashboardPage.tsx` 2547 → **1056**；`WatchlistPage.tsx` 2281 → **375**
- **测试**：`trendok-display.test.ts`、`dashboard-format.test.ts`、`dashboard-export.test.ts`；vitest **125 passed**

#### 验证

- [x] `DashboardPage.tsx` / `WatchlistPage.tsx` 各 < 1200 行
- [x] vitest 覆盖新 lib 核心逻辑（TrendOK 展示、格式化、Markdown builder）
- [ ] 无功能回归（手动 smoke：Sync All、Copy Markdown、Watchlist 加删票）

---

### OPT-014：Industry Fund Flow 读路径 N+1 消除

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **DB**：[`get_rows_for_dates(dates)`](services/data-sync-service/src/data_sync_service/db/industry_fund_flow.py) — `WHERE date = ANY(%s)` 一次拉取全量行业行
- **聚合 lib**：[`industry_fund_flow_read.py`](services/data-sync-service/src/data_sync_service/service/industry_fund_flow_read.py) — `series_map_from_rows` / `sum_by_industry_from_rows` / `positive_days_from_rows`
- **`get_cn_industry_fund_flow`**：3 次 DB 读（`dates_upto` + `top_rows` + `get_rows_for_dates`），移除循环 `get_series_for_industry`
- **`mainline._flow_context`**：`dates_20` 一次 batch + slice 得 `dates_10/5`；移除 10× `flow_rows_by_date` 与 2× `get_sum_by_industry_for_dates`
- **测试**：`test_industry_fund_flow_read.py`（7 用例：聚合、JSON shape、查询次数、mainline batch）

#### 验证

- [x] pytest industry / mainline 相关用例通过
- [x] `get_cn_industry_fund_flow(top_n=30, as_of_date=固定)` mock 下 DB 调用 = 3
- [x] API 响应 JSON shape 不变（单元 + `test_api` endpoint shape）
- [ ] Dashboard / IndustryFlow 页加载变快（curl benchmark 需手动）

---

### OPT-015：Watchlist Automation 去重 TrendOK 计算

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `record_score_snapshots` 返回 `(trade_date, count, rows_out)`，单次 `compute_trendok_for_symbols` 后同时写 snapshot 并返回原始 TrendOK rows
- `run_watchlist_automation` 复用 `rows_out` 构建 `trendok_by_symbol`，删除第二次 compute
- 测试：`test_record_score_snapshots_returns_rows`、`test_run_watchlist_automation_computes_trendok_once`

#### 验证

- [x] pytest `test_watchlist_automation.py` 通过
- [x] mock 下单次 run 中 `compute_trendok_for_symbols` 调用 = 1
- [x] score snapshot 与 removal 路径仍执行（单元 mock 覆盖）

---

## P0 — 第三轮最高收益

### OPT-016：TrendOK 行业资金流上下文批量化

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **纯函数**：[`build_trendok_flow_context_from_rows`](services/data-sync-service/src/data_sync_service/service/industry_fund_flow_read.py) — 从 batch rows 内存聚合 today/yesterday hotspot 与 5d top/bottom
- **`_build_industry_flow_context`**：`get_dates_upto(5)` + `get_rows_for_dates` 一次 batch；移除 `get_dates_upto(2)`、`get_rows_by_date` ×2、`get_sum_by_industry_for_dates`
- **`_lookup_stock_basic`**：合并 name + industry 单次 `stock_basic` 查询；`compute_trendok_for_symbols` 改用之
- **测试**：`test_industry_fund_flow_read.py`（+2）、`test_trendok_industry_flow.py`（batch read 断言）、`test_trendok_performance_path.py`（stock_basic 单次）；pytest trendok **49 passed**

#### 背景

OPT-014 已在 `get_cn_industry_fund_flow()` 与 `mainline._flow_context()` 使用 `get_rows_for_dates()` + `industry_fund_flow_read.py` 聚合，但 **TrendOK 热路径未迁移**。

`compute_trendok_for_symbols()` 每次请求调用 `_build_industry_flow_context()`，产生 **4–5 次独立 DB 读**（`get_dates_upto` ×2、`get_rows_by_date` ×2、`get_sum_by_industry_for_dates`）。该函数被高频调用：

- `GET /market/stocks/trendok`
- Dashboard `fetchWatchlistRiskRows`（60s 轮询）
- Watchlist 10 min 轮询、Automation、Screener 导入

附带：`_lookup_names()` 与 `_lookup_industries()` 对 `stock_basic` 各一次 `WHERE ts_code = ANY`，可合并为单次查询。

#### 目标

- `_build_industry_flow_context()` 改为 `get_rows_for_dates(dates_5)` 一次拉取 + `industry_fund_flow_read` 内存聚合
- 合并 `_lookup_names` / `_lookup_industries` 为单次 `stock_basic` batch read
- 批量化后 `_industry_flow_score_adjustment` 输出 **bit-identical**（分数逻辑不变）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/trendok.py` — `_build_industry_flow_context`, `_lookup_names`, `_lookup_industries` |
| 复用 | `service/industry_fund_flow_read.py`（已有） |
| DB | `db/industry_fund_flow.py` — `get_rows_for_dates`（已有，无需 schema 变更） |
| 测试 | `tests/test_trendok_industry_flow.py`（扩展）、`tests/test_trendok_performance_path.py` |

#### 验证

- [x] mock 下 `_build_industry_flow_context` 仅 `get_dates_upto(5)` + `get_rows_for_dates`；不再调用 `get_rows_by_date` / `get_sum_by_industry_for_dates`
- [x] `_industry_flow_score_adjustment` 既有 6 个用例通过；纯函数 golden fixture 覆盖 hotspot / 5d rank
- [x] pytest trendok 相关用例通过（49 passed）

---

### OPT-017：Dashboard 去重 `get_index_signals` + TTL

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **`get_index_signals`**：60s TTL cache（`INDEX_SIGNALS_CACHE_TTL_SECONDS`）；计算逻辑下沉 `_compute_index_signals`；`clear_index_signals_cache()` + `clear_market_regime_cache()` 联动清理
- **`dashboard_summary`**：ThreadPool 前预取 signals；交易时段 1 次共享注入 sentiment + macro；盘后 historical as_of 仍 2 次（语义不变）
- **`build_macro_snapshot(cn_index_signals=...)`** / **`_build_market_sentiment_bundle(index_signals=...)`** 支持注入
- **测试**：`test_market_regime_cache.py`（+3 TTL）、`test_dashboard_index_signal.py`（realtime=1 / historical=2 调用）；**10 passed**

#### 背景

`dashboard_summary()` 在 `ThreadPoolExecutor` 内并行构建 sentiment 与 macro，**各自独立调用** `get_index_signals(include_breadth=False)`：

- `_build_market_sentiment_bundle()` → L270
- `build_macro_snapshot()` → `macro_snapshot.py` L162

`get_index_signals()`（`market_regime.py` ~751 行）每次含 realtime quotes、80 日 K 线 fetch、MA/MACD 计算。`get_market_regime()` 有 600s TTL，但 **`get_index_signals` 无缓存**。交易时段 Dashboard 每 **60s** 轮询 → **每次 summary 算两遍** index signals；响应还重复携带 `marketSentiment.indexSignals` 与 `macroSnapshot.cnIndexSignals`。

#### 目标

- `dashboard_summary()` 内 **单次** 计算 index signals，注入 sentiment bundle 与 macro snapshot
- 可选：为 `get_index_signals(as_of_date, include_breadth)` 加短 TTL（如 30–60s），供独立 endpoint 复用
- 保留盘后 `as_of_date` vs 交易时段 realtime 的语义差异

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `service/dashboard.py` — `dashboard_summary`, `_build_market_sentiment_bundle` |
| Service | `service/macro_snapshot.py` — `build_macro_snapshot`（接受预计算 signals 或共享 cache） |
| Service | `service/market_regime.py` — `get_index_signals`（可选 TTL） |
| 测试 | `tests/test_dashboard_index_signal.py`、`tests/test_market_regime_cache.py` |

#### 验证

- [x] mock 下交易时段 `dashboard_summary()` 中 `get_index_signals` 调用 = 1；盘后 historical as_of = 2
- [x] JSON shape 不变（`test_dashboard_summary_endpoint_shape` 通过）
- [x] pytest dashboard / market_regime cache 相关用例通过

---

## P1 — 第三轮高价值

### OPT-018：Watchlist Risk 复用 `watchlist-market` Query 缓存

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **`watchlistMarketQueryOptions`**：共享 queryKey + queryFn；`useWatchlistMarketQuery` / `refetchWatchlistMarket` 复用
- **`fetchWatchlistRiskRows(queryClient)`**：`queryClient.fetchQuery(watchlistMarketQueryOptions(symbols))`；删除独立 trendok/quote chunk 与 `parseDashboardQuoteItem`
- **`buildWatchlistRiskRowsFromSnapshot`**：纯函数，从 snapshot 构建 risk rows
- **`useWatchlistAutomation`**：apply 后 invalidate `watchlistMarketKey` + `watchlistRiskQueryKey`
- **测试**：`dashboard.test.ts`（+2）、`watchlist.test.ts`（+1）；vitest **128 passed**

#### 背景

OPT-012 后 Watchlist 走 `useWatchlistMarketQuery` + `fetchWatchlistMarketSnapshot`（含 `fetchTrendOkMap` inflight 去重），但 Dashboard **`fetchWatchlistRiskRows` 仍独立** chunk 请求 trendok + quote，query key 不共享。同一 watchlist 在 Dashboard ↔ Watchlist 切换时 **重复打 API**；quote 解析在 `dashboard.ts` 与 `watchlist-market.ts` 各写一份。

#### 目标

- `fetchWatchlistRiskRows` 改为 `queryClient.fetchQuery(watchlistMarketKey(symbols))` 或共用 `fetchWatchlistMarketSnapshot`
- 对齐 `isShanghaiSyncWindow()`（risk）与 `isShanghaiQuoteWindow()`（watchlist）的 realtime 语义，避免 alert 行为漂移
- `applyAutomationRun` 完成后 invalidate `watchlistMarketKey`（可与 OPT-012 阶段 B 一并做）

#### 文件范围

| 层 | 文件 |
|----|------|
| Query | `apps/desktop-ui/src/lib/queries/dashboard.ts` — `fetchWatchlistRiskRows`, `useWatchlistRiskQuery` |
| 复用 | `lib/watchlist-market.ts`, `lib/queries/watchlist.ts`, `lib/watchlist-metrics.ts` |
| Hook | `hooks/useWatchlistRisk.ts` |
| 测试 | `lib/queries/dashboard.test.ts`（扩展 cache 共享用例） |

#### 验证

- [x] `fetchWatchlistRiskRows` 使用 `watchlistMarketKey`（单测 mock fetchQuery）
- [x] risk alert 逻辑仍走 `buildWatchlistRowMetrics`（`riskAlerts` golden 单测）
- [x] vitest 通过（128 passed）

---

### OPT-019：TV Screener 最新快照 N+1 → 批量查询

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **DB**：[`list_latest_snapshots_for_screeners`](services/data-sync-service/src/data_sync_service/db/tv.py) — `DISTINCT ON (screener_id)` 一次 batch；`_snapshot_meta_from_row` 共用 payload 解析
- **Dashboard**：`_screeners_status` / `_sync_screeners_step` skip 预检改为 batch；`_skip_screener_after_close_from_meta` 纯函数
- **测试**：`test_dashboard_screener_sync.py`（+3 batch/shape）；**9 passed**

#### 背景

`_screeners_status()`（`dashboard.py` L284–314）对每个 enabled screener 循环 `list_snapshots_for_screener_full(sid, limit=1)` — **N 次 DB round-trip**，且 SELECT 含 `payload` JSONB（只为 `filtersCount`）。发生在每次 `GET /dashboard/summary`。Sync 预检 `_should_skip_screener_after_close()` 与 `_sync_screeners_step()` 同样循环。

#### 目标

- 新增 `list_latest_snapshots_for_screeners(screener_ids)`（`DISTINCT ON (screener_id)`；meta-only 列或轻量 JSON 抽取 `filters`）
- `_screeners_status` / skip 预检 / sync 预检改为 **1 次 batch 读**
- 响应字段不变：`capturedAt`, `rowCount`, `filtersCount`

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `services/data-sync-service/src/data_sync_service/db/tv.py` — 新增 batch 函数 |
| Service | `service/dashboard.py` — `_screeners_status`, `_should_skip_screener_after_close`, `_sync_screeners_step` |
| 测试 | `tests/test_dashboard_screener_sync.py` |

#### 验证

- [x] mock 下 `_screeners_status` batch 调用 = 1（3 screener 单测）
- [x] 返回 shape 不变（`filtersCount` / `capturedAt` / `rowCount`）
- [x] pytest dashboard screener 用例通过（9 passed）

---

### OPT-020：ScreenerPage React Query + 并行 snapshot 加载

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **Shared**：扩展 `tvCapture.ts` — `TvScreener` / `TvSnapshotSummary` / `TvSnapshotDetail` + list response schemas
- **Query**：新建 `lib/queries/screener.ts` — `useScreenerListQuery` + `useScreenerSnapshotsQuery`；`fetchScreenerSnapshotsMap` 用 `Promise.all` 并行 per-screener fetch；`SCREENER_STALE_MS=5min`
- **Page**：`ScreenerPage` 移除串行 `refreshAll` + inline types；sync 后 `invalidateScreenerQueries`；History 仍按需本地 fetch
- 测试：`tvCapture.test.ts` snapshot schemas；`screener.test.ts` query key + parallel fetch

#### 验证

- [ ] N screener 加载改为并行（Network waterfall 无串行阶梯 — 需 UI 手动 benchmark）
- [x] staleTime 5min 内复进页面命中 React Query cache（逻辑已实现）
- [x] vitest query key / parallel fetch 单测通过

---

## 推荐执行顺序

### 第二轮（已完成）

```
Week 1:  OPT-011（Watchlist 刷新）→ 立刻改善手动刷新体验
Week 2:  OPT-015（Automation 去重）→ 小改动、后端立刻省一半 TrendOK
Week 3:  OPT-014（Industry N+1）→ Dashboard / 行业页加载加速
Week 4:  OPT-012（React Query）→ 系统性前端数据层
Later:   OPT-013（God Page 拆分）→ 与 OPT-012 可并行，但 hooks 边界在 Query 之后更清晰
```

### 第三轮（待执行）

```
Week 1:  OPT-016（TrendOK 行业流 batch）→ 全站 TrendOK  latency ↓，延续 OPT-014 模式
Week 2:  OPT-017（index signals 去重）→ Dashboard 60s 轮询 CPU/IO 减半
Week 3:  OPT-018（Risk ↔ Watchlist cache）→ 前端重复请求消除
Week 4:  OPT-019 + OPT-020（Screener batch）→ Dashboard summary + Screener 页加载加速
Later:   useWatchlistAutomation → React Query（OPT-012 阶段 B）、Dashboard Sentiment 卡片拆分（OPT-013 阶段三）
```

---

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | OPT-012 完成：React Query 替换 Dashboard/Watchlist/Index 手写轮询 |
| 2026-06-18 | OPT-013 完成：Dashboard/Watchlist God Page 拆分为 hooks + components + lib |
| 2026-06-18 | OPT-014 完成：Industry Fund Flow 读路径 N+1 消除（batch `get_rows_for_dates`） |
| 2026-06-18 | OPT-015 完成：Watchlist Automation 去重 TrendOK 计算 |
| 2026-06-18 | OPT-019 完成：TV Screener 最新快照 `DISTINCT ON` batch 查询 |
| 2026-06-18 | OPT-020 完成：ScreenerPage React Query + 并行 snapshot 加载 |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
