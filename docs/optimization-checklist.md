# Karios Desktop 优化 Checklist

> 记录架构审查结论与优化方案，供后续逐个 Agent 任务执行。  
> 创建日期：2026-06-18（第二轮）  
> 背景：OPT-001 ~ OPT-010 已完成；功能稳定，本轮聚焦剩余最高 ROI 项。

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

## 推荐执行顺序

```
Week 1:  OPT-011（Watchlist 刷新）→ 立刻改善手动刷新体验
Week 2:  OPT-015（Automation 去重）→ 小改动、后端立刻省一半 TrendOK
Week 3:  OPT-014（Industry N+1）→ Dashboard / 行业页加载加速
Week 4:  OPT-012（React Query）→ 系统性前端数据层
Later:   OPT-013（God Page 拆分）→ 与 OPT-012 可并行，但 hooks 边界在 Query 之后更清晰
```

---

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | OPT-012 完成：React Query 替换 Dashboard/Watchlist/Index 手写轮询 |
| 2026-06-18 | OPT-013 完成：Dashboard/Watchlist God Page 拆分为 hooks + components + lib |
| 2026-06-18 | OPT-014 完成：Industry Fund Flow 读路径 N+1 消除（batch `get_rows_for_dates`） |
| 2026-06-18 | OPT-015 完成：Watchlist Automation 去重 TrendOK 计算 |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
