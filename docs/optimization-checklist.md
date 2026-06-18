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
| OPT-013 | Dashboard / Watchlist God Page 拆分（阶段二） | P1 | 3–5 天 | [ ] |
| OPT-014 | Industry Fund Flow 读路径 N+1 消除 | P1 | 1–2 天 | [ ] |
| OPT-015 | Watchlist Automation 去重 TrendOK 计算 | P1 | 0.5–1 天 | [ ] |

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

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

OPT-003 阶段一已完成统一 API client，但两个核心 Page 仍是 God component：

| 文件 | 行数 | 规模 |
|------|------|------|
| `DashboardPage.tsx` | ~2700 | 23 useState、23 顶层函数、8 useEffect |
| `WatchlistPage.tsx` | ~2450 | 30 useState、25 顶层函数、8 useEffect |

混合 UI、数据拉取、Markdown 导出、SSE 同步、localStorage 布局 — 任何小改易引发无关 re-render，测试困难。

#### 方案（阶段二 scope，不要一次拆完）

**Watchlist 提取：**

- `hooks/useWatchlistItems.ts` — hydrate / persist / event
- `hooks/useWatchlistTrend.ts` — trend、quotes、poll（若 OPT-012 未完成则保留 interval 封装）
- `components/watchlist/WatchlistTable.tsx` — 主表格 JSX
- `components/watchlist/WatchlistToolbar.tsx` — 工具栏

**Dashboard 提取：**

- `hooks/useDashboardSummary.ts`
- `hooks/useWatchlistRisk.ts`
- `hooks/useDashboardSync.ts` — SSE Sync All
- `components/dashboard/IndustryFundFlowCard.tsx` — 行业资金流卡片（~400 行 JSX）

**共享 lib（顺手抽取，低 scope）：**

- `lib/trendok-display.ts` — `TREND_OK_CHECKS` / `trendOkSummary()`（Dashboard + Watchlist 重复）
- `lib/symbols.ts` — `toTsCodeFromSymbol()`（3 处重复）

#### 涉及文件

| 文件 | 改动 |
|------|------|
| `apps/desktop-ui/src/components/pages/DashboardPage.tsx` | 瘦身，import hooks/components |
| `apps/desktop-ui/src/components/pages/WatchlistPage.tsx` | 同上 |
| `apps/desktop-ui/src/hooks/useDashboard*.ts` | 新建 |
| `apps/desktop-ui/src/hooks/useWatchlist*.ts` | 新建 |
| `apps/desktop-ui/src/components/dashboard/*` | 新建 |
| `apps/desktop-ui/src/components/watchlist/*` | 新建 |
| `apps/desktop-ui/src/lib/trendok-display.ts` | 新建 |
| `apps/desktop-ui/src/lib/symbols.ts` | 新建 |

#### 验证

- [ ] 无功能回归（手动 smoke：Sync All、Copy Markdown、Watchlist 加删票）
- [ ] `DashboardPage.tsx` / `WatchlistPage.tsx` 各 < 1200 行
- [ ] vitest 覆盖新 hooks 核心逻辑

---

### OPT-014：Industry Fund Flow 读路径 N+1 消除

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

`get_cn_industry_fund_flow()`（`industry_fund_flow.py:305-325`）对 top_n 个行业**逐条**调用 `get_series_for_industry()`：

```python
for r in top_rows:
    series = get_series_for_industry(industry_name=name, dates=dates)
```

`top_n=30`、`days=10` 时 → **30 次**独立 DB 往返。该 API 被 Dashboard summary、IndustryFlowPage 高频读取。

同类问题：`mainline.py` 的 `_flow_context` 对 10 个交易日循环 `flow_rows_by_date(d)`（~10 次查询）。可在本 OPT 一并合并为 `WHERE date = ANY(%s)` 批量查询，或拆为 OPT-014b。

#### 方案

1. 在 `db/industry_fund_flow.py` 新增 `get_series_for_industries(industry_names, dates)` 或 `get_rows_for_dates(dates)` 一次拉取后在 Python group。
2. `get_cn_industry_fund_flow` 改为单次（或 2 次）查询 + 内存聚合。
3. 可选：读路径 30–60s 进程内 TTL cache（key = `as_of_date + days + top_n`）。
4. Mainline `_flow_context` 同样改为批量 date 查询。

#### 涉及文件

| 文件 | 改动 |
|------|------|
| `services/data-sync-service/src/data_sync_service/db/industry_fund_flow.py` | 批量查询函数 |
| `services/data-sync-service/src/data_sync_service/service/industry_fund_flow.py` | 消除 N+1 |
| `services/data-sync-service/src/data_sync_service/service/mainline.py` | `_flow_context` 批量化 |
| `services/data-sync-service/tests/test_industry_fund_flow.py` | 查询次数断言 / 回归 |

#### 验证

- [ ] pytest industry / mainline 相关用例通过
- [ ] `get_cn_industry_fund_flow(top_n=30)` DB 查询次数 ≤ 3（mock/spy 或 query log）
- [ ] API 响应 JSON shape 不变
- [ ] Dashboard / IndustryFlow 页加载变快（curl benchmark 记录）

---

### OPT-015：Watchlist Automation 去重 TrendOK 计算

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

`run_watchlist_automation()` 对同一 symbol 列表**连续两次**调用 `compute_trendok_for_symbols`：

| 调用点 | 行号 | 用途 |
|--------|------|------|
| `record_score_snapshots()` | ~92 | 写入 score snapshot |
| `run_watchlist_automation()` 主体 | ~271 | 构建 `trendok_by_symbol` 供 removal 逻辑 |

TrendOK 是 CPU + DB 最重路径之一（~1250 行算法 + 批量 K 线）。自动化每日/手动触发时 **计算量近似翻倍**。

#### 方案

1. `record_score_snapshots` 返回 `(trade_date, count, rows_out)` 或完整 `list[dict]`。
2. `run_watchlist_automation` 复用该结果构建 `trendok_by_symbol`，删除第二次 `compute_trendok_for_symbols`。
3. 保持 score snapshot 写入与 removal 判定行为不变。

#### 涉及文件

| 文件 | 改动 |
|------|------|
| `services/data-sync-service/src/data_sync_service/service/watchlist_automation.py` | 去重 TrendOK |
| `services/data-sync-service/tests/test_watchlist_automation.py` | 断言只调用一次 compute |

#### 验证

- [ ] pytest `test_watchlist_automation.py` 通过
- [ ] mock `compute_trendok_for_symbols` 在单次 run 中调用次数 = 1
- [ ] score snapshot 条数与 removal 结果与改造前一致

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

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
