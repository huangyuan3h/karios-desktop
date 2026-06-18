# Karios Desktop 优化 Checklist

> 记录架构审查结论与优化方案，供后续逐个 Agent 任务执行。  
> 创建日期：2026-06-18（第二轮）  
> 第四轮审查：2026-06-18  
> 背景：OPT-011 ~ OPT-020 已完成；第四轮聚焦 **读路径去重算**、**后端热路径缓存**、**前端 Query 覆盖剩余页面**。

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
| OPT-021 | Watchlist Alerts 禁用全市场 Breadth 扫描 | P0 | 0.5 天 | [x] |
| OPT-022 | Mainline 读路径去 compute-on-read | P0 | 1–2 天 | [x] |
| OPT-023 | TrendOK 进程内 TTL 缓存 | P0 | 1 天 | [x] |
| OPT-024 | Macro Snapshot DB 批量读 + on-demand 并行 | P0 | 1 天 | [x] |
| OPT-025 | IndustryFlowPage React Query + Dashboard 缓存种子 | P0 | 1–2 天 | [x] |
| OPT-026 | Dashboard Industry Bundle 重复 SQL 合并 | P1 | 0.5–1 天 | [x] |
| OPT-027 | Sync All / Post-Close 串行步骤并行化 | P1 | 0.5–1 天 | [x] |
| OPT-028 | useWatchlistAutomation → React Query | P1 | 0.5–1 天 | [x] |
| OPT-029 | StockPage React Query 按 symbol 缓存 | P1 | 1–2 天 | [x] |
| OPT-030 | ChatPanel / Markdown Export 复用 Query 缓存 | P1 | 1–2 天 | [x] |

---

## P0 — 最高收益

### OPT-021：Watchlist Alerts 禁用全市场 Breadth 扫描

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `_get_regime()` 改为 `include_breadth=False`；新增 `_latest_bar_date()` hoist 一次 regime
- v5 / momentum alerts 循环内不再重复调用 `get_market_regime`
- 测试：`test_watchlist_alerts_performance_path.py`（2 用例）

#### 验证

- [x] mock 下 alert 路径 `get_market_regime` 调用 `include_breadth=False`
- [x] pytest alert 相关用例通过
- [x] v5 / momentum 响应 JSON shape 不变

---

### OPT-022：Mainline 读路径去 compute-on-read

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `get_cn_industry_mainline()` 移除 `ensure_metrics_for_dates` / `ensure_scores_for_dates`，纯读 scores 表
- scores 缺失时返回 `warning: "scores_not_ready"` + 空列表
- `sync_cn_industry_mainline()` 不变；IndustryFlow `onSync` 追加 mainline sync（OPT-025）
- 测试：`test_mainline_read_path.py`

#### 验证

- [x] mock 下 GET 路径不调用 `_compute_industry_metrics_for_date`
- [x] sync 路径仍调用 ensure
- [x] pytest mainline 相关用例通过

---

### OPT-023：TrendOK 进程内 TTL 缓存

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `TRENDOK_CACHE_TTL_SECONDS=60`；key=`(frozenset(symbols), realtime, latest_bar_date)`
- `clear_trendok_cache()` 在 `sync_daily_for_ts_code` upsert 后调用
- 测试：`test_trendok_performance_path.py`（+3 cache 用例）

#### 验证

- [x] 相同 symbol set 60s 内第二次调用命中 cache（lookup/regime 仅 1 次）
- [x] `clear_trendok_cache()` 后下次 miss
- [x] pytest trendok performance 用例通过

---

### OPT-024：Macro Snapshot DB 批量读 + on-demand 并行

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- DB：`fetch_last_closes_batch` + `get_latest_rows_batch`（`macro_daily.py`）
- `build_macro_snapshot` 改为 2 次 batch 读；`_backfill_macro_pct_chg` 复用 batch closes
- `enrich_macro_items_on_demand` 用 `ThreadPoolExecutor(max_workers=4)` 并行
- 测试：`test_macro_snapshot_batch.py`

#### 验证

- [x] mock 下 `build_macro_snapshot` 仅 batch closes + batch latest（无单条 fetch）
- [x] JSON shape 不变
- [x] pytest macro batch 用例通过

---

### OPT-025：IndustryFlowPage React Query + Dashboard 缓存种子

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 新建 `lib/queries/industryFlow.ts` — `useIndustryFundFlowQuery` / `useIndustryMainlineQuery`，`staleTime: 5min`
- `IndustryFlowPage` 移除 `useEffect` 手写 fetch；`runIndustryFlowSync` 含 fund-flow + mainline sync + invalidate dashboard
- 测试：`industryFlow.test.ts`（7 passed）

#### 验证

- [x] query key / parallel fetch 单测通过
- [x] sync 后 invalidate industry + dashboard summary
- [x] vitest 通过

---

## P1 — 高价值

### OPT-026：Dashboard Industry Bundle 重复 SQL 合并

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `industry_fund_flow_read.py` 新增 `build_dashboard_industry_bundle` 及 helper 纯函数
- `_build_industry_bundle()` 改为 `get_dates_upto` + `get_rows_for_dates` 单次 batch read
- 删除 `_industry_top_by_date` / `_industry_flow_5d_items` 等重复 SQL
- 测试：`test_dashboard_industry_bundle.py`

#### 验证

- [x] mock 下 `_build_industry_bundle` 行业流 DB 调用 = 1
- [x] `test_dashboard_summary_endpoint_shape` 通过
- [x] pytest dashboard 相关用例通过

---

### OPT-027：Sync All / Post-Close 串行步骤并行化

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `post_close_sync.py`：`ThreadPoolExecutor(max_workers=3)` 并行 index / macro / eastmoney
- `industry_fund_flow.py`：`_hist_rows_for_top_row` + hist fetch `ThreadPoolExecutor(max_workers=4)`，单行业失败 isolation
- 修复 `test_run_post_close_sync` mock 缺失 eastmoney
- 测试：`test_industry_fund_flow_sync_parallel.py`

#### 验证

- [x] mock 下 post-close 三步同时启动
- [x] industry sync 单行业失败不阻塞其余
- [x] pytest sync 相关用例通过

---

### OPT-028：useWatchlistAutomation → React Query

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 新建 `lib/queries/automation.ts` — `useAutomationPendingQuery`（60s `refetchInterval`，`enabled: isAutomationPollWindow()`）
- `useWatchlistAutomation.ts` 移除 `setInterval`，保留 apply + invalidate 语义
- 测试：`automation.test.ts`

#### 验证

- [x] 全项目无 `setInterval`（grep 确认）
- [x] apply 后 watchlist / risk cache invalidate
- [x] vitest 通过

---

### OPT-029：StockPage React Query 按 symbol 缓存

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 新建 `lib/queries/stock.ts` — `stockDetailQueryKey` / `fetchStockDetail` / `useStockDetailQuery`（`staleTime: 10min`）
- fund-flow 与 quote 并行；localStorage force 节流保留在 `queryFn`
- `StockPage.tsx` 移除 `useEffect` 手写 fetch
- 测试：`stock.test.ts`

#### 验证

- [x] 同一 symbol 10min 内 remount 命中 cache
- [x] manual refresh 强制 refetch
- [x] vitest 通过

---

### OPT-030：ChatPanel / Markdown Export 复用 Query 缓存

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `fetchDashboardSummaryCached`（`lib/queries/dashboard.ts`）
- `buildWatchlistMarkdown` / `buildScreenersMarkdown` / `buildDashboardCopyAllMarkdown` 接受可选 `queryClient`，优先 `fetchQuery` 读 cache
- `ChatPanel`：`dashboardAll` / `industryFundFlow` 引用改用 Query；`DashboardPage` Copy/PDF 注入 `queryClient`
- `industryFundFlowQueryOptions` 支持可选 `asOfDate`
- 测试：`dashboard-export.test.ts`（cache hit 断言）

#### 验证

- [x] Dashboard 已加载时 Copy Markdown 不重复打 summary API（mock 断言）
- [x] cache miss 仍正确 fallback fetch（无 queryClient 路径保留）
- [x] vitest 通过

---

## 推荐执行顺序

```
Week 1:  OPT-021（Alerts breadth）→ 一行级修复，立刻消除隐藏 P0
         OPT-023（TrendOK cache）→ 全站最高频计算降载
Week 2:  OPT-022（Mainline read-only）→ Industry / Alpha Radar latency
         OPT-024（Macro batch）→ Index / Dashboard IO
Week 3:  OPT-025（IndustryFlow Query）→ 前端热路径缓存
         OPT-026（Dashboard industry SQL）→ 60s 轮询 DB 减半
Week 4:  OPT-027（Sync 并行）→ Sync All / 盘后 job 加速
         OPT-028（Automation Query）→ 完成 OPT-012 阶段 B
Later:   OPT-029 + OPT-030（Stock / Export 缓存）→ 导航与 Copy 体验
```

### 后续候选（未列入本轮 Top 10）

| 主题 | 说明 |
|------|------|
| Index signals K 线 batch | `market_regime._compute_index_signals` 对 3 指数各查 80 日 K 线 |
| Screener 2N 请求 | 每个 screener list + detail 两次请求；`importFromScreener` 串行 |
| Dashboard 阶段三 | Sentiment + News 卡片拆分（`DashboardPage` 仍 1056 行） |
| Index ↔ Dashboard 信号 dedup | `indexSignals` 双端点双轮询 |
| `ensure_table()` 热路径 once | Alembic 已接管 schema 后冗余 DDL 检查 |
| `sync_daily_full` 遗留串行 | fallback job 逐股 Tushare，应迁移至 close_sync |
| SimTradePage 拆分 | 1017 行，串行分页 + 指数 loop |

---

## 已完成归档（OPT-011 ~ OPT-025）

| ID | 标题 | 完成日期 |
|----|------|----------|
| OPT-011 | Watchlist 手动刷新并行化 + TrendOK 统一 fetch | 2026-06-18 |
| OPT-012 | React Query 替换手写轮询 | 2026-06-18 |
| OPT-013 | Dashboard / Watchlist God Page 拆分（阶段二） | 2026-06-18 |
| OPT-014 | Industry Fund Flow 读路径 N+1 消除 | 2026-06-18 |
| OPT-015 | Watchlist Automation 去重 TrendOK 计算 | 2026-06-18 |
| OPT-016 | TrendOK 行业资金流上下文批量化 | 2026-06-18 |
| OPT-017 | Dashboard 去重 `get_index_signals` + TTL | 2026-06-18 |
| OPT-018 | Watchlist Risk 复用 `watchlist-market` Query 缓存 | 2026-06-18 |
| OPT-019 | TV Screener 最新快照 N+1 → 批量查询 | 2026-06-18 |
| OPT-020 | ScreenerPage React Query + 并行 snapshot 加载 | 2026-06-18 |
| OPT-021 | Watchlist Alerts 禁用全市场 Breadth 扫描 | 2026-06-18 |
| OPT-022 | Mainline 读路径去 compute-on-read | 2026-06-18 |
| OPT-023 | TrendOK 进程内 TTL 缓存 | 2026-06-18 |
| OPT-024 | Macro Snapshot DB 批量读 + on-demand 并行 | 2026-06-18 |
| OPT-025 | IndustryFlowPage React Query + Dashboard 缓存种子 | 2026-06-18 |
| OPT-026 | Dashboard Industry Bundle 重复 SQL 合并 | 2026-06-18 |
| OPT-027 | Sync All / Post-Close 串行步骤并行化 | 2026-06-18 |
| OPT-028 | useWatchlistAutomation → React Query | 2026-06-18 |
| OPT-029 | StockPage React Query 按 symbol 缓存 | 2026-06-18 |
| OPT-030 | ChatPanel / Markdown Export 复用 Query 缓存 | 2026-06-18 |

---

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | 第二轮：OPT-011 ~ OPT-015 完成 |
| 2026-06-18 | 第三轮：OPT-016 ~ OPT-020 完成 |
| 2026-06-18 | 第四轮审查：新增 OPT-021 ~ OPT-030；归档已完成项 |
| 2026-06-18 | 第四轮 P0 完成：OPT-021 ~ OPT-025 |
| 2026-06-18 | 第五轮 P1 完成：OPT-026 ~ OPT-030 |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
