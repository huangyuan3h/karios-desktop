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
| OPT-041 | Watchlist 港股 (HK) 闸门打通 | P1 | 0.5–1 天 | [x] |
| OPT-042 | Watchlist ETF 通用化（fund_basic + 全量 ETF） | P1 | 1–2 天 | [x] |
| OPT-043 | HK 日线 cron 改每日 + yfinance 增量 | P1 | 0.5 天 | [x] |

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
Week 5: OPT-041（HK 闸门）→ OPT-042（ETF 通用化）
```

**建议 PR 拆分**：

- **PR-A（backend P0）**：OPT-032 + OPT-033 + OPT-034 + OPT-035 + OPT-038
- **PR-B（frontend P1）**：OPT-036 + OPT-037 + OPT-039 + OPT-040
- **PR-C（多市场 P1）**：OPT-041 + OPT-042（HK 闸门 + ETF 通用化）

---

## P1 — 多市场扩展

### OPT-041：Watchlist 港股 (HK) 闸门打通

**状态**：[x]  
**完成日期**：2026-07-29  
**PR/Commit**：–

#### 背景

Watchlist 当前仅支持 CN A 股，但底层 Tushare 港股基础数据（`hk_basic` → `stock_basic.market='HK'`）和港股日线（`hk_daily` → `daily` 表）已经在月度 cron 同步进 DB。Symbol → ts_code 转换层（`symbol_to_ts_code`、`_parse_symbol`、`_symbol_to_ts_code` 等）共 8 处 CN-only 闸门拒收 HK:`前缀`，导致 StockPage / WatchlistPage 对港股直接 400。

#### 目标

- `HK:00700` 等 4-5 位 HK 编码在 symbol→ts_code 转换层全部走通
- `/market/stocks/{symbol}/bars` 支持 HK 增量（调 `hk_daily`，复用 `hk_daily_job` 的 ts_code-单股模式）
- `/market/stocks/resolve` 支持 HK 名称查询
- `/market/stocks/trendok` 对 HK 输出 TrendOK/Score（与 CN 同一套 EMA/RSI 指标，阈值后续调参）
- 前端 `symbols.ts` `toTsCodeFromSymbol` 加 HK 分支；`watchlist-market.ts` `forceRefreshWatchlistBars` 不再过滤 HK
- `chips` / `fund-flow` 端点保持 400（数据源 CN-only；HK 筹码 / 资金流暂不支持）

#### 数据源复用

| 数据 | 现状 | 来源 |
|------|------|------|
| 港股列表 | ✅ 已同步 | Tushare `pro.hk_basic` |
| 港股日线 | ✅ 已同步 | Tushare `pro.hk_daily` |
| 港股实时报价 | ⚠️ 待验证 | Tushare `pro.realtime_quote` (支持 `00700.HK` 格式) |
| 港股指数 | ✅ 已用 | yfinance `^HSI` |

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/market_quotes.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/market_bars.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/market_detail.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/trendok.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/watchlist_automation.py` |
| Service | `services/data-sync-service/src/data_sync_service/service/hk_daily.py`（新增 `sync_hk_daily_for_ts_code`） |
| Service | `services/data-sync-service/src/data_sync_service/service/realtime_quote.py`（HK EM push2 fallback） |
| API | `services/data-sync-service/src/data_sync_service/api/query_routes.py` |
| Frontend | `apps/desktop-ui/src/lib/symbols.ts` |
| Frontend | `apps/desktop-ui/src/lib/watchlist-market.ts` |
| Tests | `tests/test_market_quotes_utils.py`、`test_market_bars_utils.py`、`test_market_detail_utils.py`、`test_realtime_quote_utils.py`、`watchlist-market.test.ts` |

#### 验证

- [x] `symbol_to_ts_code("HK:00700")` → `"00700.HK"`
- [x] `_parse_symbol("HK:00700")` → `("HK", "00700", "00700.HK")`
- [x] `/market/stocks/HK:00700/bars?force=true` 返回非空 bars（增量走 `sync_hk_daily_for_ts_code`）
- [x] `/market/stocks/resolve?symbols=HK:00700` 返回 name/market
- [x] `/market/stocks/trendok?symbols=HK:00700` 返回有效 score（不带 `unsupported_market`）
- [x] pytest + vitest 通过（后端 845 + 前端 342）
- [x] 港股实时报价 EM push2 兜底（tushare `realtime_quote(00700.HK)` 当前 key 列不匹配 → 自动 fallback 到 `push2.eastmoney.com secid=116.{ticker}`）

---

### OPT-042：Watchlist ETF 通用化（fund_basic + 全量 ETF）

**状态**：[x]  
**完成日期**：2026-07-29  
**PR/Commit**：–

#### 背景

Dashboard 的 ETF 资金流硬编码 6 只（`etf_fund_flow.py:41-48`），用户无法把任意 ETF（如 510300、513050、159819）加进 Watchlist。`stock_basic` 表是通用 schema，但 ETF 没有走 `pro.fund_basic` 同步，Tushare `pro.fund_daily` 也未启用。

#### 目标

- 新增 `service/fund_basic.py` 调 `pro.fund_basic(market='E', status='L')`，写入 `stock_basic` `market='ETF'`
- 新增 `service/etf_daily.py` 调 `pro.fund_daily` 把全量 ETF 日线写进 `daily` 表
- `market_quotes.symbol_to_ts_code` / `_parse_symbol` / TrendOK 加 `ETF:` 前缀分支
- `etf_fund_flow.py` universe 从 6 只扩到 `stock_basic.market='ETF'`
- 前端 `normalizeSymbolInput` 支持 `5xxxxx`/`1xxxxx` 6 位 ETF 输入 → `ETF:xxxxxx`

#### 数据源

- Tushare `pro.fund_basic(market='E')` —— 普通会员通常已开通，需 `force=true` 验证
- 失败兜底：东方财富 `fund.eastmoney.com/data/fundranking.html` HTTP 抓取
- 实时资金流：复用 `etf_fund_flow_em.py`（已支持全市场推送 `MK0021/MK0022/MK0023/MK0024/MK0827`）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/fund_basic.py`（新建） |
| Service | `services/data-sync-service/src/data_sync_service/service/etf_daily.py`（新建） |
| Service | `services/data-sync-service/src/data_sync_service/service/etf_fund_flow.py`（加 `get_etf_watchlist_extended`） |
| Service | `services/data-sync-service/src/data_sync_service/service/market_quotes.py`（symbol→ts_code 加 ETF） |
| Service | `services/data-sync-service/src/data_sync_service/service/market_bars.py`（_parse_symbol 加 ETF） |
| Service | `services/data-sync-service/src/data_sync_service/service/market_detail.py`（_parse_symbol 加 ETF） |
| Service | `services/data-sync-service/src/data_sync_service/service/trendok.py`（_symbol_to_ts_code 加 ETF） |
| Service | `services/data-sync-service/src/data_sync_service/service/watchlist_automation.py` |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/fund_basic_job.py`（新建） |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/etf_daily_job.py`（新建） |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/__init__.py` |
| API | `services/data-sync-service/src/data_sync_service/api/sync_routes.py`（`/sync/etf-fund-basic`、`/sync/etf-daily`） |
| API | `services/data-sync-service/src/data_sync_service/api/query_routes.py`（`/resolve` 加 ETF） |
| Frontend | `apps/desktop-ui/src/lib/symbols.ts`（ETF 分支） |
| Frontend | `apps/desktop-ui/src/hooks/useWatchlistItems.ts`（normalizeSymbolInput ETF 输入） |
| Tests | 新建 `test_fund_basic_utils.py`、`test_etf_fund_flow_universe.py`、`symbols.test.ts`、`useWatchlistItems.test.ts` |

#### 验证

- [x] `pro.fund_basic(market='E')` 在当前 Tushare key 可调用（实测 2156 只 ETF）
- [x] `stock_basic` 包含 ETF 行（market='ETF'），数量 2102 已上市
- [x] `daily` 包含 `510300.SH` 等 ETF ts_code 历史 K 线（`fund_daily` 已验证）
- [x] `ETF:510300` 在 Watchlist 流程中可加、可查行情、可拿 TrendOK
- [x] pytest + vitest 通过
- [x] `etf_fund_flow.py` 加 `get_etf_watchlist_extended(max_size=N)` 动态从 `stock_basic.market='ETF'` 扩展（默认不破坏 dashboard 6 只核心 ETF）

---

### OPT-043：HK 日线 cron 改每日 + akshare 高优先级源 + 5y backfill

**状态**：[x]  
**完成日期**：2026-07-29  
**PR/Commit**：–

#### 背景

OPT-041 打通了 HK 闸门，但 `hk_daily_full` cron 是 `30 18 1 * *`（每月 1 号跑一次），新加 HK tickers 到 Watchlist 后等不到下次同步就只能走 `bars?force=true` 单股 tushare 调用（1次/分钟）。watchlist 整体要看 HK 行情就必须等下个月。

而且 yfinance 在某些 IP 上会被 rate limit（实测全量限流），tushare 受 1次/分钟限制，不适合全市场 batch。**akshare `ak.stock_hk_daily()` (Sina 源) 实测 30 次连续调用 0 失败，平均 0.12s/call**，应作为最高优先级源。

#### 目标

- HK daily cron 改为每日跑（每天 17:30 Asia/Shanghai = HK 收盘 1.5h 后）
- 数据源优先级：**akshare (Sina) → yfinance → tushare**（akshare 最快 + 最稳）
- 首次同步只拉**最近 5 年**数据；已有老数据保留不删
- 每只 HK 走**增量**（last_trade_date+1 → 今天）
- 单股失败不阻塞整批；resume 用现有 `sync_job_record.last_ts_code`
- 暴露 `/sync/hk_daily/status` 让运维能查进度

#### 文件范围

| 层 | 文件 |
|----|------|
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/hk_daily_job.py`（cron 改 `30 17 * * *`） |
| Service | `services/data-sync-service/src/data_sync_service/service/hk_daily.py`（akshare > yfinance > tushare；进度日志；5y 兜底） |
| Service | `services/data-sync-service/src/data_sync_service/service/hk_daily_ak.py`（新建：akshare 封装 + 5y 窗口） |
| Service | `services/data-sync-service/src/data_sync_service/service/hk_daily_yf.py`（first-time backfill 改为 5y） |
| API | `services/data-sync-service/src/data_sync_service/api/sync_routes.py`（`GET /sync/hk-daily/status`） |
| Script | `services/data-sync-service/scripts/sync_hk_ak.py`（一次性 akshare 全量 `--only-missing --years 5`） |
| Script | `services/data-sync-service/scripts/sync_hk_yf.py`（保留 `--years N` 选项） |
| Tests | `tests/test_hk_daily_ak_utils.py`（13 个新测试：5y 窗口、pre_close/change 计算） |
| Tests | `tests/test_hk_daily.py`（priority chain / resume / cron 频率 / 单股失败） |

#### 验证

- [x] cron 从 `30 18 1 * *` 改为 `30 17 * * *`（每天）
- [x] akshare 优先级最高：30/30 连续调用 0 失败，平均 0.12s/call
- [x] 全量同步实测：**1868/1868 success, 0 failed, 1,553,162 rows in 16.5 min**（5y 窗口）
- [x] DB 状态：2767/2767 HK 都有 bars；新加的 5y 数据 = 2,402,893 行；老数据（>=1998）保留不删
- [x] `sync_hk_daily_full` 内 `get_last_trade_date` 已经做增量；只对缺失日期窗口拉数据
- [x] 单 ticker raise 时继续循环（不 abort），记 `failed_count`
- [x] resume 用 `sync_job_record.last_ts_code`；第二天从下一个 ticker 继续
- [x] pytest 896 + vitest 344 全绿
- [x] `scripts/sync_hk_ak.py --only-missing --years 5 --delay 0.2` 给首次手工全量用
- [x] HK industry 由 Xueqiu `mbu` 截前 24 字 → `stock_basic.industry`（`/sync/hk-industry`，cron 02:00 Asia/Shanghai，限速 1s/call + 2 retry）
- [x] `hk_basic` 同步加 `keep_industry=True` — COALESCE 保留已有 industry（不被 tushare None 覆盖）
- [x] watchlist `POST /watchlist/registry` 自动回填 `name` from `stock_basic`（HK / ETF / CN 通用）
- [x] `POST /watchlist/registry/backfill-names` 一键回填所有 null name 给运维

---

### OPT-044：HK watchlist 显示 name + industry

**状态**：[x]  
**完成日期**：2026-07-29  
**PR/Commit**：–

#### 背景

OPT-041 打通了 HK 闸门，watchlist 能加 HK 股票，但截图里 `Name` 和 `Industry` 两列都显示 `—`：
- **Name**: `_addAndResolve` 调 `/market/stocks/resolve` 但 backend 没在 `upsert_registry` 时持久化 client-side resolve 失败时的回退；老的 HK items name 一直 null
- **Industry**: tushare `pro.hk_basic` 不返回 industry；akshare `stock_hk_spot` / `stock_hk_security_profile_em` 都不含 industry；雪球 `stock_individual_basic_info_hk_xq` 提供 `mbu` (主营业务)

#### 目标

- HK watchlist `Name` 列显示真实公司名（不是 `—`）
- HK watchlist `Industry` 列显示主营业务描述（截前 24 字）
- 不破坏 CN watchlist 已有的 `tushare` 行业分类
- 雪球限流（soft rate-limit 返回 all-None）时 retry，不浪费之前已 resolve 的 entry

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/hk_industry.py`（新建：Xueqiu mbu → industry，retry × 2） |
| Service | `services/data-sync-service/src/data_sync_service/service/hk_basic.py`（改：`keep_industry=True`） |
| DB | `services/data-sync-service/src/data_sync_service/db/stock_basic.py`（新增 `update_industry()` + `UPSERT_KEEP_INDUSTRY_SQL`） |
| API | `services/data-sync-service/src/data_sync_service/api/sync_routes.py`（`POST/GET /sync/hk-industry`） |
| API | `services/data-sync-service/src/data_sync_service/api/watchlist_routes.py`（`POST /watchlist/registry` 自动回填 name + 新 `POST /watchlist/registry/backfill-names`） |
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/hk_industry_job.py`（新建：02:00 Asia/Shanghai daily） |
| Tests | `tests/test_hk_industry.py`（新建 21 个：truncate / fetch_xueqiu_mbu retry / sync / status / db.update_industry / keep_industry COALESCE） |
| Tests | `tests/test_watchlist_registry.py`（加 3 个：backfill HK name / preserve client name / backfill endpoint） |

#### 验证

- [x] HK watchlist `Name`: backend `GET /watchlist/registry` 已返回 `HK:01810 → 小米集团-W`、`HK:00700 → 腾讯控股`
- [x] HK watchlist `Industry`: `POST /sync/hk-industry?symbols=01810.HK` 调用接口就绪（雪球限流当下走 retry→返回 None 时 ok=False；雪球放行即可填）
- [x] `upsert_from_dataframe(df, keep_industry=True)` 用 `COALESCE(stock_basic.industry, EXCLUDED.industry)` — 验证手动 UPDATE industry 后跑 sync_hk_basic 不被清空
- [x] `POST /watchlist/registry` 自动回填缺失的 name（HK/CN/ETF 全覆盖）
- [x] `POST /watchlist/registry/backfill-names` 一键运维：填 3 个 null → 返回 `updatedCount=2`（不存在的 CN:999999 仍 null）
- [x] pytest 896 + vitest 344 全绿
- [x] `/sync/hk-industry/status`: `totalHk=2767 / mappedHk=0 / missingHk=2767`（雪球首次实跑即可填）

---

## P0 — API 开放（OPT-045 起 · Phase A 在跑）

### OPT-045：OpenAI 兼容 `/v1/*` + AI 助手可发现性

**状态**：[ ] Phase A done · Phase B/C 待开  
**优先级**：P0  
**关联 todo**：[§3 API 开放 P0](../todo.md) · [§12 实施清单 #1](../todo.md)  
**关联设计稿**：[`docs/designs/api-contract.md`](../designs/api-contract.md) · [`docs/designs/freelancer-architecture.md`](../designs/freelancer-architecture.md)

#### 背景

Karios 与外部 AI 助手（用户独立项目）唯一的桥是 OpenAI 兼容 `/v1/*`。
但 API 会经常改 —— AI 助手需要**稳定发现性 endpoint** 自己查当前怎么调，不靠人手维护外部文档。

完整 4-5 天 OPT 拆为 3 个 Phase：

| Phase | 范围 | 预计工时 |
|-------|------|----------|
| **A（本 OPT）** | 4 个稳定发现性 endpoint + API Key 鉴权 + version 常量 | 1.5-2 天 |
| B（OPT-046） | 业务 endpoint：`/v1/market/snapshot` + `/v1/watchlist/items` + `/v1/decision-journal/query` | 2-3 天 |
| C（OPT-047） | `/v1/explain/{symbol}` + `docs/api/` 人类可读文档 + `version bump` 脚本 | 1 天 |

#### Phase A 目标

- 暴露 4 个**稳定发现性 endpoint**（路径不变）：
  - `GET /v1/version` → `{version, min_compatible, released_at}`
  - `GET /v1/schema` → OpenAPI 3.1 JSON（FastAPI 自动生成）
  - `GET /v1/errors` → 错误码字典（`{code, http_status, meaning, recovery_hint, since}[]`）
  - `GET /v1/changelog?since=...` → 接口变更 diff（Phase A 先返回空数组）
- API Key 鉴权中间件（`KARIOS_API_KEYS` 环境变量，逗号分隔多 Key）
- `KARIOS_API_VERSION` 常量（init 时 "0.1.0"，每次改动走 bump 脚本）
- `Authorization: Bearer <key>` 缺/错 → 401
- 不动现有 16 个 router（兼容性零风险）

#### 文件范围

| 层 | 文件 |
|----|------|
| Config | `services/data-sync-service/src/data_sync_service/config.py`（加 `karios_api_version` + `karios_api_keys`）|
| API | `services/data-sync-service/src/data_sync_service/api/auth.py`（**新** — API Key 鉴权依赖）|
| API | `services/data-sync-service/src/data_sync_service/api/discovery_routes.py`（**新** — 4 个稳定 endpoint）|
| App | `services/data-sync-service/src/data_sync_service/main.py`（include + 加 dependency）|
| Tests | `services/data-sync-service/tests/test_discovery_endpoints.py`（**新** — 4 endpoint + 401 + schema 完整）|

#### 验证

- [x] `GET /v1/version` 返回 200 + JSON（无 API Key 也能访问——稳定性 > 鉴权）
- [x] `GET /v1/schema` 返回 200 + OpenAPI 3.1 JSON（包含现有所有 router + 新 4 个）
- [x] `GET /v1/errors` 返回 200 + 至少 1 个示例错误码
- [x] `GET /v1/changelog` 返回 200 + `{changes: []}`（Phase A 暂不实现 git diff）
- [x] 业务 endpoint（`/watchlist/registry` 等）缺 API Key → 200（保持现状，不破坏现有前端）
- [x] 业务 endpoint 错 API Key → 401（auth 启用时）
- [x] pytest `test_discovery_endpoints.py` 全绿（**17/17 passed** in 1.36s）
- [x] pytest `test_api.py` 无 regression（**19/19 passed**）

#### 反模式

- ❌ 改现有 16 个 router 的路径名（破坏现有前端）
- ❌ 把 4 个稳定 endpoint 加 API Key 鉴权（AI 助手**启动时**就要调，加 Key 会死锁）
- ❌ 让 `/v1/schema` 返回手写 JSON（永远 `app.openapi()`）
- ❌ 在 Phase A 实现 git diff 解析（放到 Phase C）

### OPT-046：OpenAI 兼容 `/v1/*` 业务 endpoint（Phase B）

**状态**：[x] done  
**完成日期**：2026-08-01  
**优先级**：P0  
**关联 todo**：[§3 API 开放 P0](../todo.md) · [§12 实施清单 #1](../todo.md)  
**前置**：[`OPT-045` Phase A done](./optimization-checklist.md#opt-045openai-兼容-v1--ai-助手可发现性)
**关联设计稿**：[`docs/designs/api-contract.md`](../designs/api-contract.md)

#### 背景

OPT-045 Phase A 给 AI 助手提供了"稳定的 4 个发现性 endpoint"。但 AI 助手**实际想调**的是业务数据。本 OPT 暴露 3 个只读业务 endpoint，作为 AI 助手"自服务"基础。

#### 目标

3 个**只读**业务 endpoint（**禁止改仓**；写操作走现有 `/watchlist/*` / `/execution/*` 路径）：

| Endpoint | 包装现有 | 用途 |
|----------|----------|------|
| `GET /v1/market/snapshot?symbols=...` | `query_routes /market/stocks/trendok` + `/market/stocks/{symbol}/bars` | AI 助手一次拿 N 个标的的 TrendOK / Score / 当前价 / 关键指标 |
| `GET /v1/watchlist/items` | `watchlist_routes /watchlist/registry` | AI 助手拿当前 watchlist（含 `positionPct` / `costPrice` / Action / Trigger / HardStop）|
| `GET /v1/decision-journal/query?since=...&limit=...` | `execution_journal_routes /execution/changes` | AI 助手拿近期决策变更（Why 码 + 触发时间）|

**关键设计约束**（继承自 `api-contract.md`）：

- **只读**；改仓走现有 API，不在 `/v1/*` 暴露
- 字段 `description` 写人话（给 LLM 看的）
- 每个 endpoint 加 `asOfDate` 字段（数据新鲜度）
- 鉴权依赖 `require_api_key`（**opt-in**：未配 KARIOS_API_KEYS 时仍可访问，与现有前端一致）
- 路径前缀 `/v1/`，独立 router（与 `discovery_routes` 区分）

#### 文件范围

| 层 | 文件 |
|----|------|
| API | `services/data-sync-service/src/data_sync_service/api/v1_business_routes.py`（**新** — 3 个 endpoint）|
| App | `services/data-sync-service/src/data_sync_service/main.py`（include router + `dependencies=[require_api_key]`）|
| Tests | `services/data-sync-service/tests/test_v1_business_endpoints.py`（**新** — 3 endpoint + 鉴权 + description 校验 + 错误码）|

#### 验证

- [x] `GET /v1/market/snapshot?symbols=CN:000001,HK:00700` 返回 200 + 数组
- [x] `GET /v1/market/snapshot` 缺 symbols → 422
- [x] `GET /v1/watchlist/items` 返回 200 + `{items, count, asOfDate}`
- [x] `GET /v1/decision-journal/query?since=YYYY-MM-DD&limit=50` 返回 200 + `{changes, asOfDate}`
- [x] 业务 endpoint 缺 API Key（启用鉴权时）→ 401
- [x] `why` 字段在 journal query 中保留（LLM 聚合用）
- [x] `positionPct` 字段在 watchlist items 中保留 null
- [x] pytest 全绿：**18/18**（test_v1_business_endpoints） + 17/17（test_discovery） + 19/19（test_api 无 regression）+ 8/8（alembic）= **62/62**

#### 反模式

- ❌ 暴露任何写操作到 `/v1/*`（写仓由前端 / 现有 API 负责）
- ❌ 字段 description 写"内部代号"（AI 助手无法理解）
- ❌ 复用现有 router 路径（破坏 `api-contract.md` 路径稳定）
- ❌ 跳过 `asOfDate` 字段（AI 助手必须知道数据新鲜度）

---

### OPT-047：`/v1/explain/{symbol}` + 人类可读文档 + version bump（Phase C）

**状态**：[x] done  
**完成日期**：2026-08-01  
**优先级**：P0  
**关联 todo**：[§3 API 开放 P0](../todo.md) · [§12 实施清单 #1](../todo.md)  
**前置**：[`OPT-046` done](./optimization-checklist.md#opt-046openai-兼容-v1--业务端endpointphase-b)
**关联设计稿**：[`docs/designs/api-contract.md`](../designs/api-contract.md)

#### 背景

Phase A 给了 4 个发现性 endpoint，Phase B 给了 3 个业务 endpoint。Phase C 闭合 OPT-045 整圈：

1. **/v1/explain/{symbol}** — 把"AI 助手想解释一个 symbol"需要的**全部素材**打包返回（**Karios 不调用 LLM**；让外部 AI 助手自带 LLM 处理）
2. **人类可读 API 文档** — `docs/api/`（自动从 OpenAPI + 错误码字典 + 接口契约 markdown 生成）
3. **version bump 脚本** — `scripts/bump-api-version.sh`（CI 可调，校验三处 version 一致）

#### 目标

| 任务 | 内容 |
|------|------|
| **A. /v1/explain/{symbol}** | 一次性返回：symbol 基础 + 完整 trendok 字段（scoreParts / stopLossParts）+ 在 watchlist 状态 + 最近 N 条 journal changes。**Karios 不调 LLM**——素材包给 AI 助手自己生成解释 |
| **B. docs/api/** | `README.md` 索引 + `discovery.md` / `business.md` / `explain.md` 人类可读 + `errors.md` 错误码字典 + `CHANGELOG.md` 骨架 |
| **C. version bump 脚本** | `scripts/bump-api-version.sh` 接受 `major` / `minor` / `patch` + 一行 message；写入 `docs/api/CHANGELOG.md`；打印 diff；检查 git 干净 |

#### 文件范围

| 层 | 文件 |
|----|------|
| API | `services/data-sync-service/src/data_sync_service/api/v1_explain_routes.py`（**新**）|
| App | `services/data-sync-service/src/data_sync_service/main.py`（include）|
| Docs | `docs/api/README.md`（**新**）|
| Docs | `docs/api/discovery.md`（**新**）|
| Docs | `docs/api/business.md`（**新**）|
| Docs | `docs/api/explain.md`（**新**）|
| Docs | `docs/api/errors.md`（**新**）|
| Docs | `docs/api/CHANGELOG.md`（**新** 骨架）|
| Script | `services/data-sync-service/scripts/bump-api-version.sh`（**新**）|
| Tests | `services/data-sync-service/tests/test_v1_explain_endpoint.py`（**新**）|

#### 验证

- [x] `GET /v1/explain/CN:000001` 返回 200 + 完整素材包（trendok + watchlist + journal 5 条）
- [x] `GET /v1/explain/UNKNOWN` 仍 200（trendok={}, watchlist.inWatchlist=False, recentChanges=[]）
- [x] `recentChanges` 严格只含该 symbol（其他 symbol 的行被过滤）
- [x] `recentChanges` 最多 5 条（cap 测试守住）
- [x] `recentChangesWindowDays=30` 一致
- [x] `docs/api/` 含 6 份子文档（README + discovery + business + explain + errors + CHANGELOG）
- [x] `scripts/bump-api-version.sh` 接受 major/minor/patch + 一行 message；git 不干净时拒绝
- [x] pytest 全绿：**14/14** explain + 17/17 discovery + 18/18 business + 19/19 test_api（无 regression）+ 8/8 alembic = **76/76**

#### 反模式

- ❌ **Karios 调 LLM** 生成解释（违反"功能不重合"——LLM 解释归外部 AI 助手）
- ❌ 文档里写"实现细节"（文档只描述"怎么调"和"返回什么"）
- ❌ bump 脚本自动 commit（让人 review 后手 commit）
- ❌ `/v1/explain` 路径变化（破坏 AI 助手缓存的 schema）

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | 第六轮审查：OPT-001 ~ OPT-030 全部完成；新增 OPT-031 ~ OPT-040 |
| 2026-06-18 | 第六轮实施完成：OPT-031 ~ OPT-040（backend P0 + frontend P1 Query 收尾） |
| 2026-08-01 | 第七轮规划：OPT-045 `OpenAI 兼容 /v1/* + AI 助手可发现性`（对应 todo §12 #1） |
| 2026-08-01 | OPT-045 Phase A 完成：4 稳定发现性 endpoint + API Key 鉴权 + 17 单测全绿 |
| 2026-08-01 | OPT-046 规划：Phase B — 3 个只读业务 endpoint（/v1/market/snapshot + /v1/watchlist/items + /v1/decision-journal/query） |
| 2026-08-01 | OPT-046 完成：3 个只读业务 endpoint + 18 单测全绿；test_api 无 regression |
| 2026-08-01 | OPT-047 完成：/v1/explain/{symbol} + docs/api/ 6 份人类可读 + scripts/bump-api-version.sh；49 v1/* 单测全绿 |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
