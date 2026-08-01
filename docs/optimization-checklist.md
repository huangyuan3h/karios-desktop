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

---

### OPT-048：Cloudflare Tunnel 部署（todo §12 #2）

**状态**：[x] 脚本骨架 done · 真实端到端验证 pending（需用户装 cloudflared）  
**完成日期**：2026-08-01（脚本/文档/测试）  
**优先级**：P1（todo §12 #2）  
**关联 todo**：[§4 工程与部署 P1](../todo.md) · [§12 实施清单 #2](../todo.md) · [§11 自由人](../todo.md)  
**关联设计稿**：[`docs/designs/cloud-deployment-options.md`](../designs/cloud-deployment-options.md) · [`docs/designs/freelancer-architecture.md`](../designs/freelancer-architecture.md) · [`docs/designs/cloudflare-tunnel-setup.md`](../designs/cloudflare-tunnel-setup.md)（新建）

#### 背景

`/v1/*` 端到端完成（OPT-045/046/047），但 Karios 仍只在 `127.0.0.1:4310` 监听。外部 AI 助手（Telegram 推送代理 / 钓鱼旅行时）无法跨网调。

按 [`docs/designs/cloud-deployment-options.md`](../designs/cloud-deployment-options.md) 决议：**不上云**，**用 Cloudflare Tunnel 把本地服务暴露到公网**，套在自己域名上。

#### 目标

- **Quick Tunnel 脚本**（零配置，立即可用，URL 含 `*.trycloudflare.com`）：`scripts/start-quick-tunnel.sh`
- **Named Tunnel 脚本**（生产模式，套自有域名）：`scripts/setup-named-tunnel.sh`
- **人类可读 setup 文档**：`docs/designs/cloudflare-tunnel-setup.md`（含两种方案 + 域名配置 + 验证步骤 + 回退到 Tailscale Funnel）
- **测试覆盖**：`tests/test_tunnel_scripts.py` —— 脚本存在 + 可执行 + 文档完整 + 路径引用正确（**不真连 Cloudflare**）
- **职责边界明确**：Tunnel 跑在 macOS 上，由用户登录时 launchd 启动（**不进 Docker**）

#### 文件范围

| 层 | 文件 |
|----|------|
| Script | `services/data-sync-service/scripts/start-quick-tunnel.sh`（**新**）|
| Script | `services/data-sync-service/scripts/setup-named-tunnel.sh`（**新**）|
| Design | `docs/designs/cloudflare-tunnel-setup.md`（**新**）|
| Tests | `services/data-sync-service/tests/test_tunnel_scripts.py`（**新**）|
| Todo | `docs/todo.md` §12 #2 标 done + 链接到 archive |

#### 验证

- [x] `bash scripts/start-quick-tunnel.sh` 检测 cloudflared 是否已装；缺时给安装指引（`test_quick_tunnel_help` + 错误路径）
- [x] `bash scripts/setup-named-tunnel.sh --help` 打印 3 步流程（`test_named_tunnel_help`）
- [x] `docs/designs/cloudflare-tunnel-setup.md` 含 4 节：why / quick-tunnel / named-tunnel / 验证 + 回退（`test_setup_doc_has_four_main_sections`）
- [x] `pytest tests/test_tunnel_scripts.py --no-cov` 全绿：**12 passed + 1 skipped**（skipped = cloudflared 未装的 preflight 测试）
- [ ] 真实端到端验证（**等用户装好 cloudflared 后**）：`cloudflared tunnel --url http://127.0.0.1:4310` 拿到 trycloudflare URL → 外部 `curl /v1/version` 返回 200 → 在 `archive/` 写 `opt-048-cloudflare-tunnel-verified.md`

#### 反模式

- ❌ Tunnel 进 Docker（Cloudflare 客户端不该在容器里漂移；macOS launchd 才是 host 守护）
- ❌ 把 cloudflared token 写进 git（用 `~/.cloudflared/` 目录 + 600 权限）
- ❌ 把 quick-tunnel URL 写进 .env（URL 每次重启会变；应该从 cloudflared 启动日志抓）
- ❌ 用 Tunnel 暴露 dev-only 内部服务（`/market/stocks/...` 等）—— 只暴露 `/v1/*` 业务 + `/docs` FastAPI Swagger

---

### OPT-049：Paper-trading 启动（todo §12 #3 / §8 回测）

**状态**：[x] done（v0 — CN only）  
**完成日期**：2026-08-01  
**优先级**：P0（todo §8 收益 / §12 #3 实施）  
**关联 todo**：[§3 收益 / §8 回测](../todo.md) · [§12 实施清单 #3](../todo.md)  
**关联设计稿**：[`docs/designs/freelancer-architecture.md`](../designs/freelancer-architecture.md) · [`docs/designs/api-contract.md`](../designs/api-contract.md)  
**摘要**：[`archive/2026-08-01-opt-049-paper-trading.md`](../archive/2026-08-01-opt-049-paper-trading.md)

#### 背景

`docs/todo.md §8` 旧 BacktestPage 效果差已隐藏。重启前置条件之一：必须有 paper-trading 先于回测——回测容易过拟合，paper-trading 不会。

按 §8 决议：**paper-trading 跑一周 → 拿真实策略表现数据** → 反向给 §3 收益输血。

#### 目标

| 任务 | 内容 |
|------|------|
| **A. 数据模型** | `paper_trades` 表（id, symbol, entry_date, side, entry_price, score/why_at_entry, status, close_date, pnl_pct, holding_days, close_reason）+ 幂等 unique index `(symbol, entry_date, side)` |
| **B. service 层** | `run_intake`（找未跟随的 BUY/ADD 候选 → 落库，幂等）· `run_update`（每日更新 pnl + 触发 v0 关闭条件）· `compute_stats`（胜率 / 平均收益）|
| **C. scheduler** | 2 cron：`paper_trading_intake` 17:40 + `paper_trading_update` 17:45（Asia/Shanghai 工作日）|
| **D. /v1 暴露** | `GET /v1/paper-trades?status=&since=&limit=` + `GET /v1/paper-trades/stats?since=` |
| **E. 测试** | 19 测试（db shape + service 过滤 / 关闭条件 / 幂等 + API shape + 鉴权）|
| **F. Alembic** | `0011_paper_trades` migration + `db/paper_trading.py` `CREATE_SQL` 同步 |

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `services/data-sync-service/src/data_sync_service/db/paper_trading.py`（**新**）|
| Migration | `services/data-sync-service/alembic/versions/0011_paper_trades.py`（**新**）|
| Service | `services/data-sync-service/src/data_sync_service/service/paper_trading.py`（**新**）|
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/paper_trading_intake_job.py`（**新**）|
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/paper_trading_update_job.py`（**新**）|
| Scheduler | `services/data-sync-service/src/data_sync_service/scheduler/__init__.py`（注册 2 cron）|
| API | `services/data-sync-service/src/data_sync_service/api/v1_business_routes.py`（+ `/v1/paper-trades` + `/v1/paper-trades/stats`）|
| Tests | `services/data-sync-service/tests/test_paper_trading.py`（**新** — 19 tests）|
| Tests | `services/data-sync-service/tests/test_alembic_baseline.py`（HEAD_REVISION 更新）|

#### v0 关闭条件

- `pnl_pct <= -5%` → `close_reason="stop_hit"`
- `holding_days >= 5` → `close_reason="max_hold"`

P2 加：`pnl_pct >= +10%`（`target_hit`）+ `score 跌穿`（`score_floor`）+ 离开 watchlist（`pool_exit`）。

#### v0 范围限定

- **CN only**（HK 需 FX + T+0/T+2 结算差异，留 OPT-050+）
- **不复制 live Execution Gate**——paper-trade 用同一 BUY/ADD 触发器（decision journal）+ 同一 entry_price（日线收盘），不重写规则
- **不作为发布决策依据**（避免过拟合）——只作"如果跟着信号走会怎样"的真实数据

#### 验证

- [x] Alembic baseline test + upgrade head idempotent
- [x] `pytest tests/test_paper_trading.py --no-cov` 全绿：**19/19**
- [x] `test_api.py` 无 regression：**19/19**
- [x] 全部 v1 + integration 测试：**107 passed, 1 skipped**（skip = cloudflared 未装的 preflight）

#### 反模式

- ❌ 让 paper-trading 改 live watchlist / live positionPct（只读 source of truth）
- ❌ 用回测框架重写一份 BUY/ADD 规则（必须用 live Execution Gate 同口径）
- ❌ 把 paper-trade stats 当成"发布决策依据"（过拟合风险——paper-trade 是 simulation 而非 reality）
- ❌ 触发关闭条件后没写 `close_reason`（强制枚举值测试守住）

---

### OPT-050：数据源质量审计（todo §12 #4 / §3 收益 / §6 数据源）

**状态**：[x] done  
**完成日期**：2026-08-01  
**优先级**：P0（todo §3 收益 / §12 #4 实施）  
**关联 todo**：[§3 收益 P0](../todo.md) · [§6 数据源 P1](../todo.md) · [§12 实施清单 #4](../todo.md)  
**关联设计稿**：[`docs/designs/data-source-audit-2026-08.md`](../designs/data-source-audit-2026-08.md)（**新**）  
**摘要**：[`archive/2026-08-01-opt-050-data-source-audit.md`](../archive/2026-08-01-opt-050-data-source-audit.md)

#### 背景

按 todo §3 / §6 / §12 #4："现有源'非常杂，质量不高'，评估是否替换/补强"——决定下年要不要续 Tushare 200 + 候选源 ROI 评估。

#### 目标

| 任务 | 内容 |
|------|------|
| **A. 现有源矩阵** | grep 出 codebase 实际使用的所有外部数据源（Tushare / akshare / yfinance / EM push2 / 雪球 / RSSHub），每源标覆盖 + 用途 + 风险 |
| **B. 候选源对比** | 聚宽 / Wind mini / Choice / iFinD / 自建 5 候选 × 5 维度（价格 / 覆盖 / 限频 / 质量 / ROI）|
| **C. 决策** | 续 / 不续 / 加 / 切 → 出每源明确行动 |
| **D. health check 脚本** | `scripts/data-source-healthcheck.sh`（轻量，**不**真连外部；只检查 API key 是否配置）|

#### 决策摘要

| 源 | 决策 | 理由 |
|----|------|------|
| Tushare Pro 200/年 | ✅ **续** | 主力：CN daily / HK basic / fund_basic / industry / index / adj_factor 全覆盖，断了 → 7 个 cron 全废 |
| akshare (Sina HK) | ✅ **保留** | OPT-043 验证最稳；30/30 连续 0 失败，平均 0.12s/call |
| akshare (其他) | ✅ **保留** | 行业资金流主力；本地多源兜底 |
| yfinance | ⚠️ **降级为 backup** | rate-limit 严重；仅作 HK 日线最后兜底 |
| 东方财富 push2 | ✅ **保留** | HK 实时报价兜底（云 IP 被拉黑，本机 OK）|
| 雪球 Xueqiu | ✅ **保留** | HK industry 抓取（mbu 主营）|
| RSSHub | ✅ **保留** | Alpha Radar 新闻源 |
| **聚宽 JQData** | ❌ **不引** | 已有 akshare HK + Tushare CN；聚宽没有不可替代的覆盖 |
| **Wind mini** | ❌ **不引** | 5000+/年贵 25 倍；Tushare Pro 已覆盖卫星仓所有数据需求 |
| **Choice / iFinD** | ❌ **不引** | 同上 + 各自数千/年 |
| **自建爬虫（ego-lite）** | 🔄 **P2 调研** | OPT-051（todo §12 #8）—— 0 成本替代 Chrome TV 抓取 |

#### 文件范围

| 层 | 文件 |
|----|------|
| Design | `docs/designs/data-source-audit-2026-08.md`（**新**）|
| Script | `services/data-sync-service/scripts/data-source-healthcheck.sh`（**新**）|
| Tests | `services/data-sync-service/tests/test_data_source_audit.py`（**新**）|
| Doc | `archive/2026-08-01-opt-050-data-source-audit.md`（**新** — 决策摘要）|
| Doc | `docs/todo.md` §6 + §12 #4 标 done + 链接 archive |

#### 验证

- [x] `docs/designs/data-source-audit-2026-08.md` 含 5 节（现有源 / 候选 / 决策 / ROI / 反原则）
- [x] `bash scripts/data-source-healthcheck.sh` 在缺 key 时给清晰指引
- [x] `pytest tests/test_data_source_audit.py --no-cov` 全绿
- [x] todo.md §12 #4 标 ✅ + 链接到 archive

#### 反模式

- ❌ 一年一审（成本只 200，但断了会同时废 7 个 cron）→ **保留 6 月 1 日做一次轻审**
- ❌ 看到别人用 Wind 就跟（贵 25 倍，**没有不可替代的覆盖**）
- ❌ 多个源同质数据做"双保险"（维护成本翻倍；不如让 1 个主源 + 1 个真正互补的 backup）
- ❌ 自建爬虫作为主力（0 成本但维护累；做兜底 OK，做主力 → 后期崩）

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
| 2026-08-01 | OPT-048 脚本骨架完成：scripts/start-quick-tunnel.sh + setup-named-tunnel.sh + docs/designs/cloudflare-tunnel-setup.md + 12 单测全绿（真实端到端验证等用户装 cloudflared）|
| 2026-08-01 | OPT-049 完成：paper-trades 表 + 2 cron + 2 /v1 endpoint + 19 单测全绿（Alembic 0011 + service 关闭条件 + 幂等 intake）|
| 2026-08-01 | OPT-050 完成：数据源审计 + 5 候选对比 + healthcheck 脚本 + 13 单测全绿（决策稿 → `docs/designs/data-source-audit-2026-08.md`）|
| 2026-08-01 | OPT-051 / §12 #5 完成：API Key 多 Key + 三窗口滑动配额（in-mem）+ /v1/quota + FastAPI metadata + openapi_tags + Swagger/Redoc + 8 节人类可读 `docs/api/openapi.md`；23+11 单测全绿（向后兼容旧 KARIOS_API_KEYS 格式）|
| 2026-08-01 | OPT-052 / §12 #6 完成：Alpha Radar 扩展 HK 标的识别（ai-service 加 hk_mapping 字段 + python resolve_hk_mapping + trend_json.hkSymbols + aggregate_catalyst_stocks 合并 CN+HK + compute_alpha_additions 跳过 HK EM industry 闸门）；13+1 单测全绿 |
| 2026-08-01 | OPT-053 / §12 #10 完成：DB 走向决策（5 选项对比 + 备份 3 副本策略 + 6 触发条件 + 半年期复审 2027-02-01）；决策真值 → `docs/designs/db-direction-2026-08.md`；零代码改动，纯文档拍板 |
| 2026-08-01 | **OPT-054 触发**：用户 review 后明确真痛点（换电脑 / 长期生命力 / 远程访问）→ 新建 `docs/designs/karios-longevity-2026-08.md`（系统级真值），§13 todo；DB 决策保持 + 加 §13 行动项（Docker / Neon 副本 / Tailscale / 临时 VM）|
| 2026-08-01 | **OPT-055 / §14 立**：用户 review 后暂缓远程部署项（§13 #1/#2/#3）+ 提级 §14 #1 AI agent 打通 + §12 #8 ego-lite 调研；新建 `docs/integrations/ai-agent-cookbook.md`（目标）|
| 2026-08-01 | **OPT-055 §14 #1 完成**：AI Agent 集成 cookbook 10 节（4 步启动 + 4 场景 + 错误处理 + 配额监控 + Python/Node client + 上线 checklist + FAQ）；`docs/integrations/ai-agent-cookbook.md` |
| 2026-08-01 | **OPT-056 触发**：用户 §12 #7 启动 — Docker 一键起 + UPS 自动恢复；§13 longevity 真痛点；范围锁定（Dockefile ×3 + 4 compose 服务 + 5 脚本 + 1 文档 + 1 测试）|

### OPT-056：Docker 一键起 + UPS 自动恢复（todo §12 #7 / §13 / [Mac mini 时代方案](../designs/mac-mini-deployment.md)）

**状态**：[x] 完成（脚本骨架 + Dockerfiles + compose + tests + docs；端到端实跑需 Docker Desktop；**作为 Mac mini 时代的部署方案就绪，不是当前日常开发工具**）
**完成日期**：2026-08-01
**优先级**：P0（todo §13 longevity · Mac mini 时代整体部署的底座）
**关联 todo**：[§13 Longevity](../todo.md) · [§12 实施清单 #7](../todo.md) · [§13.1 Mac mini 时代整体部署](../todo.md)
**关联设计稿**：[`docs/designs/mac-mini-deployment.md`](../designs/mac-mini-deployment.md)（完整架构 + 触发条件 + 实施时序） · [`docs/designs/karios-longevity-2026-08.md`](../designs/karios-longevity-2026-08.md) §3.1 / §3.2 / §3.5
**摘要**：[`archive/2026-08-01-opt-056-docker-one-click.md`](../archive/2026-08-01-opt-056-docker-one-click.md)

> ⚠️ **使用边界**：日常开发仍用 `pnpm dev`（OPT-056 文档 §"与开发模式的区别"）。Docker 栈是为 **Mac mini 长期开机部署** 准备的——用户拿到 Mac mini 那一天按 [`mac-mini-deployment.md`](../designs/mac-mini-deployment.md) §5 实施。
>
> **Mac mini 时代的 diff**（相对于当前实现）：
> - 移除 compose 里的 `postgres` + `migrate` service
> - `data-sync` 改连 `host.docker.internal:5432`（本地 PG）
> - 单源数据 = brew services 起的本地 Postgres
> - 见 [`mac-mini-deployment.md` §4](../designs/mac-mini-deployment.md)

#### 背景

用户原话："我关心的无非换电脑也能正常跑这个系统，让这个系统长期有生命力，远程也能访问这几个痛点。" 当前 §13 已落地决策真值，但 Docker / UPS 都是绿地。

当前实情（2026-08-01 调研）：
- 仓库无任何 `Dockerfile` · `docker-compose.yml` 只含 `postgres / pgadmin / rsshub` 三个基础设施
- `data-sync-service` 是 FastAPI + uv（Python 3.13），dev 监听 `127.0.0.1:4330`，**容器内需改 `0.0.0.0`**
- `ai-service` 是 Hono + tsc（Node 22），dev 监听 `4310`，`/healthz` 只验进程
- `desktop-ui` 是 Next.js 16.1 + `output: "export"`，**生产形态是静态文件 → 必须配 nginx 容器**
- `@karios/shared` 是 monorepo workspace package → Node Dockerfile 不能只 COPY 单 app
- 当前 `init-scripts` 挂载路径不存在 → 即使 Postgres 启动后也无 schema
- 无任何 launchd plist、无任何 UPS 脚本

#### 目标

| 任务 | 内容 |
|------|------|
| **A. Dockerfile × 3** | `data-sync-service` (python:3.13-slim + uv) · `ai-service` (node:22-alpine + pnpm) · `desktop-ui` (node:22 build stage → nginx:alpine runtime) |
| **B. 扩展 docker-compose.yml** | 4 新服务：`data-sync`、`ai-service`、`desktop-ui`、`migrate`（一次性 init）+ healthchecks（`pg_isready` / `curl /healthz`）+ `depends_on: service_healthy` |
| **C. 一键脚本 × 3** | `scripts/docker-up.sh` / `docker-down.sh` / `docker-status.sh` —— 唯一入口，**用户只需要会 `docker-up.sh`** |
| **D. LaunchAgent** | `scripts/install-launchd.sh` 安装 `~/Library/LaunchAgents/com.karios.docker-up.plist`（macOS 登录后跑 docker-up.sh）+ `uninstall-launchd.sh` |
| **E. UPS shutdown hook** | `scripts/ups-shutdown.sh` —— 由外部监控（`nut` 或 APC `apcupsd`）调 `lowbattery` 时触发：`docker compose down` → `pmset shutdown now`；**Karios 自身不做电池监控** |
| **F. 文档** | `docs/setup/docker-one-click.md`：前置条件 → `pnpm install:all` → `cp .env.example .env` → `scripts/docker-up.sh` → 访问 `http://localhost:8080` → 可选 `scripts/install-launchd.sh` |
| **G. 测试** | `tests/test_docker_one_click.py`：所有脚本存在 + `bash -n` 通过 + `--help` 返回 0 + plist XML 用 `plutil -lint` 校验 + `.env.example` 包含所有必备 key |

#### 文件范围

| 层 | 文件 |
|----|------|
| Dockerfiles | `services/data-sync-service/Dockerfile`（**新**）· `apps/ai-service/Dockerfile`（**新**）· `apps/desktop-ui/Dockerfile`（**新**）|
| Compose | `docker-compose.yml`（**改**——加 4 service + healthchecks + 默认 network）|
| Ignore | `.dockerignore`（**新**）|
| Scripts (root) | `scripts/docker-up.sh`（**新**）· `scripts/docker-down.sh`（**新**）· `scripts/docker-status.sh`（**新**）· `scripts/install-launchd.sh`（**新**）· `scripts/uninstall-launchd.sh`（**新**）· `scripts/ups-shutdown.sh`（**新**）|
| Docs | `docs/setup/docker-one-click.md`（**新**）· `.env.example`（**新**——根目录模板）|
| Tests | `services/data-sync-service/tests/test_docker_one_click.py`（**新**）|

#### 范围限定（**不**做的事）

- ❌ **不**把 `.env` 真实 secret COPY 进 image；只用 `env_file:` 挂载 + compose `secrets:` 留作 OPT-056.x
- ❌ **不**改 `data-sync-service` 默认端口（仍 `4330`）—— 一致性 vs 漂移：代码 / Tauri / Compose 全部统一
- ❌ **不**改 `ai-service` 默认端口（仍 `4310`）
- ❌ **不**自建 UPS 电池监控（macOS 无原生 API）—— 只提供 hook 脚本，由 `nut` / `apcupsd` 调用
- ❌ **不**做生产 TLS —— Cloudflare Tunnel 已处理
- ❌ **不**改 Tauri 桌面（Tauri 已在 §13 降级，独立路线）
- ❌ **不**改 TV Chrome capture 路径（macOS 仍走宿主 Chrome + `host.docker.internal:9222`）
- ❌ **不**改 `desktop-ui` 的 `next.config.ts` 的 `output: "export"`（这是 Tauri 与 Docker 唯一的共同基线）
- ❌ **不**让 Compose 内的容器互调 `localhost` —— Data Sync 的 `DATABASE_URL` 必须是 `postgres:5432`，且文档明确写出

#### 验证

- [x] `bash -n` 全部新脚本通过（6/6：`docker-up.sh` / `docker-down.sh` / `docker-status.sh` / `install-launchd.sh` / `uninstall-launchd.sh` / `ups-shutdown.sh`）
- [x] `python3 -m pytest tests/test_docker_one_click.py --no-cov` 全绿：**57/57**
- [x] `plutil -lint` LaunchAgent plist 通过（实际生成 `/Users/huangyuan/Library/LaunchAgents/com.karios.docker-up.plist` 通过 lint）
- [x] `docker compose config --quiet` 解析新 compose 无误（7 services：postgres / pgadmin / rsshub / data-sync / ai-service / desktop-ui / migrate）
- [x] `docker compose config --images` 显示 4 个 build image（karios/data-sync-service, karios/ai-service, karios/desktop-ui）+ 3 个 pull image
- [x] Dockerfile 全部 pinned（无 `:latest` 标签；`postgres:16-alpine` / `python:3.13-slim-bookworm` / `node:22-alpine` / `nginx:1.27-alpine`）
- [x] `.env.example` 覆盖所有 compose 引用 key（`POSTGRES_USER/PASSWORD/DB/PORT`、`TU_SHARE_API_KEY`、`AI_SERVICE_PORT/DATA_SYNC_PORT/DESKTOP_UI_PORT/RSSHUB_PORT/PGADMIN_PORT`、`NEXT_PUBLIC_*`、`KARIOS_API_KEYS`）
- [x] install-launchd.sh 实际安装并加载 LaunchAgent（已在用户机器上验证：`launchctl list` 显示 `com.karios.docker-up`，PID 1）
- [x] ai-service + desktop-ui typecheck/lint 不受影响（126 tests 全绿，tsc 无 error）
- [ ] 端到端 `docker compose up -d` 实跑：未在本次 session 跑通（build 全栈需 5-10 分钟，且需要先 `--migrate` 停掉旧 orphan 容器 `postgres-db` / `pgadmin-web` / `karios-rsshub`）。脚本已就绪，**用户首次实跑前必须 `scripts/docker-up.sh --migrate` 一次**。
- [ ] 真实 Chrome capture 路径（`host.docker.internal:9222`）：需用户在 macOS 宿主上手动启动 Chrome `--remote-debugging-port=9222`。已记入 setup doc。

#### 反模式

- ❌ **不**用 `:latest` 镜像标签（postgres:16-alpine / node:22-alpine / nginx:1.27-alpine 全部钉死）
- ❌ **不**用 `network_mode: host`（破坏 `depends_on` healthcheck 链路）
- ❌ **不**在 `docker-up.sh` 里 echo 任何 secret 值（与 OPT-051 一致）
- ❌ **不**让 launchd plist 跑在 root（LaunchAgent 必须用户级 → 不需要 sudo）
- ❌ **不**让 `ups-shutdown.sh` 默认开机自启 —— 必须显式 `enable` 才挂载（防误关）
- ❌ **不**在 Desktop UI Dockerfile 里跑 `next dev`（只 build static + nginx serve）
- ❌ **不**给 AI service 容器持久化 secret 文件（`KARIOS_APP_DATA_DIR` 卷挂载由用户在 `docker-up.sh` 设置，默认关闭）
- ❌ **不**修改既有 `docker-compose.yml` 的 postgres 凭据 / `init-scripts` 路径（保留向后兼容；如需换 dev 凭据 → 单独 PR）

---

### OPT-057：TV Capture 三轨架构 + 新建 screener 模板化（todo §12 #8.5 / §3 收益 / §6 数据源）

**状态**：[x] 完成（40 新单测 + 1055 全绿 · 0 regression · 5 模板 live API 验证通过）
**完成日期**：2026-08-01
**优先级**：P1（todo §3 收益 / §6 数据源双线收益；3 个主 screener 数据源稳定性）
**关联 todo**：[§12 #8.5](../todo.md) · [§3 收益](../todo.md) · [§6 数据源](../todo.md)
**关联设计稿**：[`docs/designs/tv-capture-data-source-2026-08.md`](../designs/tv-capture-data-source-2026-08.md)（落地决策） · [`docs/designs/ego-lite-spike-2026-08.md`](../designs/ego-lite-spike-2026-08.md)（Phase 1 spike）
**摘要**：[`archive/2026-08-01-opt-057-tv-capture-three-track.md`](../archive/2026-08-01-opt-057-tv-capture-three-track.md)

#### 背景

ego-lite spike（2026-08-01）确认 TV Scanner API 可用。本 OPT 把 spike 转成主线产品：**三轨架构（API / ego-lite / Chrome）按 screener 调度**，新建 screener 流程模板化（消除"用户必须先去 TV 网站存 screener"的认知负担）。

#### 范围

1. **DB schema**：加 `mode` / `filter_json` / `api_columns` 3 列（`url` 改 nullable）
2. **`tv/scanner_api.py`**：POST `scanner.tradingview.com/global/scan`，返回结构化结果
3. **`tv/ego_lite.py`**：Playwright headless chromium（无 Chrome profile）抓 screener URL
4. **`service/tv.py` dispatcher**：按 `mode` 调度 + 失败 fallback 链 `api → ego_lite → chrome`
5. **`tv/templates.py`**：5 个内置 screener 模板（Karios Pullback v3 CN/HK/US / Falcon Launch / Industry Top5）
6. **前端 `SettingsPage`**：新建 screener 三模式（Template / Custom URL / Filter JSON）+ mode 切换 + JSON 编辑器
7. **`shared/schemas/tvCapture.ts`**：扩展 `TvScreener` schema 加 `mode` / `market` / `filterJson` / `apiColumns`
8. **测试**：单测 `test_tv_scanner_api.py`(17) + `test_tv_ego_lite.py`(3) + `test_tv_dispatcher.py`(9) + `test_tv_templates.py`(9) + `test_migrate_screeners_to_api_mode.py`(9) = 47 tests

#### 反模式

- ❌ **不**完全砍 Chrome（保留 6 个月 fallback；ego-lite spike 决策）
- ❌ **不**自动 fallback 后静默改写 snapshot `payload.mode`（保持 `payload.capturedVia` 字段可审计）
- ❌ **不**改 TV screener URL 的解析路径（仍走 Playwright，只是换 driver）
- ❌ **不**在模板里塞所有 TV Screener 可选字段（只暴露稳定字段）
- ❌ **不**让 `filter_json` 接受任意 TV 内部结构（只接受验证过的字段白名单）
- ❌ **不**改 `screenTitle` 的"合同"语义（TIP-006 合同：仍手工构造 `Karios Pullback` 等子串）
- ❌ **不**在 dispatcher 里把 fallback 链写成"全部试一遍"（按 `mode` 决定初始入口，失败一次只降一级）

#### 验证

- [x] Alembic `0012_tv_screeners_api_mode.py` 升级成功（`alembic upgrade head`）
- [x] `db/tv.py` CREATE_SQL 同步新列（空 DB parity）
- [x] 5 个主 screener 注册到新模板表 + live API 验证通过（`scripts/preview_screener_template.py`）
- [x] `_capture_via_api` 端到端走通：5 模板均返回 100 行数据
- [x] `_filters_from_filter_json` 支持数组格式 filter JSON
- [x] `docs/modules/screener.md` 更新"为什么使用 CDP"节 → "三轨架构"
- [x] 单元测试：47 tests 全绿 + 1055 total（含 OPT-056）
- [x] OPT-057 完成后写 `archive/2026-08-01-opt-057-tv-capture-three-track.md`

#### 已知限制

- TV Scanner API 是 undocumented internal API，无 SLA/contract。失败视为 transient，触发 fallback。
- Filter JSON 必须是数组格式（`[{left, operation, right}, ...]`），不支持 `{"and": [...]}`。
- Column-to-column 比较（如 `close > EMA20`）在 nullable 列上会报错。模板仅用标量比较。
- ego-lite 需要用户手动 `playwright install chromium`。
- HK 股票在 TV API 中 `country = "China"`（不是 "HK"），US 股票 `country = "United States"`（不是 "US"）。

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)
