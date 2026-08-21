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

**状态**：[x] 完成（40 新单测 + 1055 全绿 · 0 regression · 5 模板 live API 验证通过）→ **2026-08-12 整体退役**
**完成日期**：2026-08-01
**退役说明**：universe 全市场化后 TV 无核心消费方，全部代码/UI/路由/cron 已剥离（历史数据保留只读）；详见 todo「TV screener 全功能下线」
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

### OPT-058：漏斗 N 日表格（TIP-002 收尾）+ Paper-trading v0.1 关闭条件

**状态**：[x] done
**完成日期**：2026-08-02
**优先级**：P0（todo §12 #20/#21 · 2026-08-02 review 最高 ROI 两件）
**关联 todo**：[§12 #20](../todo.md)（漏斗 N 日转化率表格）· [§12 #21](../todo.md)（paper v0.1 关闭条件）· [TIP-002](../trading-improvement-checklist.md) · [OPT-049](./optimization-checklist.md#opt-049paper-trading-启动todo-12-3--8-回测)
**摘要**：[`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](../archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)

#### 背景

TIP-002 埋点已就绪（ack `meta.funnel`），但 N 日表格从未落地——整个 TIP 系列改动有效与否无法周度复盘；OPT-049 v0 只有 `stop_hit` / `max_hold` 两条关闭路径，正在积累的 paper 数据统计口径不完整（+10% target / score 跌穿 / 池内剔除缺失）。

#### 目标

| 任务 | 内容 |
|------|------|
| **A. 漏斗 N 日表格** | `GET /watchlist/automation/runs?limit=N`（每交易日一行的 ack run + `meta.funnel`，DISTINCT ON trade_date）+ shared Zod + `useFunnelHistoryQuery` + `FunnelHistoryTable` 挂 WatchlistPage |
| **B. paper v0.1 关闭条件** | `target_hit` (+10%) / `score_floor` (<30) / `pool_exit`（不在 watchlist registry）；优先级 stop > target > score_floor > pool_exit > max_hold；score/registry 数据缺失 fail-open 不关闭 |

#### 文件范围

| 层 | 文件 |
|----|------|
| DB | `db/watchlist_automation.py`（`list_recent_runs` + `fetch_latest_score_since`）、`db/paper_trading.py`（CLOSE_REASONS + 阈值常量） |
| Service | `service/watchlist_automation.py`（`get_automation_runs`）、`service/paper_trading.py`（`_pick_close_reason`） |
| API | `api/watchlist_routes.py`（`GET /watchlist/automation/runs`，**须注册在 `{run_id}` 路由前**）、`api/v1_business_routes.py`（closeReason description 同步） |
| Shared | `packages/shared/src/schemas/watchlist.ts`（FunnelHistoryResponse） |
| Frontend | `lib/queries/funnel.ts`（新）、`components/watchlist/FunnelHistoryTable.tsx`（新）、`lib/watchlist-automation.ts`（`fetchFunnelHistory` + 导出 `funnelFromMeta`）、`WatchlistPage.tsx` |
| Tests | `tests/test_paper_trading.py`（+8）、`tests/test_funnel_history.py`（新 4）、`FunnelHistoryTable.test.tsx`（新 5） |

#### 验证

- [x] `GET /watchlist/automation/runs` 返回 `{ok, runs, asOfDate}`；`runs` 字面路径优先于 `{run_id}` 动态路由（单测守住）
- [x] paper v0.1 五条关闭条件 + 优先级（stop 压 target、target 压 max_hold）+ 两处 fail-open 单测
- [x] 后端 50 相关测试全绿；全量 1316 passed（1 pre-existing flaky 除外）
- [x] 前端 FunnelHistoryTable 5 测试全绿 + `tsc --noEmit` 0 error + shared build 通过
- [x] todo §12 #20/#21 标 ✅ + archive 摘要 + §10 补行

#### 反模式

- ❌ 把 `runs` 注册在 `GET /watchlist/automation/{run_id}` 之后（FastAPI 按注册顺序匹配 → "runs" 被当 run_id → 404）
- ❌ score / registry 数据缺失时硬关闭 paper 仓（必须 fail-open，与"主线数据未就绪不误 TRIM"同哲学）
- ❌ 同一天多次 automation 在表格里占多行（DISTINCT ON trade_date 收敛为最新一次）
- ❌ 改 `close_reason` 枚举而不同步 `/v1/paper-trades` description（LLM 契约漂移）

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Agent 指南](../AGENTS.md)

---

### OPT-060：形态迁移 · Tauri 降级（todo §12 #11 / §2 形态决策 / §4 P2）

**状态**：[x] 完成（活跃 dev 路径 + 顶层文档同步 + 5 单测；`src-tauri/` Rust 源码 + sidecar build 脚本按 "保留 build 配置" 不动）
**完成日期**：2026-08-04
**优先级**：P0（todo §2 形态决策两条 P0 · §12 #11 · 长期减少维护面）
**关联 todo**：[§2 形态决策](../todo.md) · [§4 P2 Tauri 构建降级](../todo.md) · [§12 实施清单 #11](../todo.md)
**关联设计稿**：[`docs/designs/mac-mini-deployment.md` §2.3](../designs/mac-mini-deployment.md)（vs Tauri 桌面形态对比）· [`docs/designs/karios-longevity-2026-08.md` §3](../designs/karios-longevity-2026-08.md)（§12 #11 在 longevity 行动清单中）
**摘要**：[`archive/2026-08-04-opt-060-tauri-deprecation.md`](../archive/2026-08-04-opt-060-tauri-deprecation.md)

#### 背景

`apps/desktop-ui/src-tauri/` Tauri v2 build 完整就绪（Cargo.lock + sidecar 编译脚本 + 4 个 macOS sidecar 已 commit），但 Tauri 桌面形态已与 Karios 长期方向（`pnpm dev` + Docker compose 一键起 + Cloudflare Tunnel 远程）不重合：

- 用户 2026-08-01 review §13 时明确："我倾向于系统整体稳定部署 docker，然后自动启动，db 用现在的，不用两个"
- Tauri 桌面适合"笔记本 + 单人 + 出差"；当前路径是"家里 7×24 跑 + 多端访问"
- `/v1/*` OpenAI 兼容 API（OPT-045~047）已通，AI agent 走 web + Tunnel 即可，不需要桌面 client
- Tauri 维护面：Rust toolchain + Tauri CLI + sidecar 编译（PyInstaller + Bun compile）+ 跨平台 bundling —— 与"长期减少维护面"目标冲突

#### 目标

| 任务 | 内容 |
|------|------|
| **A. 移除活跃 dev 路径** | 根 `package.json` 删 `predev:tauri` / `dev:tauri`；删 devDep `concurrently`（仅被 `dev:tauri` 使用）；`apps/desktop-ui/package.json` 删 `tauri` / `tauri:dev` / `tauri:build`；删 dep `@tauri-apps/api`（src 内零引用）；删 devDep `@tauri-apps/cli` |
| **B. 同步顶层文档** | `README.md` 行 163（表行）+ 行 179（Rust 工具链）；`AGENTS.md` 表行；`docs/README.md` 子项目行；`docs/setup/docker-one-click.md` "与开发模式的区别" 表删 dev + tauri 行；`apps/desktop-ui/next.config.ts` 注释改 "Static export required by Docker nginx"；`services/data-sync-service/Dockerfile` 注释 "live/Tauri conventions" → "live conventions" |
| **C. 保留 build 配置** | `apps/desktop-ui/src-tauri/` 整个目录（`Cargo.toml` / `Cargo.lock` / `tauri.conf.json` / `tauri.backends.conf.json` / `build.rs` / `src/lib.rs` / `src/backends.rs` / `src/main.rs` / `icons/` / `capabilities/` / `gen/` / `sidecars/`） + `scripts/build-sidecars-macos.sh` —— 按 §2 P0 "**暂保留 build 配置**" 落地 |
| **D. 单测** | `apps/desktop-ui/src/lib/tauri-deprecation.test.ts`：5 tests — 根 + apps/desktop-ui 的 scripts/deps 移除；`src-tauri/` + `scripts/build-sidecars-macos.sh` 仍存在 |

#### 文件范围

| 层 | 文件 |
|----|------|
| 脚本 | `package.json`（根 · **改**） · `apps/desktop-ui/package.json`（**改**）|
| 文档 | `README.md`（**改**） · `AGENTS.md`（**改**） · `docs/README.md`（**改**） · `docs/setup/docker-one-click.md`（**改**）|
| 注释 | `apps/desktop-ui/next.config.ts`（**改**） · `services/data-sync-service/Dockerfile`（**改**）|
| 测试 | `apps/desktop-ui/src/lib/tauri-deprecation.test.ts`（**新**）|

#### 范围限定（**不**做的事）

- � **不**删除 `apps/desktop-ui/src-tauri/` 整个目录（§2 P0 "暂保留 build 配置"）
- ❌ **不**删除 `scripts/build-sidecars-macos.sh`（sidecar 编译入口，Tauri 复活时唯一线索）
- ❌ **不**手工改 `pnpm-lock.yaml`（下次 `pnpm install` 自动清掉 `@tauri-apps/*` + `concurrently` 块）
- ❌ **不**改 `apps/desktop-ui/eslint.config.mjs` 的 `src-tauri/target/**` ignore（src-tauri/ 仍存在，target/ 仍需忽略）
- ❌ **不**回写 OPT-056 历史记录（反模式节里 "代码 / Tauri / Compose 全部统一" 等是当时 scope-bound 的真实约束）
- ❌ **不**改 `docs/designs/*` 三份设计稿里"vs Tauri 桌面"的对比章节（这些是历史决策依据，删掉会让"为什么不上 Tauri"不可考）
- ❌ **不**做"完整删除 Cargo.lock 等" —— 同上，保留 = 未来复活时省 0.5 天接入

#### 验证

- [x] 根 `package.json` 无 `tauri` script；无 `concurrently` devDep
- [x] `apps/desktop-ui/package.json` 无 `tauri` / `tauri:dev` / `tauri:build` script；无 `@tauri-apps/api` dep；无 `@tauri-apps/cli` devDep
- [x] `apps/desktop-ui/src/` 内无 `@tauri-apps` 引用（grep 验证）
- [x] `apps/desktop-ui/src-tauri/` 目录完整保留（`Cargo.toml` + `Cargo.lock` + `tauri.conf.json` + `src/lib.rs` + `src/backends.rs` + icons + sidecars）
- [x] `scripts/build-sidecars-macos.sh` 完整保留
- [x] `apps/desktop-ui/src/lib/tauri-deprecation.test.ts` 5/5 tests 全绿
- [x] 前端 typecheck 干净（`pnpm typecheck`）
- [x] 前端 lint 0 error（`pnpm lint`）
- [x] 后端 pytest 未受影响（仅改 1 行 Dockerfile 注释）
- [x] todo §1 / §2 / §4 / §12 #11 / §10 同步更新

#### 反模式

- ❌ **不**只删 `pnpm dev:tauri` 不删 `predev:tauri`（用户跑 `pnpm dev:tauri` 时 `ensure-ports` / `ensure-rsshub` 不跑，会撞端口）
- ❌ **不**只删 scripts 不删 deps（`@tauri-apps/cli` 在 devDep 但无 script 调用 = 死代码）
- ❌ **不**为"清理彻底"把 `src-tauri/` 整个删了（违反"暂保留 build 配置"）
- ❌ **不**在注释里写"Tauri 已 deprecated"等情绪化字眼（按本文档"保留 build 配置"基调，仅改事实）
- ❌ **不**改 `apps/desktop-ui/next.config.ts` 的 `output: 'export'`（Docker nginx 仍需静态 export；OPT-056 已写"不**改"）

---

### OPT-061：DB 本地备份自动化 + 跨机迁移包（todo §12 #18 / §13 Longevity / §4 P0）

**状态**：[x] 完成（2026-08-04 · 3 脚本 + 1 plist + 1 design + 1 端到端 restore 演练）
**完成日期**：2026-08-04
**优先级**：P0（todo §13 "换电脑也能跑" 的**数据侧**补完；与 §12 #7 Docker 一键起互补）
**关联 todo**：[§12 实施清单 #18](../todo.md) · [§13 Longevity "换电脑也能跑"](../todo.md) · [§4 P0 DB 本地备份自动化](../todo.md)
**关联设计稿**：[`docs/designs/db-backup-and-migrate-2026-08.md`](../designs/db-backup-and-migrate-2026-08.md)（决策真值 · 用户"电脑就休眠"约束的兜底机制）
**摘要**：[`archive/2026-08-04-opt-061-db-backup-migrate.md`](../archive/2026-08-04-opt-061-db-backup-migrate.md)

#### 背景

OPT-053 已立"备份 3 副本策略"，但仓库里**零 backup 脚本 / cron**：

- 用户原话（todo §13）："我关心的无非换电脑也能正常跑这个系统，让这个系统长期有生命力，远程也能访问"
- §12 #7 Docker 一键起解决了**代码侧**（换电脑 2 小时即可起 stack），但**数据侧**（1.7 GB 数据库迁移）无工具
- 用户电脑使用模式：**休眠随用随醒**，不是 7×24 —— launchd `StartCalendarInterval` 在 sleep 时不跑、唤醒后**不会补跑**错过的事件，必须用脚本内 last-age 检查兜底
- iCloud Drive 客户端在 Mac 睡眠时仍同步 → 是 §13 "不上云"约束下唯一能用的"异地副本"

#### 目标

| 任务 | 内容 |
|------|------|
| **A. db_backup.sh** | `pg_dump -Fc -Z 9`（docker exec 进 Postgres 容器）；保留本地 30d + iCloud 14d；TOC 校验失败标 `.corrupt`；last-age 25h 跳过；写 manifest (pg version / table count / size) |
| **B. db_restore.sh** | `docker cp` 进容器后 `pg_restore --jobs=4`；可选 `--drop-existing`；自动跑 `alembic upgrade head`；manifest cross-check 表数 |
| **C. karios_migrate_export.sh** | 调 db_backup.sh 出新 dump → bundle dump + manifest + env.template + restore.sh + checksums.sha256 + README.txt → tar.gz（~244 MB）|
| **D. install-db-backup-launchd.sh + plist** | `com.karios.db-backup` LaunchAgent；StartCalendarInterval 03:00 + Wake=true + RunAtLoad=true + DATABASE_URL env（写到 plist 而不是 shell）；可选 append ~/.zshenv hook；提供 `--status` / `--unload` |
| **E. 休眠兜底** | 三个 trigger 叠加（cron / RunAtLoad / zshenv hook）+ 脚本内 last-age 25h 检查，保证最坏情况（睡眠 N 天后醒来开 shell）也能在 30s 内完成 dump |
| **F. 验证** | (1) round-trip：dump → drop → restore，44 表全 + daily 10.9M 行；(2) 新 Mac 模拟：全新 postgres 容器 + 解 tarball + restore.sh → 00700.HK 2026-08-04 close 487.6 数据完整 |

#### 文件范围

| 层 | 文件 |
|----|------|
| 脚本（data-sync） | `services/data-sync-service/scripts/db_backup.sh`（**新** · ~140 行） · `services/data-sync-service/scripts/db_restore.sh`（**新** · ~170 行） · `services/data-sync-service/scripts/karios_migrate_export.sh`（**新** · ~140 行）|
| 脚本（顶层） | `scripts/install-db-backup-launchd.sh`（**新** · ~180 行 · 模仿 install-launchd.sh 风格）|
| 设计 | `docs/designs/db-backup-and-migrate-2026-08.md`（**新**）|
| 不动 | `services/data-sync-service/src/data_sync_service/**`（无业务代码改动）· `apps/desktop-ui/**`（无前端改动）· `apps/ai-service/**`（无 AI 改动）|

#### 验证（2026-08-04 实测）

- [x] `bash db_backup.sh --dry-run` 输出正确路径；`--force` 端到端 dump 1m33s / 245 MB / iCloud 同步 OK
- [x] `bash db_backup.sh`（无 --force）上次 dump 3s 前 → 正确 skip（last-age 25h 阈值生效）
- [x] `bash db_restore.sh <dump> --drop-existing` drop + pg_restore 21s + alembic upgrade 1s + 表数 44 = manifest 44
- [x] 新 Mac 模拟：`docker run postgres:16-alpine` → 解 tarball → `KARIOS_PG_CONTAINER=karios-migrate-test ./karios_restore.sh ... --drop-existing` → 44 表 + 00700.HK 2026-08-04 487.6 完整
- [x] `plutil -lint` plist OK；`launchctl load -w` 加载 OK；RunAtLoad 触发后 stdio 显示 "skip"（读到 DATABASE_URL + last-age 5 分钟前）
- [x] `bash install-db-backup-launchd.sh --status` LOADED 显示；zshenv hook 默认未安装（需要 TTY 询问）

#### 反模式（不**做）

- ❌ **不**写加密层 —— iCloud Drive 已 E2E 加密；本地副本明文已是事实
- ❌ **不**做 WAL archiving / PITR —— §13 "长期生命力"非"任意秒级回滚"
- ❌ **不**自动上传 S3 —— §13 #1 Neon 副本暂缓；iCloud 2 副本已够
- ❌ **不**做 ZFS / BTRFS snapshot —— docker volume 不适用
- ❌ **不**打包成 .dmg —— tarball + README.txt 已够；新 Mac 上 git clone + pnpm install 即可
- ❌ **不**改 launchd 实现补跑机制（不存在） —— 走"3 trigger + last-age 检查"组合
- ❌ **不**写定时 watchdog —— cron + RunAtLoad + zshenv 已覆盖
- ❌ **不**写测试（脚本 + tarball round-trip 验证已替代） —— 脚本内的 manifest / checksums 是天然校验

---

### OPT-062：Paper v0.2 —— HK 接入 + 成本/滑点建模（todo §16 L3-P1 / §8 回测）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：todo §16 立 L3-P1「度量基座」：paper v0.1 只覆盖 CN 且零成本口径，胜率虚高。v0.2 补两块：(1) 分市场成本模型（滑点/佣金/印花税）——**pnl_pct 重定义为净盈亏**；(2) HK 接入（HK bars 与 CN 同存 `daily` 表，ts_code 如 `00700.HK`，`fetch_last_ohlcv_batch` 直接复用）。

#### 成本模型（`service/paper_cost_model.py` · 保守默认值，单点可调）

| 市场 | 佣金（每边） | 印花税 | 滑点（每边） | 往返成本 |
|------|--------------|--------|--------------|----------|
| CN | 2.5 bps（万2.5） | 卖出 5 bps（0.05%） | 10 bps | **30 bps ≈ 0.30%** |
| HK | 5 bps | 买卖各 10 bps（0.1%） | 15 bps | **60 bps ≈ 0.60%** |

- 触发口径：stop_hit / target_hit 按**净盈亏**判定（保守、贴近实盘）
- ETF 与 FX 汇率转换本轮不做（记入 L3-P3 精化项）

#### 数据模型（Alembic 0022）

| 列 | 语义 |
|----|------|
| `market` | 'CN' \| 'HK'，legacy 回填 'CN' |
| `gross_pnl_pct` | 平仓前毛盈亏（legacy 回填 = pnl_pct） |
| `costs_pct` | 往返成本 %（legacy 回填 0） |
| `pnl_pct` | **重定义为净盈亏** = gross - costs（open 行仍为当日毛盈亏，关闭时才落地净值） |

#### 统计与 API

- `/v1/paper-trades` 支持 `market=CN|HK` 过滤；`/v1/paper-trades/stats` 新增 `byMarket` 分桶
- `service/decision.py::analysis_stats` 新增 `paperByMarket`（决策 Agent 页 CN/HK 分市场胜率）
- 消费方自动变净口径（零改动）：M4 分析、TIP-011 来源归因、Dashboard Copy、Alpha QA 低胜率主题

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/paper_cost_model.py`（**新**） |
| Service | `services/data-sync-service/src/data_sync_service/service/paper_trading.py`（intake/update/stats 市场感知 + 净口径） |
| DB | `services/data-sync-service/src/data_sync_service/db/paper_trading.py`（CREATE_SQL + CRUD + byMarket 统计） |
| API | `services/data-sync-service/src/data_sync_service/api/v1_business_routes.py`（PaperTrade 模型 + market 过滤 + stats byMarket） |
| API | `services/data-sync-service/src/data_sync_service/api/decision_routes.py`（透传 paperByMarket） |
| Service | `services/data-sync-service/src/data_sync_service/service/decision.py`（analysis_stats byMarket） |
| Migration | `services/data-sync-service/alembic/versions/0022_paper_trades_v02.py`（**新** + CREATE_SQL 同步） |
| FE | `apps/desktop-ui/src/components/decision/AnalysisView.tsx`（净口径标注 + CN/HK 分市场） |
| FE | `apps/desktop-ui/src/lib/queries/decision.ts`（类型 + paperByMarket） |
| 测试 | `tests/test_paper_trading.py` 扩展 + `tests/test_paper_cost_model.py`（**新**） |

#### 反模式（不**做**）

- ❌ **不**做 HK 汇率转换（系统无 FX 数据源；记 L3-P3）
- ❌ **不**做 ETF paper（TrendOK/评分闸对 ETF 语义未定义；保持 skip 记录可见）
- ❌ **不**动 `count_by_source` / source-stats 形状（净口径自动生效，避免 API 破坏）
- ❌ **不**给 open 行实时扣成本（往返成本在关闭时一次性落地，语义清晰）
- ❌ **不**做参数敏感性工程（L3-P3 再做；本轮只立口径）
---

### OPT-063：回测引擎 v0 —— 信号回放 + live 平仓逻辑同口径（todo §16 L3-P2 / §8 回测）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：L3-P2「回测引擎」。关键约束（todo §8）：与 live 同口径（同一份规则代码）、历史 bars、只作参数敏感度不作发布依据。数据深度实测：daily bars 1998 起、index_daily 2023 起、**tv_screener_snapshots 2025-12-21 起**、**watchlist_score_daily 2026-06-18 起**（系统当时实际打的 TrendOK 分）、行业资金流 2025-12 起、机构席位仅 2 个月（无历史，降级）。

#### 设计：信号回放（signal replay）而非全因子重算

```
watchlist_score_daily（当时实际 TrendOK 分）
        │  score >= threshold
        ▼
建仓（信号日收盘价）── 每日更新 ──► _pick_close_reason（LIVE 同码复用）
                                        │  stop/target/score_floor/max_hold
                                        ▼
                              平仓：net pnl（复用 paper_cost_model）
```

- **同口径铁律的落地**：平仓逻辑 100% 复用 `service/paper_trading._pick_close_reason`——唯一改动是给它加 `score` 显式注入参数（回测传 as-of 当日历史分，live 传 None 行为不变），避免回测读当前分造成**前视偏差**
- **信号**：watchlist_score_daily（系统当时打的分，不存在重写规则问题）；tv_screener_snapshots 提供宇宙参考（v0 不消费，v0.2 加回撤区间过滤）
- **成本**：平仓净口径（round_trip_cost_pct，与 paper v0.2 同一模型）
- **明确前视/降级清单**：机构席位、ETF 资金流、主线 SRV 无历史 → 不参与 v0（这些因子在 score 里已隐含）；pool_exit 无 registry 历史 → v0 关闭

#### 参数敏感度（v0 网格）

score_threshold ∈ {70, 80, 85, 90} × max_hold ∈ {5, 10, 20} × stop ∈ {-3, -5, -8}

#### 交付

- `service/backtest_engine.py`（**新**）：simulate(config) → trades + summary（胜率/均值净盈亏/最大回撤/按 score 分桶）；run_sensitivity() → 网格对比
- `scripts/run_backtest.py`（**新**）：CLI 单配置 / 网格，输出 JSON + markdown 报告（可喂 AI agent）
- `GET /api/backtest/run`（单配置）+ `GET /api/backtest/latest-report`（最近一次报告）
- 不重复造：**BacktestPage UI 属 §8 P2 / §12 #12**（等引擎数字稳定后单独排期）

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/backtest_engine.py`（**新**） |
| Service | `services/data-sync-service/src/data_sync_service/service/paper_trading.py`（`_pick_close_reason` 加 score 注入参数，默认 None 行为不变） |
| API | `services/data-sync-service/src/data_sync_service/api/backtest_routes.py`（**新**，挂载到 app） |
| 脚本 | `services/data-sync-service/scripts/run_backtest.py`（**新**） |
| 测试 | `tests/test_backtest_engine.py`（**新**） |

#### 反模式（不**做**）

- ❌ **不**重写一份 TrendOK/规则代码（铁律：回测=历史信号+live 平仓逻辑）
- ❌ **不**让回测读当前数据（score/registry 一律 as-of 注入；读不到就 fail-open）
- ❌ **不**做参数寻优（只做敏感度对比；结论不作发布依据，以 paper 实绩为准）
- ❌ **不**做 BacktestPage UI（§8 P2 单独排期）
- ❌ **不**做 HK/ETF 回放（v0 只 CN，得分历史以 CN 为主）
---

### OPT-064：卖出归因 + 敏感性报告 + 回测页（todo §16 L3-P3 / §8 回测 UI）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：L3-P3「归因与敏感度」：(1) 卖出归因分桶（卖早/卖对/中性）量化 Chandelier/止盈/止损质量；(2) 参数敏感性报告 UI；(3) 卫星仓上限复核（15%/30%/sleeve）初版数据。用户要求「有一个位置能看到」→ 新增回测页（BacktestPage 雏形，§8 P2 正式重写仍待排期）。

#### 卖出归因（`service/exit_attribution.py`）

```
closed paper_trades ──► 平仓后 N 日 forward return（daily 表 window fetch）
        │
        ▼
bucket: fwd_N ≥ +2% → 卖早（exit_early）
        fwd_N ≤ -1% → 卖对（exit_well）
        其余       → 中性
        ▼
聚合：by close_reason（stop_hit / target_hit / max_hold / score_floor / pool_exit / end_of_window）
     → 每种卖出理由的「后验质量」：fwd 均值 + 卖早率 + 卖对率
```

- N 日 = 平仓日后的第 N 个交易日（configurable，默认 5）
- 数据不足（paper 刚起步）→ 返回空并提示「继续积累」
- 同时输出组合暴露：最多同时持仓数 → 单票权重下界（卫星仓 15%/30% 复核参考）

#### 回测页（`BacktestPage.tsx` · 用户可见位置）

SidebarNav 新增「回测」入口。三区块：
1. **单配置回测**：窗口 + score/stop/max_hold 参数 → `GET /api/backtest/run` → 摘要卡（交易数/胜率/均净盈亏/最大回撤）
2. **敏感度网格**：默认窗口 `GET /api/backtest/sensitivity` → 36 行对比表（按 win_rate 排序）
3. **卖出归因**：`GET /api/exit-attribution/analysis` → by-reason 归因表 + 组合暴露

#### 文件范围

| 层 | 文件 |
|----|------|
| Service | `services/data-sync-service/src/data_sync_service/service/exit_attribution.py`（**新**） |
| API | `services/data-sync-service/src/data_sync_service/api/backtest_routes.py`（+`/exit-attribution` 与 `/portfolio-exposure` 或并入） |
| FE | `apps/desktop-ui/src/components/pages/BacktestPage.tsx`（**新**） |
| FE | `apps/desktop-ui/src/components/layout/SidebarNav.tsx` + `AppShell.tsx`（入口 + 路由） |
| FE | `apps/desktop-ui/src/lib/queries/backtest.ts`（**新**，react-query hooks） |
| 测试 | `tests/test_exit_attribution.py`（**新**）+ API 测试 |

#### 反模式（不**做**）

- ❌ **不**在页面做参数寻优（只展示敏感度，发布依据仍以 paper 实绩为准）
- ❌ **不**改引擎行为（OPT-063 已定型；页面只消费 API）
- ❌ **不**做真实交易 UI（无下单能力，纯分析页）


---

### OPT-065：周度决策质量复盘（todo §16 L3-P4 · 决策 Agent M2 v0）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：L3-P4 M2 v0——TIP-015 M1（时点问答）升级为数据驱动周度复盘：决策量 / paper 净口径实绩 / 卖出归因 / 漏斗健康度 → 中文 markdown 报告（可复制喂 AI agent）。

#### 交付

- `service/weekly_review.py`（**新**）：ISO 周聚合 + 4 节报告（决策量 / Paper 实绩 / 卖出归因 / 本周观察）；auto-notes 只从数据触发；样本不足明确标注
- `GET /api/backtest/weekly-review?end=`（默认今天）
- FE：决策 Agent「分析」tab 顶部 `WeeklyReviewCard`（报告 + 复制 + 刷新）
- 复用：`analyze_exit_attribution`（L3-P3 同样本语义）、paper 净口径

#### 反模式

- ❌ LLM 不进关键路径（数字 100% 数据驱动；深度解读归外部 agent）
- ❌ 不做自动推送（归外部 AI 助手）
- ❌ 报告不做参数建议（只提示先跑 paper 对照）
---

### OPT-066：journal 上游 symbol 防御层（2026-08-07 遗留修复）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：OPT-064 清理了 967 条测试污染 journal 行（manual-test 快照 + snap-agg/snap-bf 假 id），但写入路径本身无校验——任何来源（前端 snapshot 提交、未来 AI agent、alpha 通道）都可能再写入坏 symbol（如 CN:99{uuid}）。补防御层，让坏 symbol 永远进不了 journal。

#### 防御（双层）

| 层 | 改动 |
|----|------|
| 后端（权威） | `service/execution_journal.py` 新增 `is_valid_watchlist_symbol()`（CN:6位 / HK:1-5位 / ETF:6位，与 trendok._symbol_to_ts_code 对齐）；`_cards_by_symbol` diff 前过滤非法卡片；`ingest_snapshot` 在**存储前**剥离非法卡片并返回 `rejectedCards` 计数（可观测，不静默丢卡） |
| 前端（双保险） | `buildExecutionSnapshotPayload` 构建卡片前跳过非法 symbol（`WATCHLIST_SYMBOL_RE`，与后端同规则） |

#### 验证

- 后端 1379 passed / 2 skipped（唯一失败为既有 flaky）；前端 495 passed；tsc 干净
- 实测：`ingest_snapshot` 传 [CN:600000, CN:9901ae04, HK:00700] → `rejectedCards: 1`，坏卡不入库
- 新测试：`test_is_valid_watchlist_symbol`（10 断言）、`test_diff_ignores_malformed_symbol_cards`、`test_split_valid_cards_counts_rejects`、FE `skips items with malformed watchlist symbols`

#### 反模式（不**做**）

- ❌ 不修前端 localStorage 的存量 symbol（registry 已验证 0 污染）
- ❌ 不做 symbol 规范化（把坏 symbol 映射到真实代码是幻觉——直接拒绝）
- ❌ 不阻塞合法提交（拒绝只针对非法格式，正常 CN/HK/ETF 不受影响）
---

### OPT-067：组合相关性防火墙（todo §16 L3-P5 · V7.0-01 转正）

**状态**：[x]  
**完成日期**：2026-08-07  
**背景**：L3-P5 组合风控——V7.0-01 从「暂缓」转正。语义因子簇为主 + 20 日经验相关性为辅（日历对齐，<15 样本 fail-open）；簇暴露 >30% → 簇内新 BUY/ADD 拦（CORRELATION_CAP_BLOCK），Suggest% 经 roomCorrelation 进 min 链；不强制平仓。

#### 交付

- `service/correlation.py`（**新**）：9 个语义簇（港股科技/半导体/通信/金属/新能源/消费/医药/金融/宽基）+ ETF 前缀映射 + 东财行业规则 + 日历对齐相关性 + cap 评估
- `GET /api/backtest/correlation-status`：当前持仓簇暴露 + 超限 + blockedSymbols + topPairs
- FE：`isCorrelationClusterBlocked` / `suggestFireSizePct.roomCorrelation`（note='correlation'）/ `evaluateNewEntryGates` CORRELATION_CAP_BLOCK / deriveActionCard 透传；回测页「组合相关性防火墙」面板；WatchlistTable 每行传簇暴露
- 实测命中设计场景：tech_hk 34.2%（腾讯+恒生科技 ETF）超限；00700×513180 r=0.926

#### 反模式

- ❌ 纯统计相关性唯一依据（语义层为主，fail-open）
- ❌ 强制卖出（只拦新开仓）
- ❌ other 簇参与 cap

---

### OPT-068：真实交易记录 + 期望值看板（todo §3 P2 · 2026-08-08 用户拍板）

**状态**：[x]  
**完成日期**：2026-08-08  
**背景**：用户实际买卖闭环——watchlist 已有买入价（costPrice）+ 仓位（positionPct），缺卖出记录与胜率度量。不做 Alpha 191 因子全量落地，走「纪律 + 真实数据验证」路线（关联 TIP-013）。

#### 交付

- `db/user_trades.py`（**新**）+ alembic `0023_user_trades`：append-only 真实交易日志（BUY/ADD/SELL 三条腿；SELL 带 costBasis/entryDate/pnlPct/holdingDays，毛利口径）
- `service/user_trades_stats.py`（**新**）：期望值 = 胜率×平均盈利 − 败率×平均亏损 − 0.3% 往返成本；bySource（TV/ALPHA/MANUAL/RESEARCH 对齐 TIP-011）/ bySymbol 分桶；profitFactor / avgHoldingDays
- API：`POST /trades`（SELL 由后端算 pnlPct+holdingDays）、`GET /trades`、`GET /trades/stats`、`DELETE /trades/{id}`
- shared：`schemas/userTrades.ts`（Zod：UserTrade / UserTradeRequest / UserTradesStats + schema 测试）
- FE：`TradeActionDialog`（买入/加仓/卖出弹窗，卖出预填现价 + 预计盈亏预览）；Watchlist 行内按钮替换 ETF 专用快键；`TradeStatsPanel` 期望值看板（胜率/平均盈利/平均亏损/盈亏比/净期望值 + 分来源 + 最近卖出）；`lib/trade-recording.ts` 加权成本混合 + PnL/持有天数纯函数
- 加仓识别：持仓标的新增买入 → `blendAddCost` 加权平均成本 + 记 ADD leg

#### 反模式

- ❌ 用 paper_trades（模拟信号日志）冒充真实交易
- ❌ 把净值/费用模型塞进 user_trades（毛利 + 展示期扣 0.3% 成本，口径单一）
- ❌ 样本 <50 时给出结论（看板明示"仅作趋势参考"）


### OPT-069：52W 回撤关改用 DB K 线（2026-08-09 发现并修复）

**状态**：[x]
**完成日期**：2026-08-09

**背景**：8/02 起 Funnel History 连续 4 个交易日「回撤 0、转化率 0%、走兜底」——诊断确认 TV Scanner API 模式下 `High.Interval52Week` 列对几乎所有行返回空字符串（8/01 16:00 screener 从 Chrome 模式切到 api 模式为分界），FE `getRetracementRatioFromScreenerRow` 全部拿不到 52W 高 → 回撤关全灭 → 兜底宇宙接管。**不是市场没有 5~15% 回撤票**（K 线验证 101 只候选有 31 只 in window）。

#### 交付

- BE `filter_pullback_window()`（`service/watchlist_automation.py`）：52W 回撤 = 最新 close vs 最近 300 根 K 线 max(high)（阈值 -15%~-5% 不变）；不足 60 根标记 missing；复用 `db.daily.fetch_last_ohlcv_batch` 单次批量查询；symbol→ts_code 复用 `symbol_to_ts_code`（CN/HK/ETF）
- API：`POST /watchlist/automation/pullback-filter`（入参 symbols，返回 results/symbol/tsCode/price/high52w/pullbackRatio/inWindow/windowBars/missing + asOf + unparsed）
- FE `importFromScreener`：回撤关改调该端点（`setStep('52W pullback check (K-line)')`），删除 TV 列解析 helper（parseScreenerNumber/pickFirstRowValue/getRetracementRatioFromScreenerRow）；兜底触发条件不变
- 测试：BE `tests/test_watchlist_pullback_filter.py`（9 个：in/out 窗口、max(high) 跨棒、不足窗口 missing、HK 解析、unparsed、空输入）；FE `src/lib/watchlist-screener-import.test.ts`（2 个：pullback-filter 走 K 线 + 全灭时兜底）
- 实测：8/08 最新快照候选 101 → in_window 31（修复前 0），BE 3419 passed / 93.28%，FE 723 passed，baseline OK

#### 监控（回撤关连续 3 天 0 → 告警）

- `service/watchlist_funnel_health.py`（新）：盘后 18:10 用最新 TV 快照 + K 线**离线重放入池漏斗**（tvHit / passPullback / missing / fallbackWouldTrigger）；每次运行写 sync_job_record（metrics 存 error_message、streak 存 last_ts_code）
- 连续 3 个交易日回撤关 0 通过 → `insert_record(success=False)` → 出现在 `GET /api/health/job-failures`（健康页 Job Failures 区域自动可见）
- 同日多次运行（定时+手动）按日期去重，不虚增 streak；collect 异常单独记 failure
- 手动触发：`POST /watchlist/automation/funnel-health/check`；Scheduler 页新增「漏斗健康检查」条目（shared `SCHEDULER_JOB_CATALOG`，18:10 工作日 + 立即检查按钮）
- 测试：`tests/test_watchlist_funnel_health.py`（7 个：健康/首日 anomaly/3 天连续 failure/中断/失败记录延续 streak/同日去重/collect 错误）

### OPT-070：回测引擎 v1.5 入池闸门（2026-08-09 · 用户拍板「与真实决策匹配」）

**状态**：[x]
**完成日期**：2026-08-09

**背景**：回测信号只按 score 阈值（v0），不含真实决策链的指数红绿灯/板块资金流/mainline
白名单 → 网格调出的参数与实盘口径脱节（实测 85/5/-5 在弱市窗口回测 21 笔全入场，
而真实系统该窗口 29/36 天 Weak regime 根本不开新仓）。

#### 交付

- `BacktestConfig.gates`（none|regime|full，默认 full=实盘口径）：`none` 维持 v0 只看分；
  `regime` 指数红绿灯全绿（REGIME_STRONG）才开新仓（复用 `get_index_signals(as_of)`
  + `classify_market_regime`，与 live gate 同函数）；`full` 再加全行业 SW L1 净流入
  ≤0 挡（同 sectorOutflowBlock 规则）+ 个股 EM 行业 ∈ 5D 净流入 Top3 才开（同
  mainline 白名单）。闸门数据缺失 fail-closed（同 live 姿态），拦截次数记入
  `summary.gated_blocks`（regime/flow/mainline 分项）
- `BacktestData` 按日预载 regime / 全市场资金流 / 5D Top3 行业 / ts→行业映射
  （一次批量查询 + as-of 现算，窗口内每 config 共享）
- `run_sensitivity` 重构：同窗口 config 共享一份 BacktestData（之前每 config 独立加载）；
  默认网格 = score×hold×stop×闸门两档（none/full）= 72 组
- API：`GET /api/backtest/run?gates=`（默认 full）、`/api/backtest/sensitivity`
  （网格含 none/full 两档）；summary.config 带 gates、新增 gated_blocks
- FE：参数面板加「入池闸门」下拉（全套/仅红绿灯/只看分数）；单配置结果加「闸门拦截」
  卡片；网格表格加「闸门」列；说明文案更新
- 测试：BE 新增 7 个（none 忽略闸门 / regime 挡 Weak+Diverging / full 挡 flow+mainline
  / 缺失数据 fail-closed / 网格 72 组含闸门维度 / 路由 gates 参数），43 passed；
  FE backtest query 测试 9 passed

#### 实测（2026-06-18 ~ 2026-08-08 窗口）

- 36 交易日 regime 分布：Weak 29 / Diverging 5 / Strong 2 → full 闸门 0 笔入场
  （真实系统本就该弱市空仓）；none 口径 21 笔 38.1% 胜率 -0.9% 均净（虚高，勿参考）
- 72 组网格：full 列全部 0 笔（窗口内无 STRONG 行情）；等行情转强后 full 才有样本

#### 反模式

- ❌ 回测信号只看 score（忽略 regime/资金流 → 与实盘纪律脱节）
- ❌ 每 config 独立加载 BacktestData（网格数据重复拉取）

### OPT-071：回填历史 score（2026-08-09 · 回测窗口 6/18 → 3/2）

**状态**：[x]
**完成日期**：2026-08-09

**背景**：`watchlist_score_daily` 起点 2026-06-18（全池每日盘后打分），回测窗口被锁死在该
日期之后——而 6/18 后 36 个交易日 Strong 仅 2 天，full 闸门 0 样本。全市场（8563 票）回填
太耗算力，需要聪明筛选。

#### 交付

- `scripts/backfill_watchlist_scores.py`（新）：回填 2026-03-02 起（76 交易日 × 752 只 CN
  票 = 56854 行，全量仅 24s）
- **Universe 聪明筛选**（不扫全市场）：① TV 快照历史中（6/18 前）出现 ≥2 次的 CN 票
  （=当时被 screener 选中/关注的票）② score 表已有记录的 CN 票 ③ 当前 registry CN 票
- **复用 live 纯函数**：`_trendok_one`（bars 截断至目标日 as-of），行业资金流
  `_build_industry_flow_context(D)`、regime `get_market_regime(as_of=D)`、CSI300 as-of；
  stoploss resolver 传 no-op（回填不写 stoploss 表）；inst 缺失降级不阻断 score
- 幂等：`upsert_score_daily`（ON CONFLICT 覆盖）；不触碰 6/18+ 已有真实行

#### 附带修复（as-of regime 前视污染 + 网络卡死）

- `market_regime.py`：`get_index_signals(as_of_date=...)` 的 HK 分支**跳过 on-demand
  网络拉取**（用今天数据算历史日是前视污染；且 HK 不在 index_daily 时每次卡 30s+，
  回测 112 天窗口曾 240s 超时）。as-of 语义下 HK 标 no-data（不影响 CN regime）
- `macro_snapshot_on_demand.py`：yfinance 失败缓存 300s（`_yf_fail_cache`）——HK/US
  网络失败不再让每次快照/信号调用卡满超时（生产体验修复）

#### 口径修正（2026-08-09 用户质疑后复查）

- **flow 闸门**：初版"全行业净流入合计 ≤0 挡"≠ 真实 `isSectorOutflowBlock`
  （hot-industry-picks.ts:247 = **所有行业都 ≤0 才挡**）→ 改为
  `flow_any_positive_by_day`（任一 SW L1 行业 net_inflow > 0 即放行）
- **mainline 缺动量突破**：真实 `buildMainlineAllowSet` = 5D Top3 ∪ 动量突破
  （今日净流入 ≥20 亿 且 排名升 ≥10）→ 补上（阈值同 FE 常量）
- paper 才上线一周（仅 1 笔 8/08 买入）→ 无法与回测对照；回测是唯一长样本验证工具

#### 实测（窗口 2026-03-02 ~ 2026-08-07 · 112 交易日 · 口径修正后）

- regime：Weak 78 / Diverging 26 / **Strong 8**（回填前 6/18+ 仅 2 天 Strong）
- 85/5/-5 单配置：full **25 笔（72% 胜率 +4.8% 均净 回撤 16.9%）** vs none 1106 笔
  （42.4% 胜率 -0.12% 均净）——闸门拦截 2020 次（regime 1751 / mainline 269，
  flow 0——Strong 日都有正流入行业，口径修正后不再误挡）
- 结论：系统纪律（弱市空仓 + 强市精选）在 5 个月样本上显著跑赢"只看分"；25 笔
  样本仍偏小，继续积累

#### 一年窗口扩展（2026-08-09 · 用户「最近一年 + 移动止损」）

- 回填延伸至 2025-08-01：145 交易日 × 791 票 = 113,374 行（53s），全段 Strong 33 天
- **移动止损**：`trailing_stop_pct`（峰值回撤平仓，0 关闭，reason=trailing_stop）；
  与固定止损/target/max_hold 共存，先触发者平仓
- **资金模型**：`position_pct`（单笔仓位，默认 5%）× `max_positions`（持仓上限，默认 10）——
  修正 v0 "单票全仓"复利失真（906 笔 totalNet 1366% → 117 笔 1.6%）；累计收益与回撤按仓位折算
- **修复 OPT-063 遗留 bug**：`_pick_close_reason` 的 `max_hold_days` 传参被丢弃
  （`_ = ...` 直接忽略，永远用 live 常量 5）→ 网格的 max_hold 参数此前从未生效
- **闸门数据缺失降级**（fail-open）：fund flow 2025-12-15 才有；更早日期的 full 回测
  降级为仅 regime（复刻"当时系统能力"，否则 8-12 月全被挡 0 样本）
- 累计收益展示：`summary.total_net_pnl_pct`（按仓位折算）

#### 一年实测（2025-08-01 ~ 2026-08-07 · 257 交易日 · 5%×10 笔模型）

| 配置 | 交易 | 胜率 | 均净% | 累计% | maxDD% |
|---|---|---|---|---|---|
| full·hold5·固定-5% | 117 | 47.9% | +0.27 | +1.60 | 6.2 |
| full·hold20·固定-5% | 86 | 41.9% | +1.20 | +5.16 | 6.4 |
| full·hold20·移动-8% | 86 | 41.9% | +1.28 | **+5.51** | 6.0 |
| none·hold5 | 550 | 44.4% | +0.04 | +0.98 | 8.8 |

分段（闸门价值）：
- 2025-08~12-14（无 fund flow 降级段）：full 90 笔 -0.39% 均净 -1.74% 累计
- 2025-12-15~2026-08-07（闸门齐全段）：full 27 笔 **63% 胜率 +2.48% 均净 +3.35%**
  vs none 327 笔 43.4% +0.55%——**行业资金流 + mainline 闸门把胜率从 43% 抬到 63%**
- 移动止损影响小：5 天持有下被 max_hold/target 抢先；hold20 时 trail-8 最优（+5.51%）

#### 趋势跟随方案（2026-08-09 · 用户手动 15% vs 系统 5.5% → 根因分析）

**根因**：score 是"买点评分"（偏爱低位刚启动），不是"趋势质量评分"——中际旭创
（300308，一年 +337%）score 均值 48.4、≥85 仅 1 天 → 85 分短线规则**结构性错过
趋势主升浪**。score≥70 的窗口（2025-08/12、2026-04~06）正好对应主升段——门槛
降到 70 + 长持有即可抓住。

**方案对比**（2025-08-01~2026-08-07 · 5%×10 笔 · full 闸门）：

| 方案 | 交易 | 胜率 | 均净% | 年累计% | maxDD% |
|---|---|---|---|---|---|
| 现状 85/5天/-5% | 117 | 47.9 | +0.27 | +1.6 | 6.2 |
| **趋势 70/60天/移动-10%** | 75 | 35.0 | +4.01 | **+15.0** | 5.2 |
| 趋势 70/60天/移动-15% | 75 | 34.7 | +3.83 | +14.4 | 5.2 |
| 趋势 75/120天/移动-20% | 76 | 31.6 | +2.64 | +10.0 | 5.1 |
| 高分快打 95/5天 | 105 | 46.7 | +0.30 | +1.6 | 7.4 |

- 27 组网格（score 65-75 × hold 30-90 × trail -10~-20）：最优 70/60/-10 = **+15.0%**，
  且参数不敏感（多数组合 11-15%）→ 稳健
- 趋势策略特征：胜率仅 35%（赚大赔小，靠 +4% 均净）；与用户手动 15% 吻合
- 回测页默认参数已切为趋势方案（70/60/-10）

**后续专业增强方向**（未实现）：RS 全市场排名过滤（只买相对强度前 10%）、板块主线
过滤、金字塔加仓、波动率目标仓位

### 评估框架（2026-08-09 · 方法论：基准对比 + 超额目标）

**交付**：
- 基准模块 `load_benchmarks`：5 指数（上证/创业板/沪深300/中证500/**科创50**）窗口
  总收益 + 年化（科创50 已补进 index_daily 同步，871 行历史）
- summary 新指标：年化（`annual_net_pnl_pct`）、均赢/均亏、夏普（平仓收益序列近似，
  标注 approx）、超额（`excess_vs_best_benchmark_pct`，vs 最强基准年化）
- API `/run` 与 `/sensitivity` 返回 `benchmarks` + 超额；FE 网格加列（年化/超额/夏普/
  trail）+ 基准条 + 目标线（最强指数 +10%，达标 ✓ 标记）

**一年实测（2025-08-01~2026-08-07）**：
- 基准：上证 +10.5% / 沪深300 +15.5% / 中证500 +28.0% / 创业板 +52.6% / **科创50 +67.2%**
- 目标线 = 科创50 +10% = **77%/年**
- 当前信号系统上限：趋势 70/60/-10 满仓（10%×20）= **+31.3%/年**（夏普 4.0）——
  差距是**结构性**的：科创50 = 权重股满仓吃 beta（68%），策略 = 纪律性空仓 + 5-10% 仓位
  → 牛市中落后 beta；要靠 alpha（RS 排名/主线过滤）追平，不是参数能解决
- 仓位利用率量化：5%×10 → 11.5%；10%×20 → 31.3%（3 倍差距）

### OPT-072：A1 RS 相对强度过滤（2026-08-09 · §19 作战计划第 1 步）

**状态**：[x]
**完成日期**：2026-08-09

**交付**：
- `_load_rs_ranks`（backtest_engine.py）：全市场 20 日收益（daily 窗口函数 lag(close,20)）
  减沪深300 同期（as-of）→ 当日全市场排名百分位（最强=1.0）→ 只保留 universe 票
- `BacktestConfig.rs_rank_min`（0-1，0 关闭；API/FE 参数）：entry 前过滤，缺数据
  fail-closed，拦截计入 `gated_blocks["rs"]`；加载 ~19s（204 万行一次查询，网格共享）
- 测试：BE +4（过滤方向/缺数据 fail-closed/默认关闭/校验）；FE 路径断言更新

**walk-forward 证据（防过拟合纪律）**：
| RS 阈值 | 训练窗 2025-08~02（年化/胜率） | 验证窗 2026-03~08（年化/胜率） |
|---|---|---|
| 无 RS | 60.5% / 40.6% | 5.5% / 43.5% |
| **前 50%（0.5）** | **65.8% / 42.3%** | **5.5% / 43.5%（零伤害）** |
| 前 70%（0.7） | 67.3% / 43.6% | -1.7% / 36.4%（劣化） |
| 前 20%（0.8） | 69.2% / 46.2% | 1.4% / 40.9%（劣化） |

- 结论：**0.8/0.7 是数据挖掘阈值（训练窗最好、验证窗劣化=过拟合）**；0.5 是
  "只碰全市场前一半强票"的业务自然截断——训练窗 +5.2 个点、验证窗零伤害
- 全年定案：RS 前 50% → 年化 **34.8%**（+3.5）、胜率 **41.4%**（+2）、夏普 3.5、
  maxDD 14.7%；FE 默认 rsRankMin=0.5

**教训**：固定最优阈值 = 过拟合；参数必须过 walk-forward 双窗 + 有业务故事。

#### 反模式

- ❌ 全市场回填 score（8563 票 × 76 天 = 浪费；快照历史已天然给出"当时被关注"的池子）
- ❌ as-of 历史重算走网络拉今天数据（前视 + 卡死）
- ❌ 回填时写 stoploss 表（副作用污染）
- ❌ 闸门口径凭印象实现（flow 合计 vs 全负、mainline 缺动量突破）——先读 live 源码再写
- ❌ 回测不带资金模型（单票全仓 → 复利数字无意义；必须按 5% 仓位折算）

- ❌ 依赖 TV 快照列算 52W 回撤（列值可能为空且不可告警）
- ❌ 用 EMA/SMA 近似 52W 高（口径漂移，K 线是权威源）

### OPT-073：S-3 持仓体检面板 + 决策 Agent 回测感知（2026-08-09 · 用户「让 agent 同步知道回测信息」）

**状态**：[x]
**完成日期**：2026-08-09

**背景**：决策 Agent 之前无回测知识，用户靠 copy all markdown + prompt 手动喂；
持仓状态（止损线/移动线/到期日）只能手算。目标是 Agent 自动用回测知识回答
卖/买/加三类问题，且用户打开执行页直接看到体检。

#### 交付（后端）

- `service/portfolio_health.py`（新）：`build_portfolio_health(trade_date)` —— 真实持仓
  （watchlist registry positionPct>0）逐票按 S-3 退出规则体检（固定止损 -5% / 移动止损
  -8% 峰值回撤（**close 口径**，与回测引擎一致）/ 60 天上限，常量同 paper 模块）+
  金字塔触发线（成本+2.5%）+ 是否已加仓（paper_trades pyramid-add 标记）+ 市场状态
  （regime/sentiment/恐慌冷却/当日 S-3 候选明细，候选补 stock_basic 名称）
- `GET /v1/agent/portfolio-health`（v1_business_routes.py）
- 修复口径 bug：trailing 峰值原用 high → 改 close（亿联回撤 -4.0%→-1.2%）；
  `maxHoldDate` 原错返回 entry_date → entry+60
- 测试：`tests/test_portfolio_health.py`（6 passed，close 峰值/双退出触发/到期日/build 层组装）

#### 交付（ai-service）

- `routes/decision.ts`：新增 `query_s3_holdings_health` tool（强制"问减仓/加仓/买什么先调"）+
  `S3_RULES_KNOWLEDGE` system prompt（Weak 只挡开仓不触发卖出、4 条退出规则、
  弱市年 +80.5% 证据、参数表、金字塔纪律、9 票 RS 轮出）；138 tests passed

#### 交付（desktop-ui）

- `components/watchlist/PortfolioHealthCard.tsx`（新）：WatchlistPage 执行 Gate 下方——
  市场状态 chips（regime/sentiment/恐慌冷却/候选数）+ 每持仓（盈亏色/回撤/动作 badge/
  止损线·移动线·金字塔线·到期/已加仓）+ 今日开仓候选 chips；5min 自动刷新 + 手动刷新；
  空持仓/弱市/错误态都有明确文案
- `lib/queries/portfolioHealth.ts`（新）；`PortfolioHealthCard.test.tsx`（4 passed）；
  FE 全量 737 passed、tsc/eslint 干净

#### 反模式

- ❌ 测试依赖真实 journal 残留（smoke 测试断言 candidates==1 → 只断言自己的插入）
- ❌ 组件显式 `retry: 1` 覆盖测试 client 的 retry:false（重试退避 1s+ 导致测试假 pending）
- ❌ 周末 cron 按日历日拉 tushare 空转（已修：`last_trading_day()` 交易日感知）

### OPT-074：全系统健壮性审查（2026-08-12 · 三路扫描）

**状态**：[x]
**完成日期**：2026-08-12

**背景**：三路并行 agent 扫描前端（TS/React）、后端（Python）、桌面壳（Tauri Rust）+ AI service，
共 60+ 处缺陷（挂死/静默吞错/进程泄漏/无超时/竞态）。本轮修复 HIGH/MEDIUM 全量 + LOW 大部分。

#### 交付（desktop-ui）

- `lib/api/client.ts`：`DEFAULT_API_TIMEOUT_MS=30s` 默认超时（90+ 调用点受益）；显式 timeoutMs 不受影响
- `hooks/useDashboardSync.ts`：SSE 全流程 5min 兜底超时 + 卸载清理（原后端挂起 → spinner 永久旋转）；
  AI 摘要失败 console.warn
- `components/chat/ChatPanel.tsx`：流式 fetch AbortController + 5min 超时 + 卸载 abort（原 composer
  永久锁死）；reference 三 fetch 各 15s 超时；/title 10s 超时；AbortError 显式文案
- `components/pages/DecisionPage.tsx`：初始化 try/catch + 错误态 + 重试按钮（原永久卡初始化）；
  流式请求 abort + 5min 超时 + 卸载清理；三个 handler 补 try/catch
- 小修：WeeklyReviewCard/BrokerPage/WatchlistPage clipboard 与 FileReader onerror 补 catch、
  AlphaIncubatorPage `.then` 补 catch、IndexDetailPage 竞态守卫、ModelSettingsPanel timer 清理、
  useWatchlistItems catch

#### 交付（src-tauri）

- `backends.rs`：sidecar 启动移入后台线程（原主线程忙等最长 35s 冻结 UI）；wait_port 失败 kill+wait
  收割；端口被孤儿占用时显式报错；stop_all 限时收割防僵尸；锁中毒降级
- `lib.rs`：处理 `RunEvent::ExitRequested`（macOS Cmd+Q 原不清理 sidecar → 孤儿进程占端口）；
  run 失败 eprintln+exit(1)

#### 交付（ai-service）

- `model.ts`：`withFetchTimeout` fetch 层 10min 硬顶（ai v5 无 timeout 参数，覆盖全部 provider）+
  `AbortSignal.any` 兼容外部 signal；**去掉 process.env 全局改写**（google 分支显式 apiKey，
  原并发请求互相干扰）；gemini/ollama/openai 全走带超时 fetch
- `routes/chat.ts`：ReadableStream `cancel()` → 上游 `result.abort()`（客户端断开即停，不白烧 token）
- `routes/decision.ts`：删除死代码 retrieveSnapshot；index.ts PORT 校验 + uncaughtException 退出交
  supervisor；config.ts 损坏配置记日志

#### 交付（data-sync-service）

- `service/news.py`：RSS 抓取带 20s timeout（原 feedparser 无限挂死调度线程，coalesce 吞后续 run）
- `service/close_sync.py`：多日回补每日期 `_with_retry`（指数退避，原单次限流中止整段回补）；
  resume marker/sync_at 脏数据解析保护
- `service/post_close_sync.py`：6 个并行子任务逐个 try/except 隔离（原一个失败整批 abort）
- `service/hk_daily_yf.py`：yfinance 线程 + 60s 超时（原请求路径无超时挂死）
- `db/__init__.py`：连接加 `statement_timeout=120s`（防挂死 SQL 永久占调度线程）
- 静默吞错补日志：market_sentiment×5、market_regime×3、trade_calendar、hk_daily_tx、top_inst_flow
  （信号侧降级必须可见）
- 7 个 scheduler job + alpha_radar_process/mapping 的 `print` → `logger.info/warning`
- `db/_ensure_guard.py` 加锁；`watchlist_automation._rs_rank_cache` 加锁；realtime_quote 去
  `os.environ["TS_TOKEN"]` 全局副作用；dashboard 三个 bundle 降级隔离；catchup 链逐步 try/except

#### 交付（测试）

- 后端 3284 passed；前端 728 passed；ai-service 142 passed；tsc/ruff/eslint/cargo check 干净
- 修 alembic/env.py `disable_existing_loggers=False`（原 fileConfig 禁用全库 logger，污染 pytest
  caplog —— 二分定位到 test_alembic_baseline）
- 修 trading_brief 测试 mock `_candidates`（原依赖 DB 真实候选，非确定性）

#### 反模式

- ❌ 正则批量改代码（print→logger 三套脚本把引号改坏、logger 插进 import 块）→ 引号修复脚本再次
  破坏（`(\w)""\)` 误伤正常代码）→ 教训：**文本批量替换必须有语法/编译验证门**
- ❌ `_fallback_from_sync_at` 用真实今天而非参数 today（重构引入，被既有测试抓住）
- ❌ caplog 断言需 `caplog.set_level(logging.INFO)`（默认只捕 WARNING+）

### OPT-075：健壮性审查遗留项（2026-08-12 登记 · 未处理）

**状态**：[ ]
**背景**：OPT-074 修复后仍保留的 LOW/MEDIUM 项，不影响稳定性，属性能与恢复效率类。

1. **`/sync/close` 并发锁**（原 M10）：全市场多日同步跑在 HTTP 请求里且无 in-process 锁，
   两个并发请求会重复拉同一批数据互相撞 tushare 限流。方案：sync 函数外 `threading.Lock`
   去重，或请求内只入队立即返回。
2. **tushare 统一重试**（原 M7）：15 个 tushare 调用点中仅 close_sync 有 `_with_retry`；
   index_daily/hk_daily/adj_factor 等单次限流即整个 job 失败。方案：抽公共 `retry.py`，
   `_with_retry` 四份实现（market_sentiment/etf_fund_flow/industry_fund_flow/top_inst_flow）
   合并去重，参数统一（tries/base_delay）。
3. **chat 流式 localStorage 写放大**（前端 #13）：每 chunk 全量序列化整个会话（含附件 dataURL）。
   方案：流式期间内存态 + 结束时落盘，或防抖持久化。
4. 零星 LOW：`alpha_radar_process` urlopen 180s 单文档阻塞（batch 路径已有时限）；
   stock_basic 查询失败零日志；`health_routes.py:162` except:pass；`_rs_rank_cache` 读取
   在锁外（GIL 安全但可顺带收敛）。

### OPT-076：执行闸与回测闸门口径统一（2026-08-12 · 红绿灯改日终 + Diverging 对齐）

**状态**：[x]
**完成日期**：2026-08-12

**背景**：用户发现实盘红绿灯/闸门与回测打架。根因两层：
1. **信号口径**：dashboard 盘中用实时行情信号（sync window 内 `get_index_signals(None)`），
   回测只用日终 as-of 信号——盘中波动把"日终仍绿"的进攻日误判为 Weak
   （实证：HSI 8-11 日终绿、8-12 盘中跌 1.2% 转红 → 实时口径 HK=Weak/DEFEND vs 日终口径 Diverging）。
2. **mode 映射**：execution_gate 把 Diverging 压成 HOLD_ONLY，但 S-3 定案
   （strategy-params §1：diverging_scale=1.0 满仓开仓）与回测引擎 `_gate_blocked`
   （Diverging+scale>0 放行）及 paper_s3 实盘引擎都是"Diverging 允许开仓"——实现偏离定案。

**改动**：
- `service/dashboard.py`：删除 `use_realtime_index` 实时分支（sync window 内也不再走
  `get_index_signals(None)`），闸门/红绿灯一律日终 as-of；meta 字段恒 false
- `service/execution_gate.py`：
  - CN：Diverging（无 SRV_ELEVATED）→ `MODE_ATTACK`（原 HOLD_ONLY）；SRV_ELEVATED
    仍单独压 HOLD_ONLY（回测之外的拥挤防御）
  - HK：Diverging → `MODE_ATTACK`（原 HOLD_ONLY）；risk defend 保留
- 测试：6 个断言适配（Diverging→ATTACK；overflow override 在 Diverging 下不再触发——
  本来就是 ATTACK 不需要升级）

**验收**：后端 3285 passed + 前端 728 passed；手动验证 dashboard/summary：
CN=DEFEND(Weak+SRV_EXTREME_HIGH，回测同口径禁开 ✓) · HK=ATTACK(Diverging，回测同口径可开 ✓)
信号全部 daily as-of（无 realtime）

**第二轮（同日 · 红绿灯全面对齐回测）**：
- 移除 sentiment bundle 的 `apply_breadth_panic_index_signals` 改色（panic 日 CN 全红）——
  回测口径里 panic 只通过 sentiment 闸（risk_mode=extreme_caution，SENTIMENT_BLOCK_MODES）
  拦截、红绿灯保持日终原色；execution_gate 的 BREADTH_PANIC 硬闸不变，防御等价
- `build_macro_snapshot` 默认路径 `get_index_signals(None)` → 日终
  `get_index_signals(as_of_date=shanghai_today_iso())`（内部回退最近日终）
- 全库核查：所有 `get_index_signals`/`get_hk_regime`/`classify_market_regime` 消费点
  均日终 as-of（backtest / dashboard / allocation / portfolio_health / macro_snapshot）；
  实时分支（market_regime.py:297/497）仅剩 include_breadth=True 或无 as_of 时才触发，
  当前无调用点，为死路径
- 手动验证：dashboard 闸门 5 信号全部 rt=False · CN 实盘 Weak ↔ 回测 classify=Weak ✓ ·
  HK 实盘 Diverging ↔ 回测 get_hk_regime=Diverging ✓

### OPT-078：体检页"提醒买入"一键加自选（2026-08-12）

**状态**：[x]

**需求**：S-3 持仓体检"下午2点买入清单"里每只候选加"提醒买入"——不用手动输代码，
弹框设置（目标价 + 备注）后直接加入自选 watchlist。

**实现**（纯前端，零后端改动）：
- `apps/desktop-ui/src/lib/buy-reminders.ts`：localStorage 本地提醒存储
  （symbol/name/targetPrice/note/createdAt）+ 事件广播
- `apps/desktop-ui/src/components/watchlist/BuyReminderDialog.tsx`：portal 弹框
  （目标买入价可选 · 备注可选 · 显示 S-3 建议仓位）
- `PortfolioHealthCard.tsx`：BuyList 每行"提醒买入"按钮 → 确认后
  `saveWatchlist`（source='research'，已补 normalize 白名单）+ 写本地提醒；
  Card 顶部"买入提醒"条（可删，自选保留）
- 测试：PortfolioHealthCard.test.tsx 新增 2 例（加提醒/移除提醒）

**说明**：目标价仅为本地备忘（系统暂无到价通知机制，如需系统级预警另开任务）；
加自选后 watchlist 全功能（行情/趋势/信号/体检）自动盯盘。

### OPT-079：下午2点买入清单"买入"一键记模拟盘（2026-08-12）

**状态**：[x]

**需求**：体检卡"下午2点买入清单"每行加「买入」——简单 modal 只设仓位（默认建议 10%）
+ 价格（自动按最近行情预填），确认即记 paper trade；**不做加自选/add 等操作**。

**实现**（纯前端）：
- `QuickBuyDialog.tsx`：portal modal；打开时 `fetchWatchlistMarketSnapshot` 预填最近价；
  校验价格>0、仓位 0-100；确认后 `recordUserTrade({side:'BUY', source:'RESEARCH'})`
- `PortfolioHealthCard.tsx`：BuyList 行内「买入」按钮（与「提醒买入」并排）；
  成功后行内「✓ 已买入」标记（session state）；错误显示在卡片底部
- 测试：新增 quick-buy 用例（断言 POST /trades body 字段 + 不触 registry/localStorage）

**边界**：价格预填用日终/最新行情（realtime=false）；买入记录不影响 watchlist 持仓
字段——持仓仍以自选页仓位为准（体检持仓块会同步显示）。

### OPT-080：回测 vs Paper 对账融入体检卡 + 操作卡贴近回测（2026-08-12）

**状态**：[x]

**需求**：独立"回测 vs Paper 对账"卡作用不明显（数字空洞无法行动）；
希望对账结果融入 S-3 持仓体检，并让操作卡内容"贴近回测"。

**改动**：
1. **删除独立 BacktestReconCard**（组件+测试+页面引用），对账数据融入体检卡：
   - `PortfolioHealthCard` 每个市场区块（CN/HK）新增 **回测口径行**：
     ✓/⚠ + 对账日期 + 回测应持/实持/缺/多
   - 缺票 > 0 可**展开缺票清单**（symbol · 入场 score · 建议仓位%），
     每只缺票可**提醒买入**（加自选+目标价/备注）→ 缺票从数字变成可行动清单
2. **操作卡（action brief）新增"回测口径"段**（`trading_brief._recon_section`）：
   - 每市场：回测应持/实持/缺/多 + 缺票 top5（入场 score · 建议仓位 · 入场日）
3. **修 limit=1 丢市场 bug**：recon 快照按 market 每行一条，limit=1 只取到 CN 行
   ——前后端统一 limit=2（`useBacktestReconQuery` 默认值改 2，_recon_section 同）

**测试**：后端 +1（action brief 渲染 recon 段），前端 +1（体检卡内嵌对账行 +
缺票展开 + 提醒买入）；后端 3286 passed · 前端 729 passed。

### OPT-081：BacktestPage 重写为回测结论展示页（2026-08-12）

**状态**：[x]

**需求**（todo §8 P2）：现有 BacktestPage 是参数敏感度工具（研究用途），等 paper
数据有数字后重写为"回测结论展示"页——回测是 source of truth，页面要先回答
"定案结论是什么"，再谈研究。

**实现**：
- 后端 `GET /api/backtest/overview`：读 `walk_forward_baseline.json` /
  `walk_forward_hk_baseline.json`（三窗 OOS2/train/valid 收益/胜率/DD/夏普）+ 
  `rolling_oos_latest.json`（最近 90 天窗 + warning 列表）+ 长窗固化常量
  （LONG_WINDOW_CN：2021-08~2026-08 +250.8%/DD40.9/夏普2.65/1401 笔 + 年度明细，
  单点定义在 backtest_routes.py，与 strategy-params §1 同源）
- 前端 `BacktestPage` 重写：
  1. **S-3 回测结论**（CN/HK 双栏）：三窗行 + 定案参数徽章（score65/RS 前50%(HK40%)/
     止损-5/移动-8(HK-12)/60天/10%/≤20/闸门/熔断-25）+ 长窗年度明细（2021 +341 …
     2023 -263 …）
  2. **滚动 OOS**：窗口区间 + CN/HK 收益/胜率/DD/夏普/笔数，亏损或夏普<0 红标 + warning
  3. **回测 vs Paper 对账**：recon 快照（✓/⚠ + 缺/多）
  4. **高级**：原参数敏感度工具（单窗回测/网格/相关性/卖出归因）折叠，默认收起
- 测试：后端 +2（overview 读取/缺文件容错），前端 +3（结论板/滚动 OOS/折叠）；
  前端 732 passed + 后端相关 21 passed + ruff/tsc/lint 干净

**验收**：/api/backtest/overview 实测 200（CN OOS2 112.654 · rolling HK warning=True）；
BacktestPage 首屏即回测结论，参数工具退居折叠。

### OPT-082：全局通知中心（任何页面提醒 + 跳 watchlist 详情）（2026-08-12）

**状态**：[x]

**需求**：任何页面都能弹出 notification 组件，点击跳 watchlist 页并在其中
看到详细标识，提醒用户做操作（买入提醒/接近止损/回测缺票/cron 失败）。

**实现**：
- 后端 `service/notifications.py` + `api/notifications_routes.py`：
  `GET /api/notifications` 聚合现有产物（零新数据采集）——① 接近止损/移动线
  （复用 portfolio-health holdings，距线 ≤1.5pt）+ EXIT 建议；② recon 缺票
  （latest_recon missing>0）；③ 今日交易链 cron 失败（sync_job_record 24h，
  白名单 8 个 job）；④ 滚动 OOS warning。每条带 severity(high/medium)/anchor
  （holdings|recon|scheduler|backtest）
- 前端 `components/notifications/NotificationHub.tsx`（AppShell header 全局挂载）：
  铃铛 + 未读角标（localStorage 已读持久化）· 新事件 toast（6s，仅 high/medium，
  按"新出现 id"而非"未读"触发，点掉一条不会连环弹）· 面板列出全部通知 +
  本地买入提醒（buy-reminders 合并）。点击 → hash 跳 #/watchlist + 事件
  scrollTo 锚点区块 + 高亮闪烁（health-flash CSS）
- watchlist 页：PortfolioHealthCard 持仓容器/ReconBlock 加 id（holdings、
  holdings-hk、recon、recon-hk），WatchlistPage 监听 karios-scroll-to 滚动+闪烁
- 复用：portfolio-health/recon/sync_job_record 产物 · hash-router · buy-reminders ·
  PortfolioHealthCard 已有区块（详情标识就是体检卡本身）

**测试**：后端 +6（4 聚合 + 排序 + 路由），前端 +3（toast 跳转/未读面板/本地
提醒合并）；后端 3294 passed + 前端 735 passed + ruff/tsc/lint 干净。

### OPT-083：决策 Agent 周度自动复盘（H2 · 周报→下周行动计划自动产出）（2026-08-12）

**状态**：[x]

**需求**（todo H2）：周报→下周行动计划自动产出，用户只确认。周报数据端
（weekly_review：决策量/paper 实绩/归因/对账/自动观察）早已齐，缺：cron 自动生成 +
LLM 行动计划 + 前端确认界面。

**实现**：
1. 后端 `scheduler/weekly_review_job.py`：周一 07:40 自动聚合上周（周一~周五）→
   morning_briefs（brief_type='weekly-review'）+ sync_job_record；注册 scheduler +
   SYNC_JOB_TYPES + SCHEDULER_JOB_CATALOG（coreClose 组）
2. 后端 `/api/backtest/weekly-plan`（POST 存储 / GET 最新）：行动计划落 morning_briefs
   （brief_type='weekly-plan'，按周一 key）
3. ai-service `POST /weekly-plan`：预取上下文（周报 markdown（上周五）+ 实时体检 +
   recon 对账 + 滚动 OOS + 长窗定案）→ LLM 产出「下周行动计划」markdown（买入/卖出/
   条件单/观察项 + 数据引用铁律）→ Gemini primary + openai fallback → 自动落库
4. 前端 WeeklyReviewCard：加「下周行动计划」区（自动显示已存计划 + 生成/重新生成
   按钮 + 复制 + 状态提示）

**踩坑**：`maxOutputTokens: 1800` + Gemini thinking high → 思考 token 吃光预算、
输出截断在 ~150 字符（两次复现）——去掉 maxOutputTokens（与生产 decision 流式
同参数）后完整输出 1062 字符 ✓；周报日期计算（上周五 = 今天 - (weekday+2)%7）。

**验证**：实际生成（google）：正确引用 Weak 纪律（A 股不开新仓）+ HK 缺票 19 只
（HK:02343 score100 等）+ 4 持仓条件单（止损/移动/到期）✓；后端 3299 passed（+3）·
ai-service 9 passed（+3）· 前端 737 passed（+2）· ruff/typecheck 干净

### OPT-084：条件单"需调单"标记（移动线/止损线上移 + 临近到期）（2026-08-12）

**状态**：[x]

**需求**：券商固定价条件单会因移动线随峰值上移而过时——线值变化时标记"需要操作"。

**实现**：
1. `db/watchlist_automation.py`：`update_registry_payload`（JSONB 合并写回，不删行）
2. `portfolio_health.py`：`_detect_line_ops` 纯函数——对比上次通知基线与当前线值：
   trail 上移 → `trail_up`；stop 上移（金字塔加仓）→ `stop_up`；持有期剩 ≤5 天 → `expire_soon`
   基线存 registry payload `conditionalOps`（首次只存不提醒，每次变化提醒一次）
3. `notifications.py`：`line_update`（需调单 · 移动线/止损线上调，附旧→新值）/ `expire_soon`（临近到期，券商条件单无自动到期）
4. `trading_brief.py`：持仓行尾 `🛠移动线上调 36.828→37.52` / `⏰剩 3 天到期`（前端 markdown 直渲，无需改 UI）

**踩坑**：`list_registry` 把 payload 展开为顶层字段（`**payload`），
`payload.get("conditionalOps")` 恒空 → 基线永不匹配，lineOps 永不产生——改为
`r.get("conditionalOps") or payload.get(...)` 兜底；`_build_holdings_block` 内
误用未定义变量 `hold`（NameError 被外层 try 吞成空 holdings）——改用
`MAX_HOLD_DAYS` 常量。

**验证**：端到端（真实 DB）：改小 HK:2099 基线 170 → 体检出
`trail_up [170, 178.464]` ✓；后端 3304 passed（+8）· ruff 干净

### OPT-085：体检区信息层 P1——α 事件 + CN 行业资金流叠加 S-3 持仓体检（2026-08-12）

**状态**：[x]（todo §3 P1：持仓×α事件 + 持仓×行业资金流）

**需求**：回测底层不动（score/gate/止损 = source of truth），叠加两个正交信息层辅助判断。

**实现**（`portfolio_health.py`，纯展示字段 `alphaEvents` / `industryFlow`，不进任何门槛）：
1. `_alpha_events_for_symbols`：`fetch_trends(limit=200, max_age_days=14)` 一次批量 → cnSymbols/
   hkSymbols 精确匹配持仓（HK 4 位→5 位补零 `_alpha_sym_key`）→ 每只 ≤3 条按 confidence 排序，
   含 trend/grade/daysAgo/riskStatus/focus
2. `_l1_industry_for_symbols`：watchlist_score_daily 最新行取 SW L1 行业（与 S-3 候选同源口径，
   非 stock_basic 的 L2 名）
3. `_industry_flow_map`：最近 5 交易日 SW L1 主力净流入（flow_items_from_rows 同款）→
   {行业: {netInflow5d 亿元, rank5d, total}}；ETF/HK 无数据 → 字段缺省
4. 前端 PortfolioHealthCard：持仓行内嵌 `📰 事件（催化A · 2天前 · 映射0.85）` +
   `🧭 通信 5日 -47.69亿（第26/31）`（流入绿/流出红）；PortfolioHolding 类型扩展

**踩坑**：
- `_alpha_sym_key` len 判 6 实际 7（"HK:2099" 7 字符）→ KeyError
- bucket key 统一 5 位归一化，读取端同步 normalize
- JSX `A || B && (JSX)`：A=true 时表达式值为 true 而非 JSX → 条件整体加括号
- `_l1_industry_for_symbols`/`get_connection`/`datetime` 模块级 vs lazy import 混乱

**验证**：真实数据——CN:300628 → 通信 5日 -47.69 亿（第 26/31，流出红）；14 天内
67 条趋势无持仓匹配（真实）。后端 3308 passed（+4）· 前端 738 passed（+1）· tsc/ruff 干净

### OPT-085 续：信息层 P2 + C4 验证铺垫（2026-08-12）

**状态**：[x]

**P2**（候选 + 汇总）：
1. `_health_block`：info 层 map 统一构建一次（持仓 ∪ 候选），候选行附加
   `alphaEvents` / `industryFlow`（候选行业 = score 表 L1，已在 mainline 白名单）
2. block 新增 `infoSummary`（holdingsCount / eventHoldings / industryOutflow / industryInflow）
3. 前端：MarketBlock 头部「信号 · N 持仓 · 无事件冲突 · N 只行业资金流出 ⚠」汇总行 +
   BuyList 候选行 `📰 事件（催化A · 1天前）` / `🧭 有色金属 5日+8.2亿（第2/31）`

**C4 铺垫**（alembic 0029）：`paper_trades.signal_snapshot JSONB`——S-3 intake 开仓时
记录行业 5 日净流入排名/总额 + α 事件数（`_signal_snapshot_for`，CN flow + CN/HK α，
HK 无资金流数据源）。paper 平仓 ≥20 笔（现 3 笔）后做「行业资金领先/事件标签 vs 收益」
对照——纯验证数据，不进任何门槛。

**踩坑**：def 误插 run_intake_s3 函数体内（模块级移动）；`{alphaEvents: 0}` 视为无数据 → None；
alembic HEAD 基线测试同步。

**验证**：后端 3315 passed（+6，含 alembic 8）· 前端 739 passed · ruff/tsc 干净

### OPT-086：防守向回测攻击全组实验（2026-08-12 · 用户方针：防守大于进攻）

**状态**：[x]（全 23 项 · 零采纳 · 文档 §7 全记录）

**执行**：`backtest-strategy.md` §7 全清单 20 项候选 + 3 基线，每条 `run_walk_forward
--param` 三窗 + HK 线。结果：拒收 17 · 中性 4 · 采纳 0。

**新增引擎能力（全部默认禁用，live 常量 0）**：
- `profit_trail_trigger_pct/profit_trail_pct`（A6 盈利后回撤分档）——engine + paper_trading 同码
- `industry_flow_exit_days`（B1 行业资金流退出）+ `CLOSE_REASON_FLOW_EXIT`——覆盖段证伪
- `mainline_top_k`（B2 白名单深度）
- `score_confirm_days`（C1 分数连续确认）

**核心结论**：S-3 定案经得起 20 项防守向收紧攻击——退出更早/资金流退出/组合分散/
熔断收紧/分数确认全部截断趋势利润腿或增加机会成本；资金流作为退出信号被证伪
（行业净流出是常态，退出=底部割肉）。防守空间已 quantified 为"不存在于参数收紧"，
在准入质量（RS 轮动 6.8 已落地）+ 纪律执行。

**验证**：后端 3318 passed（+4 引擎测试）· ruff 干净 · 文档 §7 实验表 23 行完整

### OPT-087：C4 paper-vs-backtest 对照框架 + trailing 口径对齐（2026-08-12）

**状态**：[x]（框架已搭 · 等 paper ≥20 笔平仓出统计结论）

**实现**：
1. `scripts/paper_vs_backtest_report.py`：读 paper closed（S3/S3HK）→ 每市场跑一个
   **连续完整窗**（最早入场 -60 天 ~ 最晚平仓 +5 天，S3_CONFIG/HK_S3_CONFIG 同参）→
   逐笔匹配回测孪生（同 symbol+entry 精确，否则最近 entry）→ 对照表（入场价差/
   收益差/平仓原因）+ 汇总（paper vs 回测匹配胜率/均盈亏）→
   `data/backtest_reports/paper_vs_backtest_latest.json`（样本 <20 标"未定案"）

**首个发现（系统级 bug · 已修）**：live paper S-3 trailing 用**盘中最高价**做峰值，
回测/体检卡用**收盘价**——口径漂移（HK:00622 实证：盘中 spike 峰值触发 live trailing，
close-based 回测永不触发）。修复 `paper_trading.py` trailing peak 改 close-based，
与回测引擎/体检卡同码（+3 测试）。方向：live 更敏感 → 修复后 paper 退出与回测收敛。

**踩坑**：逐笔小窗起点批量满仓 → sleeve 假满（"回测未入场"假象）→ 改连续完整窗；
窗口尾部 end_of_window 伪差（对照窗口需覆盖到实际退出，BUFFER 可调）。

**验证**：后端 3320 passed（+3）· 2 笔样本雏形可跑（HK:00697 同因 stop_hit 价差 1.4%；
HK:00622 揭示口径 bug 已修）· ruff 干净

### OPT-088：体检卡闸门状态醒目 + S-3 可行性核验（2026-08-12）

**状态**：[x]

**需求**：用户在 12:00-14:30 盘中买入，要求"闸门开着时前 5 候选恒有；除非闸门关闭"。

**核验（真实数据）**：
- Strong/Diverging 日候选恒满 20（历史 2026-02 四天实测）→ TOP5 保证 ✓
- 最近 CN 连续 Weak + 恐慌冷却 → 0 候选 = 闸门正确关闭（S-3 铁律 Weak 空仓）
- 盘中 10:30/14:00 intraday 分数（实时价）→ 用户盘中出现即买 ≈ 回测信号日收盘口径；
  隔夜测量：次日开盘 vs 收盘均值 +0.15%~+0.24% 中位 0%（新工具 scripts/measure_entry_lag.py）
- 盘中 flow 闸门用昨日数据（当日 17:35 才有）= 数据天花板，如实保留

**实现**：
1. 修 live bug：B1 引擎 `_load_flow_mainline_data` 改 3 元返回时漏改 `paper_s3.py`
   （候选构建崩溃路径）+ 测试 mock 同步
2. 前端：HealthPanel 头部加粗红标「闸门关闭 · 今日不买」（Weak/panic/熔断时；
   block 为 null 不误标）；+2 测试

**验证**：后端 3320 passed · 前端 741 passed（+2）· tsc/ruff 干净

### OPT-089：BacktestPage 重写——C4 paper-vs-backtest 对照 + 交互式扫描（2026-08-12）

**状态**：[x]

**需求**：todo §12 #12——基于 paper 数据的参数敏感度工具；含 paper-vs-backtest 对照展示
（复用 scripts/paper_vs_backtest_report.py 输出）+ trailing/stop/gate 参数交互式扫描。

**实现**：
1. 后端 `GET /api/backtest/paper-vs-backtest`：读 `data/backtest_reports/paper_vs_backtest_latest.json`
   （404 未跑脚本 / 500 损坏同 latest-report 风格）+ 3 测试
2. 前端 `usePaperVsBacktestQuery`（lib/queries/backtest.ts 类型对齐 JSON：rows/summary/verdict）+ 
   BacktestPage「C4 · paper vs 回测逐笔对照」卡片：verdict 横幅（<20 笔标"未定案"）、
   paper vs 回测匹配胜率/均盈亏四格、逐笔表（入场价差 >0.5% 琥珀标记）、口径说明
3. 交互式扫描：敏感度网格行点击 → 载入该配置到单配置回测并运行（score/hold/stop/gates 联动）

**现状**：样本 2 笔（HK:00697 stop_hit 价差 1.4% · HK:00622 trailing 口径差异）——
框架就绪，≥20 笔平仓后 C4 自动出统计结论（文档注释已写明）。

**验证**：后端 91 passed（test_backtest_routes_extra + engine）· 前端 742 passed（+1）· tsc/ruff 干净

### OPT-090：webhook 事件订阅 P1（2026-08-12 · todo §14 #3 · 设计稿拍板后实现）

**状态**：[x]（P1；P2 事件待挂载）

**拍板**（§7 四项全决）：两者都要（个人 AI 助手 + 决策 Agent）· E3 盘中巡检 1 小时一轮
（券商条件单兜底）· 先 API + cookbook 示例（前端页 P2）· E5 候选 diff P2 评估后做。

**实现**（三层 + 2 事件源）：
1. **表**（alembic 0030 + db/webhook.py CREATE_SQL 同步）：`webhook_events`（dedupe_key 唯一）、
   `webhook_subscriptions`（url + HMAC secret + event_types[]）、`webhook_deliveries`
   （pending→sent/failed×3→dead 状态机，5/15/60 分钟退避）
2. **API**：POST/GET/DELETE `/api/webhook/subscriptions`（secret 自动生成 token_hex(16)）+
   `POST /api/webhook/test`（连通性测试）
3. **投递器** `service/webhook_delivery.py`：HMAC-SHA256（X-Karios-Signature: sha256=hex）、
   5s 超时、30 条/订阅/分钟限频；scheduler `webhook_delivery_job` 每分钟
4. **E1** `job_failed`：sync_job_record.insert_record 失败分支 emit（当日按 job 去重）
5. **E3** `intraday_drawdown`：`intraday_alarm_job` 工作日 10-14 点整点，open paper 仓
   实时价 ≤ 入场价 -8% emit（每票每日一次）
6. shared SCHEDULER_JOB_CATALOG +2 job；cookbook §9（订阅 curl + Python 接收端签名校验示例）

**验证**：后端 3340 passed（+20：db 集成 4 / 路由+投递 8 / E1+E3 5 / alembic / scheduler）·
shared 64 passed · 前端 742 passed · ruff 干净 · alembic head=0030（本地已 upgrade）·
E2E 手工链路（订阅→emit→投递→清理）通过

### OPT-091：webhook P2 挂载 + 系统稳定性审计（2026-08-12）

**状态**：[x]

**P2 事件挂载**（E2/E4/E5/E6/E7，各 ~10 行，全部 dedupe 按日）：
- E2 `paper_chain_issue`：paper_chain_watchdog 链断（close_sync 缺/self-heal 后仍缺）
- E4 `near_stop`：trading_brief midday/action 组装时按 alert 行 emit（每 symbol+line+日一次）
- E5 `candidate_added`：新 job（17:35）对比上一交易日 S-3 候选，**只推新增**（评估：消失=闸门关闭属正常噪音）；17:35 在 automation 后 intake 前
- E6 `oos_warning`：rolling_oos 收尾 warning 非空时 emit
- E7 `recon_missing`：backtest_recon missing>0 时 emit（按日一次）
- 前端 WebhookPage（sidebar 新增）：订阅列表/创建（secret 一次性展示）/删除/测试事件 + 事件类型 chips

**稳定性审计**（2026-08-12 晚间）：
- ✅ 核心链全绿：17:10 close_sync → 17:30 automation → 17:35 industry → 17:40/17:42/17:45 paper → 18:05 watchdog（sync_job_record 实证）
- ✅ 数据新鲜度：daily/score 均到 2026-08-12（23844 行今日）
- ✅ DB 备份正常：03:00 dump 372MB + age-check 安全网 + iCloud 镜像
- ✅ 磁盘 43Gi 可用 · alembic head=0030（已 upgrade）· db_rows_baseline 已重存（系统真实活动致过期，非测试污染；清理 webhook_events 测试残留 2 行后重存）
- ✅ 测试纪律：全量测试后 webhook 表 0 残留（E5 local import 修 patch 失效路径）
- ⚠️ 发现：uvicorn 22:25 被用户手动重启为**前台 + --reload**（08-11 曾去 --reload 修复 misfire 循环）——提醒用户用 nohup 无 --reload 方式；tushare 限频（etf/adj_factor/top_inst 瞬时 FAIL，10:01 后恢复，非本轮引入）

**验证**：后端 3349 passed（+9 P2 挂载测试）· 前端 745 passed（+3 WebhookPage）· shared 64 ·
ruff 干净 · webhook API 已上线（curl 实证）

### OPT-092：一键启动统一到 npm run dev（2026-08-12）

**状态**：[x]

**背景**：用户希望所有服务都由 `npm run dev`（根目录 turbo dev）启动，不再手动
nohup uvicorn。此前 08-11 因 uvicorn `--reload` 引发调度器 misfire 循环（每 2-3
分钟 job 重建失败）改走 nohup 单独跑；但用户手动重启时带回了 --reload 且前台
挂终端（22:25 那次），并曾出现"根目录跑 nohup 命令 → ModuleNotFoundError
（--app-dir src 是相对路径，必须 cd services/data-sync-service）"。

**变更**：
1. `services/data-sync-service` `dev` script：去掉 `--reload`（调度器稳定性根因），
   仍是 4330；新增 `dev:reload` 备用（--reload + 4331，开发调试不碰调度器）
2. 验证：`npm run dev` 一键起 3000（Next.js 200）/ 4310（tsx watch）/ 4330
   （uvicorn health 200）——predev 的 ensure-ports 会自动清 4330 旧进程
3. 清理：22:25 手动前台 uvicorn（16799/16763）已终止

**用法**：以后只跑 `npm run dev`（仓库根目录）；改 Python 代码需重启 dev
（调度器稳定优先，08-11 教训）；改前端/ai-service 代码热更新不受影响。

**E2E 实证（2026-08-12 22:50）**：本地接收器 + 订阅 + test 事件 → 每分钟投递 job
真实 POST 到接收器，HMAC-SHA256 签名校验通过（sig_ok=True）；订阅已删、测试事件
已清。webhook_delivery job 运行时实证 ✓。

**补充**：`SYNC_JOB_TYPES` + webhook_delivery/intraday_alarm/candidate_diff
（Scheduler 页展示完整性；三者不写 sync_job_record，todayRun 恒 None 属预期）。

### OPT-093：红绿灯回测验证 + HK 仓位启发式删除（2026-08-12）

**状态**：[x]

**需求**：用户质疑红绿灯仓位建议（红→0-10%、黄→30%、绿→50-60%、深绿→80-100%）
是否与回测一致——"回测一下红绿灯，判断定义是否正确；不符合就替换或删除"。

**方法**：`scripts/backtest_index_lights.py`（可重复）——get_index_signals 逐日回放
（as-of、无实时、无 breadth，杜绝前视）+ S-3 引擎同窗口模拟（CN 长窗 2021-08~2026-08
1196 笔 / HK 2024-08~2026-08 599 笔）→ 按入场日红绿灯分层（均值+中位数+分位）。

**结果**（report: data/backtest_reports/index_light_backtest_latest.json）：

| 市场 | 灯 | 笔数 | 胜率 | 中位盈亏 | 判定 |
|------|-----|------|------|---------|------|
| CN | green | 258 | 41% | -2.1% | 正常 |
| CN | yellow | 655 | 42% | -2.5% | 正常 |
| CN | **red** | 283 | **27%** | **-5.5%** | **显著差 → 定义正确** |
| HK | green | 335 | 36% | -5.1% | 无区分 |
| HK | yellow | 116 | 46% | -2.0% | 无区分 |
| HK | red | 148 | 39% | -5.0% | 均值 +18% 系右上尾少数暴利单 |

**结论与动作**：
1. **CN 红绿灯定义正确**（红灯日入场显著差：胜率 27% vs 42%、中位 -5.5%）——保留
2. **HK 红绿灯无区分度**（红/黄/绿中位数 -5.0/-2.0/-5.1%，胜率无单调）——删除 HK
   `positionRangeHint`（execution_gate.py，注释写明回测依据；前端已处理 None 不渲染
   "仓位"）；绝不按均值反转（右上尾假象 = 过拟合）
3. 警示：均值统计在 HK 会被少数暴利单误导——今后分层验证一律看中位数+分位

**验证**：后端 71 passed（gate 2 文件）· 前端 12 passed · tsc 干净 · API 实证
（HK hint=None，CN 30% 保留）· ruff 干净

### OPT-094：CN 红灯日禁开仓——回测验证 + 全链路落地（2026-08-12）

**状态**：[x]（定案 · 已上线）

**背景**：OPT-093 证实 CN 红灯日入场是负 EV（胜率 27% vs 42%）。用户拍板：
"A 股红灯日确保无购买、无推荐"（港股无回测支持，维持现状）。

**反事实验证**（trades 层，三窗）：剔除红灯日入场——OOS2 胜率 48→54% 且总收益
不降；train 持平（红灯几乎不触发）；valid 胜率 61→79%、总收益 +10%——无窗变差。

**walk-forward 三窗**（light_red_block=True）：OOS2 113.7%（+1.0pt）/胜率 51.3%（+3.3pt）；
train 持平；valid 98.9%（+10.7pt）/胜率 78.6%（+17.3pt）/回撤 11.8→1.5%——**通过定案**。

**落地（回测与 live 同码）**：
1. 引擎：`BacktestConfig.light_red_block`（默认关，不破坏基线）+ `GATE_REASON_INDEX_RED`
   + `_load_light_red_days`（get_index_signals as-of 回放，与 regime 加载共享缓存，无前视）
2. live：`paper_s3.S3_LIGHT_RED_BLOCK=True` + `_index_light_red()`——CN 红灯日候选
   build_s3_candidates 返回 []（无推荐）；HK 不检查（OPT-093 无区分度）
3. 前端：A 股闸门 indexLight=red 时红标「红灯日 · 禁开新仓」（即使 mode=ATTACK）；
   HK 不标（无回测支持）

**验证**：后端 68 passed（引擎）+ 33 passed（paper_s3，含红灯/绿灯/helper 3 新测试）·
前端 14 passed（+2 红灯标/港股不标）· ruff 干净 · 实证：2026-06-01 红灯日候选=0，
今天（非红）正常 · 服务已重启生效

### OPT-095：体检卡与主表退出信号对齐（2026-08-12）

**状态**：[x]

**问题**：用户发现 S-3 持仓体检卡与 watchlist 主表 action 不一致——同一持仓
一个 HOLD 一个 EXIT。

**根因（两套退出规则并存）**：
- 主表 `deriveActionCard`：trendok `exit_now`（**趋势结构破坏**：EMA5<EMA20 /
  收盘<EMA20 / 动量衰竭 MACD 萎缩转负+量能干涸）+ 实时价跌破 stop——结构信号即 EXIT
- 体检卡 `_holding_check`：仅 S-3 回测口径（-5% 固定止损 / -8%/-12% 吊灯 / 60 天）
  ——**没有结构信号规则** → 触发了结构破坏但未破价格线的持仓两处显示矛盾
  （实证：CN:300628 pnl -2.36% 未破线，主表 EXIT / 体检卡 HOLD）

**修复**：`_build_holdings_block` 对持仓调 `compute_trendok_for_symbols`（收盘口径，
与主表同数据），合并：`exit_now` → EXIT + 原因（趋势破坏/动量衰竭，中文标注）；
`warn_reduce_half` → HOLD + note「减半警告（结构信号 · 主表 TRIM 同口径）」
（ETF 隔离语义与主表一致：ETF 结构信号降级为警告）。S-3 价格/时间规则保留。

**验证**：+2 测试（exit_now 合并 / warn 合并，20 passed）· ruff 干净 ·
API 实证：CN:300628 → EXIT（趋势结构破坏）✓ 与主表一致 · 服务已重启

### OPT-096：结构信号退出 vs S-3 规则——回测定论（2026-08-12）

**状态**：[x]

**问题（OPT-095 追问）**：主表 trendok 结构信号（EMA5<EMA20 / close<EMA20 / 动量衰竭）
退出 vs 体检卡 S-3 价格/时间规则——**哪套正确？从未有数据证明**。

**实验**（`scripts/backtest_trend_exit.py`，逐笔反事实：持仓期内首个结构信号日收盘退出，
同滑点/成本口径；1196 CN + 599 HK 笔 + 三窗）：

| 场景 | 结构信号退出 | S-3 原规则 | 差 |
|------|-------------|-----------|-----|
| CN 长窗 close<EMA20 | -796% | -285% | **-511pt** |
| CN 长窗 momentum | +211% | +499% | **-288pt** |
| OOS2 close<EMA20 | -124% | +37% | -161pt |
| train close<EMA20 | -108% | +46% | -154pt |
| valid close<EMA20 | -53% | +135% | -188pt |
| HK momentum | +396% | +878% | -481pt |

**结论**：结构信号作为退出信号在**所有窗口/所有市场全部劣化**——截断趋势利润腿，
与 OPT-086 防守攻击结论互相印证（更早退出=更差）。**S-3 价格/时间规则是退出权威**。

**处置**：
1. 回滚 OPT-095 的错误合并（结构信号曾把持仓标 EXIT）——现在结构信号仅作 note
   「主表结构信号（…）——回测证实按结构信号卖出劣化，以 S-3 止损/吊灯为准」
2. 主表（trendok 结构 EXIT）不动（历史 V6.x 看盘规则，非 S-3 持仓范围）；
   用户应知悉：对 S-3 持仓，主表结构 EXIT 建议按回测不应执行
3. 测试 20 passed · ruff 干净 · 服务已重启 · API 实证（CN:300628 → HOLD + 警示 note）

### OPT-097：完全 S-3 执行——主表/体检卡退出信号统一（2026-08-12）

**状态**：[x]

**背景**：OPT-096 回测证实 trendok 结构信号（EMA5<EMA20 / close<EMA20 / 动量衰竭）
作为退出信号全面劣化（长窗 -511pt / momentum -288pt，全窗全市场一致）。
用户拍板：「完全走 S-3，保证看到的提示/操作只有 S-3 的」。

**变更**：
1. 前端 `deriveActionCard`（主表）：held 持仓的退出**只保留 priceAtOrBelowTrigger**
   （S-3 止损/吊灯线实时价触发，ETF 触及吊灯线仍 TRIM）；删除 exit_now /
   warn_reduce_half / heldTrim（mainline fade / DEFEND 板块减仓）三类非 S-3 退出分支
2. 后端 `portfolio_health`（体检卡）：移除结构信号合并（连 note 都不出现）；
   `_trendok_exit_signals` helper 删除——体检卡只有 S-3 止损/吊灯/持有期
3. 保留：非持仓观察票的既有入池提示（BUY/ADD 观察逻辑不变，S-3 候选在
   体检卡/决策 Agent 是权威）

**验证**：前端 748 passed（execution-action 144，8 个测试改口径 + etfExitDowngraded/
exitNow 未用变量清理）· 后端 18 passed · tsc 干净 · API 实证（CN:300628 结构信号
日 → HOLD，不再出现任何非 S-3 提示）· 服务已重启

**结论存档**：主表=实时价格预警（S-3 规则时点提前）；体检卡=S-3 收盘规则；
两者都只输出 S-3 规则结果。结构信号不再出现在任何持仓操作提示中。

### OPT-098：盘中 action 时点锁定（12:00-14:30 稳定信号 · 2026-08-12）

**状态**：[x]

**需求**：用户每天 14:00 交易，要求 12:00-14:30 时段 action 基本不变且正确——
此前实时价在止损/吊灯线附近抖动时 action 反复横跳（HOLD↔EXIT）。

**实现**（`lib/intraday-lock.ts`，仅 action 派生用，价格显示/PnL 仍实时）：
- 12:00-13:00（午休）：action 价 = 上午收盘（trendClose），天然稳定
- 13:00-14:00：实时价（盘中预警保持在线）
- 14:00-15:00：**14:00 单次快照**——窗口内首个报价按 symbol+day 冻结，
  之后价格再变 action 不变（用户大部分交易在 2 点以后）
- 其他时段/非交易时间：实时价照旧；缓存按日重置

**接入**：WatchlistRow `deriveActionCard.currentPrice` 改用锁定价。

**验证**：前端 754 passed（+6 锁定单测：午休/冻结/跨日重置/无报价 fallback/
窗外实时/非交易时间）· tsc 干净 · Next.js 热更新已生效（无需重启后端）

**语义**：14:00-15:00 用户看到的是「2 点快照」的 action——2 点后操作
不会因盘中抖动而误判；15:00 收盘后恢复实时。

### OPT-099：copy markdown / 主表 Action 与 S-3 回测口径完全对齐（2026-08-13）

**状态**：[x]

**背景（用户最后核对）**：copy markdown 与 S-3 回测"action 部分、内容完全一致"检查——
S-3 候选区（`paper_s3.build_s3_candidates`）与回测引擎已同码同参 ✓；
但发现残留不一致：**主表/copy 持仓表的止损线 ≠ 回测线**。

**不一致清单**：
| 项 | 旧口径（主表） | S-3 回测 / 体检卡 / live paper |
|----|----------------|-------------------------------|
| 硬止损 | trendok 波动自适应 6/8/10% + ATR 缓冲 + 支撑 | **固定 -5%（cost×0.95）** |
| 吊灯线 | peak − 2×ATR14，浮盈≥10% 才武装 | **固定 peak×0.92（HK ×0.88），入场即武装** |
| 持有期 | 无 | **60 天到期 EXIT** |

**变更**（`lib/execution-action.ts`，UI + copy 统一）：
1. held 非 ETF 持仓：hardStop = cost×(1-5%)（`s3FixedHardStop`）；
   trail = peak×(1-8% CN / 1-12% HK)（`s3FixedTrail`，入场即武装）；
   exitStop = max(两者)——常量 `S3_STOP_LOSS_PCT / S3_TRAILING_STOP_PCT(_HK) / S3_MAX_HOLD_DAYS`
2. 主判定链新增 `MAX_HOLD` 分支：持有 ≥60 个日历日 → EXIT（T+1 锁仍 fail-closed）
3. 保留特例：ETF（ATR 吊灯 + TRIM 语义）、空仓观察（trendok 线作入场参考）、
   防御仓（DEFEND 下 EMA10 线更紧者仍生效）
4. 新增 `daysBetweenDates` helper；why 中文映射 +`MAX_HOLD:持有期满60天`

**验证**：execution-action 147 passed（+3：MAX_HOLD 触发/未触发/口径更新 3 处）·
全量 757 passed · tsc 干净 · copy markdown Exit_Stop/HardStop/TrailStop 列
与体检卡 stopLossLine/trailingLine 同式（cost×0.95 / peak×0.92）

**结论存档**：现在主表 / copy markdown / 体检卡 / live paper / 回测引擎——
五处退出口径同式（-5% 固定止损 · -8%/-12% 吊灯 · 60 天），
用户 14:00 看到的每个 EXIT 数字就是回测验证过的数字。

### OPT-100：HK 及时信号落地——体检卡实时判定 + 收盘峰值前端对齐 + HK 冻结至 16:00（2026-08-13）

**状态**：[x]

**背景（用户拍板）**：HK 最优解 = trailing **-12%**（完整灵敏度扫描见 strategy-params §1b，
唯一三窗全过；-10 OOS2 高 19.6pt 但近端劣化 25.4pt，-14/-16 OOS2 崩 -160pt）。
用户要求"用及时信号更新到 HK 的 action，保证能 follow"——14:00 看到的 HK 动作
必须与可执行口径一致（此前体检卡 HK 用昨日收盘显示 HOLD，主表实时价已 EXIT，两处打架）。

**变更**：
1. **体检卡 HK 行实时判定**（后端 `portfolio_health.py`）：`_holding_check` 新增
   `realtime_price`（HK 分支批量取 `fetch_realtime_quotes`，Sina HK 源）；action/pnl/
   回撤用实时价评估，**峰值保持收盘口径**（回测铁律）；输出 `evaluatedPrice`/`realtime`
   字段。CN 行保持收盘口径不变（OPT-097 双轨：CN=收盘权威，HK=及时信号）
2. **前端 S-3 trail 峰值对齐收盘**（`execution-action.ts` + trendok `s3PeakClose`）：
   后端 trendok 按 registry entryDate 计算"自入场起最高收盘"（`s3PeakClose`），
   前端 `s3FixedTrail` 优先用它（回退 maxPrice）——主表/copy 移动线 = 回测/体检卡
   同源（OPT-087 已修 paper，此为前端最后一块）
3. **HK 冻结窗口延至 16:00**（`intraday-lock.ts`）：14:00 快照冻结到 HK 收盘
   （CN 仍 15:00）——15:00-16:00 HK action 不再跳变

**验证**：后端 19 passed（+1 realtime 判定测试）· 前端 758 passed（+1 HK 冻结）·
tsc/ruff 干净 · API 实证：HK:2099 → `EXIT stop_loss（净亏6.0%>=5%）rt=True eval=190.7`
（实时价已破 192.66，与主表/copy 一致）；CN:300628/ETF:513180 → HOLD rt=False（收盘口径不变）·
服务已重启

**结论存档**：HK 三条线（体检卡/主表/copy/paper/回测）全部统一：trail -12% 收盘峰值、
止损 -5%、60 天；体检卡 HK action 与主表/copy 实时同步——用户 14:00 follow 任意一处
都是同一个数字。

### OPT-102：蒙特卡洛置信度分析 + 涨跌停缺口记录（2026-08-13）

**状态**：[x]

**工具**：`scripts/run_monte_carlo.py --market CN|HK --iters N`——单次长窗 simulate
（同引擎同口径）→ 平仓日账户收益序列（close-date 分组 Σ pnl×position_pct）→
**block bootstrap（块长 5 日，保持短期收益簇）** 5000/3000 次 → 收益/DD/夏普分布。

**CN（2021-08~2026-08 · 1196 笔 · 5000 次）**：
- 单次 +250.8% / 夏普 2.65（与固化 LONG_WINDOW_CN 完全一致 ✓ 口径验证）
- 分布：5%→+93.7% / 中位 +247.5% / 95%→+422.3%；单次位于 51.4% 分位
- **最差 5% 情形（95% 置信下界）仍 +93.7%——运气极差也不亏**

**HK（2022-06~2026-08 · 599 笔 · 3000 次）**：
- 单次 +363.8% / 夏普 2.17；5%→+103.8% / 中位 +345.2% / 95%→+712.6%
- 单次位于 54% 分位；最差 5% 情形 +103.8%

**结论**：两条线单次结果都落分布中位附近（稳定，非偶然）；95% 置信下界仍大正——
即使执行偏差/运气差，策略期望不亏。用户"操作与回测有出入"的偏差落在分布正常噪音内。

**口径标注**：MC 收益分布与固化常量同口径（可靠）；MC 的 DD 基于平仓日序列
（无持仓期 mark-to-market），比引擎日级 DD（CN 40.9）乐观——DD 分布仅作方向参考。

**涨跌停缺口（记录，未修）**：回测引擎无涨跌停建模（grep 零命中）——涨停买不进/
跌停卖不出未建模，滑点 0.05% 不覆盖跳空；影响方向=回测略乐观（入场高估+出场
按跌停价成交 vs 实际次日更低）。蒙特卡洛测"运气"不测"摩擦"——涨跌停需单独建模。

### OPT-103：回测涨跌停建模（更真实的成交假设 · 2026-08-13）

**状态**：[x]

**需求**：回测从未考虑涨跌停（涨停买不进/跌停卖不出）——回测略乐观
（入场高估 + 出场按跌停价成交 vs 实际次日更低）。

**实现**（`backtest_engine.py`，无需新数据源）：
- `_board_limit_pct`：主板 10% / 创业板+科创（300/301/688）20% / 北交所（8/4 开头）30%；
  ST 5% 未建模（无 ST 标记）；HK 无涨跌停
- `_at_limit`：前一日收盘推导涨停/跌停价（qfq 价按比例缩放，1 分容差）
- **入场**：收盘封涨停 → 当日买不进，跳过该信号（引擎逐日重估，次日仍合格自然再入场）
- **出场**：收盘封跌停 → 当日卖不出，所有退出/加仓顺延次日（连续跌停自然滚动）

**验证**：+3 测试（涨停阻入场次日再入 / 跌停顺延次日成交 / HK 无限制），
引擎 71 passed · ruff 干净；HK 三窗与旧基线完全一致（正确性验证：代码对 HK 不生效）

**重固化基线（铁律：引擎变化 → 三窗重跑 --save-baseline）**：
- CN：OOS2 **+119.4%/15.4/5.27** · train **+67.2%/16.6/2.85** · valid **+89.1%/11.8/9.07**
  （旧基线 113.7/76.7/98.9：OOS2 +6.7pt=跳过追涨停亏损入场；train -9.5pt=强势期
  涨停入场被拦+跌停顺延；valid 微降——**数字更保守更真实**）
- HK：不变（+2.2pt 容差内）· 已固化

### OPT-104：波动率自适应止损（ATR% × mult）三窗实验——拒收（2026-08-13）

**状态**：[x]（拒收 · 固定 % 维持）

**需求（用户提出）**：每只股票波动率不同，统一 -5%/-12% 止损是否合理？
**实现**：引擎 `atr_stop_mult` 配置（>0 时 stop/trail = 入场时 ATR14% × mult，
锁定于入场日；`atr14_pct_for` 复用 bars OHLC，无新数据）。+1 测试（ATR 止损
比固定 -15 更紧触发）。

**三窗结果（vs 涨跌停版基线 119.4/67.2/89.1）**：
| mult | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| 2.0 | +114.4 (-5.0) | +92.7 (+25.5) | +50.6 (-38.4) | ✗ valid 崩 |
| 2.5 | +109.6 (-9.8) | +85.0 (+17.8) | +42.4 (-46.6) | ✗ valid 崩 |
| 3.0 | +111.9 (-7.5) | +56.5 (-10.7) | +44.3 (-44.7) | ✗ 三窗皆劣 |

**结论**：train（强势段）ATR 止损大胜（低波动票不被紧止损扫掉）；valid（弱市段）
崩盘（高波动票止损距离过大 → 回撤 11.8%→17-19%）。**固定紧止损的价值恰在弱市
快砍保命；波动率自适应把"松"给了弱市最危险的高波动票**。与 2026-08-09 ATR 仓位
实验结论一致（波动率类调整在该体系不占优）。**维持固定 -5/-8（HK -12）**。

### OPT-105：按市场强弱结合的动态止损——Strong-only ATR 固化（2026-08-13）

**状态**：[x]（全链路完成）

**需求（用户架构洞察）**：卖出本质是 `f(止损线, 市场强弱, ...) → 卖/不卖` 的函数。
用户提出"两种止损按市场强弱结合"：强势段 ATR 让利润跑，弱市段固定线快砍。

**实验链**（引擎 `atr_stop_mult` + `atr_stop_strong_only`）：
| 变体 | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| 纯 ATR 2.0/2.5/3.0 | 114.4/109.6/111.9 | 92.7/85.0/56.5 | 50.6/42.4/44.3 | ✗ valid 崩（高波动票弱市止损太松）|
| hybrid（Strong+Diverging=ATR）2.0 | 112.6 | 89.7 | 87.2 | ✗ OOS2 -6.7pt（Diverging 段劣化源）|
| **Strong-only 2.0** | **+123.3 (+3.9)** | **+73.8 (+6.6)** | **+89.1 (持平)** | ✅ **三窗全过 → 固化** |
| Strong-only 2.4 | 122.4 | 74.1 | 89.1 | 平台期（2.0 为定案档）|

**固化内容**（`S3_CONFIG`：atr_stop_mult=2.0 + atr_stop_strong_only=True）：
- 回测：Strong 日止损/吊灯 = **入场锁定 ATR14% × 2.0**（低波动票不被紧止损扫掉）；Diverging/Weak 回退固定 -5/-8（弱市快砍）
- 回测基线已重固化（OOS2 123.3/12.7/5.33 · train 73.8/15.7/2.85 · valid 89.1/12.1/9.42）

**全链路落地**：
1. **live paper**（`paper_trading.py`）：CN S-3 paper（source=S3）出场按当日 regime
   切换——`_atr_pct_at_entry`（entry 前 bars 回溯算，与回测同式）x `S3_ATR_STOP_MULT`
   （db/paper_trading.py 常量）x Strong；Diverging/Weak/失败回退固定 -5/-8（fail-closed）；
   `_cn_regime_today` 复用引擎 regime loader（延迟 import 避循环）。+3 测试
2. **体检卡**（`portfolio_health.py`）：`_holding_check` 加 regime——Strong 用 ATR 线
   （entry 前 45 天 bars 算锁定 ATR），输出 `stopRule`/`stopRuleDetail`（'atr'/'fixed'）；
   `_build_holdings_block` 传 regime（CN only）。+2 测试
3. **前端**（`execution-action.ts`）：`useAtrStop`（held && !ETF && regime=Strong && atr14）
   → `s3AtrHardStop` + trail 同式（当前 ATR 近似，健康卡为权威锁定值）；`S3_ATR_STOP_MULT` 常量
4. **UI 规则检查**（`PortfolioHealthCard`）：每行止损线旁显示「规则：」徽章
   （Strong·ATR×2.0 入场锁定 / 固定 -5%/-8%），hover 有说明——用户可直接核对今天用的是哪条线
5. 验证：前端 759 passed（+1 ATR 线测试，14 个固定线测试改用 Diverging gate）·
   后端 137 passed（+6）· ruff/tsc 干净 · API 实证（今日 Diverging → fixed 线 37.905）

**验证**：+2 测试（ATR 线触发 / Weak 日走固定线）· 引擎 73 passed · ruff 干净

### §19.2 D1：连续市场强度分（0-100）驱动止损——分箱实验全拒收（2026-08-13）

**状态**：[x]（关闭 · 无增量 · 维持 OPT-105 regime 规则）

**想法（用户）**：动态市场强弱数字（0-100）回测验证已知函数，本质像反向传播。
**关键事实**：该数字已存在——`regime_strength_score`（0-100，CN/HK 共用标尺，
绿灯占比+动量+结构三分量），体检卡头部显示；离散 regime 是它的粗离散化；
as_of_date 支持历史回放（29ms/天，5 年 0.6 分钟）→ 回测可行性成立。

**实现**：引擎 `atr_stop_strength_min`（>0 时 strength ≥ X 用 ATR 线，替代 regime 条件；
lazily 缓存每日 strength）+1 测试（strength 高低切换 ATR/固定线）。

**扫描（X=40/50/60/70 × 三窗，vs 固化基线 123.3/73.8/89.1）**：全部拒收——
train 全线大胜（+5~+20pt：宽松档更多 ATR 日）；valid 随 X 恢复（-11.8→+0.5）；
**OOS2 全面劣化 -9~-14pt**。根因：2024-25 弱市里"分数高但未全绿"的脆弱强势日
ATR 止损依然亏——离散 regime（全绿=Strong）天然过滤了它们，连续分数做不到。

**结论**：strength 分数作为止损选择器无增量；维持 OPT-105 的 regime 2 档规则。
引擎开关保留（默认关）。**不新增连续拟合**（样本 1400 笔撑不起连续参数，
反向传播需要百万级样本——三窗铁律就是我们的正则化）。

### OPT-106：行为对账——watchlist 提醒不符合回测的操作（2026-08-13）

**状态**：[x]

**需求（用户）**：保证系统稳定且与回测一致——当有不符合回测的行为，在 watchlist 提醒：
① 买了不该买的 ② 没有卖应该卖的。

**实现**：
1. **`reconciliation.reconcile_registry`**：真实持仓（watchlist registry positionPct>0）vs
   回测应持（simulate 当日 positions_by_day 快照）——extra（持而回测不持）按
   `kind` 分类：`exited`（回测曾入场且已退出 = 该卖没卖）/ `never_entered`（回测从未
   入场 = 买了不该买）；missing（回测应持而用户没持 = 该持没买）
2. **`db/behavior_audit.py`** 新表 + alembic 0031（audit_date/market 唯一，幂等 upsert）
3. **API**：`GET /api/backtest/behavior-audit/latest`（读）+ `POST .../refresh`
   （触发 simulate，实测 32 秒）
4. **前端**：WatchlistPage 顶部 `BehaviorAuditBanner`——extra 红/橙标
   （🔴该卖没卖/🟠买了不该买）+ missing 蓝标（🔵该持没买）+ 刷新对账按钮；
   无差异时显示 ✅ 一致（保持可见）
5. 顺带修复：reconciliation 的 S3_CONFIG 同步 OPT-105 ATR 止损参数（此前对账
   用的是旧固定线——不同口径！）

**验证**：+2 后端测试（extra 分类 exited/never_entered）· +2 前端测试 · 前端 761 passed ·
后端相关 210 passed · ruff/tsc 干净 · **真实对账实证（32 秒）**：
CN:300628 → never_entered（买了不该买 ✓）；HK 13 只 → missing（该持没买）· 迁移已跑

**使用**：watchlist 页顶部横幅即看；每次交易后可点「刷新对账」（约 30 秒-4 分钟）。

### OPT-107：主表"隐藏不符合回测"filter（2026-08-13）

**状态**：[x]

**需求（用户）**：watchlist 主表加一个 filter——不符合回测的（行为对账 extra 的
买了不该买/该卖没卖）像 silent-dead filter 一样可以一键隐藏。

**实现**：
- `WatchlistTable`：`auditExtraSymbols`/`hideAuditExtra`/`setHideAuditExtra` props；
  visibleSortedItems 过滤加入 `shouldHideForAuditFilter`（纯函数，可测）
- 表头 toggle 按钮「隐藏不符合回测 N」（琥珀高亮态 + aria-label），与
  showHidden 按钮并列；开启时过滤、关闭时恢复
- `WatchlistPage`：`hideAuditExtra` state + `auditExtraSymbols`（共享
  useBehaviorAuditQuery 缓存，与 Banner 零重复请求）

**验证**：+3 纯函数测试（开/关/空集）· 前端 763 passed · tsc 干净
**使用**：表头按钮一键切换；Banner 仍保留（提醒 + 刷新入口）。

### OPT-108：LLM job 移至平峰时段（2026-08-13）

**状态**：[x]

**背景（用户）**：LLM API 高峰（工作日 9:00-12:00/14:00-18:00）价格翻倍；
平峰（00:30-08:30/18:00-24:00 + 周末）原价。用户拍板：**晚上 7 点起跑**。

**变更**（3 个 LLM job 从 IntervalTrigger → CronTrigger，全部落在平峰窗口）：
| Job | 原调度 | 新调度（Asia/Shanghai）|
|-----|--------|------------------------|
| alpha_radar_pipeline | 每 12h（进程启动起算）| **19:30** 每天 |
| alpha_radar_process | 每 1h | **20:30/23:30/02:30/05:30** |
| news_enrich | 每 2h | **20:00/23:00/05:00** |

- `SCHEDULER_JOB_CATALOG`（packages/shared）同步显示新 scheduleCron
- decision_action_tracking（18:30）已处平峰（18:00 起）不动；morning/trading brief 不调 LLM
- env 覆盖保留（ALPHA_RADAR_PROCESS_NIGHTLY_CRON=0 回退 interval）

**验证**：调度测试改口径（+1 CronTrigger 字段断言）· 后端 3368 passed · ruff 干净 · 服务已重启

### OPT-109：Alpha Radar 指导性初版验证（真实交易对照 · 2026-08-13）

**状态**：[x]（初版 · 样本不足待 C4）

**背景（用户问"alpha 指导高不高"）**：alpha 的防御价值（auto-QA 防错映射）已验证；
进攻价值（催化建议→收益命中率）从未统计过。

**工具**：`scripts/alpha_guidance_report.py`——每笔 user_trades 对照买入日前 90d 内
同 symbol 的 S/A 级 α 事件（createdAt ≤ 买入日 + 30d 容差），分组对比真实 PnL。

**初版结果（4 笔，全部亏损——行情差）**：
| 分组 | n | 均 PnL | 胜率 |
|------|---|--------|------|
| 有 α 背书 | 2（紫金 -4.53 / 腾讯 -2.44）| **-3.48%** | 0% |
| 无 α 背书 | 2（力量发展 / 中国黄金国际）| **-5.76%** | 0% |

**方向性**：有 α 背书的亏得少（-3.48 vs -5.76）——支持"α 有保护/筛选价值"，
但 **n=2 不作定案**；随 user_trades + paper 平仓积累（≥20 笔）重跑脚本出实证。

### OPT-110：Alpha 前向数据收集（user_trades 快照 · 2026-08-13）

**状态**：[x]（§19.3 收集项 1+2 已落地；第 3 项依赖 trends 保留期，暂不建表）

**背景（用户拍板"从现在开始收集"）**：alpha 是 S-3 参数封闭后"为数不多的变数"；
历史回测不可行（alpha 数据源 2026-08 才上线）→ 从现在起前向收集，6-12 个月后
用真实数据验证：入场背书 / 事件兑现 / **α 做退出的假设（验证通过才允许进退出）**。

**改动**：
- `db/alpha_radar.py::fetch_trends_as_of(day, window_days)`——无前视窗口过滤
  （事件时间 = 文档 published_at 兜底 fetched_at，∈ [day-14d, day) 上海日界）
- `service/user_trades_alpha.py::alpha_snapshot_for(symbol, trade_date)`——
  as-of 聚合（nEvents / hasSA / maxConfidence / riskStatuses / top3 events）
- alembic `0032_user_trades_alpha_snapshot`：`user_trades.alpha_snapshot JSONB`
- `POST /trades` 每笔 BUY/ADD/SELL 自动落 as-of 快照（best-effort：失败不阻断记录）
- `packages/shared/schemas/userTrades.ts`：`AlphaSnapshotSchema` + `alphaSnapshot` 字段

**纪律**：快照只收集不改信号；alpha 退出在验证前不进入任何退出逻辑（OPT-097 铁律）。

**验收**：as-of 过滤（窗口内/当日/未来/超窗/他人 symbol）✅ · DB 往返 ✅ ·
路由链路 + 故障不阻断 ✅ · alembic baseline（HEAD=0032）✅ · 后端 3373 passed ·
前端 763 passed · ruff/tsc 干净 · db_rows_baseline check OK

### OPT-111：行为对账横幅感知买入闸门（2026-08-14）

**状态**：[x]

**背景（用户："我没有办法做操作的时候就不用告诉我买什么，只告诉我需要卖"）**：
宏观死锁/闸门关闭日（CN panic cooldown、HK regime=Weak 空仓观望），横幅仍列出 13 只
HK"该持没买"——买入被强制拦截时这些建议不可执行，纯噪音。

**改动**：
- `lib/queries/portfolioHealth.ts::isMarketGateClosed`——闸门判断唯一真值
  （regime=Weak / regime 未知 / panicCooldown.active / circuitBlocked）
- `PortfolioHealthCard` 改用共享函数（原内联逻辑消除重复）
- `BehaviorAuditBanner` 复用 `['portfolio-health']` 缓存（不重复请求），按市场
  gateClosed 过滤：**隐藏该市场"该持没买"**（不可执行），保留"该卖没卖/买了不该买"
  （可执行/既成事实）；隐藏条数以一行说明披露；全部被隐藏时横幅转安静态
  （"无待操作提醒 — 已隐藏 N 条该持没买"）

**验收**：前端 765 passed（+2：闸门关闭隐藏/安静态）· tsc/eslint 干净 ·
gates open 时行为不变（原有测试保持）

### OPT-112：行为对账自动 cron（2026-08-14）

**状态**：[x]

**背景（"让提醒变成日常"）**：行为对账（OPT-106）只能手动点"刷新对账"（simulate 3-4 分钟）。

**改动**：`scheduler/behavior_audit_job.py`——工作日 18:45（收盘链 17:45 之后）自动跑
`run_registry_and_persist(today)` 落库 → watchlist 横幅免手动刷新；发现不符项
（extra/missing）→ emit `audit_issues` webhook（买不了也提示可操作的卖出项）。
注册 scheduler + SYNC_JOB_TYPES + 前端 catalog（behavior_audit）。

### OPT-113：14:00 执行卡（操作卡时点对齐 + webhook 推送 · 2026-08-14）

**状态**：[x]

**背景（"到点主动告诉我买什么/卖什么"）**：操作卡 14:30 生成，晚于用户 14:00 交易时点；
且只存 brief，不推送。

**改动**：
- `trading_brief_job.py`：action 时点 **14:30 → 14:00**（对齐 intraday-lock 14:00 冻结快照）
- `trading_brief.py::generate_trading_brief`：action 分支 emit `execution_card` webhook
  （gate 状态 CN/HK + 买入候选 + EXIT 持仓，每日 dedupe）
- 前端 catalog 文案/时点同步

### OPT-114：Webhook 事件目录全量 + 订阅落地指引（2026-08-14）

**状态**：[x]（目录已全量；订阅待用户创建接收端）

**改动**：cookbook §9.1 事件目录补全 8 类事件（job_failed / intraday_drawdown /
near_stop / candidate_diff / recon_missing / execution_card / audit_issues / test）。
订阅 + 接收端示例见 cookbook §9.2/9.3——用户创建接收端点后即全链路打通。

### OPT-115：Bark 推送通道（iPhone webhook 接收端 · 2026-08-14）

**状态**：[x]（代码+迁移完成；待用户提供 Bark 设备 key 创建订阅）

**背景（用户选 iPhone + Bark）**：webhook 投递是通用 JSON 格式，Bark 需要
title/body 结构。为 provider='bark' 的订阅增加格式化通道。

**改动**：
- alembic `0033_webhook_provider`：`webhook_subscriptions.provider`
  （'generic' | 'bark'，默认 generic）
- `service/webhook_format.py::format_bark`——8 类事件 → 中文 title/body
  （执行卡/对账/止损/跌穿/新候选/周对账/任务失败/测试）
- `webhook_delivery.py`：provider='bark' 时 body 用 Bark 格式（仍 HMAC 签名）
- 订阅 API 接受 provider 字段（正则校验 generic|bark）

**验收**：formatter 单测 4 例 + delivery bark 用例 + routes 全过；后端 3382 passed；
ruff 干净；服务已重启（provider 字段已生效）。

**用户侧**：装 Bark app → 复制 `https://api.day.app/<key>` → 创建订阅
（provider=bark，事件全选）→ `POST /api/webhook/test` 验证手机收到。

### OPT-116：Family Hub Phase 0 — Cloudflare Tunnel + PWA（2026-08-14）

**状态**：[x]（隧道/PWA/常驻完成；Cloudflare Access 待用户在控制台配置）

**背景（用户愿景）**：家庭投资平台统一入口——手机访问 Mac 上的全部软件，
语音控制、数据说话（docs/designs/family-hub-2027.md）。

**改动**：
- **PWA**：`manifest.webmanifest` + 图标（icon-192/512 + apple-touch-icon，
  PIL 生成，深色底金色柱状图=数据说话）+ `sw.js`（静态缓存、导航 network-first、
  跳过 API 拦截）+ layout metadata（manifest/themeColor/appleWebApp）
- **Tunnel**：Cloudflare 命名隧道 `karios`（id 8d60d5d1…），三个子域
  `karios.it-t.xyz`（UI 3000）/ `api-karios.it-t.xyz`（API 4330）/
  `ai-karios.it-t.xyz`（AI 4310）；~/.cloudflared/config.yml ingress；
  launchd 常驻（plist 修正为 `tunnel run karios`）
- **前端动态 base**：`endpoints.ts` 按 hostname 判断——it-t.xyz 走公网子域，
  本地仍 127.0.0.1（手机/本地同一构建，无需注入环境变量）

**验收**：三个子域 curl 全通（UI 200 / API healthz ok / AI healthz ok）；
前端 84 文件全过（+2 endpoints tunnel 测试）；tsc 干净。
**安全（2026-08-14 增强）**：改为**本地 Basic Auth 网关（caddy :8443，launchd 常驻）**——
密码认证在 Mac 本地完成，不依赖 Cloudflare Access（其验证码流程依赖
login.cloudflareaccess.org，国内网络不稳）。单域名架构：`karios.it-t.xyz` 一
个密码覆盖 UI+API+AI（caddy 按路径分流：/api /v1 及无前缀路由→4330，
/ai→4310，/与静态资源→3000）。密码存 `~/.karios/gateway-password.txt`
（chmod 600）。无认证访问返回 401。

### OPT-117：MobileShell 手机端独立 UI（2026-08-14）

**状态**：[x]（v1：执行/持仓/对账三 tab；后续按需增强）

**背景（用户反馈"手机太难操作"）**：桌面工作区（sidebar+agent 面板+密集表格）在
手机不可用。方案：**移动端独立视图**，不复用桌面组件。

**改动**：
- `components/mobile/MobileShell.tsx`：手机优先 3-tab（底部导航）
  ① 执行：闸门徽章（A股/港股可买与否）+ 下午 2 点买入清单 + 🚩需要卖出
  ② 持仓：每票卡片（盈亏/止损线/移动线/到期/EXIT 标记/盘中预警）
  ③ 对账：该卖没卖/买了不该买（行为审计偏差）
- `AppShell`：`matchMedia(max-width:768px)` 检测 → MobileShell（hooks 隔离，
  React hooks 规则合规）
- 数据全复用现有 API（portfolio-health / behavior-audit），无后端改动

**验收**：4 个 MobileShell 测试（闸门/候选/持仓/EXIT/对账）+ 771 passed ·
tsc/eslint 干净 · tunnel UI 200。
**说明**：v1 只读展示（看）；操作（买入/卖出/对账刷新）后续按需加。

### OPT-118：Gateway 认证改为 X-Karios-Key Header + 前端登录页（2026-08-14）

**状态**：[x]

**背景（用户反馈"手机反复让我登录"）**：caddy Basic Auth 原生弹框在 iOS PWA
standalone 模式下凭据不持久，反复弹框。

**改动**：
- **caddy v3**：去掉 basic_auth——UI/静态资源放行（壳，无数据）；API/AI 全部
  路径校验 `X-Karios-Key` header（环境变量 KARIOS_GATEWAY_KEY，launchd 注入），
  无/错 key → 401 JSON（不弹框）
- **前端**：`lib/auth.ts`（installFetchAuth 全局包装 window.fetch，API 请求带
  header；401 → 清 key + 广播 UNAUTHORIZED_EVENT）+ `AuthGate` 登录页
  （密码存 localStorage，提交后 reload；401 自动回登录页）
- page.tsx 挂载 AuthGate + installFetchAuth

**验收**：UI 200（免认证）；API 无 key 401 / 带 key 200 / 错 key 401；AI 同；
auth 单测 3 例 + AuthGate 4 例；774 passed；tsc/eslint 干净。

### OPT-119：T6 套筒组合 NAV 模拟器 + 回测页卡片（2026-08-21）

**状态**：[x]（三窗验证通过；paper 自动配置仍未落地——见 todo T6 条目）

**背景（补齐回测页局限：ETF 套筒 + 组合 NAV 不可验证）**：回测页此前只覆盖 S-3 短线主线，
无法验证核心仓规则与 ETF 套筒。T6 已拍板"最优 = 纳指ETF(513100) + 空仓日才持有 +
自身 px>200dMA + 破线切 GC001"，但无落地模拟器。

**改动**：
- `service/portfolio_nav_sim.py`：组合 NAV 模拟器——S-3 引擎 `positions_by_day` 的
  已部署比例逐日吃 close-to-close mark-to-market，闲置现金吃 513100 日收益
  （站上 200dMA 持有）/ GC001（破线），逐日复利；基线=闲置 0% 收益
- `scripts/sleeve_nav_sim.py`：三窗验证脚本（S3_CONFIG + third_asset_cache.json）
- `api/backtest_routes.py`：`GET /api/backtest/sleeve-nav`（读 sleeve_nav_latest.json）
- 回测页 `SleeveNavCard`：三窗对比表（基线/套筒收益、增量pt、DD、持有天数、平均闲置）
- 切换语义实测：当天切出（valid +30.4pt）优于次日切出（+14.0pt）——破线次日往往续跌

**三窗结果（2026-08-21 固化报告）**：
| 窗口 | 基线% | 套筒% | 增量pt | 基线DD% | 套筒DD% |
|------|-------|-------|--------|---------|---------|
| OOS2 | 211.5 | 214.3 | +2.8 | 17.4 | 17.3 |
| train | 134.8 | 157.9 | +23.1 | 15.3 | 15.3 |
| valid | 139.4 | 169.8 | +30.4 | 5.7 | 13.7 |

**验收**：三窗增量全正（todo §19 铁律；设计稿目标 +3.1/+15.3/+39.0pt，方向一致）；
valid 套筒 DD 13.7% > 基线 5.7% —— 高闲置 × 513100 波动传导，如实展示（设计稿
"maxDD 略降"为旧引擎口径，不再成立）；模拟器单测 7 例；后端 50 测试 + 前端 804
全过；tsc/eslint 干净。

### OPT-120：核心仓操作核对 + paper 套筒自动配置（2026-08-21 · T6 剩余缺口）

**状态**：[x]（核心仓 audit + paper 自动配置 + allocation 扩展全部落地）

**背景（补齐回测页局限之二：核心仓手动操作无法对照规则）**：S-3 主线自动执行，
但核心仓（300628/513110 等）是手动管理——"我的操作是否符合策略"没有工具回答。

**改动**：
- `service/core_holding_audit.py`：操作 vs 规则核对——**逆向回放**（从当前成本/仓位
  反推每笔操作前状态，8/21 加仓用加仓前成本 39.9×1.025=40.897 判定而非加仓后混仓）；
  金字塔 ADD（regime-independent，恐慌不拦）、SELL（-5% 止损/恐慌降险）、ETF ADD
  （套筒 MA200 语义）；`GET /api/backtest/core-audit` + 回测页 `CoreAuditCard`
- `service/sleeve_paper_auto.py`：paper 书套筒自动配置——BUY_513100 开仓（sleeve_pct=
  闲置%）、SELL_TO_REPO/SELL_TO_A_SHARE 平仓（close_reason=`sleeve_exit`，新增枚举）；
  幂等（ON CONFLICT / 只关 open）；`scheduler/sleeve_paper_job.py` 工作日 18:20
- `service/allocation.py`：R5c 资金池扩展——双市场皆弱时闲置池进套筒
  （`weights_with_sleeve` 三元权重：CN/HK/ETF）；ETF 站上 MA200 才承接

**验收**：audit 实测 8/21 全部操作 4 ok / 0 warn / 0 violation（300628 金字塔
41.83≥40.897 ✅ 半仓 3.0% ✅、ETF 两笔站上 MA200 ✅、2099 恐慌降险 ✅）；单测
28 例（audit 11 + sleeve_auto 5 + allocation 5 + nav_sim 7）；后端全量 3497+
通过（含时间敏感测试修复：top_inst 的 `_is_today` mock）；前端 804 + tsc/eslint 干净。
