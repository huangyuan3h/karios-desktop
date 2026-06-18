# Karios Desktop 优化 Checklist

> 记录架构审查结论与优化方案，供后续逐个 Agent 任务执行。  
> 创建日期：2026-06-18  
> 背景：功能可用，但存在性能、数据持久化、可维护性方面的结构性问题。

---

## 如何使用

1. 按 **优先级（P0 → P1 → P2）** 顺序执行，不要跳 P0。
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
| OPT-001 | TrendOK 热路径性能修复 | P0 | 1–2 天 | [x] |
| OPT-002 | Watchlist 存储权威源明确化 | P0 | 2–3 天 | [x] |
| OPT-003 | 前端 API 层 + God Page 拆分（阶段一） | P1 | 3–5 天 | [x] |
| OPT-004 | 东财行业预热脱离请求路径 | P1 | 1–2 天 | [x] |
| OPT-005 | TV Screener Sync 并行化 | P1 | 1–2 天 | [ ] |
| OPT-006 | TrendOK `refresh` 语义对齐 | P2 | 0.5–1 天 | [ ] |
| OPT-007 | DB Migration 工具（Alembic） | P2 | 2–3 天 | [ ] |
| OPT-008 | TV Capture 异步化 / Job Queue | P2 | 2–4 天 | [ ] |
| OPT-009 | packages/shared 类型共享 | P2 | 1–2 天 | [ ] |
| OPT-010 | 过时 UI 文案清理（SQLite → Postgres） | P3 | 0.5 天 | [ ] |

---

## P0 — 最高收益

### OPT-001：TrendOK 热路径性能修复

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- `get_market_regime()` 新增 `include_breadth` 参数 + 10 分钟进程内 TTL cache（`clear_market_regime_cache()` 供测试）
- TrendOK 热路径：`get_market_regime(include_breadth=False)`，移除 `ensure_em_industries_for_ts_codes` HTTP
- 新增 `lookup_em_industries_for_ts_codes()`（DB-only，供后续 OPT-004 复用）
- 测试：`test_market_regime_cache.py`、`test_trendok_performance_path.py`、扩展 `test_eastmoney_industry.py`

#### 验证

- [x] pytest trendok / market_regime / eastmoney 相关用例通过（558 passed；`test_sync_window_excludes_night` 为既有 20:00 边界问题，非本 PR 引入）
- [x] `curl "http://127.0.0.1:4330/market/stocks/trendok?symbols=CN:600519&refresh=true"` ~0.21s（2026-06-18 本地 benchmark）
- [ ] Watchlist 页刷新 TrendOK 明显变快（需 UI 手动确认）
- [x] Dashboard summary 的 index signals 行为不变（未改 dashboard 路径）

---

### OPT-002：Watchlist 存储权威源明确化

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 后端新增 `GET /watchlist/registry`（`list_registry()`）
- 前端重构 [`watchlist-storage.ts`](apps/desktop-ui/src/lib/watchlist-storage.ts)：`hydrateWatchlist` / `persistWatchlist` / `ensureWatchlistHydrated` / `pendingSync`
- `saveWatchlist` 改为 async，POST-first 再写 localStorage
- AppShell 启动 hydrate；WatchlistPage 等待 hydrate；清理 automation 冗余 sync
- 测试：`test_watchlist_registry.py`（4）、`watchlist-storage.test.ts`（7）

#### 验证

- [x] pytest `test_watchlist_registry.py` 通过
- [x] vitest `watchlist-storage.test.ts` 通过
- [ ] 新安装：加票 → 重启后仍在（需 UI 手动确认）
- [ ] 老用户 localStorage uplift（需 UI 手动确认）
- [ ] POST 失败 pendingSync 重试（需 UI 手动确认）

#### 后续（阶段 B，另开任务）

- Tauri `tauri-plugin-store` 或本地 SQLite 替代 localStorage
- 多设备冲突检测 UI

#### 读写顺序（设计备忘）

```
启动: GET registry → 有则覆盖 local；空则 uplift local → POST
写入: POST registry → 成功则 local + 清 pendingSync；失败则 local + pendingSync
冲突: registry 非空时以 registry 为准
```

---

## P1 — 高价值，次优先

### OPT-003：前端 API 层 + God Page 拆分（阶段一）

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- 新建 `lib/api/client.ts`（`apiGetJson` / `apiPostJson` / `apiPutJson` / `apiPatchJson` / `apiDeleteJson` / `apiFetchJson`，支持 `baseUrl`、`timeoutMs`）
- 新建 `lib/api/types.ts`（`TrendOkResult`、`WatchlistQuote`）、`lib/chunk.ts`、`lib/market-hours.ts`
- 新建 `lib/api/trendok.ts`：`fetchTrendOkMap` + inflight 去重；`screenerExport.ts` re-export 保持兼容
- 迁移全部 Page/lib callsite（Dashboard、Watchlist、15+ Page、`watchlist-screener-import`、`watchlist-automation`、`alpha-radar-catalyst`）
- Dashboard Copy all Markdown 前 `await ensureWatchlistHydrated()` 避免 hydrate 竞态
- 单元测试：`client.test.ts`、`trendok.test.ts`、`chunk.test.ts`、`market-hours.test.ts`（98 passed）

#### 问题

- **20+ 文件**各自定义 `apiGetJson` / `apiPostJson`，无统一错误处理、retry、类型。
- `DashboardPage.tsx` (~2790 行)、`WatchlistPage.tsx` (~2588 行) 混合 UI + 数据 + 导出 + SSE。
- `TrendOkResult`、`chunk()`、`isShanghaiTradingTime()` 多处重复定义。
- 无 React Query/SWR，全靠 `useEffect` + `setInterval(60s)` 轮询。

#### 目标（阶段一 scope，不要一次拆完）

1. 新建统一 API client。
2. 抽取共享 types 与工具函数。
3. TrendOK fetch 去重（同 symbol 集合并发请求合并）。
4. **不**在本阶段大规模拆 UI 组件（留给阶段二）。

#### 方案

**新建文件：**

```
apps/desktop-ui/src/lib/api/
  client.ts       # apiGetJson, apiPostJson, apiFetch
  trendok.ts      # fetchTrendOkMap（从 screenerExport 迁入或 re-export）
  types.ts        # TrendOkResult, DashboardSummary 等

apps/desktop-ui/src/lib/market-hours.ts   # isShanghaiTradingTime（从 Page 抽出）
apps/desktop-ui/src/lib/chunk.ts          # chunk 工具
```

**迁移顺序：**

1. 创建 `client.ts`，从 `DashboardPage.tsx` 复制并增强。
2. `screenerExport.ts`、`watchlist-screener-import.ts` 改用 client。
3. 其余 Page 逐个替换（可分批 PR）。
4. `TrendOkResult` 统一到 `api/types.ts`，删除重复定义。

**TrendOK 去重（可选在本任务或单独 PR）：**

- 模块级 `inflight Map<string, Promise<TrendOkResult[]>>`，key = sorted symbols + realtime flag。

#### 涉及文件

| 文件 | 改动 |
|------|------|
| `apps/desktop-ui/src/lib/api/*` | 新建 |
| `apps/desktop-ui/src/lib/screenerExport.ts` | 改用 api client |
| `apps/desktop-ui/src/components/pages/DashboardPage.tsx` | 删除本地 apiGetJson |
| `apps/desktop-ui/src/components/pages/WatchlistPage.tsx` | 同上 |
| 其他 15+ Page/lib | 分批迁移 |

#### 验证

- [x] 现有 vitest 通过（`cd apps/desktop-ui && npm run test`）
- [x] grep 确认无 Page/lib 本地 `apiGetJson` 副本（统一走 `lib/api/client`）
- [ ] `npm run typecheck` 通过（仍有 pre-existing 错误：AlphaIncubator `outline` variant、chart.tsx recharts 等，非本任务引入）

#### 阶段二（后续任务，不在本 scope）

- 拆 `useDashboardSync`、`useWatchlistTrend` hooks
- 拆 `components/dashboard/*`、`components/watchlist/*`

---

### OPT-004：东财行业预热脱离请求路径

**状态**：[x]  
**完成日期**：2026-06-18  
**PR/Commit**：_(local — pending commit)_

#### 实施摘要

- **OPT-001 已完成热路径**：TrendOK 仅 DB `lookup_by_ts_codes`，miss → `stock_basic.industry` fallback，禁止 HTTP
- 新增 `sync_eastmoney_industry_incremental`（missing/stale 模式 + `sync_job_record` resume）
- 新增 `coverage_stats()`、`GET /sync/eastmoney-industry/status`
- `post_close_sync` 挂接 1 批 missing sync（batch_size=500）
- 新建 `eastmoney_industry_job`（工作日 18:00 Asia/Shanghai）
- `ensure_em_industries_for_ts_codes` 标记 deprecated；离线同步走 incremental API / scheduler
- 测试：扩展 `test_eastmoney_industry.py`（9 用例）+ 保持 `test_trendok_performance_path.py`

#### 问题（历史背景）

改造前 TrendOK 热路径对 miss 逐股东财 HTTP，200 miss ≈ 70–100s；miss 时 Tushare industry fallback 与行业资金流板块名不一致，加分失真。

#### 验证

- [x] TrendOK 热路径不触发东财 HTTP（`test_trendok_performance_path.py`）
- [x] incremental sync / coverage / status 单元测试通过
- [ ] 稳定运行 1–2 周后 watchlist EM 覆盖率 > 99%（需生产观察）
- [ ] Score 行业加分在 EM 命中时正常（需 UI/spot check）

```bash
pytest services/data-sync-service/tests/test_eastmoney_industry.py \
  services/data-sync-service/tests/test_trendok_performance_path.py -q
curl -s http://127.0.0.1:4330/sync/eastmoney-industry/status
curl -X POST "http://127.0.0.1:4330/sync/eastmoney-industry?mode=missing&limit=500"
```

---

### OPT-005：TV Screener Sync 并行化

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

Dashboard Sync All 对 enabled screeners **串行**调用 `sync_screener()`（`dashboard.py:443-461`）。  
单个 CDP capture 可达 60s+，多个 screener 叠加线性变慢。

#### 方案

1. 使用 `ThreadPoolExecutor` 或 `asyncio.gather` 限流并行（建议 `max_workers=2`，避免 CDP 争抢）。
2. 单个 screener 失败不阻塞其余（已有 try/except，确认 error 汇总完整）。
3. SSE progress 事件增加 per-screener 状态。

#### 涉及文件

| 文件 | 改动 |
|------|------|
| `services/data-sync-service/src/data_sync_service/service/dashboard.py` | `_sync_screeners_step` 并行 |
| `services/data-sync-service/src/data_sync_service/service/tv.py` | 可选：超时隔离 |

#### 验证

- [ ] 2+ enabled screener 时 Sync All 总耗时 < 串行之和
- [ ] 一个 screener 失败，其余仍成功
- [ ] SSE stream 仍正确结束

---

## P2 — 中期改进

### OPT-006：TrendOK `refresh` 语义对齐

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

前端普遍传 `refresh=true`，后端 `compute_trendok_for_symbols` 直接 `_ = refresh` 忽略。  
用户/开发者误以为会触发网络拉取最新 K 线。

#### 方案（二选一）

| 选项 | 做法 |
|------|------|
| A | 实现 refresh：对请求的 ts_codes 先 trigger bars sync 再计算 |
| B | 删除参数：前后端去掉 `refresh`，文档说明 TrendOK 只读 DB |

推荐 **B**（简单）+ Watchlist 手动刷新走独立 `/bars?force=true` 路径（已存在）。

#### 涉及文件

- `services/data-sync-service/src/data_sync_service/service/trendok.py`
- `services/data-sync-service/src/data_sync_service/api/query_routes.py`
- `apps/desktop-ui/src/lib/screenerExport.ts`
- `apps/desktop-ui/src/components/pages/WatchlistPage.tsx`
- `apps/desktop-ui/src/lib/watchlist-screener-import.ts`

---

### OPT-007：DB Migration 工具（Alembic）

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

Schema 分散在 24 个 `db/*.py` 的 `ensure_table()` / `CREATE TABLE IF NOT EXISTS`，无版本追踪，演进风险高。

#### 方案

1. 引入 Alembic，baseline migration 对应当前 schema。
2. 新表/列变更走 migration，保留 `ensure_table()` 作为 dev 便利（或逐步移除）。
3. 文档补充 `alembic upgrade head` 到 README。

---

### OPT-008：TV Capture 异步化 / Job Queue

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

- `tv/capture.py` 在 sync FastAPI route 内 `asyncio.run()`，阻塞 worker。
- Scroll 循环最多 200 步 × 200ms，单任务耗时长。

#### 方案

1. Sync endpoint 改为「提交 job → 返回 job_id」。
2. 后台 worker 执行 capture，完成后写 snapshot。
3. UI 轮询或 SSE 查 job 状态。
4. 可选：减少 scroll 步数 / 智能停止条件优化。

---

### OPT-009：packages/shared 类型共享

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

`packages/shared` 仅有 portfolio/artifact schema，desktop-ui 未引用。TrendOK、Watchlist 类型前后端各写一份。

#### 方案

1. 在 shared 增加 `TrendOkResult`、`WatchlistItem` 等 Zod schema + 导出 TS type。
2. desktop-ui 引用 `@karios/shared`。
3. 长期：OpenAPI → 生成 Python pydantic（可选）。

---

## P3 — 清理类

### OPT-010：过时 UI 文案清理

**状态**：[ ]  
**完成日期**：  
**PR/Commit**：

#### 问题

部分 Page 仍写「Cached in SQLite」，实际已是 Postgres。

#### 涉及文件（grep 确认）

- `apps/desktop-ui/src/components/pages/IndustryFlowPage.tsx`
- `apps/desktop-ui/src/components/pages/BrokerPage.tsx`
- `apps/desktop-ui/src/components/pages/SettingsPage.tsx`（SQLite 迁移按钮是否仍需要）

---

## 模块级问题备忘（不单独开任务，合并到上述 OPT）

### data-sync-service

| 模块 | 优点 | 待优化 | 关联 OPT |
|------|------|--------|----------|
| `trendok.py` | 算法集中、有测试 | God file 1252 行；热路径性能 | OPT-001, OPT-004 |
| `market_regime.py` | breadth 可开关 | TrendOK 未用开关 | OPT-001 |
| `dashboard.py` | 并行 summary | screener 串行 | OPT-005 |
| `eastmoney_industry.py` | DB 缓存 | 请求路径 HTTP | OPT-004 |
| `tv/capture.py` | DOM 启发式完整 | 阻塞 worker | OPT-008 |
| `db/*.py` | JSONB upsert 幂等 | 无 migration | OPT-007 |

### desktop-ui

| 模块 | 优点 | 待优化 | 关联 OPT |
|------|------|--------|----------|
| `DashboardPage.tsx` | 功能完整 | 2790 行 God file | OPT-003 |
| `WatchlistPage.tsx` | 功能完整 | 2588 行；串行 force bars | OPT-002, OPT-003 |
| `watchlist-storage.ts` | API 清晰 | localStorage 权威 | OPT-002 |
| `screenerExport.ts` | 近期抽取良好 | TrendOK 重复 fetch | OPT-001, OPT-003 |
| `storage.ts` | 极简 | 无 quota 处理 | OPT-002 |

### Watchlist 串行 force bars（可并入 OPT-003 或单独小 PR）

`WatchlistPage.tsx:816-826`：手动刷新时对每只股票串行 `GET /bars?force=true` + 120ms sleep。

**建议：**

- 改为 batch endpoint 或并行度限制（如 p-limit 3）。
- 去掉固定 120ms sleep，改指数退避仅在 429 时触发。

---

## 推荐执行顺序

```
Week 1:  OPT-001（TrendOK 性能）→ 立刻改善日常使用
Week 2:  OPT-002（Watchlist 存储）→ 数据安全
Week 3:  OPT-004（东财预热）+ OPT-003 阶段一（API client）
Week 4:  OPT-005（Screener 并行）+ OPT-006（refresh 语义）
Later:   OPT-007 ~ OPT-010
```

---

## 审查记录

| 日期 | 说明 |
|------|------|
| 2026-06-18 | 初始版本：全栈架构审查，功能正常，识别 P0–P3 共 10 项优化 |
| 2026-06-18 | OPT-001 完成：TrendOK 热路径 skip breadth + regime TTL cache + EM DB-only；benchmark ~0.21s |
| 2026-06-18 | OPT-002 完成：Watchlist registry 权威源 + GET API + hydrate/persist + 11 项自动化测试 |
| 2026-06-18 | OPT-003 阶段一完成：统一 API client + 共享 types/utils + TrendOK inflight 去重；全 Page 迁移 |
| 2026-06-18 | OPT-004 完成：EM 行业 offline incremental sync + post_close/scheduler job + status API |

---

## 相关文档

- [模块文档索引](./modules/README.md)
- [Screener 模块](./modules/screener.md)
- [Watchlist 模块](./modules/watchlist.md)
