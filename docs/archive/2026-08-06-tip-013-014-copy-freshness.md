# TIP-013/014 · Copy 数据新鲜度 + 强制刷新 · 归档于 2026-08-06

## 当时的目标（用户担心）

系统越来越复杂，担心「复制到外部决策系统的数据没有同步 / 不是最新」，且没有任何可见机制能验证新鲜度。

## 分析结论（先验证担心，再动手）

两个担心都**部分成立**：
- 已有保障：scheduler 全部 job 带 `misfire_grace_time=12h`（重启补跑）、`coalesce`、`max_instances=1`；交易时段前端 60s 轮询 dashboard、10min 轮询 watchlist；Copy 时缺 Trend 输入自动 forceMarket
- 真实缺口：① Copy All 输出无 per-source 时间戳，无法自我验证；② Copy 不强制刷新，收盘后前端不轮询，新闻/研报可能在 Copy 时停留在旧快照；③ job 失败静默，用户无感知

## 实际做了什么

### TIP-013 新鲜度可见性（后端 + 前端）

- 新 endpoint `GET /api/health/datasources`（`api/health_routes.py`，注册进 main.py）：
  - 6 个决策数据源：行情 / 新闻 / 研报 / Watchlist 评分 / 宏观 / Alpha Radar
  - 每个源 = `max(sync_job_record.lastSuccess, 数据表 MAX(时间戳))`，与源相关阈值比较（新闻 6h、行情/研报 24h、评分/宏观 48h）输出 `stale` 布尔
  - 时间点用数据表真实数据（如 `news_items.fetched_at`、`research_reports.created_at`）兜底，job 成功但抓到 0 条时仍能反映
- 前端 `lib/freshness.ts`：`fetchDataSourcesHealth()` + `buildDataFreshnessMarkdown()`——Copy All 头部 `generatedAt` 后插入 `## Data freshness` 块：每源「label: 最后同步时间 (x.xh ago)」，stale 源标 `⚠ STALE`，有 stale 源时首行 `⚠ WARNING: N data source(s) stale at copy time.`（明文告知下游 agent 谨慎对待）

### TIP-014 Copy 前强制刷新（前端）

- `DashboardCopyAllOptions.forceFresh`（默认 false），Copy All 按钮调用时传 `true`：
  - watchlist market 快照 `fetchWatchlistSnapshotForCopy` → `fetchQuery` override `staleTime: 0`
  - screener snapshots `buildScreenersMarkdown` → 同样 override
  - dashboard lite summary 重新拉取（`dashboardLiteQueryKey` + `staleTime: 0`）
  - 关键坑：`staleTime: undefined` 会覆盖 queryClient 默认 `staleTime: Infinity`（测试用），导致缓存形同虚设——只有 forceFresh 时才传该字段
- `Sync & Copy` 按钮本就走 `onSyncAll`（后端强制同步 + 新 summary），同样补上 forceFresh 保持一致

## 验证 / 数据

- 实测 `/api/health/datasources`：6 源全 fresh（行情 17h、新闻 12m、研报 53m、评分 17h、宏观 77m、Alpha Radar 51m）
- 测试：后端新增 `tests/test_health.py`；前端新增 `lib/freshness.test.ts`（stale 标记 / 警告行 / never 源）+ `dashboard-export.test.ts` forceFresh 绕过健康缓存重拉用例
- 全量测试：后端 1315 / 前端 472 全绿；`tsc` clean
- 顺手修复：`tests/test_research.py` 固定日期（2026-08-05）导致的**时间衰减脆测试**——评分有 14 天半衰期，固定日期次日必挂；改为动态 `date.today()`

## 后续影响 / 留给谁

- **TIP-015（已提高优先级）**：闭环——决策 agent 直连本地 API，保留用户编辑审阅作为成长环节；TIP-013 的 freshness 结构可复用为 agent 侧的新鲜度检查（agent 拉数据前先看 age）
- 可选：job 失败告警（TIP-013 只做了可见性，没做主动通知；若用户不看 Copy 头部，失败仍然静默）
