# OPT-052 / §12 #6 · Alpha Radar 扩展 HK 标的识别 · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §3 收益 P0` / `§6 数据源 P1` / `§12 实施清单 #6`](../../todo.md)（标题已从"HK Alpha S 自动归类"改为"Alpha Radar 扩展 HK 标的识别"）
> **模块真值**：[`docs/modules/alpha-incubator.md`](../../docs/modules/alpha-incubator.md)（更新：新增 `hk_mapping` 字段说明 + OPT-052 节）
> **代码范围**（小改动，多文件）：AI 端 + Python 端 + DB + 测试

## 当时的目标

§12 #6（原标题"HK Alpha S 自动归类"，已改为更精确的"Alpha Radar 扩展 HK 标的识别"）：

> Alpha Radar pipeline 抓到 S 级信号时，如果公司只在 HK 上市，让它也能映射到 HK ticker → 进 HK watchlist → 享受同样的 `WATCH_SILENT` 豁免。

## 实际做了什么

### A. AI 端（TypeScript）

| 文件 | 改动 |
|------|------|
| `apps/ai-service/src/schemas.ts` | `AlphaRadarTrendV4Schema` 加 `hk_mapping: z.array(...).max(3).optional().default([])` |
| `apps/ai-service/src/alphaRadarPrompts.ts` | system prompt #3 + JSON fields + 批量 instruction 都加 `hk_mapping` 说明（"可选；只有当催化剂直接映射到 HK 纯映射龙头时才填"）|
| `apps/ai-service/src/alphaRadarBatchNormalize.ts` | 新增 `normalizeHkMapping()`，调用位 `normalizeAlphaRadarTrendRow` 末尾 |
| `apps/ai-service/src/alphaRadarBatchNormalize.test.ts` | 加 1 个测试覆盖：`hk_mapping` 接收 + 去重 + 限 3 个 + 缺省 `[]` |

**关键设计**：`hk_mapping` 是**可选**且**可空**，不是必填——LLM 只在确实有 HK 标的时填，纯 A 股催化剂不填，避免假信号。

### B. Python 端

| 文件 | 改动 |
|------|------|
| `service/alpha_radar_symbol_resolve.py` | 新增 `resolve_hk_mapping()` + `_normalize_hk_ticker()` + `_lookup_hk_by_ticker()` + `_lookup_hk_by_name()` + `map_trend_hk()` |
| `service/alpha_radar_process.py` | `_save_trend_row` 在 CN 映射之外**独立**调 `map_trend_hk`（失败不阻断 CN 流程）|
| `db/alpha_radar.py` | 新增 `update_trend_hk_mapping()` + `_trend_row` 把 `trend_json.hkSymbols` 提升到顶层 `hkSymbols` |
| `service/alpha_radar_catalyst.py` | `aggregate_catalyst_stocks` 把 `cnSymbols` 和 `hkSymbols` 合并到同一 bucket map |
| `service/watchlist_automation.py` | `compute_alpha_additions` 让 HK 标的跳过 EM industry 闸门（`missing_industry` / `defense_sector` / Top10）|

### C. 设计取舍（核心）

| 取舍 | 选择 | 理由 |
|------|------|------|
| HK 存储 | `trend_json.hkSymbols` vs 新列 | **`trend_json`**——HK 命中频次低，新列 = Alembic migration + 全表读取开销，无读收益 |
| HK 解析失败 | LLM knowledge-fallback vs 留空 | **留空**——HK ticker 错误 = 投错标的，比错过更差；CN 有 fallback 但 HK 不放 |
| HK 进 watchlist 闸门 | 跳过 EM industry 闸门 | **跳过**——EM 行业数据只覆盖 A 股；仍保留 catalystScore + Max Grade=S 上游闸门 |
| 闸门放宽 | 不放宽 | **保留** catalystScore 上限 + S 级要求——只是把"HK 没有 EM 行业"这个**假缺失**修了 |

### D. 测试（13 新增）

`test_alpha_radar_hk_mapping.py`：

| 测试 | 覆盖 |
|------|------|
| `test_normalize_hk_ticker_pads_to_5_digits` | 700 / 00700 / 12345 → 00700 |
| `test_normalize_hk_ticker_accepts_prefix` | HK:700 / HK00700 / hk:00700 |
| `test_normalize_hk_ticker_rejects_invalid` | 空 / 非数字 / 6+ 位 / CN: 前缀 |
| `test_resolve_hk_mapping_by_ticker` | ticker 命中 |
| `test_resolve_hk_mapping_by_name` | 公司名命中 |
| `test_resolve_hk_mapping_unresolved_returns_empty` | 找不到 → unresolved 留下 |
| `test_resolve_hk_mapping_caps_at_three` | 5 候选 → 限 3 |
| `test_resolve_hk_mapping_dedupes` | 不同形式命中同一 ticker → 一次 |
| `test_map_trend_hk_writes_to_db` | monkeypatch `update_trend_hk_mapping` 验证写入 |
| `test_aggregate_catalyst_stocks_combines_cn_and_hk` | 同一 trend 同时出 CN+HK bucket |
| `test_aggregate_catalyst_stocks_hk_only_trend` | 纯 HK trend 也能出 bucket |
| `test_alpha_additions_accepts_hk_without_em_industry` | HK 不被 missing_industry 闸门拒 |
| `test_alpha_additions_rejects_cn_without_em_industry` | CN 仍被 missing_industry 闸门拒（回归守约）|

## 验证 / 数据

| 测试文件 | tests | 状态 |
|----------|-------|------|
| `test_alpha_radar_hk_mapping.py`（新）| **13** | ✅ |
| `test_alpha_radar_symbol_resolve.py`（回归）| 4 | ✅ |
| `test_alpha_radar_catalyst.py`（回归）| 11 | ✅ |
| `test_alpha_radar_filter.py`（回归）| — | ✅ |
| `test_alpha_radar_mapping.py`（回归）| — | ✅ |
| `test_alpha_radar_risk.py`（回归）| — | ✅ |
| `test_alpha_radar_trend_fields.py`（回归）| — | ✅ |
| `test_alpha_radar_upsert.py`（回归）| — | ✅ |
| `test_trendok_alpha_s_recovering.py`（回归）| — | ✅ |
| `test_key_quota.py`（回归）| 23 | ✅ |
| `test_openapi_docs.py`（回归）| 11 | ✅ |
| `test_discovery_endpoints.py`（回归）| 17 | ✅ |
| `test_v1_business_endpoints.py`（回归）| 18 | ✅ |
| `test_v1_explain_endpoint.py`（回归）| 14 | ✅ |
| `test_paper_trading.py`（回归）| 19 | ✅ |
| `test_data_source_audit.py`（回归）| 13 | ✅ |
| `test_alembic_baseline.py`（回归）| 8 | ✅ |
| `test_tunnel_scripts.py`（回归）| 12 + 1 skip | ✅ |
| `test_api.py`（回归）| 19 | ✅ |
| **总计** | **208 + 1 skip** | ✅ |

| ai-service tests | tests | 状态 |
|------------------|-------|------|
| `alphaRadarBatchNormalize.test.ts` | 6 (新 1) | ✅ |
| 其他 ai-service | 120 | ✅ |
| **总计** | **126** | ✅ |

## 后续影响 / 留给谁

### 给外部 AI 助手

- `/v1/market/snapshot?symbols=HK:00700` 可查 HK 标的的 TrendOK + Score + 当前价（已有 OPT-041 HK 闸门）
- `/v1/watchlist/items` 会看到 HK Alpha S 进 watchlist（source=`alpha_radar`）
- `/v1/paper-trades` 暂时还是 CN-only（**未扩展**到 HK paper-trades——HK 标的需要单独的策略闸门，留待 v1）

### 给 Karios 本身

- **盘后 17:30 `watchlist_automation` cron** ——下次跑会处理 HK Alpha S：
  - `aggregate_catalyst_stocks` 合并 CN+HK
  - `compute_alpha_additions` 跳过 HK EM industry 闸门
  - HK Alpha S 进 watchlist，享受 `WATCH_SILENT` 豁免
- **V6.3 recovering 加速器**：放量 + 大阳线 + Score≥60 → 解 WATCH_SILENT。**HK 标的自动享受**（同一 source=alpha_radar 路径）
- **3 日 GC**：HK Alpha S 也豁免（同上）

### 给未来 review

| 触发 | 行动 |
|------|------|
| HK 标的进了 watchlist 但缺 industry 显示 | 当前 UI 不显示 industry 字段；HK 标的进 UI 后用户应看不到 industry 列留空——确认前端没误报 |
| 港股有 HKD 计价 | `currency=HKD` 已在 stock_basic 里；watchlist UI 应正确显示 |
| HK paper-trading 需要 | 写新 §12 项（独立设计：HK 闸门 + HKD 计价 + 港股交易时段） |
| 催化剂过 50% 是 HK | 重启 mapping fallback 评估：可加 Tavily HK-specific 查询 |

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 1（test_alpha_radar_hk_mapping.py）|
| 改动文件 | 9（schema 1 / prompt 1 / normalize 1 / resolve 1 / process 1 / db 1 / catalyst 1 / watchlist 1 / alpha-incubator.md 1）|
| 总测试 | 208/208 ✅ + 1 skip（OPT-052 净增 13 + ai-service 净增 1）|
| 工期 | 1 个会话 |
| 预算 | $0 |