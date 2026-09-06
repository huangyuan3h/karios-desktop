# OPT-045 / OPT-075 冬眠（未关闭 · 可唤醒） · 归档于 2026-09-06

> 从 `docs/optimization-checklist.md` 原文移出（**状态仍是 [open]，不是 done**）。
> 原因：陈年 open（045 Phase B/C 自 08-01、075 自 08-12 登记未动），移出未完成队列给正文减负。
> 唤醒条件：用户/排期明确要做 → 整节移回正文"未完成"并定优先级。本档届时删除或标已唤醒。
> 原文逐字保留（含相对链接，已按 archive/ 位置修正）。

---

### OPT-045：OpenAI 兼容 `/v1/*` + AI 助手可发现性

**状态**：[ ] Phase A done · Phase B/C 待开  
**优先级**：P0  
**关联 todo**：[§3 API 开放 P0](../todo.md) · [§12 实施清单 #1](../todo.md)  
**关联设计稿**：[`docs/designs/api-contract.md`(./designs/api-contract.md) · [`docs/designs/freelancer-architecture.md`(./designs/freelancer-architecture.md)

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

---

---

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

---

---

### OPT-127：前端轮询 jitter + ETag + 后端限流（P2 储备）

**状态**：[ ] 储备（P2，不占当前 P0/P1；OPT-125 后再做）  
**优先级**：P2  
**关联**：[`stability-audit-2026-09-01.md`(./designs/stability-audit-2026-09-01.md) §3

#### 背景

`apps/desktop-ui/src/lib/query-client.ts` `refetchOnWindowFocus:true` + `retry:1`，多标签切回触发 `dashboard/watchlist/notifications` 同时 `Promise.all` 击后端，单 `uvicorn` worker 尾延迟放大；无 `Cache-Control` / `ETag`，无后端 `rate limit`。

#### 目标

- 前端 `refetchInterval` 加 `jitter 0.8-1.2`；`staleTime` 引入 5s 随机；`retryDelay: attempt => 300*2^attempt`
- 后端 `GET /dashboard/summary` / `GET /market/stocks/trendok` 加 `ETag + 304`（`If-None-Match`）与 `Cache-Control: private, max-age=10`
- 后端全局 `slowapi` 或 `rate limit 60/min/IP`（对 `trendok` 批量计算 CPU 密集路径）

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `apps/desktop-ui/src/lib/query-client.ts` `lib/queries/intervals.ts` `lib/queries/dashboard.ts` |
| BE | `services/data-sync-service/src/data_sync_service/api/query_routes.py` `api/market_routes.py` |
| Tests | `apps/desktop-ui/src/lib/query-client.test.ts`（**新**） |

#### 验证

- [ ] 多标签切回不再同时 N 个 query 同 ms 触发（jitter 证据）
- [ ] `If-None-Match` 命中返回 304
- [ ] 限流超限返回 429

#### 反模式

- ❌ 给所有 GET 加 `no-store`（应 `max-age=10` 让 ETag 生效）
- ❌ 前端 `refetchOnWindowFocus:false` 一刀切（保留但加 jitter）

---

---
