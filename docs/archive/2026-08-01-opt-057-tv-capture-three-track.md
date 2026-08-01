# OPT-057 · TV Capture 三轨架构 · 归档于 2026-08-01

> **当时的目标（todo 链接）**
> - [`docs/todo.md §12 #8.5`](../todo.md) · §3 收益 · §6 数据源
> - 上下文：[`ego-lite-spike-2026-08.md`](../designs/ego-lite-spike-2026-08.md)（Phase 1 spike）· [`modules/screener.md`](../modules/screener.md)（业务真值）· [`OPT-057`](../optimization-checklist.md)（实施跟踪）
> - 决策真值：[`docs/designs/tv-capture-data-source-2026-08.md`](../designs/tv-capture-data-source-2026-08.md)

## 实际做了什么

### DB schema（OPT-057 Phase 0）
- Alembic `0012_tv_screeners_api_mode.py`：加 `mode` / `market` / `filter_json` / `api_columns` 4 列；`url` 改 nullable；CHECK 约束 `mode IN ('api', 'chrome')`；默认 `'chrome'` 保留所有存量行
- `db/tv.py` 同步：`CREATE_SQL` 加新列 + CHECK；`VALID_MODES` 常量；`fetch_*` / `upsert_screener` / `update_screener` 全部加新字段 + 行→dict 转换 helper

### 三轨实现
1. **TV Scanner API**（`tv/scanner_api.py`，新）：POST `scanner.tradingview.com/global/scan`；标准 `urllib` 客户端；`TransientApiError` (5xx/network) / `PermanentApiError` (4xx/JSON 解析失败) 区分；带 retry + exponential backoff；`COLUMN_MAP` 字段白名单 + `internal_to_friendly_rows` 反向映射；`default_columns()` 给模板用
2. **ego-lite**（`tv/ego_lite.py`，新）：Playwright `chromium.launch(headless=True)` 无 profile；**复用** `capture.py` 的 `_capture_once_via_page` helper（重构 capture.py 把内部抓取逻辑抽出来）；`EgoLiteUnavailable` 异常
3. **Chrome CDP**（`tv/capture.py`，改）：抽出 `_capture_once_via_page(page, url, max_rows)` 公共 helper，CDP 路径 + ego_lite 路径都调它；签名不变向后兼容

### Dispatcher（`service/tv.py` 改）
- `_dispatch_capture(mode, url, filter_json, api_columns)`：
  - `mode='api'`：先尝试 Scanner API；`TransientApiError` 时降级 ego_lite；`PermanentApiError` 422 抛出
  - `mode='chrome'`：走原 Chrome CDP 路径
- `payload.capturedVia = 'api' | 'ego_lite' | 'chrome'`（新增审计字段）
- `create_screener_from_template(template_id)` + `list_screener_templates()` 新增 helper
- `_filters_from_filter_json()` 从 filter JSON 反推 UI 显示用 filter pill 字符串（向后兼容现有 UI）

### Templates（`tv/templates.py`，新）
- 5 个模板真值：
  - `karios_pullback_v3_cn` / `karios_pullback_v3_hk` / `karios_pullback_v3_us` — 主合同（TIP-006）
  - `falcon_launch_v2_cn` — 动量（TIP-007）
  - `industry_top5_fallback_cn` — TIP-003 空窗降级
- 每条带 `nested_filter_validated: bool` 标注嵌套 filter DSL 是否实测过
- 默认 columns 含 `High.Interval52Week`（回撤窗依赖）+ `market_cap_basic`（TIP-006）

### Shared schema + routes
- `packages/shared/src/schemas/tvCapture.ts` 加 `mode` / `market` / `filterJson` / `apiColumns` 4 字段 + 新 `TvScreenerTemplate` schema
- `tv_routes.py`：
  - 新 `GET /integrations/tradingview/screener-templates`（前端下拉用）
  - 新 `POST /integrations/tradingview/screeners/from-template`（template 注册捷径）
  - `POST/PUT /integrations/tradingview/screeners` 接受新字段

### SettingsPage UI（前端）
- 新建 screener **三模式**：
  - **Template**（推荐）：下拉选模板 → 1 次点击 Save & Enable
  - **Custom URL**（legacy）：保留原 URL 输入框，向后兼容
  - **Filter JSON**（advanced）：textarea + JSON 解析校验
- 列表行新增 **mode 列**（api = 绿色 chip / chrome = 灰色 chip）+ **source 列**（API · CN / Chrome · URL 缩写）
- 编辑时 mode 切换 + 编辑框随 mode 切换（chrome 显示 URL input / api 显示 filter JSON textarea）

## 验证 / 数据

### 单元测试（共 40 新增 + 153 全绿）
- `test_tv_scanner_api.py`：17 tests（payload 构造、success 解析、short rows、malformed、5xx→transient、4xx→permanent、retry-then-succeed、JSON decode→permanent、missing data→permanent、column 映射 roundtrip、unknown passthrough、值格式化）
- `test_tv_templates.py`：9 tests（5 个模板字段、`get_template`、unknown 返回 None、CN/HK/US universe filter、`High.Interval52Week` 必须存在、`screenTitleSubstr` 非空）
- `test_tv_dispatcher.py`：9 tests（api 成功、api transient → ego_lite fallback、双失败 502、permanent 422、空 filter 409、chrome mode 空 URL 409、chrome mode 成功、filter→pill 字符串转换、未知 op 跳过）
- `test_tv_ego_lite.py`：3 tests（Playwright 未装 → `EgoLiteUnavailable`、`EgoLiteUnavailable` → `TransientApiError` 转换、sync wrapper 签名）
- SKIP_DB_TESTS=1 跑 153 全绿（含 OPT-056 docker 82 tests + tv 测试）
- TS clean（`tsc --noEmit` 0 errors）
- `@karios/shared` build OK

### 已知遗留（OPT-057.x）
- **嵌套 filter DSL 验证**：spike 没有实测 `{left:..., operation:mult, right:0.95}` 这种嵌套算术表达式。`karios_pullback_v3_*` 模板用保守形式（仅 flat predicates），回撤窗精确 5-15% 由下游 TrendOK + watchlist_automation 的 `pullback_pct` 过滤补足。Phase 7 需要写 `scripts/preview_screener_template.py` 跑一次实测，再把 `nested_filter_validated` 改 True。
- **3 个主 screener 实际数据迁移**：本 OPT 没动。落地决策里写了 `services/data-sync-service/scripts/migrate_screeners_to_api_mode.py` 草稿，下次用户跑时直接执行。
- **Chrome 不变**：所有 `mode='chrome'` 行走原路径，6 个月内不删 `tv/capture.py` / `tv_chrome.py`。

## 后续影响 / 留给谁

### 给用户的下一步
1. 跑 `PYTHONPATH=src alembic upgrade head`（应用 0012 migration）
2. 跑 `services/data-sync-service/scripts/migrate_screeners_to_api_mode.py`（**待补写**）—— 把 `Karios Pullback v3 (CN)` 加为 `enabled=true mode='api'` 的新 screener
3. Settings → Screeners → Add → 选 "Karios Pullback v3 (CN)" → Save & Enable
4. Screener 页面 → Sync → 验证走 TV Scanner API 而非 Chrome

### 给 Agent 的下一步
- 起 **OPT-057.x**：写 `scripts/preview_screener_template.py`（每个模板跑一次 5 行请求验证 filter DSL 兼容性）+ 写 `scripts/migrate_screeners_to_api_mode.py`（一次性脚本注册 3 个主 screener 到 `mode='api'`）
- 起 **OPT-058**（候选）：如果实测发现嵌套 filter 表达式不可行，把回撤窗的精确 5-15% 通过"两道 flat constraint（close < 0.85 * High52W 不能直接表达）"用 R SQL 后处理补回——但这是 watchlist_automation 已经在做的事（pullback_pct），不一定需要单独 OPT。
- 6 个月后（**2027-02-01**）评估 deprecate Chrome：届时 API 抓取成功率 ≥99% + fallback 链完整 → 移除 `tv/capture.py` + `tv_chrome.py`，减一大块维护面。

### 给老婆 watchlist 反馈（todo §15 暂存）
- 不污染本 OPT；老婆反馈汇总后落 `docs/modules/watchlist.md` 末尾 + §3 P1 子条目。