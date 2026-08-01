# TV Capture 数据源决策 · 落地决策（OPT-057）

> **关联 todo**：[`docs/todo.md §12 #8.5`](../../todo.md) · [§3 收益](../../todo.md) · [§6 数据源](../../todo.md)
> **上下文**：[`ego-lite-spike-2026-08.md`](./ego-lite-spike-2026-08.md)（Phase 1 spike 已完成）· [`modules/screener.md`](../modules/screener.md)（业务真值）
> **OPT**：[`OPT-057`](../optimization-checklist.md) · [`OPT-008`](../optimization-checklist.md)（tv_capture_jobs async）
> **决议日**：2026-08-01
> **下次复审**：2027-02-01（6 个月后评估是否 deprecate Chrome）

## TL;DR

**用三轨架构替代单一 Chrome 路径**：

| 轨道 | 角色 | 默认启用 | 何时启用 |
|------|------|---------|---------|
| **TV Scanner API**（新） | 主线 | ✅ | 所有 `mode=api` screener |
| **ego-lite**（新） | 中端 fallback | ⚠️ 仅 API 失败时自动启用 | API 挂掉 / 字段变更 / 用户显式选 |
| **Chrome CDP**（旧） | 长期 fallback | ⚠️ 仅 API + ego-lite 双失败时自动启用 | ego-lite 也不可用 / 用户显式选 |

**新建 screener 流程**：从"用户必须自己去 TV 网站存 screener" → 三模式（**Template** / **Custom URL** / **Filter JSON**），让 90% 用户不再需要接触 TV 网站。

**用户价值**（收益 §3 + 数据源 §6 双线）：
- ✅ TV capture 完全 Docker-friendly（去掉 Chrome 依赖）
- ✅ TV capture 抓取成功率 ≥ 95%（API 兜底 + 多层 fallback）
- ✅ TV capture 维护成本 ↓（不用管理 Chrome profile / 登录态）
- ✅ 数据字段 ↑（30+ 结构化字段 vs 解析 HTML）
- ✅ 新建 screener 零摩擦（模板 1 次点击 vs 跨网站复制 URL）

---

## 1. 背景

### 1.1 现状痛点

**Chrome capture 链路**（[`tv.py:330-363`](../../services/data-sync-service/src/data_sync_service/service/tv.py)）：

```
POST /integrations/tradingview/screeners/{id}/sync
  → tv_capture_jobs queue (Postgres)
    → tv_capture_worker (max 2 concurrent)
      → _capture_and_persist_screener()
        → _ensure_cdp_ready()  ← 启动 Chrome (subprocess)
        → capture_screener_over_cdp_sync()  ← Playwright + CDP
          → 打开 TV screener URL
          → 抓 filter pills + 滚动 300 行
          → 返回 {screenTitle, filters, url, headers, rows[]}
        → tvdb.upsert_snapshot()
```

**具体痛点**：

1. **Chrome 依赖**：
   - `tv_chrome.py` 启动本地 Chrome 进程（subprocess）
   - 必须登录 TradingView 账号（profile cookies）
   - macOS 更新 / Chrome 版本变化 → 经常挂
2. **新建 screener 摩擦**：用户必须先去 TV 网站点存 screener → 复制 URL → 回 Karios 加库
3. **Docker 不友好**：Chrome 在 docker 内跑不动（需 `host.docker.internal` 特殊处理）
4. **数据字段有限**：解析 HTML table，字段名归一化脆弱

### 1.2 ego-lite spike 关键结论（已 done）

[`ego-lite-spike-2026-08.md`](./ego-lite-spike-2026-08.md) 已证明：

- TV Scanner API (`scanner.tradingview.com/global/scan`) 可用
- 无需 login / cookie / Chrome
- 30+ 结构化字段（价格 / 涨跌幅 / 量 / 市值 / PE / RSI / MACD / 行业 / Beta / EPS）
- CN / HK / US 全球股票都支持
- 响应 ~1.3s / 50 行，无明显限速
- **唯一真风险**：API 是 undocumented internal API，无 SLA / 无 contract

---

## 2. 三轨架构设计

### 2.1 数据流

```
                        ┌─────────────────────────────────┐
                        │  service/tv.py dispatcher       │
                        │  read screener.mode             │
                        └─────────────────┬───────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              ▼                           ▼                           ▼
  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
  │ tv/scanner_api.py    │  │ tv/ego_lite.py       │  │ tv/capture.py (old)  │
  │ mode=api (PRIMARY)   │  │ fallback             │  │ mode=chrome          │
  │                      │  │                      │  │ (LONG-TERM FALLBACK) │
  │ POST scanner.        │  │ Playwright headless  │  │ Playwright + CDP     │
  │ tradingview.com/     │  │ chromium             │  │ + login profile      │
  │ global/scan          │  │ (no Chrome profile)  │  │                      │
  │                      │  │                      │  │                      │
  │ ✅ no login          │  │ ✅ no TV login       │  │ ✅ full features      │
  │ ✅ Docker-ready      │  │ ✅ preserves URL     │  │ ❌ needs Chrome       │
  │ ✅ 30+ fields        │  │ ⚠️  perf TBD         │  │ ❌ heavy              │
  │ ❌ undocumented      │  │ ⚠️  needs internet   │  │ ❌ Docker-host dep    │
  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
              │                           │                           │
              └───────────────────────────┴───────────────────────────┘
                                          │
                                          ▼
                              tv_capture_jobs (Postgres)
                                          │
                                          ▼
                                  tv_screener_snapshots
                                  payload.capturedVia = "api" | "ego_lite" | "chrome"
```

### 2.2 调度规则

每条 screener 有 `mode` 字段（`api` / `chrome`，`ego_lite` 仅 fallback，不作为独立 mode）。

```python
# dispatcher 逻辑（伪代码）
def _capture_and_persist_screener(*, screener_id):
    screener = db.fetch_screener_by_id(screener_id)
    mode = screener.mode  # 'api' or 'chrome'
    
    try:
        if mode == 'api':
            return _try_api(screener)        # → TV Scanner API
        else:
            return _try_chrome(screener)     # → Chrome CDP
    except CaptureError as e:
        if mode == 'api' and e.transient:
            # API 临时失败 → fallback ego_lite
            return _try_ego_lite(screener)
        elif mode == 'chrome':
            # Chrome 失败 → 重启 Chrome 后 retry
            return _retry_chrome_after_restart(screener)
        raise
```

**关键不变量**：
- `mode=api` 失败时**仅降一级**到 `ego_lite`（不再降 Chrome，避免 fallback 链失控）
- `mode=chrome` 失败时**重启 Chrome 后 retry** 一次（Chrome 进程崩是已知失败模式）
- `payload.capturedVia` 字段写实际使用轨道，方便审计

### 2.3 数据模型（DB schema 改动）

`tv_screeners` 加 4 列（**Alembic 0012**）：

| 列 | 类型 | nullable | 默认 | 含义 |
|----|------|---------|------|------|
| `mode` | TEXT | NOT NULL | `'chrome'` | `'api'` 或 `'chrome'`；`ego_lite` 仅 fallback |
| `market` | TEXT | NULL | NULL | `'cn'` / `'hk'` / `'us'` / NULL（任意） |
| `filter_json` | JSONB | NULL | NULL | Scanner API filter payload（mode=api 时） |
| `api_columns` | JSONB | NULL | NULL | Scanner API columns list（mode=api 时） |

`url` 改 nullable（mode=api 时可不填）。向后兼容旧行。

`tv_screener_snapshots.payload` 加 `capturedVia` 字段（`'api'` / `'ego_lite'` / `'chrome'`）。

---

## 3. 新建 screener 流程（用户体验）

### 3.1 现状（必须改）

```
Settings → Screeners → Add
  ┌─────────────────────────────────────────┐
  │ Name: [_______________]                 │
  │ URL:  [https://www.tradingview.com/...] │
  │                                          │
  │ [Add]                                    │
  └─────────────────────────────────────────┘

用户必须：
1. 打开 TradingView 网站
2. 点 screener → 调整 filter → 点 "保存"
3. 复制 URL
4. 回 Karios 加库
```

### 3.2 推荐（OPT-057 落地）

```
Settings → Screeners → Add
  ┌──────────────────────────────────────────────┐
  │ Mode: ○ Template (推荐)                       │
  │       ○ Custom URL (legacy)                   │
  │       ○ Filter JSON (advanced)                │
  │                                               │
  │ Template: [Karios Pullback v3          ▼]    │
  │                                               │
  │ ── 预览 ──────────────────────────────────── │
  │ 市值 ≥ 30B | PE>0 | 营收增长 |                │
  │ EMA20>Close>EMA50>EMA200 | RSI 45-75          │
  │                                               │
  │ [Preview 5 symbols]  [Save & Enable]         │
  └──────────────────────────────────────────────┘
```

### 3.3 三模式详解

| 模式 | 适用用户 | 必填字段 | mode 默认值 |
|------|---------|---------|-------------|
| **Template** | 90% 用户（卫星仓主合同） | name + 选模板 | `api` |
| **Custom URL** | 习惯 TV 网站 / 想用任意 screener | name + url | `chrome`（因为 API 不能直接吃任意 URL） |
| **Filter JSON** | Power user / 想自定义 filter | name + filter JSON | `api` |

**Template 列表**（3 个，详见 `docs/modules/screener-templates.md`）：
1. **Karios Pullback v3**（主合同，TIP-006）— 趋势回踩 5-15%
2. **Falcon Launch v2**（momentum）— 主线动量日内
3. **Industry Top5 Fallback**（TIP-003 空窗降级）— 行业 Top5 + TrendOK

### 3.4 API 字段白名单（保证 30+ 字段稳定可用）

TV Scanner API 返回的字段在客户端映射为"友好名字"。本 OPT 锁定这套映射（避免字段名变化导致代码改）：

| 内部名 | 友好名字 | 类型 | 用途 |
|--------|---------|------|------|
| `name` | `Symbol` | str | 股票代码 |
| `description` | `Name` | str | 公司名 |
| `close` | `Price` | float | 当前价 |
| `change` | `Change %` | float | 日涨跌% |
| `volume` | `Volume` | int | 成交量 |
| `market_cap_basic` | `Market Cap` | float | 市值 |
| `sector` | `Sector` | str | 行业 |
| `industry` | `Industry` | str | 子行业 |
| `country` | `Country` | str | 国家 |
| `price_earnings_ttm` | `P/E` | float | PE |
| `RSI` | `RSI` | float | RSI 14 |
| `MACD.macd` | `MACD` | float | MACD |
| `High.Interval52Week` | `High 52W` | float | 52 周高（回撤窗依赖） |

**关键约束**：`High 52W` 是回撤过滤（`-15% ~ -5%`）的硬依赖，必须保留。

---

## 4. Phase 实施

### Phase 0：Alembic + DB schema（0.5h）

1. 写 `alembic/versions/0012_tv_screeners_api_mode.py`：
   ```python
   op.add_column('tv_screeners', sa.Column('mode', sa.Text(), nullable=False, server_default='chrome'))
   op.add_column('tv_screeners', sa.Column('market', sa.Text(), nullable=True))
   op.add_column('tv_screeners', sa.Column('filter_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
   op.add_column('tv_screeners', sa.Column('api_columns', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
   op.alter_column('tv_screeners', 'url', nullable=True)
   # existing rows: mode='chrome' (backward compat), url preserved
   ```
2. 同步 `db/tv.py` 的 `CREATE_SQL`（空 DB parity）
3. 改 `upsert_screener` / `fetch_*` / `update_screener` 加新列

### Phase 1：`tv/scanner_api.py`（半天）

```python
# tv/scanner_api.py
SCANNER_API_URL = "https://scanner.tradingview.com/global/scan"

def fetch_screener_via_api(
    *,
    filter_payload: dict,
    columns: list[str],
    range_: tuple[int, int] = (0, 100),
    timeout_s: float = 10.0,
) -> list[dict]:
    """POST scanner.tradingview.com/global/scan, return normalized rows."""
    ...
```

**关键细节**：
- timeout 10s（spike 实测 ~1.3s，留 8x buffer）
- retry 1 次 + exponential backoff（应对 transient 失败）
- 错误码映射：`HTTPError` / `JSONDecodeError` / `EmptyResponseError` / `FieldMissingError`
- 单测：`test_tv_scanner_api.py`（mock HTTP + 字段映射 + 错误处理）

### Phase 2：`tv/ego_lite.py`（半天）

```python
# tv/ego_lite.py
async def capture_screener_ego_lite(*, url: str) -> CaptureResult:
    """Playwright headless chromium (no Chrome profile), returns CaptureResult like capture.py."""
    ...
```

**关键细节**：
- Playwright `chromium.launch(headless=True)`（**不是** Chrome CDP）
- 无 login profile、无 cookies
- 抓 filter pills + 滚动 300 行（沿用 `capture.py` 的 `normalize.py` 工具）
- 单测：`test_tv_ego_lite.py`（mock browser）

### Phase 3：dispatcher 整合（半天）

改 `_capture_and_persist_screener()` 加 mode 分支：

```python
def _capture_and_persist_screener(*, screener_id: str) -> dict[str, Any]:
    screener = _validate_screener_for_capture(screener_id)
    mode = screener.get("mode") or "chrome"
    
    if mode == "api":
        try:
            result = _capture_via_api(screener)
            captured_via = "api"
        except TransientApiError as e:
            # 降级 ego_lite
            result = _capture_via_ego_lite(screener)
            captured_via = "ego_lite"
    else:  # chrome
        result = _capture_via_chrome(screener)
        captured_via = "chrome"
    
    payload = {
        "screenTitle": result.screen_title,
        "filters": [str(x) for x in (result.filters or []) if str(x).strip()],
        "url": result.url,
        "headers": result.headers,
        "rows": result.rows,
        "capturedVia": captured_via,  # ← 新增
        "capturedAt": result.captured_at,
    }
    tvdb.upsert_snapshot(...)
    return {...}
```

### Phase 4：SettingsPage 新建 screener UI（半天）

- 加 mode 三选（Template / Custom URL / Filter JSON）
- Template 下拉 = 3 个模板（来自 `docs/modules/screener-templates.md` 后端常量）
- Filter JSON 编辑器 = `<textarea>` + JSON schema 校验
- mode 切换存 screener.mode（per screener）

### Phase 5：3 个主 screener 迁移（0.5h）

写迁移脚本（一次性）：

```python
# services/data-sync-service/scripts/migrate_screeners_to_api_mode.py
# 把 falcon / blackhorse 改 mode='chrome' (legacy)
# 新增 Karios Pullback v3 → mode='api' + filter_json
# 新增 Falcon Launch v2 → mode='api' + filter_json
# 新增 Industry Top5 Fallback → mode='api' + filter_json
```

### Phase 6：测试 + 文档（半天）

- 单测 4 个新文件（`test_tv_scanner_api.py` / `test_tv_ego_lite.py` / `test_tv_dispatcher.py` / `test_tv_templates.py`）
- `docs/modules/screener-templates.md`（filter JSON 真值）
- `docs/modules/screener.md` 更新"为什么使用 CDP"节

---

## 5. 关键风险与缓解

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| **TV Scanner API 挂掉** | 中 | 高（主线全断） | (1) fallback `ego_lite`；(2) healthcheck cron 检测字段表；(3) Chrome 仍保留 6 个月 |
| **ego_lite 性能不可控** | 中 | 中（fallback 慢） | (1) timeout 30s；(2) 不在 mainline 用；(3) spike 实测验证 |
| **TV Scanner API 字段名变化** | 中 | 高 | (1) 字段名白名单 + 索引映射（见 §3.4）；(2) `FieldMissingError` 单测覆盖；(3) healthcheck cron |
| **Filter JSON 不能 1:1 复刻 TV URL filter** | 高 | 中 | (1) 3 个主 screener 用模板（已 spike 验证）；(2) 其他 screener 走 legacy URL `mode=chrome` |
| **新建 screener UI 学习成本** | 中 | 中 | (1) Template 模式 1 次点击完成；(2) Custom URL 保持现状（向后兼容）；(3) Filter JSON 是 advanced 折叠 |
| **Alembic 迁移破坏存量数据** | 低 | 高 | (1) `mode='chrome'` 默认值覆盖所有旧行；(2) `url` nullable 但旧行保留 URL；(3) `add_column` 不改 PK / FK |
| **screenTitle 命名漂移破坏 TIP-006 合同** | 低 | 高 | (1) screenTitle 仍手工构造（保持 `karios pullback` 子串）；(2) filters 字段从 filter_json 反推字符串列表 |
| **用户从 Custom URL 升级到 Template 丢 filter** | 中 | 低 | (1) SettingsPage 显示「升级到模板」按钮 + diff 预览；(2) 不强制升级 |

---

## 6. 反例（不做什么）

- ❌ **不**完全砍 Chrome（保留 6 个月 fallback；ego-lite spike 决策）
- ❌ **不**在 `KARIOS_APP_DATA_DIR` 持久化 filter JSON（filter 在 DB，DB 备份即可）
- ❌ **不**让 `filter_json` 接受任意 TV 内部结构（只接受 spike 验证过的字段白名单）
- ❌ **不**改 `screenTitle` 的"合同"语义（TIP-006：仍手工构造 `Karios Pullback` 等子串）
- ❌ **不**做"自动从 TV URL 反推 filter"的工具（不可靠 + 高维护成本；放 OPT-057.x）
- ❌ **不**在 `dispatcher` 里把 fallback 链写成"全部试一遍"（按 `mode` 决定初始入口，失败一次只降一级）
- ❌ **不**支持 `filter_json` 的 `delete` 操作符（TV Scanner API 限制；用 `{}` 表示"无限制"）
- ❌ **不**改 Chrome capture 路径的签名（向后兼容；mode='chrome' 行仍走原 `_capture_and_persist_screener`）

---

## 7. 验收清单

- [ ] Alembic `0012_tv_screeners_api_mode.py` 升级成功（`alembic upgrade head`）
- [ ] `db/tv.py` CREATE_SQL 同步新列（空 DB parity）
- [ ] 3 个主 screener 模板落到 `docs/modules/screener-templates.md` + 后端常量
- [ ] SettingsPage 模板下拉可选 → Save 后 ScreenerPage 显示 snapshot
- [ ] AM/PM cron `mode=api` 抓取成功率 ≥ 95%（30 天实测）
- [ ] AM/PM cron 失败后 fallback 到 ego_lite / chrome 路径有 telemetry（`payload.capturedVia`）
- [ ] 单元测试：`test_tv_scanner_api.py`（≥10）、`test_tv_ego_lite.py`（≥8）、`test_tv_dispatcher.py`（≥10）、`test_tv_templates.py`（≥3），共 ≥30 tests 全绿
- [ ] `docs/modules/screener.md` 更新"为什么使用 CDP"节 → "三轨架构"
- [ ] OPT-057 完成后写 `archive/2026-08-01-opt-057-tv-capture-three-track.md`

---

## 8. 审查日

- **2026-08-01**：决策文档完成，进入实施
- **下次审查**：2026-11-01（3 个月后看 AM/PM 抓取成功率）
- **deprecate Chrome 评估**：2027-02-01（6 个月后评估是否砍 `capture.py` + `tv_chrome.py`）

---

## 9. 与其他文档的关系

```
docs/designs/tv-capture-data-source-2026-08.md  (本文档 · 落地决策)
        │
        ├─► ego-lite-spike-2026-08.md  (Phase 1 spike · API 可用性已证明)
        ├─► modules/screener.md        (业务真值 · 主合同 Karios Pullback)
        ├─► modules/screener-templates.md  (filter JSON 真值 · 3 个主 screener)
        └─► optimization-checklist.md OPT-057  (实施跟踪)
```

> **未来 review**：
> - 任何关于"TV capture 怎么抓"的讨论 → 先读本文件 + OPT-057
> - 任何关于"新建 screener"的讨论 → 本文件 §3 + screener-templates.md
> - 任何关于"AM/PM cron 失败率"的讨论 → 本文件 §5 + OPT-057 验证清单

---

## 10. 决议日 + 复审日历

- **2026-08-01**：决策文档完成，进入实施
- **2026-11-01**：3 个月复审（抓取成功率 + fallback 链）
- **2027-02-01**：6 个月复审（Chrome 是否 deprecate）
- **2027-08-01**：1 年复审（保留 API 模式 + 评估 ego_lite 是否升级为主线）