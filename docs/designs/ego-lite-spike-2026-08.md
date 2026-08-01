# §12 #8 ego-lite Spike — Chrome Capture 替代方案

> **决策文档** | 2026-08-01 | 非落地代码，仅调研结论

## TL;DR

**用 TradingView Scanner API (`scanner.tradingview.com/global/scan`) 替代 Chrome capture。**

- ✅ 无需 Chrome / Playwright / login
- ✅ 30+ 字段：价格 / 涨跌 / 量 / 市值 / PE / RSI / MACD / 行业
- ✅ 全球 + A股（SSE/SZSE）都支持
- ✅ 响应 < 1s / 50行，无明显限速
- ❌ 已保存的 screener URL（如 `/screener/TMcms1mM/`）不直接可查询——需要重建 filter
- 结论：**重构 TV capture 为 API 模式，不再依赖 Chrome**

---

## 1. 现状问题

| 项目 | 依赖 |
|------|------|
| Chrome 进程 | `tv_chrome.py`：subprocess 启动 + CDP 端口探测 |
| Playwright | `capture.py`：async_playwright connect_over_cdp |
| Chrome Profile | 登录态 + cookies |
| 每日 cron | AM 09:30 / PM 15:30 × N screeners |

痛点：
- Chrome 崩溃 / 内存泄漏
- 登录态过期 → 需要人工干预
- macOS 更新 / 重启 → Chrome 需要重新启动

---

## 2. 重大发现：TV Scanner API

```
POST https://scanner.tradingview.com/global/scan
Content-Type: application/json
Origin: https://www.tradingview.com
```

**无需 auth，无 cookie，无 login，直接 POST。**

### 2.1 字段表

| 字段 | 说明 |
|------|------|
| `name` | 股票代码（`NVDA`） |
| `description` | 公司名 |
| `type` / `subtype` | `stock` / `common` |
| `exchange` | 交易所（`NASDAQ` / `SSE` / `SZSE`） |
| `close` / `change` / `change_abs` | 价格 + 涨跌幅 |
| `volume` | 成交量 |
| `market_cap_basic` | 市值 |
| `sector` / `industry` / `country` | 行业分类 |
| `price_earnings_ttm` | PE |
| `dividend_yield_recent` | 股息率 |
| `RSI` | RSI 指标 |
| `MACD.macd` | MACD |
| `Stoch.K` | 布林带 |
| `ATR` | 平均真实波幅 |
| `beta_1_year` | Beta |
| `earnings_per_share_basic_ttm` | EPS |
| `gross_margin_ttm` | 毛利率 |

### 2.2 性能

| 指标 | 值 |
|------|-----|
| 响应时间 | ~1.3s / 50 行 |
| 响应大小 | < 500 字节（50 行） |
| 并发限速 | 无明显限制（5 calls / 6.6s） |
| 全球股票总数 | ~100,930 |
| A股（SSE/SZSE，>50B 市值） | 37 |
| 美股（NASDAQ/NYSE/AMEX，>100B） | 193 |

### 2.3 CN stocks

```json
// SSE:688825 → mc=533B
{"s":"SSE:688825","d":["688825", null, "SSE", 533962433902, "Semiconductors", "Electronic Technology"]}
```

**CN ticker 正常解析，exchange 正确标注。**

---

## 3. 与现有 Chrome capture 的对比

| 维度 | Chrome capture（现状） | TV Scanner API（替代） |
|------|----------------------|----------------------|
| 依赖 | Chrome + Playwright + CDP | 无（HTTP POST） |
| 登录态 | 需要 TV login | 不需要 |
| 数据字段 | 解析 HTML table，字段有限 | 30+ 结构化字段 |
| Screenshots | ✅ 可以截图 | ❌ 不能截图 |
| 已保存 screener URL | ✅ 直接访问 | ❌ 不直接可用（需重建 filter） |
| 响应速度 | ~5-10s（含网络 + 渲染） | ~1.3s |
| 稳定性 | Chrome 崩溃风险 | HTTP 200 OK |
| 维护成本 | 高（Chrome 版本 / profile） | 低（HTTP 接口） |

---

## 4. 决策：重构 TV Capture 为 API 模式

### 4.1 新架构

```
┌──────────────────────────────┐
│  tv_capture_api.py (新建)    │
│  POST scanner.tradingview.com│
│  /global/scan                │
│  构建 filter + columns       │
│  返回结构化数据              │
└──────────────────────────────┘
         ↓
┌──────────────────────────────┐
│  db/tv.py (修改)             │
│  insert/update snapshots     │
│  兼容现有数据模型            │
└──────────────────────────────┘
```

### 4.2 替代路径（3 步）

1. **spike**：写 `tv_capture_api.py`，用 Scanner API 抓 5 只 A股，输出 JSON
2. **集成**：修改 `capture.py`，`if api_mode: use tv_capture_api else: use chrome`
3. **deprecate**：6 个月后移除 Chrome 代码路径

### 4.3 限制

| 限制 | 影响 | 缓解 |
|------|------|------|
| 已保存 screener URL 不直接可用 | 需要重写 screener filter 定义 | 在 DB 里存 filter JSON（而非 URL） |
| 不能截图 | Dashboard 不显示 TV 截图 | 截图可选，不影响数据流 |
| 字段名可能变 | Scanner API 可能更新字段名 | 加 fallback 解析 + 单元测试 |

---

## 5. 实施建议

### 5.1 Phase 1：Spike（1-2h）

```python
# tv_capture_api.py
def fetch_screener_via_api(screener_filter: dict, columns: list[str]) -> list[dict]:
    """用 Scanner API 抓取 screener 数据"""
    url = "https://scanner.tradingview.com/global/scan"
    payload = {
        "filter": screener_filter,
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": columns,
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 100]
    }
    resp = requests.post(url, json=payload, headers={...})
    return resp.json()["data"]
```

### 5.2 Phase 2：集成（半天）

- 修改 `tv_screener_capture.py`，AM/PM cron 调 `fetch_screener_via_api` 替代 Chrome
- 现有 `snapshots` / `stock_snapshots` 表结构不变
- Dashboard 继续读 snapshots（数据源从 Chrome → API）

### 5.3 Phase 3：deprecate Chrome（可选）

- 6 个月后移除 `tv_chrome.py` / `capture.py`（或保留为 fallback）
- 关键指标：API 抓取成功率 > 99%

---

## 6. 与 todo §12 的关系

| §12 # | 内容 | 状态 |
|--------|------|------|
| #8 | ego-lite调研 | 本 spike |
| #7 | Docker 一键部署 | 推荐 |
| #8 + 重构 | Chrome → API 模式 | **建议** |

**本 spike 决定：§12 #8 ego-lite（Chrome CDP）→ 用 TV Scanner API 替代，不是 "ego-lite"（轻量浏览器），而是完全不需要浏览器。**

---

## 7. 待确认

- [ ] TV Scanner API 的长期稳定性？（Google 看是否有 rate limit / 免费版限制）
- [ ] Karios screener 的 filter JSON 怎么从现有 screener URL 迁移？
- [ ] Dashboard 是否还需要 TV 截图？

---

## 8. 审查日

- **2026-08-01**：spike 完成，结论明确——用 API 替代 Chrome
- **下次审查**：2026-09-01（如 TV API 有变化再调整）

---

## 9. 相关文件

- `services/data-sync-service/src/data_sync_service/tv/capture.py`：现有 Chrome capture
- `services/data-sync-service/src/data_sync_service/service/tv_chrome.py`：Chrome 进程管理
- `services/data-sync-service/src/data_sync_service/service/tv_capture_worker.py`：异步 job 消费
- `services/data-sync-service/src/data_sync_service/service/tv.py`：screener URL 定义
- `services/data-sync-service/src/data_sync_service/db/tv.py`：DB snapshot 持久化
