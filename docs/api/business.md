# Business Endpoints（只读业务数据）

> 4 个业务 endpoint。所有都是**只读**——禁止改仓（写操作走现有 `/watchlist/*` / `/execution/*`）。  
> 详细规则：[`docs/designs/api-contract.md`](../../designs/api-contract.md)

**鉴权**：opt-in。当 `KARIOS_API_KEYS` 设置时，**所有 4 个业务 endpoint 要求 `Authorization: Bearer <key>`**；未设置时全部可达（向后兼容现有前端）。

---

## GET /v1/market/snapshot

一次拿 N 个标的的 TrendOK / Score / 当前价。

### Query

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `symbols` | string[] (repeatable) | ✅ | `MARKET:TICKER` 形式：`CN:000001` / `HK:00700` / `ETF:510300` |

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `asOfDate` | string | ISO 日期（YYYY-MM-DD，Asia/Shanghai）|
| `items[]` | array | 按输入顺序 |
| `items[].symbol` | string | `MARKET:TICKER` |
| `items[].name` | string \| null | 公司/基金名 |
| `items[].market` | string | `CN` / `HK` / `ETF` |
| `items[].trendOk` | bool \| null | 是否通过 TrendOK |
| `items[].score` | int \| null | 0-100；≥85 触发火门 |
| `items[].currentPrice` | float \| null | 实时价或最新日线收盘 |
| `items[].changePct` | float \| null | 涨跌幅（vs 前收）|
| `items[].buyAction` | string \| null | `buy` / `wait` / `avoid` / null |
| `items[].buyZoneHigh` | float \| null | 建议买入区间上界 |
| `items[].stopLossPrice` | float \| null | 硬止损价 |

### 示例

```bash
$ curl -H "Authorization: Bearer $KEY" \
    "http://karios.local/v1/market/snapshot?symbols=CN:000001&symbols=HK:00700"
{
  "asOfDate": "2026-08-01",
  "items": [
    {
      "symbol": "CN:000001",
      "name": "平安银行",
      "market": "CN",
      "trendOk": true,
      "score": 88,
      "currentPrice": 11.95,
      "changePct": 0.012,
      "buyAction": "buy",
      "buyZoneHigh": 12.0,
      "stopLossPrice": 11.0
    },
    {
      "symbol": "HK:00700",
      "name": "腾讯控股",
      "market": "HK",
      "trendOk": false,
      "score": 42,
      "currentPrice": 380.0,
      "changePct": -0.025,
      "buyAction": "wait",
      "buyZoneHigh": 370.0,
      "stopLossPrice": 360.0
    }
  ]
}
```

---

## GET /v1/watchlist/items

当前 watchlist 全量列表。

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `asOfDate` | string | ISO 日期 |
| `count` | int | 项目数 |
| `items[]` | array | 完整 watchlist 行 |
| `items[].symbol` | string | `MARKET:TICKER` |
| `items[].name` | string \| null | |
| `items[].source` | string \| null | `screener` / `screener_fallback` / `alpha_radar` / `manual` |
| `items[].positionPct` | float \| null | 卫星仓内仓位 % |
| `items[].costPrice` | float \| null | 平均成本 |
| `items[].maxPrice` | float \| null | 入场后最高价 |
| `items[].entryDate` | string \| null | ISO 日期（`==today` 时触发 T+1 锁）|

### 关键不变量

- `positionPct` 为 `null` **必须保留**（AI 助手用 null 判断"未持仓"或"无成本"）
- 改仓通过 `POST /watchlist/registry`（不在 `/v1/*` 暴露）

---

## GET /v1/decision-journal/query

近期决策变更（Gate / Action / Why）。

### Query

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `since` | string \| null | null | ISO 日期下限；null = 无下限 |
| `limit` | int | 100 | 1-500 |

### Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `asOfDate` | string | ISO 日期 |
| `changes[]` | array | 按时间正序 |
| `changes[].changeId` | string \| null | |
| `changes[].symbol` | string \| null | |
| `changes[].action` | string \| null | `BUY` / `ADD` / `HOLD` / `TRIM` / `EXIT` / `WATCH` / `WATCH_SILENT` / `PURGE` |
| `changes[].why` | string \| null | 稳定原因码（LLM 聚合用）|
| `changes[].capturedAt` | string \| null | ISO 时间戳 |
| `changes[].tradeDate` | string \| null | ISO 日期 |

### 关键不变量

- `why` 字段是 LLM 聚合的**唯一可靠字段**——保留原值
- 改 journal 通过 `POST /execution/snapshots`（不在 `/v1/*` 暴露）

---

## 下一步

- 单 symbol 完整上下文包：[`explain.md`](./explain.md)
- 错误码：[`errors.md`](./errors.md)
