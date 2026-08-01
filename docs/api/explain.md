# GET /v1/explain/{symbol}

单 symbol 完整上下文包——AI 助手想"解释"一个标的时一次拿全所有素材。

> **设计原则**：Karios **不调 LLM**。本 endpoint 返回**结构化数据**，让外部 AI 助手自带 LLM 生成解释。  
> 这与 freelancer-arch.md 职责边界一致——Karios 是被动数据 + endpoint 服务；自然语言生成在外部 AI 助手那边。

**鉴权**：opt-in（同其他业务 endpoint）。

---

## Path

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | string | `MARKET:TICKER` 形式：`CN:000001` / `HK:00700` / `ETF:510300` |

---

## Response 200

| 字段 | 类型 | 说明 |
|------|------|------|
| `asOfDate` | string | ISO 日期；助手生成消息前比对 `today` |
| `symbol` | string | 回显路径参数 |
| `name` | string \| null | 公司/基金名 |
| `market` | string \| null | `CN` / `HK` / `ETF` |
| `trendok` | object | **完整** TrendOK payload（与 desktop UI 同源）|
| `trendok.score` | int \| null | 0-100 |
| `trendok.scoreParts` | object | 子分（emaTrend / macdMomentum / volume / breakoutSmooth / rsiComfort） |
| `trendok.stopLossPrice` | float \| null | |
| `trendok.stopLossParts` | object | 子分（support / hardStop / exit_now） |
| `trendok.buyAction` | string \| null | `buy` / `wait` / `avoid` |
| `trendok.buyZoneHigh` | float \| null | |
| `trendok.currentPrice` | float \| null | |
| `trendok.changePct` | float \| null | |
| `watchlist` | object | watchlist 状态 |
| `watchlist.inWatchlist` | bool | **唯一**判断"在不在池" |
| `watchlist.source` | string \| null | `screener` / `alpha_radar` 等 |
| `watchlist.positionPct` | float \| null | 卫星仓内仓位 % |
| `watchlist.costPrice` | float \| null | |
| `watchlist.entryDate` | string \| null | ISO 日期 |
| `recentChanges[]` | array | 最近最多 5 条该 symbol 的 decision journal 变化 |
| `recentChanges[].action` | string \| null | |
| `recentChanges[].why` | string \| null | 稳定原因码 |
| `recentChanges[].capturedAt` | string \| null | |
| `recentChanges[].tradeDate` | string \| null | |
| `recentChangesWindowDays` | int | 扫描窗口（当前 30） |

---

## 示例

```bash
$ curl -H "Authorization: Bearer $KEY" \
    "http://karios.local/v1/explain/CN:000001"
{
  "asOfDate": "2026-08-01",
  "symbol": "CN:000001",
  "name": "平安银行",
  "market": "CN",
  "trendok": {
    "trendOk": true,
    "score": 88,
    "scoreParts": {
      "emaTrend": 32,
      "macdMomentum": 18,
      "volume": 16,
      "breakoutSmooth": 8,
      "rsiComfort": 9
    },
    "stopLossPrice": 11.0,
    "stopLossParts": {"support": 11.4, "hardStop": 11.0, "exit_now": false},
    "buyAction": "buy",
    "buyZoneLow": 11.8,
    "buyZoneHigh": 12.0,
    "buyRefPrice": 11.9,
    "currentPrice": 11.95,
    "changePct": 0.012
  },
  "watchlist": {
    "inWatchlist": true,
    "source": "screener",
    "positionPct": 8.5,
    "costPrice": 11.5,
    "entryDate": "2026-07-20"
  },
  "recentChanges": [
    {
      "action": "BUY",
      "why": "MAINLINE_OK",
      "capturedAt": "2026-07-21T10:00:00+08:00",
      "tradeDate": "2026-07-21"
    },
    {
      "action": "HOLD",
      "why": "TIME_LOCK_WEAK_REGIME",
      "capturedAt": "2026-07-25T14:35:00+08:00",
      "tradeDate": "2026-07-25"
    }
  ],
  "recentChangesWindowDays": 30
}
```

---

## AI 助手"一句话解释"模板

```text
[${name} / ${symbol} / ${market}] 
${trendOk ? "趋势健康" : "趋势走坏"}（Score ${score}/100，buyAction=${buyAction}）
当前位置 ${currentPrice}（${changePct >= 0 ? "+" : ""}${(changePct * 100).toFixed(2)}%）
${inWatchlist 
  ? `已在池，仓位 ${positionPct ?? 0}%，成本 ${costPrice}`
  : "未在池"}
${recentChanges.length > 0 
  ? `近期决策：${recentChanges.map(c => `${c.action} (${c.why})`).join(" / ")}`
  : ""}
```

---

## 不变量

- 始终返回 200，即使 `name` / `market` 都为 null（让调用方决定"无数据"如何呈现）
- `recentChanges` 严格只含该 symbol 的行（其他 symbol 的行被过滤）
- `recentChanges` 最多 5 条
- Karios **不会**调 LLM——`trendok.scoreParts` / `stopLossParts` 是结构化数字，AI 助手自己解释

---

## 下一步

- 错误码：[`errors.md`](./errors.md)
- 接口变更：[`CHANGELOG.md`](./CHANGELOG.md)
