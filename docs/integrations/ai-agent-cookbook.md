# AI Agent 集成 Cookbook · 与 Karios `/v1/*` 打通

> **关联 todo**：[`docs/todo.md §14 AI Agent 打通 + Chrome 替代`](../../todo.md)（用户 2026-08-01 立）
> **配套**：[`docs/api/openapi.md`](../../api/openapi.md)（机器可读 schema）+ [`docs/api/discovery.md`](../../api/discovery.md) / [`business.md`](../../api/business.md) / [`explain.md`](../../api/explain.md) / [`errors.md`](../../api/errors.md)
> **契约**：[`docs/designs/api-contract.md`](../../designs/api-contract.md)（versioning + 4 stable endpoints）
> **写作日**：2026-08-01

---

## 0. TL;DR

**4 步启动**：

```
1. GET  /v1/version              → 拿当前版本号（无须 auth）
2. GET  /v1/schema               → 拿完整 OpenAPI 3.1 JSON，生成 client
3. GET  /v1/errors               → 拿错误码字典（含 recovery_hint）
4. GET  /v1/quota                → 看自己配额还剩多少（auth 后）
```

之后**日常**只需调 4 类 endpoint：

| 场景 | 调用 |
|------|------|
| 拿当前持仓 + 决策 | `GET /v1/watchlist/items` |
| 看某标的解释素材 | `GET /v1/explain/{symbol}` |
| 看市场状态 | `GET /v1/market/snapshot?symbols=...` |
| 回顾近期决策 | `GET /v1/decision-journal/query?since=...` |

**鉴权**：当 `KARIOS_API_KEYS` 在 Karios 端非空时，每个业务 endpoint 要求 `Authorization: Bearer <key>`。

---

## 1. 前置依赖

| 项 | 要求 | 备注 |
|----|------|------|
| **Karios endpoint** | `https://karios.{your-domain}` 或 `http://127.0.0.1:4310`（开发）| 由 Karios 部署方提供 |
| **API Key** | `Bearer <key>` | 1 个 key 通常够；多 AI 助手场景用多 key 隔离（[`openapi.md §5`](../../api/openapi.md)） |
| **OpenAPI client** | `openapi-generator` / `openapi-typescript` / 手写 | 见 §7 例子 |
| **Python ≥ 3.10** / **Node ≥ 18** | 推荐 | 见 §7 |

---

## 2. 启动 checklist（4 步 · 5 分钟内完成）

### 步骤 1：版本握手

```bash
curl -sS https://karios.xxx/v1/version | jq
# → {"version":"0.1.0","min_compatible":"0.1.0","released_at":"2026-08-01T..."}
```

**判定**：
- `version` ≥ `min_compatible` → 你的 client 版本兼容
- 不兼容 → 升级 client / 降级 Karios（看 KARIOS_API_VERSION env var）

### 步骤 2：拉 schema 生成 client

```bash
curl -sS https://karios.xxx/v1/schema > karios-openapi.json

# 生成 TypeScript types
npx openapi-typescript karios-openapi.json -o karios-types.ts

# 或 Python
openapi-python-client generate --path karios-openapi.json --custom-template-path ...
```

**理由**：自己手写 client 会和真实 schema 漂移。每次 KARIOS_API_VERSION 升级重跑一次。

### 步骤 3：拉错误字典（启动一次即可）

```bash
curl -sS https://karios.xxx/v1/errors > karios-errors.json
# 存到本地：调用出错时按 code 查 recovery_hint
```

### 步骤 4：首次配额确认

```bash
curl -sS https://karios.xxx/v1/quota \
  -H "Authorization: Bearer $KEY" | jq
# → {"key_label":"...","auth_enabled":true,"windows":{"rpm":{...},"rph":{...},"rpd":{...}}}
```

**建议**：把 §2 的 4 步打包成 client 启动函数（`init()` / `warmup()`），每次重启调一次。

---

## 3. 4 个典型场景

### 场景 A：盘前 8:30 批量 `/v1/explain`

> **目的**：盘前拿当日待拍板清单的解释素材（K-line + industry + score + alpha context）

**调用模式**：

```python
# 1. 拿当前 watchlist
items = await client.get("/v1/watchlist/items")
symbols = [i["symbol"] for i in items["items"] if not i.get("inPosition")]

# 2. 串行调 explain（避免 burst 429）
contexts = []
for sym in symbols:
    try:
        ctx = await client.get(f"/v1/explain/{sym}")
        contexts.append(ctx)
    except QuotaExceeded:
        await asyncio.sleep(60)  # 等 1min
        ctx = await client.get(f"/v1/explain/{sym}")
        contexts.append(ctx)
```

**关键约束**：
- N 个串行 × 100ms = N 秒——**盘前批量场景要用串行**（避免 burst 429）
- N < 30 通常 rpm 够用；N > 50 用并行 + 限流
- 监控 `GET /v1/quota` 看剩余

### 场景 B：盘中 9:30-15:00 轮询 `/v1/market/snapshot`

> **目的**：实时盯紧 watchlist 的 score / price 变化

**调用模式**：

```python
async def watch_market(client, symbols, interval=60):
    while market_open():
        snap = await client.get("/v1/market/snapshot", params={"symbols": ",".join(symbols)})
        for item in snap["items"]:
            on_change(item)  # 你的逻辑：告警 / 写盘
        await asyncio.sleep(interval)
```

**注意**：
- `interval` 别 < 30s（避免 rph 触顶）
- 收盘后停止（用 is_trading_day 判断）
- 价格变化触发动作前，**先调 `/v1/explain/{symbol}` 二次确认**

### 场景 C：盘后 17:00 决策回顾

```python
# 拉当日决策变更
journal = await client.get("/v1/decision-journal/query", params={
    "since": today_iso(),
    "limit": 50,
})
# 生成 AI 日报 / 推 Telegram / 任何你想做的
report = generate_daily_report(journal["items"])
await push_to_telegram(report)  # 你的 AI agent 项目承担
```

### 场景 D：自定义告警（事件驱动）

```python
async def on_action_change(client, action):
    if action == "PURGE" and action["why"] == "ALPHA_S_WATCH":
        # Alpha S 进 pool → 提醒
        ctx = await client.get(f"/v1/explain/{action['symbol']}")
        await notify(f"Alpha S 进池: {action['symbol']} ({ctx['trendOk']['score']})")
```

**触发源**：AI agent 自己轮询 `/v1/watchlist/items` diff；**未来 §14 #3 会提供 webhook**

---

## 4. 错误处理（生产必读）

### 4.1 401 Unauthorized

| 触发 | 处理 |
|------|------|
| Missing `Authorization` header | 加 header |
| Malformed header | 改 `Bearer <key>` 格式 |
| Unknown key | 检查 key 是否在 `KARIOS_API_KEYS`；联系 Karios 管理员 |
| `KARIOS_API_KEYS` 空 | **不应该发生**——401 表示 Karios 端启用了 auth |

### 4.2 429 Too Many Requests（**最容易踩**）

**关键**：**不要硬编码 sleep 时间**——读 `Retry-After` header。

```python
import asyncio

class QuotaError(Exception):
    def __init__(self, retry_after: int, body: dict):
        self.retry_after = retry_after
        self.body = body
        super().__init__(f"429: retry after {retry_after}s, body={body}")

async def request_with_retry(client, method, path, **kwargs):
    try:
        return await client.request(method, path, **kwargs)
    except HTTPStatusError as e:
        if e.response.status_code == 429:
            retry_after = int(e.response.headers.get("Retry-After", 60))
            await asyncio.sleep(retry_after)
            return await client.request(method, path, **kwargs)
        raise
```

**429 后必做**：调 `GET /v1/quota` 看自己还剩多少；如果频繁 429 → 调高你的 key 的 rpm/rph/rpd 或降频轮询。

### 4.3 5xx Server Error

| HTTP | 处理 |
|------|------|
| 500 | Karios bug；记录 `body.trace_id`（如果有）+ 报告 |
| 502 / 503 / 504 | Karios 重启中或 Tunnel 抖动；**指数 backoff**（1s / 2s / 4s / 8s / 16s 上限）|

### 4.4 Schema mismatch

如果 schema 升级后 client 字段对不上：
- 检查 `/v1/changelog?since=<your_client_version>`
- 检查 `KARIOS_API_VERSION` 是否 bump 了 MAJOR
- 重新跑 §2 步骤 2 重生 client

### 4.5 故障转移（client 视角）

**Karios 不做 HA**（单机 Mac）—— 失败时：
- **盘前批量**：5xx 重试 3 次；都失败则跳过当日（不阻塞 AI agent）
- **盘中轮询**：失败 = 跳过本次；下次 interval 继续
- **盘后回顾**：失败则**当日不生成报告**（明示用户）

> **关键**：AI agent 永远不要**因为 Karios 失败而崩**——Karios 是数据源，**不是真理**。

---

## 5. 配额监控 + 自适应 rate limit

### 5.1 主动监控

```python
# 每小时调一次 /v1/quota
async def quota_watchdog(client):
    while True:
        q = await client.get("/v1/quota")
        for window, info in q["windows"].items():
            used_pct = info["used"] / info["limit"]
            if used_pct > 0.8:
                logger.warning(f"{window} usage {used_pct:.0%}")
        await asyncio.sleep(3600)
```

### 5.2 自适应节流

```python
class AdaptiveThrottle:
    def __init__(self, client, target_rpm=50):
        self.client = client
        self.target_rpm = target_rpm
        self._min_interval = 60 / target_rpm
    
    async def call(self, method, path, **kwargs):
        start = time.monotonic()
        try:
            return await self.client.request(method, path, **kwargs)
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                # 双倍 interval 直到不 429
                self._min_interval *= 2
                raise
        # 成功时缓慢恢复
        self._min_interval = max(self._min_interval * 0.95, 60 / self.target_rpm)
        elapsed = time.monotonic() - start
        await asyncio.sleep(max(0, self._min_interval - elapsed))
```

**不追求完美**：简单 `request_with_retry`（§4.2）已能覆盖 90% 场景。

---

## 6. 长期稳定性保证

| 维度 | 责任方 | 你应该做 |
|------|--------|----------|
| Schema 稳定 | Karios（`KARIOS_API_VERSION`）| 每次启动跑 `GET /v1/changelog?since=<your_version>` |
| API Key 安全 | 你 | 别 commit key；用 secret manager；定期 rotate |
| 错误码兼容 | Karios（不删旧码，加新码）| 不硬编码错误码；读 `/v1/errors` 字典 |
| 配额调整 | Karios 管理员 | 联系 Karios 端改 env var |
| Tunnel 失效 | Karios（Mac 不在线）| 你的 client 必须**容错**（§4.5）—— 失败不崩 |

---

## 7. Client examples

### 7.1 Python（httpx + tenacity）

```python
import asyncio
import os
from httpx import AsyncClient, HTTPStatusError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

class KariosClient:
    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
        self._client = AsyncClient(base_url=self.base_url, headers=self.headers, timeout=10)
    
    async def close(self):
        await self._client.aclose()
    
    @retry(
        retry=retry_if_exception_type(HTTPStatusError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    async def _get(self, path: str, params: dict | None = None) -> dict:
        r = await self._client.get(path, params=params)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 60))
            await asyncio.sleep(retry_after)
        r.raise_for_status()
        return r.json()
    
    async def version(self) -> dict:
        return await self._get("/v1/version")
    
    async def schema(self) -> dict:
        return await self._get("/v1/schema")
    
    async def errors(self) -> dict:
        return await self._get("/v1/errors")
    
    async def quota(self) -> dict:
        return await self._get("/v1/quota")
    
    async def watchlist(self) -> dict:
        return await self._get("/v1/watchlist/items")
    
    async def explain(self, symbol: str) -> dict:
        return await self._get(f"/v1/explain/{symbol}")
    
    async def snapshot(self, symbols: list[str]) -> dict:
        return await self._get("/v1/market/snapshot", params={"symbols": ",".join(symbols)})
    
    async def decisions(self, since: str, limit: int = 50) -> dict:
        return await self._get("/v1/decision-journal/query", params={"since": since, "limit": limit})


async def main():
    client = KariosClient(
        base_url=os.getenv("KARIOS_BASE_URL", "http://127.0.0.1:4310"),
        api_key=os.getenv("KARIOS_API_KEY"),
    )
    try:
        # 启动 4 步
        v = await client.version()
        print(f"Karios API {v['version']}")
        await client.schema()  # 缓存到本地
        await client.errors()
        q = await client.quota()
        print(f"Quota: {q}")
        # 盘前场景
        wl = await client.watchlist()
        for item in wl["items"][:5]:
            ctx = await client.explain(item["symbol"])
            print(f"{item['symbol']}: score={ctx['trendOk']['score']}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
```

### 7.2 Node.js (TypeScript + undici)

```typescript
import { Client } from 'undici';

interface KariosOptions {
  baseUrl: string;
  apiKey?: string;
}

export class KariosClient {
  private client: Client;
  private headers: Record<string, string>;
  
  constructor(opts: KariosOptions) {
    this.client = new Client(opts.baseUrl, { connectTimeout: 5000, headersTimeout: 10_000 });
    this.headers = opts.apiKey ? { authorization: `Bearer ${opts.apiKey}` } : {};
  }
  
  async close() { await this.client.close(); }
  
  private async get<T>(path: string, params?: Record<string, string>): Promise<T> {
    const qs = params ? '?' + new URLSearchParams(params).toString() : '';
    const { statusCode, headers, body } = await this.client.request({
      method: 'GET',
      path: path + qs,
      headers: this.headers,
    });
    if (statusCode === 429) {
      const retryAfter = parseInt(headers['retry-after'] as string || '60', 10);
      await new Promise(r => setTimeout(r, retryAfter * 1000));
      return this.get(path, params);  // one retry
    }
    if (statusCode >= 500) throw new Error(`Karios ${statusCode}`);
    return body.json() as Promise<T>;
  }
  
  version = () => this.get<{ version: string }>('/v1/version');
  schema = () => this.get<unknown>('/v1/schema');
  errors = () => this.get<unknown>('/v1/errors');
  quota = () => this.get<{ key_label: string; windows: Record<string, unknown> }>('/v1/quota');
  watchlist = () => this.get<{ items: Array<{ symbol: string }> }>('/v1/watchlist/items');
  explain = (symbol: string) => this.get<unknown>(`/v1/explain/${encodeURIComponent(symbol)}`);
  snapshot = (symbols: string[]) =>
    this.get<{ items: unknown[] }>('/v1/market/snapshot', { symbols: symbols.join(',') });
  decisions = (since: string, limit = 50) =>
    this.get<{ items: unknown[] }>('/v1/decision-journal/query', { since, limit: String(limit) });
}
```

---

## 8. Checklist · 上线前

- [ ] 跑完 §2 启动 4 步
- [ ] Python / Node client 单元测试覆盖 4 个核心 endpoint
- [ ] 配额 profile 配置（参考 [`openapi.md §5.3`](../../api/openapi.md) `external-ai:sk-XXX:60:1000:10000`）
- [ ] 429 retry + backoff 已实现（§4.2）
- [ ] 5xx 不崩（§4.5）
- [ ] quota watchdog 启动（§5.1）
- [ ] Secret manager / .env 存 API Key，不进 git
- [ ] 监控：429 / 5xx 计数 + 告警
- [ ] 集成测试：跑一次完整盘前 → 盘中 → 盘后流程

---

## 9. 与未来章节的关系

| 项 | 关联 |
|----|------|
| §14 #2 | `/v1/*` 持续稳定保证（合同 SLA / 监控 / 公告机制）|
| §14 #3 | 决策 webhook（AI agent 订阅 Karios 事件，避免轮询）|
| §12 #8 | ego-lite Chrome 替代（Karios 端活，AI agent 端不变）|
| §12 #7 | Docker 一键起（Karios 端 portability）|

---

## 10. FAQ

**Q：Karios 端 schema 升级了我不重生成 client，会发生什么？**
A：schema 加字段不影响；删字段 / 改字段名 → 你代码读 `obj['old_field']` 会 KeyError。建议每 30 天跑一次 `GET /v1/changelog?since=<last_check>`。

**Q：我怎么知道 Karios 端重启过？**
A：连续 5xx + `/v1/version` 返回 200 → 重启好了。否则调 `GET /v1/version` 失败 = Karios 没起来。**当前没有 healthz endpoint；§14 #2 会加**。

**Q：能不能 push 而不是 pull？**
A：当前**没有**。AI agent 全部走 pull。§14 #3 设计 webhook 中。

**Q：API Key 怎么申请？**
A：Karios 端管理员在 `.env` 加 `KARIOS_API_KEYS="label:secret:rpm:rph:rpd"`（参考 [`openapi.md §4.1`](../../api/openapi.md)）。

**Q：能多 agent 共享一个 key 吗？**
A：能，但**不建议**——一个 agent 429 会让另一个 agent 也 429。建议每个 agent 一个 key（`agent-a:sk-x:30:500:5000`、`agent-b:sk-y:30:500:5000`）。