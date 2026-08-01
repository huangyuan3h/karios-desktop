# 错误码字典（人类可读）

> 机器可读版本：`GET /v1/errors`。本文档是**人类可读对照**。  
> 详细规则：[`docs/designs/api-contract.md`](../../designs/api-contract.md)

---

## 当前错误码（v0.1.0）

| Code | HTTP | 一句话含义 | 出现版本 |
|------|------|-----------|----------|
| `SLEEVE_CAP_BLOCK` | 422 | 卫星仓合计仓位已达 hint 上界，禁止 BUY/ADD | 0.1.0 |
| `SECTOR_CONC_BLOCK` | 422 | 目标东财行业仓位合计已 ≥ 30%，禁止 BUY/ADD | 0.1.0 |
| `ENTRY_BELOW_STOP` | 422 | 建议买入区间（buyZoneHigh）≤ 硬止损（stopLossPrice） | 0.1.0 |

> **完整 `recovery_hint` 用 `curl /v1/errors` 拿**——本文档不复述，避免与机器版漂移。

---

## 字段约束

- `code` 永不复用；新码用新名
- `meaning` / `recovery_hint` 可改描述（不破坏契约）
- `deprecated_since` 一旦设了**永远不删**；调用方需自己判断是否迁

---

## 新增错误码流程

1. **先在 `docs/api/CHANGELOG.md` 写一条**（MINOR / PATCH）
2. **在 `api/discovery_routes.py` 的 `_SEED_ERROR_CODES` 列表里 append 一行**
3. **在 `tests/test_discovery_endpoints.py` 加测试**：
   - `test_errors_every_code_has_recovery_hint` 自动覆盖（已经做了）
4. **跑测试**：`pytest tests/test_discovery_endpoints.py --no-cov`
5. **更新 `errors.md` 表格**

> **不删旧码**：deprecated 也保留在列表里，加 `deprecated_since` 字段。

---

## 反原则

- ❌ 在 docs 里写"待补充"或"LATER"
- ❌ 复用 code 表示不同含义
- ❌ 让 `recovery_hint` 为 null/空（AI 助手无法修）
- ❌ 改 `code` 的拼写（破坏 AI 助手缓存的字典）

---

## 完整示例（`GET /v1/errors`）

```json
{
  "version": "0.1.0",
  "codes": [
    {
      "code": "SLEEVE_CAP_BLOCK",
      "http_status": 422,
      "meaning": "Refused a BUY/ADD because the satellite-sleeve positionPct sum has reached the upper bound of Gate.positionRangeHint.",
      "recovery_hint": "Either trim existing positions so the sum drops below the hint upper bound, or raise the hint in user settings. The endpoint does not modify positions.",
      "since": "0.1.0",
      "deprecated_since": null
    }
  ]
}
```
