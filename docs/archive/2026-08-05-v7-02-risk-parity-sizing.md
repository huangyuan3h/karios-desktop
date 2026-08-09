# V7.0-02 ATR 风险平价开仓尺寸  · 归档于 2026-08-05

## 当时的目标（todo 链接）

- `docs/trading-improvement-checklist.md` → V7.0-02（P0，本轮唯一选做项）

## 实际做了什么

风险平价尺寸（Risk-Parity Sizing）落地，仅 FE + shared，BE 零改动：

- `execution-action.ts`：
  - 新常量 `RISK_BUDGET_PCT = 0.5`（每笔最大账户风险 %）、`RISK_MIN_SIZE_PCT = 2.5`（下限，低于则放弃建议）、`RISK_FALLBACK_ATR_MULT = 2`
  - `suggestFireSizePct` 新增 `stopDistancePct` / `atr14` / `referencePrice` 参数；riskCap = 0.5% / 止损距离%，与 clip(5%)、单票 room(15)、行业 room(30)、Sleeve room 一起取 min；绑定约束时 note=`risk`；riskCap < 2.5% → 返回 null（不给出开仓建议）
  - 止损距离口径：**实际止损位优先**（held/ADD → (current − exitStop)/current；flat/BUY → (ref − hardStop)/ref，ref = entryTrigger ?? current）；无止损位时兜底 2×ATR% 代理
  - `deriveActionCard` 输出 `sizeStopDistancePct`（诊断字段，UI/LLM 可展示）
- `WatchlistRow.tsx`：Suggest% hover title 显示止损距离；无建议时提示「风险超预算(建议<2.5%)，放弃」
- `execution-markdown.ts`：Suggest% 规则 note 更新（含 risk 绑定语义）
- `packages/shared/src/schemas/executionGate.ts`：`ExecutionActionCardSchema` 新增 `sizeStopDistancePct` 可选字段
- BE：**未改**——`trendok.py` 已输出 `stopLossParts.atr14`（绝对值）与 `stopLossPrice`（结构位硬止损），FE 数据完备

## 验证 / 数据

- 新增 14 条单测：宽止损仍 5% clip / 窄止损 risk 绑定（12% → 4.2%）/ 下限边界（20% → 2.5%）/ 超限拒绝（25% → null）/ ATR 兜底 / 实际距离优先于 ATR / 与 sleeve room 交互 / BUY、ADD 场景
- 前端全量 467 passed（+8）、shared 57 passed、`tsc --noEmit` clean

## 后续影响 / 留给谁

- **行为变化**：高波动票（寒武纪/CPO 类，止损 10%+）Suggest% 自动缩至 2.5%–4%；极端宽止损票直接无建议；低波票仍 5% 上限
- **待观察（2 周）**：开火尺寸分布是否符合「低波大仓、高波小仓」；下限 2.5% 是否过紧（若观察期有效开火密度下降，可下调 `RISK_MIN_SIZE_PCT` 或改「降级为告警」）
- 若后续做 V7.0-01（相关性热力网），`roomCorrelation` 直接接入同一条 min 链
