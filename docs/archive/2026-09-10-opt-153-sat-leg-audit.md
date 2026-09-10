# opt-153-sat-leg-audit · 归档于 2026-09-10

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。

---

### OPT-153：卫星腿判定修正——自选卫星单不再误报「偏离」+ 腿推断去精确 12.5%（用户报障 2026-09-10）

**状态**：[x] 2026-09-10（用户拍板：自选卫星买入"视为符合(ok)"）

**报障**：
1. 核心仓操作核对卡把若干卫星单打成"偏离"，实为**主动自选**（`user_trades.source=RESEARCH/MANUAL`，paper 双子星账当天无该票）——
   `_judge_sat_open` 未命中 paper 账就 `warn`「账外卫星单」。
2. 腿的自动推断写死精确值 `=== 12.5`：整百/现金导致实际仓位 ≠12.5%（7.13/13.85/14.34%）时，
   默认落到**核心(s3)**腿 → 错尺子。

**落地**：
- `service/core_holding_audit.py::_judge_sat_open`：未命中 paper 账的卫星 BUY 由 `warn` 改 **`ok`**，
  `rule=sat_manual`，文案「主动自选卫星单（paper … 无 … 信号）——按自选处理，非偏离」；命中仍 `ok/sat_signal`。
- `PortfolioHealthCard.tsx::handlePlanAct`：默认腿改用 **`row.sleeve`（sat/core）**判，不再按 `navPct===12.5`。
- `QuickBuyDialog.tsx` + `PortfolioHealthCard.tsx`：`defaultLeg` 回退改 `'s3'`（上下文驱动，取消 size 猜测）。
- 单测：`test_core_holding_audit.py` → `test_sat_open_off_book_is_self_directed_ok`（ok + rule）。

**验证**：后端 `test_core_holding_audit.py` 7 passed；前端 tsc 0 + QuickBuyDialog/PortfolioHealthCard 33 passed。

**不做（记账）**：
- 卫星**仓位容差带**——审计当前不校验卫星仓位大小（只显示目标 12.5%），故无 size 误报；
  如未来加 size 校验，须先定容差（整百/现金现实）。
- **E5 滑点双计**（`slippage_pct=0.05` ⊕ 成本模型内置 10bps/边 → CN 往返 ≈40bps）属 realism 正确性，
  另起项评估，不在本条。
