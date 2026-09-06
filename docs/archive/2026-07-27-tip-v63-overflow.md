# TIP·V6.3 极端资金流 · 归档于 2026-07-27

> 从 `docs/trading-improvement-checklist.md` 原文迁移（只读快照，不回写）。

---

## V6.3 — 极端资金流豁免 + TrendOK 修复加速（2026-07-27）

### V6.3-01：Intraday Overflow Override → WEAK_ATTACK

**状态**：[x]  
**完成日期**：2026-07-27  
**文件**：`execution_gate.py`、`dashboard.py`、`execution-action.ts`、`packages/shared` schemas  
**规则**：单板块 1D 净流入 >500 亿 **且** upCount >4000 **且** 上海时间 ≥14:30 时，将 `DEFEND`/`HOLD_ONLY` 升级为 `WEAK_ATTACK`（`allowNewEntries=true`，Suggest% 硬顶 5%）。不覆盖 `BREADTH_PANIC` / `RISK_*`。

### V6.3-02：Alpha S TrendOK recovering

**状态**：[x]  
**完成日期**：2026-07-27  
**文件**：`trendok.py`（`apply_alpha_s_trend_recovering`）、`execution-action.ts`、TrendOK Zod  
**规则**：Max Grade=S + 今日量 ≥2.5×10 日均量 + 大阳线 → `trendStatus=recovering`、`trendOk=true`、score floor 60；解除 `WATCH_SILENT`（Why=`TREND_RECOVERING`）。不自动 BUY（准买区）。
