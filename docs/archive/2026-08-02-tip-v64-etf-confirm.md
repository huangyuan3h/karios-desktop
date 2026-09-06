# TIP·V6.4 ETF 确认因子 · 归档于 2026-08-02

> 从 `docs/trading-improvement-checklist.md` 原文迁移（只读快照，不回写）。

---

## V6.4 — ETF 资金流「资金确认因子」（2026-08-02）

### V6.4-01：ETF 资金流降级为系统级确认/过滤因子

**状态**：[x]  
**完成日期**：2026-08-02  
**文件**：`etf_fund_flow.py`（`aggregate_etf_flow_signal`）、`execution_gate.py`、`dashboard.py`、`macro_snapshot.py`、`MarketSentimentCard.tsx`、`DashboardPage.tsx`、`dashboard-format.ts`  
**规则**：ETF 资金流定位为**二级确认/过滤因子，不独立触发买卖**。

- 聚合：`ETF_WATCHLIST` 按 `category` 归并 —— broad（510300/510050/510500）→ 国家队方向（`National Team Buy/Outflow`），sector（512480/515880/159819）→ 板块方向（`Sector Momentum/Inst Outflow`）；得出 `verdict = confirm / neutral / contradict`。
- 执行闸（`compute_execution_gate`）：数据完整（`incomplete=false`，即无 shareLag 且 intradaySafe）时，`confirm` 仅追加原因 `ETF_FLOW_CONFIRM`；`contradict` 追加 `ETF_FLOW_CONTRADICT` **且把普通 ATTACK 降为 HOLD_ONLY**。永不升级；不降 `WEAK_ATTACK`（V6.3 溢出豁免）与硬 `DEFEND`；数据不完整时完全忽略。
- UI：DashboardPage **移除** ETF 大表格卡片；`MarketSentimentCard` 新增一行「资金确认 (ETF)」徽标（confirm 绿 / contradict 红 / 中性灰）。完整 ETF 明细表仅保留在 IndexPage 次级面板与 AI copy 输出中。

---
