# Archived · 旧模块文档（2026-03 版）

> 这一批文件保留在这里**仅作历史参考**。本目录文件与现行代码不一致，**不要再作为新决策的依据**。

## 为什么归档

| 文件 | 上次改动 | 与现状脱节点 |
|------|----------|--------------|
| `industry-flow.md` | 2026-03-18 | 仍写"申万一级 / 主线 80 分阈值 / 3 日持续"等旧算法；现在主线条件已演进到 `5D Top3 ∪ Momentum Breakout`、执行侧加 V6.3 `INTRADAY_OVERFLOW_OVERRIDE` |
| `market-sentiment.md` | 2026-03-18 | 主指标是 Up/Down Ratio / 涨停溢价 / 涨停失败率 等；现状是 `market_regime.py` + `SRV` 模型 + Execution Gate 三件套，与之完全平行不重合 |
| `news-brief.md` | 2026-03-18 | "AI 摘要 4h 一次"是更早设计；现在 `OPT-037/038/039` 把 News 拆成独立 Query，摘要走 `regenerateNewsSummary`，下游消费方式已变 |

如果未来要恢复任何一处的旧逻辑作为**演化对照**，来这里查，不要回写。

---

## 何时清理

- [x] 2026-08-01 — 三个旧模块文档迁移进来

## 关联现行真值

| 主题 | 现行文档 |
|------|----------|
| Industry Flow | `../../modules/README.md#投资理念` |
| Market Sentiment / Regime | `../../modules/watchlist.md`（Execution Gate 部分） + `../../modules/README.md`（红线规则） |
| News | `../../modules/watchlist.md`（News 子节）+ 当前 `OPT-037/038/039` |
