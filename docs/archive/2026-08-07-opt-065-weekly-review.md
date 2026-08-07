# OPT-065：周度决策质量复盘（todo §16 L3-P4 · 决策 Agent M2 v0）

> **完成日期**：2026-08-07
> **目标**：L3-P4「决策 Agent M2」v0——把 TIP-015 M1 的时点问答升级为**数据驱动的周度复盘**：聚合决策量 / paper 净口径实绩 / 卖出归因 / 漏斗健康度，输出可复制喂给 AI agent 的中文 markdown 报告。

## 设计：数据驱动报告（LLM 不在关键路径）

```
execution_decision_changes（决策量 by source）
paper_trades（净口径实绩 by reason/market）
exit_attribution（前向收益 → 卖早/卖对）
watchlist_automation_runs（漏斗健康度）
watchlist_registry（池状态）
        │
        ▼
build_weekly_review(end_date) → 结构化 stats + markdown 报告
```

- **诚实原则**：样本不足（<10 前向）明确标注「暂不归因」，不产误导胜率；auto-notes 只从数据触发（胜率 <50%、卖早率 ≥50%、单通道占比过高、automation 未跑）
- **复用**：卖出归因直接调 L3-P3 的 `analyze_exit_attribution`（同一样本语义）；paper 口径与 live 一致（净盈亏）

## 交付物

| 件 | 说明 |
|----|------|
| `service/weekly_review.py` | week_bounds / 聚合 / markdown 渲染（4 节：决策量、Paper 实绩、卖出归因、本周观察） |
| API | `GET /api/backtest/weekly-review?end=YYYY-MM-DD`（默认今天；ISO 周，周一起算） |
| FE | `WeeklyReviewCard`（决策 Agent 页「分析」tab 顶部）：渲染报告 + 复制按钮 + 刷新 |
| FE | `lib/queries/weekly-review.ts`（react-query hook） |
| 测试 | `tests/test_weekly_review.py`（6 个：周界、渲染数字、高卖早率提示、空簿、API、坏参数） |

## 首次实测输出亮点（2026-08-03~08-08）

- 本周 38 条 BUY/ADD 信号：**ALPHA 37 · TV 1**（97% 来自 Alpha 通道）→ 报告自动提示「供给单一化」
- Watchlist 池 40 只（持仓 4）· Automation 7 次
- Paper 首笔平仓（pool_exit，净 -0.3%）——paper 引擎真实闭环后的第一笔数据
- 卖出归因正确标注「前向样本不足」

## 验证

- 后端 1376 passed / 2 skipped（唯一失败为既有 flaky）；前端 494 passed；tsc 干净
- 真实数据端到端：API 200 + markdown 完整渲染

## 反模式确认（未做）

- ❌ 未把 LLM 放关键路径（报告数字 100% 数据驱动；AI 深度解读留给外部 agent，用户点复制即可投喂）
- ❌ 未做自动推送（周报推送归外部 AI 助手，Karios 只提供数据 + 复制）
- ❌ 未在报告里做参数建议（只提示「改参数前先用 paper 跑一周对照」）
