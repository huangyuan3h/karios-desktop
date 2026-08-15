# Karios · 文档索引

> **设计目标**：把"做什么 / 为什么"集中到 [`todo.md`](./todo.md)。本索引只做导航，不再复制内容。

---

## 必读（注意力集中地）

| 文档 | 干什么用 | 何时看 |
|------|----------|--------|
| [`todo.md`](./todo.md) | 产品级路线图（领域分章 + `[P0..P4]`） | 每次开工前先扫 §0 / §1 |
| [`AGENTS.md`](../AGENTS.md) | Agent / 维护者必读（Alembic、schema 改、OPT 任务模板） | 改 schema 或开 Agent 会话 |
| 仓库根 [`README.md`](../README.md) | 项目背景 / 启动方式 / 模块协作 | 新成员第一天 / 部署时 |

---

## 业务模块（活文档 / 与代码对齐）

| 模块 | 文档 |
|------|------|
| 模块索引 · 投资理念 · V6.x 红线规则 | [`modules/README.md`](./modules/README.md) |
| Watchlist（含 Execution Gate / Decision Journal） | [`modules/watchlist.md`](./modules/watchlist.md) |
| Alpha Incubator（V4 双核捕猎） | [`modules/alpha-incubator.md`](./modules/alpha-incubator.md) |
| 下游 AI Prompt（V7.6） | [`modules/downstream-ai-prompt.md`](./modules/downstream-ai-prompt.md) |
| S-3 策略参数真值（含红绿灯禁开定案） | [`modules/strategy-params.md`](./modules/strategy-params.md) |
| 回测结论 / 实验记录 | [`modules/backtest-strategy.md`](./modules/backtest-strategy.md) |
| **回测实验记录文件夹（成功/失败全记录）** | [`backtests/README.md`](./backtests/README.md) |
| 交易系统总纲（含 S-3 生命周期） | [`modules/trading-system.md`](./modules/trading-system.md) |
| 行业资金流 | [`modules/industry-flow.md`](./modules/industry-flow.md) |
| 市场情绪 | [`modules/market-sentiment.md`](./modules/market-sentiment.md) |
| 新闻 / 早报 | [`modules/news-brief.md`](./modules/news-brief.md) |

> 已下线/脱节模块文档 → [`archive/modules-legacy/`](./archive/modules-legacy/)（screener 等，仅历史参考）。

---

## 工程执行清单（与 todo 互为上下层）

> 这些是"已经在跑 / 排队中"的实现任务，**与 todo.md 是平级不同维度**：
> - todo.md = **做什么、为什么**（产品/战略层）
> - 以下清单 = **怎么落地**（某一类任务的执行栈）

| 清单 | 命名 | 适用范围 | 状态 |
|------|------|----------|------|
| [`optimization-checklist.md`](./optimization-checklist.md) | `OPT-001` ~ `OPT-094` | 架构 / 性能 / 兼容 / 工程债 | 滚动维护（最新 OPT-094 CN 红灯日禁开仓） |
| [`trading-improvement-checklist.md`](./trading-improvement-checklist.md) | `TIP-001` ~ `TIP-012`、`V6.2-*`、`V6.3-*`、`V7.x` | 业务规则校准 / 交易闸 | 滚动维护（已归档至 archive/ 的见 §10 沉淀表） |

**已完成项归档**：每批完成的 OPT/TIP 按天归档到 [`archive/`](./archive/)（见 archive/README.md
模板 + todo.md §10 沉淀表），optimization-checklist 只保留最新条目。

**工作流**：todo 上新一条 → 评估是工程债（→ OPT-xxx）还是业务规则（→ TIP-xxx） → 对应清单创建条目 → 实现 → 勾选 + 引用。

---

## 设计与归档

| 目录 | 用途 |
|------|------|
| [`designs/`](./designs/) | 未落地 / 还在构思的设计稿（云部署选型、回测形态等） |
| [`archive/`](./archive/) | 已完成事项的快照 + 历史文档（旧版模块文档等） |
| └ [`archive/modules-legacy/`](./archive/modules-legacy/) | 与现行代码脱节的旧模块文档，仅作历史参考 |

---

## 子项目文档

| 子项目 | README |
|--------|--------|
| Backend / DB / Alembic / 调度 | [`../services/data-sync-service/README.md`](../services/data-sync-service/README.md) |
| Frontend (Next.js) | [`../apps/desktop-ui/README.md`](../apps/desktop-ui/README.md) |
| AI Service | [`../apps/ai-service/README.md`](../apps/ai-service/README.md) |
| Shared Schema | [`../packages/shared/README.md`](../packages/shared/README.md) |

---

## 维护规则

1. **新增文档先自问**：是 todo / design / module / archive / 子项目五类中的哪一类？
2. **不要在 docs 根再创建与 todo 并列的"规划类"文件**——todo 是唯一真值。
3. **每个文件开头要有"何时看 / 何时不看"提示**，避免被误读。
