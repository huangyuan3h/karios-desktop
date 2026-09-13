# Karios · 文档索引

> **设计目标**：把"做什么 / 为什么"集中到 [`todo.md`](./todo.md)。本索引只做导航，不再复制内容。

---

## 必读（注意力集中地）

| 文档 | 干什么用 | 何时看 |
|------|----------|--------|
| [`todo.md`](./todo.md) | 产品级路线图（领域分章 + `[P0..P4]`） | 每次开工前先扫 §0 / §1 |
| [`AGENTS.md`](../AGENTS.md) | Agent / 维护者必读（Alembic、schema、**改策略前先读回测实验**） | 改 schema、开 Agent 会话、用户要调策略参数 |
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
| **港湾 Harbor（产品策略真值 · S-3 核心 + 闲置现金 ETF 停车场）** | [`modules/pick-strong-track.md`](./modules/pick-strong-track.md) |
| 回测结论 / 实验记录（2026-08-09 归档 · 现行真值见 strategy-params.md） | [`modules/backtest-strategy.md`](./modules/backtest-strategy.md) → [`archive/modules-legacy/backtest-strategy-legacy.md`](./archive/modules-legacy/backtest-strategy-legacy.md) |
| **回测实验记录（最终指向港湾 Harbor）** | [`backtests/README.md`](./backtests/README.md) · [`SUMMARY.md`](./backtests/SUMMARY.md) · 调策略先看 [B11 停车场基线](./backtests/stable/etf-parking-baseline-2026-09-13.md)（卫星 REJECT 见 [B12](./backtests/stable/twin-star-parking-refit-2026-09-13.md)） · **新想法先自查** [`first-principles-2026-09-05.md`](./backtests/first-principles-2026-09-05.md) |
| **因子库（所有已验证因子 · 无论成败 · 定义/数据/结果/判定）** | [`factor-library/README.md`](./factor-library/README.md) |
| 交易系统总纲（含 S-3 生命周期） | [`modules/trading-system.md`](./modules/trading-system.md) |
| 行业资金流（已归档，只读） | [`archive/modules-legacy/industry-flow.md`](./archive/modules-legacy/industry-flow.md) |
| 市场情绪（已归档，只读） | [`archive/modules-legacy/market-sentiment.md`](./archive/modules-legacy/market-sentiment.md) |
| 新闻 / 早报（已归档，只读） | [`archive/modules-legacy/news-brief.md`](./archive/modules-legacy/news-brief.md) |

> 已下线/脱节模块文档 → [`archive/modules-legacy/`](./archive/modules-legacy/)（screener 等，仅历史参考）。

---

## 工程执行清单（与 todo 互为上下层）

> 这些是"已经在跑 / 排队中"的实现任务，**与 todo.md 是平级不同维度**：
> - todo.md = **做什么、为什么**（产品/战略层）
> - 以下清单 = **怎么落地**（某一类任务的执行栈）

| 清单 | 命名 | 适用范围 | 状态 |
|------|------|----------|------|
| [`optimization-checklist.md`](./optimization-checklist.md) | `OPT-001` ~ `OPT-146` | 架构 / 性能 / 兼容 / 工程债 | 滚动维护（最新 OPT-146 factor_signals 港股符号误标） |
| [`trading-improvement-checklist.md`](./trading-improvement-checklist.md) | `TIP-001` ~ `TIP-015`、`V6.2-*`、`V6.3-*`、`V6.4-*`、`V7.0-*` | 业务规则校准 / 交易闸 | 滚动维护（最新 TIP-015 决策 Agent 闭环；已归档至 archive/ 的见 todo.md `沉淀`表） |

**已完成项归档**：每批完成的 OPT/TIP 按天归档到 [`archive/`](./archive/)（见 archive/README.md
模板 + todo.md `沉淀`表），optimization-checklist 只保留最新条目。

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
| AI Service | [`../apps/ai-service/`](../apps/ai-service/)（无独立 README，看目录 + package.json） |
| Shared Schema | [`../packages/shared/README.md`](../packages/shared/README.md) |

---

## API 与外部打通（2026-09-06 补收录 · 之前漏索引）

| 文档 | 内容 |
|------|------|
| [`api/README.md`](./api/README.md) | API 分区导航（business / discovery / explain / errors / openapi / CHANGELOG.md） |
| [`api/openapi.md`](./api/openapi.md) | OpenAPI 接口真值 |
| [`api/business.md`](./api/business.md) · [`discovery.md`](./api/discovery.md) · [`explain.md`](./api/explain.md) · [`errors.md`](./api/errors.md) | 业务语义 / 发现 / 解释 / 错误码 |
| [`integrations/ai-agent-cookbook.md`](./integrations/ai-agent-cookbook.md) | 外部 AI Agent 调用手册（对接 openapi + designs/api-contract） |
| [`setup/docker-one-click.md`](./setup/docker-one-click.md) | Docker 一键部署 |

---

## Agent 阅读顺序（按任务，2026-09-06）

| 任务 | 按序读（读完再动手） |
|------|---------------------|
| 新开 agent 会话 | `AGENTS.md` → 本索引 → `todo.md` §0/§1 → `modules/README.md` |
| 改策略参数 | 仓库根 `AGENTS.md` → Strategy / parameter changes（主源：自查 + 拒收总表 + 调参查找） |
| 跑回测 / 加回测机制 | `backtests/README.md`（验证纪律） → `backtests/audit-2026-08-22.md` → `AGENTS.md`（Backtest walk-forward 章） |
| 改 DB schema | `AGENTS.md`（Database 章） → `services/data-sync-service/README.md` → `optimization-checklist.md` |
| 加 API 字段 | `AGENTS.md`（Shared API types 章） → `packages/shared/README.md` → `api/README.md` |
| 动 Watchlist/执行闸 | `modules/watchlist.md` → `trading-improvement-checklist.md` |

---

## 旧编号对照（2026-08-27 精简前的 todo §号）

> 2026-08-27 精简后，`todo.md` 只有 `§0/§1/P0-0~P0-8/实施清单/沉淀`。
> 正文里凡写 `todo §数字`（如 §19/§22/§12）的，一律指精简前的旧编号，
> 原文在 [`archive/2026-08-27-todo-full-snapshot.md`](./archive/2026-08-27-todo-full-snapshot.md) 同号章节。
> 常用映射：旧 §19 策略优化作战计划 → 现行三窗铁律（`backtests/README.md` 验证纪律）；
> 旧 §22.7 脉冲天平 → 现行 `todo.md` P0-1；旧 §10 沉淀表 → 现行 `todo.md` 沉淀节。

---

## 维护规则

1. **新增文档先自问**：是 todo / design / module / archive / 子项目五类中的哪一类？
2. **不要在 docs 根再创建与 todo 并列的"规划类"文件**——todo 是唯一真值。
3. **每个文件开头要有"何时看 / 何时不看"提示**，避免被误读。
