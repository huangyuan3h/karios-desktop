# OPT-055 / §14 #1 · AI Agent 集成 Cookbook · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §14 AI Agent 打通 + Chrome 替代`](../../todo.md)（用户 2026-08-01 立）
> **集成手册真值**：[`docs/integrations/ai-agent-cookbook.md`](../../integrations/ai-agent-cookbook.md)（**新**——长期真值）
> **配套**：[`docs/api/openapi.md`](../../api/openapi.md) + [`api-contract.md`](../../designs/api-contract.md)

## 当时的目标

§14 #1：让用户的个人 AI agent 项目能**5 分钟内启动 + 长期稳定**接入 Karios `/v1/*`。

## 实际做了什么

10 节 cookbook（**docs/integrations/ai-agent-cookbook.md**）：

| 节 | 内容 | 重要性 |
|----|------|--------|
| §0 TL;DR | 4 步启动 + 4 类 endpoint | ⭐⭐⭐ |
| §1 前置依赖 | endpoint / key / client | ⭐⭐ |
| §2 启动 checklist | 4 步：version → schema → errors → quota | ⭐⭐⭐ |
| §3 4 场景 | 盘前批量 / 盘中轮询 / 盘后回顾 / 自定义告警 | ⭐⭐⭐ |
| §4 错误处理 | 401 / 429（不硬编码 sleep）/ 5xx / schema mismatch | ⭐⭐⭐ |
| §5 配额监控 | watchdog + 自适应 throttle | ⭐⭐ |
| §6 长期稳定性 | 责任矩阵 | ⭐⭐ |
| §7 Client examples | Python（httpx + tenacity）+ Node（undici）| ⭐⭐⭐ |
| §8 上线 checklist | 9 项 | ⭐⭐ |
| §9 关联章节 | §14 #2 #3 + §12 #8 #7 | ⭐ |
| §10 FAQ | 5 常见问题 | ⭐⭐ |

### 关键设计

| 决策 | 选择 | 理由 |
|------|------|------|
| 文档位置 | `docs/integrations/` (新建目录) | API + integrations 双目录清晰 |
| 启动 4 步顺序 | version → schema → errors → quota | "先发现自己" → "再使用自己" |
| 429 处理 | **读 `Retry-After` header**——不硬编码 sleep | 429 自带可机读数据 |
| 5xx 处理 | 指数 backoff + 不阻塞 AI agent | Karios 是数据源，不是真理 |
| client examples | 2 套（Python + Node） | 覆盖最常见的 2 生态 |
| §14 #2 #3 留口 | rate-limit cookbook / webhook | §14 #1 立即做，#2 #3 设计稿 |

### 验证

- **无代码改动**（纯文档）—— cookbook 是给用户 AI agent 项目的**集成指南**
- 已链接到所有相关 `/v1/*` 文档（api/ + designs/）
- Python / Node client 代码块**自包含可运行**（用户可直接复制粘贴）

## 后续影响 / 留给谁

### 给外部 AI 助手（用户自己的项目）

| 现在能做的 | 怎么做 |
|------------|--------|
| **5 分钟接入** | 跑 §2 4 步 |
| **盘前批量** | §3 场景 A |
| **盘中轮询** | §3 场景 B |
| **盘后回顾 + 日报** | §3 场景 C（结合 Telegram push / Notion / 任何你用的）|
| **429 容错** | §4.2 retry + §5 自适应 throttle |
| **上线** | §8 9 项 checklist |

### 给 Karios 本身

| 行动 | 来源 |
|------|------|
| `/v1/healthz` 加 endpoint | §10 FAQ Q2 → §14 #2 任务 |
| webhook（push 模式）| §3 场景 D → §14 #3 任务 |
| rate-limit cookbook 文档 | §4.2 → §14 #2 任务 |

### 给未来 review

- **新 `/v1/*` endpoint** → 更新 §3 + §7 client
- **新错误码** → 更新 §4 + §10 FAQ
- **schema 升级 MAJOR** → §6 "Schema 稳定" + §10 FAQ Q1
- **AI agent 项目进展** → 用户在外部更新

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 1（`docs/integrations/ai-agent-cookbook.md`）+ 1（archive 摘要）|
| 改动文件 | 4（todo.md + longevity + checklist + 此 archive）|
| 总测试 | 208/208 ✅ + 1 skip（零代码改动）|
| 工期 | 1 个会话 |
| 预算 | $0（纯文档）|