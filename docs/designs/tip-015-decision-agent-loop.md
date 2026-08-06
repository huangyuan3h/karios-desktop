# TIP-015 · 决策 Agent 闭环（Decision Loop）· 设计文档

> 状态：设计稿 · 2026-08-06
> 关联：TIP-013（数据新鲜度可见）/ TIP-014（Copy 强制刷新）/ TIP-011（开火归因）/ V7.8 downstream-ai-prompt（决策合同）

## 1. 背景与目标

用户当前决策流程：Dashboard → Copy All → 粘贴到外部决策 agent（高智商模型 + 网页搜索）→ 5-10 轮对话 → 手动回填执行。痛点：

1. **数据同步靠"感觉"**：复制时是否最新、是否漏块，无可见性（TIP-013/014 已缓解但只是补丁）
2. **Context 撑不住**：想带 10 天全量上下文，但每天重发全量快照 → token 线性膨胀 + 长上下文注意力中段衰减，且"必须全量重发"是静态快照的结构性缺陷
3. **智商无法迭代**：外部 agent 每次会话失忆，看不到自己过去判断的胜负 → 没有反馈闭环，换更贵模型只是线性收益
4. **复制动作本身是成长环节**：编辑 Copy 的过程是用户的决策练习，不能简单消灭

### 用户明确约束（原话提炼）

- UI 上要有**独立的决策区域**（现在 agent 是侧边 dock，不独立）
- **Context 管理对用户透明、有主次**：能看到 context 里装了什么、哪部分是重点
- **Context 必须能被分析**：可检查、可审计
- 足够描述 **10 天**的信息，又必须**重点决策**
- 每天 **5-10 轮对话**要撑得住
- **事实时**（数据是活的，不是复制时点冻结的快照）

### 设计目标

> 决策 agent 变成系统内的一等公民：**会话持久化 + 分层 context（活跃/窗口/归档）+ 全透明审计 + 反馈闭环**。复制行为退化为"导出到外部"的可选旁路，成长环节（编辑/审阅）保留在 UI 内。

## 2. 现状盘点（可复用资产）

| 资产 | 位置 | 复用方式 |
|---|---|---|
| 统一 LLM 出口 ai-service | `apps/ai-service`（4310，Hono + Vercel AI SDK，openai/google/ollama profile） | 决策 chat 路由直接加这里 |
| 决策合同 V7.8 | `docs/modules/downstream-ai-prompt.md` | 作为决策 agent 的权威 system prompt（原文注入，不再维护第二份） |
| Copy All payload | `dashboard-export.ts` `buildDashboardCopyAllMarkdown` | 活跃决策层的**数据源**（保持纯 payload，不含行为指令） |
| 新鲜度 API | `/api/health/datasources`（TIP-013） | 决策区 freshness 指示器 + agent 侧数据可信度提示 |
| 引用装配 buildReferenceBlock | `ChatPanel.tsx`（16 种引用类型） | 拆出可复用的 context 装配器 |
| 开火归因 | TIP-011 execution journal sourceStats | 反馈闭环的胜负来源 |
| 会话 UI 组件 | `components/chat/*`（流式/消息列表/composer） | 决策区复用组件，不做第二套 | 
| 结果数据 | `execution_journal`、`paper_trades`、`trade_reviews` | 反馈回灌数据源 |
| System prompt 管理 | `system_prompts_routes` + SystemPromptEditor | 决策 agent 的 prompt preset 直接复用该存储 |

**主要缺口**：
- 对话无服务端持久化（仅 localStorage `karios.chat.v0`）→ 不可被分析、不可跨会话回看
- 无"每日决策快照"归档 → 10 天历史无法按需检索
- 无反馈回灌作业 → agent 不知道自己昨天判断的胜负
- 无 context 透明度 UI → 用户看不到注入量

## 3. 核心设计：三层 Context 架构

一句话：**10 天历史不进 token，进索引**。模型每轮只看到"活跃层 + 对话窗口"，历史按需检索。

```
┌─────────────────────────────────────────────────────┐
│  Layer 1 · 活跃决策层 (Active)   ~3-6k token         │  ← 每轮注入，实时拉取
│  P0 操作表（watchlist 表格 + Gate + Exec Attention    │
│     + Cond order draft）                              │
│  P1 战情（今日焦点≤3 + 7 行扫描 + 新闻摘要 + freshness) │
│  P2 背景（行业资金流 Top5 / 宏观 / 情绪，可折叠）        │
├─────────────────────────────────────────────────────┤
│  Layer 2 · 对话窗口 (Window)   ~15-25k token         │  ← 滑动窗口
│  最近 N 轮消息（N 默认 12，超过的折叠为摘要行）          │
│  折叠规则：旧轮压缩成 "date: 判断/结果" 一行            │
├─────────────────────────────────────────────────────┤
│  Layer 3 · 10 天归档 (Archive)   ~0 token（按需）      │  ← 检索式
│  每日决策快照（操作表 + agent 判断 + 结果回顾）          │
│  检索函数：by_date / by_symbol / by_judgment_type      │
│  agent 用函数调用拉取，拉取结果只进当前轮               │
└─────────────────────────────────────────────────────┘
```

### 3.1 为什么这个结构同时满足全部约束

| 约束 | 解法 |
|---|---|
| 描述 10 天信息 | Layer 3 归档**完整保留**10 天（甚至更久）每日快照，只是不注入；需要时检索。信息量不丢 |
| 重点决策 | Layer 1 只有决策必须项（P0 永远全量、P1 默认、P2 折叠）；注入量恒定，不被 10 天历史稀释 |
| 每天 5-10 轮撑得住 | 每轮注入 = Layer1(~5k) + Layer2(~15k) ≈ 20k；10 轮 = 200k 输入 token。200k context 的模型当天会话内不超（Layer2 滑动丢弃最旧轮，折叠为摘要行而非删除） |
| 事实时 | Layer 1 每次消息前实时拉取（60s 内新鲜）；数据变化时 UI 与 agent 同时可见；freshness 过期标红 + 一键刷新（复用 TIP-013/014） |
| Context 透明有主次 | 见 §4 Context Inspector——每层可见、每块有 P0/P1/P2 标记与 token 数 |
| Context 可分析 | 见 §5——每轮记录 context 快照（注入清单 + token），SQL 可审计 |

### 3.2 归档快照（Layer 3 的数据形态）

每天盘后 automation 完成后自动生成 `decision_snapshots`：

```
{
  date, market_summary(ref), active_layer_ref(操作表快照),
  agent_exchanges: [{judgment, rationale, refs}],   // 当天 agent 判断
  outcome: {later: execution/paper results},        // 反馈回灌（T+1 后补）
}
```

- 快照是**引用**不是拷贝：操作表存 id，正文从 DB 现取，避免双份数据漂移
- 反馈回灌作业：每日盘后扫描 execution_journal.changes + paper_trades，把结果写回对应日期的快照 `outcome`

### 3.3 Token 预算（200k context 模型）

| 项 | 预算 | 说明 |
|---|---|---|
| System prompt（V7.8） | ~4k | 恒定 |
| Layer 1 活跃层 | ~5k | P0 3k + P1 1.5k + P2 0.5k（P2 可折叠为一行） |
| Layer 2 窗口（12 轮） | ~15k | 超出的轮折叠为摘要行 |
| 预留（本轮检索结果/输出） | ~8k | 按需检索最大注入 |
| **每轮总计** | **~32k** | 10 轮 ≈ 320k 输入 token；实际每轮重发 = 恒定 32k，不随天数增长 |

> 关键算术：**复制模式每天重发 10 天全量 ≈ 150-300k；闭环模式每天 10 轮 × 32k ≈ 320k 上限但恒定**——区别在于闭环的 320k 全是"今天活的 + 窗口"，信息密度远高于 300k 的 10 天旧快照。若要进一步压：模型支持 context 缓存（如 Anthropic cache_control）时 Layer1+System 只付增量费用。

## 4. UI 设计：独立决策区

新增导航项 **「决策」**（SidebarNav，放在 Research 之后），`DecisionPage.tsx`。

### 4.1 布局（左对话 + 右 Context Inspector）

```
┌──────────────────────────────────────────────────────────────┐
│ 决策 Agent  [模型: gpt-4o-decision▼] [决策合同 V7.8] [新建会话]  │
│ ├───────────────────────────────┬────────────────────────────┤
│ │  对话流（复用 ChatMessageList) │  Context Inspector           │
│ │  · 消息带"本轮注入"徽标        │  ├ Layer 1 活跃层            │
│ │  · 系统消息显示 freshness 变化 │  │  ☑ P0 操作表   3.1k tok   │
│ │  · agent 判断可"标记为采用/否决"│  │  ☑ P1 战情     1.4k tok   │
│ │  · 检索历史时插入"归档引用"块   │  │  ☐ P2 背景     0.5k tok   │
│ │                              │  ├ Layer 2 窗口 12 轮 15k tok │
│ │                              │  ├ Layer 3 归档索引 10 天     │
│ │                              │  │  · 08-06 ✓ 已反馈 3 判断    │
│ │                              │  │  · 08-05 ✗ 未反馈          │
│ │                              │  └ token 预算条 [32k/200k]    │
│ ├───────────────────────────────┤                              │
│ │ Composer + freshness 状态条    │                              │
│ │ [行情 12m ✓ 新闻 5m ✓ 研报 2h⚠]│                              │
│ └───────────────────────────────┴────────────────────────────┘
```

### 4.2 Context Inspector（透明 + 主次的核心）

- **每层可见**：Layer1 各块展开可见实际内容预览 + token 数；Layer2 显示窗口轮数与最早轮摘要；Layer3 显示归档日期索引 + 反馈状态
- **主次标记**：P0/P1/P2 徽标，可开关（默认 P0+P1 注入，P2 折叠为一行）
- **token 预算条**：注入量/窗口/预留 vs 模型上限，直观
- **每轮"注入审计"**：每条消息下可展开"本轮 context 清单"（系统 prompt + 哪些块 + 哪些归档检索），即 §5 的每轮快照
- **数据新鲜度状态条**（复用 TIP-013）：composer 上方恒显，过期标红 + 一键刷新（TIP-014 逻辑）

### 4.3 与现有 AgentPanel 的关系

- 现有全局 dock 聊天（AgentPanel/ChatPanel）**保留**，定位：通用聊天/杂项
- 决策区是**专用页面**：固定装配 V7.8 决策合同 + 分层 context + 归档检索 + 反馈标记；UI 组件（消息列表/流式/composer）复用，**context 装配与存储独立**

## 5. Context 可分析（审计能力）

三层分析能力，全部服务端落库：

| 能力 | 实现 | 价值 |
|---|---|---|
| 每轮注入审计 | `decision_messages.context_snapshot`（JSONB：注入的块 id 列表 + token 估算 + 归档检索记录） | 回答"这轮决策基于什么数据" |
| 判断胜负统计 | 快照 `outcome` 回灌后，按判断类型统计胜率（复用 TIP-011 归因口径） | 回答"agent 哪类判断靠谱" |
| 归档检索日志 | 检索函数调用记录（查了什么、命中什么） | 回答"10 天里 agent 实际用了多少历史" |

开放问题：token 估算用字符数粗估（~4 chars/token 中文，~1 token/4 chars 英文混合）还是接真实 tokenizer？**M2 先粗估，够用于预算条**。

## 6. 数据模型与 API

### 6.1 新表（data-sync-service，alembic 0020）

```
decision_sessions(id, title, model_profile, created_at, last_active_at)
decision_messages(id, session_id, role, content, context_snapshot JSONB, created_at)
decision_snapshots(id, snapshot_date, active_layer_ref JSONB, agent_exchanges JSONB,
                   outcome JSONB, status, created_at)
```

- `decision_snapshots` UNIQUE(snapshot_date)；`outcome` 由反馈作业更新
- 消息正文含 markdown 渲染所需全字段（与前端 ChatMessage 对齐）

### 6.2 API

| 端点 | 服务 | 用途 |
|---|---|---|
| `POST /decision/sessions` `GET /decision/sessions` | data-sync (4330) | 会话 CRUD |
| `GET /decision/sessions/{id}/messages` | data-sync | 窗口消息（分页，旧轮折叠摘要由服务端算） |
| `POST /decision/chat`（SSE 流式） | **ai-service (4310)** | 决策对话：服务端装配 Layer1（实时拉 4330 各接口）+ Layer2 + 函数调用（检索归档），完成后写回 4330 持久化 |
| `GET /decision/snapshots?date=` | data-sync | 归档索引/快照 |
| `POST /decision/snapshots/{date}/outcome` | data-sync | 反馈回灌（内部作业调用） |

> 职责切分：**ai-service 无状态**（每次请求装配 context），**data-sync-service 有状态**（会话/快照/反馈）。决策 chat 的上下文装配放 ai-service（它已持有模型配置），但数据全从 4330 拉——对齐现有 `freelancer-architecture.md` 的"Karios 不调 LLM，数据侧无 LLM"切分。

### 6.3 归档与反馈作业（scheduler）

| 作业 | 时间 | 动作 |
|---|---|---|
| `decision_snapshot_job` | 每交易日 18:00（automation 后） | 生成当日快照（操作表 ref + 当天会话判断） |
| `decision_outcome_job` | 每交易日盘后 | 扫描 execution_journal.changes + paper_trades → 更新前 N 日快照 outcome |

## 7. 里程碑（开发顺序）

| 里程碑 | 内容 | 验收 |
|---|---|---|
| **M1 会话持久化** | 3 张表 + 4 组 API + DecisionPage 基础聊天（SSE 流式，直接调 ai-service /chat + V7.8 合同） | 决策区可对话，重启不丢，可回看历史会话 |
| **M2 分层 context + Inspector** | ai-service 装配 Layer1（实时拉取）+ Layer2 窗口 + 折叠；Context Inspector UI + token 预算条 + freshness 状态条 | 每轮注入 ≤35k 恒定；Inspector 每块可见/可开关；freshness 过期标红可刷新 |
| **M3 归档 + 检索 + 反馈** | snapshot/outcome 作业 + 检索函数调用 + 会话里"归档引用"块 | 10 天快照可检索；T+1 胜负回灌可见 |
| **M4 分析视图** | 每轮注入审计展开 + 判断胜率统计（决策区「分析」tab） | 可回答"agent 判断的胜率与依据" |
| **M5 外部导出旁路** | 保持 Copy All（导出外部），对比两路胜率 | 数据支持"是否彻底去掉复制"的决策 |

## 8. 风险与开放问题

1. **窗口折叠质量**：旧轮折叠成一行摘要会丢细节——折叠保留"判断/依据/结果"三字段，不存废话；折叠可展开
2. **模型上限假设 200k**：若换小上下文模型（如 64k），Layer2 窗口自动缩（N 12→6），预算条按模型上限自适应
3. **"事实时"成本**：每轮拉 Layer1 有延迟（5-8 个接口）→ 并行拉 + 60s 内联缓存（TIP-014 的 forceFresh 只对显式"立即刷新"生效）
4. **判断标记（采用/否决）**：M1 先不做，M4 分析视图需要时才做（避免过早 UI 复杂化）
5. **multi-agent 意图**：M5 若保留外部 agent，需要统一归因口径（TIP-011 已有 source 分桶，可直接复用）
6. **会话注入重复消耗**：10 轮 × 32k 输入 ≈ 320k/天 ≈ $1-2/天（deepseek 级模型可忽略，gpt-4o 级可接受）；若想省钱 → 模型 context caching（Layer1+System 只付增量）

## 9. 与 TIP-013/014 的关系（不重复实现）

- TIP-013 `freshness` 结构直接嵌入 Layer1（P1 战情块内）+ composer 状态条，**不新写**
- TIP-014 `forceFresh` 逻辑复用为决策区的"立即刷新"按钮行为
- Copy All 保留为 M5 外部旁路，buildDashboardCopyAllMarkdown 的 payload 即 Layer1 的数据源（同源不双写）
