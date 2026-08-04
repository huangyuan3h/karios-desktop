# TIP-011 · 开火来源归因（TV / ALPHA / MANUAL）

> **完成日期**：2026-08-04
> **关联 todo**：[§2 收益 · P2 开火来源归因](../todo.md) + [§3 业务规则 · TIP-011](../trading-improvement-checklist.md)
> **用户工作流前提**：日常只做 Dashboard "Sync and Copy" → 把 markdown 喂给外部 AI agent → AI 决策 BUY/ADD → 写回 Watchlist。开火来源归因完全**被动可见**（Copy section），0 增量操作。

## 1. 问题

不知道实际 BUY/ADD 主要来自 TV、Alpha 还是手动 → 无法把精力投在真正贡献收益的入口。两周后要能回答「本周开火来源占比 + 每来源胜率」。

## 2. 方案：source 贯穿 write-path（前端写、后端存、读侧聚合）

来源 closed enum：**`TV` / `ALPHA` / `MANUAL`**；NULL（TIP-011 之前的存量数据）归入统计桶 **`UNKNOWN`**。

```
前端 deriveActionCard（snapshot 构建时）
  └─ 按「TV screener 最新快照符号 ∪ Alpha catalyst 符号」推断 source
      ├─ 命中 TV 集合 → 'TV'（TV 优先于 ALPHA）
      ├─ 命中 Alpha 集合 → 'ALPHA'
      └─ 否则 → 'MANUAL'
        ↓ card.source
后端 diff_snapshots 透传 → execution_decision_changes.source
        ↓ paper_trades intake 镜像
paper_trades.source
        ↓
GET /v1/execution/source-stats?sinceDays=30
  ├─ bySource: { buySignals, closed, wins, losses, winRate }
  └─ openTradesBySource: { TV: n, ... }
```

关键决策点：
- **`source` 不参与 decision hash**（`decision_payload_for_hash` 排除它）——归因是注释/标注，不是决策内容本身，同内容改来源不应产生新 decision 记录。
- **`compute_alpha_additions`（watchlist_automation）输出候选带 `source="ALPHA"`**（后端 write-path 一处显式标注）。
- 前端 `inferSource`（`lib/execution-source.ts`）是防御性兜底 + backfill 用，热路径 canonical attribution 是 snapshot 构建时写入的 `card.source`。

## 3. 改动清单

### 数据库（alembic 0018）

- `alembic/versions/0018_source_attribution.py`：`execution_decision_changes` + `paper_trades` 加 `source TEXT` + 部分索引（`idx_execution_changes_source` / `idx_paper_trades_source`，`WHERE source IS NOT NULL`）。
- **坑**：`alembic_version` 是 VARCHAR(32)，原名 `0018_execution_source_attribution`（33 字符）会炸 → 改名 `0018_source_attribution`（23 字符）。
- `db/paper_trading.py` / `db/execution_journal.py` 的 CREATE_SQL 同步加列（fresh-DB parity）。
- 实跑：`PYTHONPATH=src alembic upgrade head` 成功（live DB 已到 0018）。

### 后端

| 文件 | 内容 |
|------|------|
| `db/paper_trading.py` | `SOURCES=(TV,ALPHA,MANUAL)`；`insert_paper_trade(source=)` 校验；`count_by_source(since, status)` → `{source:{total,wins,losses,winRate}}`；`_row_to_dict` 兼容 source 列（**在表尾 index 16**，named+positional 双路径） |
| `db/execution_journal.py` | `insert_changes`/`list_changes` 带 source；`count_changes_by_source(since, field, new_value)` |
| `service/paper_trading.py` | intake 透传 `source = ch.get("source") if in SOURCES else None` |
| `service/execution_journal.py` | `_norm_source`（大写 + closed enum 校验，非法→None）；`diff_snapshots` 透传 `card.source` |
| `service/execution_source.py`（新） | `aggregate_source_stats` + `infer_source` + `backfill_paper_trades_source(dry_run=True)` |
| `service/watchlist_automation.py` | `compute_alpha_additions` 输出 `source="ALPHA"` |
| `api/v1_business_routes.py` | `GET /v1/execution/source-stats?sinceDays=1..365`（默认 30，env `EXECUTION_SOURCE_STATS_LOOKBACK_DAYS`） |

### 前端

| 文件 | 内容 |
|------|------|
| `packages/shared/schemas/executionGate.ts` | `ExecutionSourceSchema`（TV/ALPHA/MANUAL）+ `ExecutionActionCardSchema.source` |
| `lib/execution-source.ts`（新） | `inferSource` / `buildSourceContext` / `withSymbol` / `fetchSourceStats` / `formatSourceAttributionMarkdown` / `fetchTvSourceSymbols`（enabled screeners 最新快照）/ `fetchAlphaSourceSymbols` / `fetchSourceContext` |
| `lib/execution-action.ts` | `deriveActionCard` 接受 + 输出 `source` |
| `lib/execution-journal.ts` | `buildExecutionSnapshotPayload` 每卡按 sourceContext 推断 source；`captureAndPushExecutionSnapshot` 构建 context（一次 catalyst fetch 复用） |
| `lib/dashboard-export.ts` | Copy markdown Journal 之后新 section：`## Execution · Source attribution (30d)` 表格 |

## 4. Copy markdown 新 section（被动可见）

```markdown
## Execution · Source attribution (30d)
| Source | BUY signals | Closed | Wins | Losses | Win rate | Open |
|--------|-------------|--------|------|--------|----------|------|
| TV     | 12          | 8      | 5    | 3      | 62.5%    | 4    |
| ALPHA  | 4           | 2      | 2    | 0      | 100.0%   | 1    |
| MANUAL | 3           | 1      | 0    | 1      | 0.0%     | 0    |
```

- `buySignals` = changes 中 `field=action AND new_value=BUY` 的条数（含 ADD 由 cond order 表体现，信号口径只算 BUY 转换）。
- 空窗口输出 note 行，不出现空表。

## 5. 验证 / 数据

- 新增后端测试 23 个：`test_execution_source.py`（7）+ `test_execution_journal_source.py`（8）+ `test_execution_source_db.py`（8，requires_postgres）。
- 后端全量：**1295 passed, 3 skipped**（含 alembic baseline 更新 `HEAD_REVISION=0018_source_attribution`）。
- 前端：**456 passed**（新增 `execution-source.test.ts` 16 个；shared schema +1）。
- `pnpm typecheck` / `pnpm lint` 全绿（0 error；lint 仅既有 warning）。

## 6. 后续影响 / 留给谁

- **TIP-010（开火决策事后复盘）**：来源归因数据就绪后，复盘可以直接按 source 切片，成本降低。
- **backfill**：存量 `paper_trades.source IS NULL` 可用 `backfill_paper_trades_source(dry_run=True)` 先看匹配量再实跑；未匹配的进 UNKNOWN 桶。
- 连续 2 周后看 Copy 里的 attribution 表即可回答「本周开火来源占比」（TIP-011 验证标准）。
- 如果未来 source 字段要参与决策（如按来源差异化闸门），需重新考虑它是否进入 decision hash。
