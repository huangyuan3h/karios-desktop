# OPT-049 · Paper-trading 启动（v0） · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §3 收益 / §8 回测 / §12 实施清单 #3`](../../todo.md)
> **关联 OPT 条目**：[OPT-049](../../optimization-checklist.md)

## 当时的目标

`docs/todo.md §8` 旧 BacktestPage 效果差已隐藏。要重启回测，第一步**不是写回测框架**，而是先把"如果跟着信号走会怎样"的真实数据积累起来——paper-trading 是回测的前提。

按 §8 决议：**paper-trading 跑一周** → 用真实策略表现数据反过来给 §3 收益（最高优先级）输血。

## 实际做了什么

### A. 数据模型 `paper_trades` 表

- `id` UUID PK
- `symbol` `entry_date` `side` (BUY/ADD) `entry_price` `score_at_entry` `why_at_entry` `sleeve_pct`
- `status` (open/closed) `close_date` `close_price` `pnl_pct` `holding_days` `close_reason`
- `created_at` `updated_at`

**索引**：
- 唯一索引 `(symbol, entry_date, side)` —— **intake 幂等键**
- 部分索引 `(status, entry_date DESC) WHERE status='open'` —— update cron 快速拉 OPEN
- 部分索引 `(close_date DESC) WHERE status='closed'` —— stats 查询

Alembic `0011_paper_trades` 同步迁移。

### B. service 层（3 个入口）

| 函数 | 触发 | 职责 |
|------|------|------|
| `run_intake` | 17:40 cron | 找当日 decision journal 中 `BUY/ADD` 且 live position == 0% 的标的 → 落库。**幂等** |
| `run_update` | 17:45 cron | 拉所有 OPEN trade 的最新收盘价 → 更新 pnl + holding days → 触发 v0 关闭条件 |
| `compute_stats` | API 调 | `closedCount` / `winningCount` / `winRate` / `avgPnlPct` |

### C. scheduler（2 cron）

```
17:40  paper_trading_intake      工作日
17:45  paper_trading_update      工作日
```

时序：**close_sync (17:10) → watchlist_automation (17:30) → cn_industry_post_close (17:35) → paper_intake (17:40) → paper_update (17:45)**。

### D. /v1 暴露（OPT-045 兼容）

```
GET /v1/paper-trades?status=open&since=2026-08-01&limit=50
GET /v1/paper-trades/stats?since=2026-08-01
```

字段全 camelCase + description，AI 助手可直接调。

## v0 关闭条件

| 条件 | `close_reason` |
|------|---------------|
| `pnl_pct <= -5%` | `stop_hit` |
| `holding_days >= 5` | `max_hold` |

**未实现**（P2 OPT-050+）：

- `pnl_pct >= +10%` → `target_hit`（止盈）
- `score 跌穿 30` → `score_floor`
- 离开 watchlist → `pool_exit`

## v0 范围限定（明确）

- **CN only**：HK paper-trading 需要 FX + T+0/T+2 结算差异，留 OPT-050+
- **不重写 BUY/ADD 规则**：直接消费 decision journal + 日线收盘——和 live Execution Gate 同口径
- **不作为发布决策依据**：避免过拟合；只作"如果跟着信号走会怎样"的真实数据
- **idempotent intake**：`ON CONFLICT (symbol, entry_date, side) DO NOTHING` 守住——重跑同一天不会产生重复行

## 验证 / 数据

- **19/19** test_paper_trading 全绿
- **107 + 1 skip** 全部测试（49 v1/* + 19 paper + 12 tunnel + 19 test_api + 8 alembic）
- 1 个 alembic migration（0011）成功
- 字段 `description` 全非空
- 关闭条件测试：`stop_hit` / `max_hold` / 不触发 时各路径单测覆盖
- 幂等测试：`insert_paper_trade` 返回 None → summary 计入 `skipped: duplicate`

## 后续影响 / 留给谁

### 给外部 AI 助手那边

- 启动后可调 `GET /v1/paper-trades?status=open` 看当前持仓
- 调 `GET /v1/paper-trades/stats?since=N天前` 出"最近 N 天胜率"消息
- 关闭的 trade 自动进入 stats（无需手动统计）

### 给 Karios 本身

- 跑 5 个工作日就有第一批真实数据（5 天 = 最长持仓期）
- 跑 20 个工作日（约 1 个月）有"有意义"的胜率样本
- 数据反过来给 §3 收益（最高优先级）输血：哪些信号 true positive / 哪些是噪音

### P2 留给未来 review

- 加 `target_hit` / `score_floor` / `pool_exit` 关闭条件
- HK 适配（FX + 结算差异）—— 留 OPT-050
- per-industry break-down（哪些行业信号更准）—— 留 OPT-051
- max drawdown / Sharpe 比率 —— 留 OPT-052

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 8（db + service + 2 scheduler + alembic + 1 test + 4 内部 fixture）|
| 改动文件 | 4（api/v1_business_routes.py + scheduler/__init__.py + test_alembic_baseline.py + OPT-049 doc）|
| 新 cron | 2（intake 17:40 + update 17:45）|
| 新表 | 1（paper_trades + 3 index）|
| 新 endpoint | 2（list + stats）|
| 总测试 | 19 paper + 19 test_api + 8 alembic + 49 v1 + 12 tunnel = **107/107 ✅** + 1 skip |
| 工期 | 1 个会话集中 |
| 预算 | $0 |
