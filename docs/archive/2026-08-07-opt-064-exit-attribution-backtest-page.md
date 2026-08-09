# OPT-064：卖出归因 + 敏感性报告 + 回测页（todo §16 L3-P3 / §8 回测 UI）

> **完成日期**：2026-08-07
> **目标**：L3-P3「归因与敏感度」+ 用户可见位置（回测页）。

## 交付物

| 件 | 说明 |
|----|------|
| `service/exit_attribution.py` | 卖出归因：平仓后 N 日前向收益 → 卖早（≥+2%）/ 卖对（≤-1%）/ 中性；按 close_reason 聚合 + 组合暴露（最多同时持仓 → 单票权重下界，对照 15%/30%/sleeve 红线） |
| API | `GET /api/backtest/exit-attribution?days=5` |
| FE `BacktestPage` | SidebarNav「回测」入口：单配置运行（摘要卡）+ 敏感度网格 36 组对比表 + 卖出归因表 + 组合暴露 |
| FE `lib/queries/backtest.ts` | react-query hooks（run / sensitivity / exit-attribution） |

## ⚠️ 过程中发现并修复的两个 live 级 bug（比交付物更重要）

### Bug 1：intake 读取 journal 的 key 与真实 shape 不符 → paper 从未有真实数据

`ej_db.list_changes` 返回 `{field, newValue, source, ...}`（camelCase），intake 却读
`ch.get("action")` / `ch.get("score")` —— `action` 恒为 None → **intake 自 OPT-049
上线起从未匹配到任何信号（candidates=0）**。paper_trades 里仅有的 230+ 条全是
requires_postgres 测试残留。

修复：intake 按 `field == 'action'` + `newValue` 解析；journal 无 score/why/sleeve 字段
（数据限制，close 条件改读实时数据，不受影响）。

### Bug 2：service 层用 snake_case 读 db 层 camelCase → run_update 永不更新

`db/paper_trading._row_to_dict` 返回 camelCase（`entryDate`/`entryPrice`），
`run_update` 读 `entry_price` → None → 所有 open 交易被 `entry_price <= 0` 跳过。
**修复后首次真实平仓闭环跑通**（CN:600000 → pool_exit，净口径 -0.3%）。

同时把 `_pick_close_reason` 改为兼容双 shape 读取（engine 的 snake 位置 dict + db 的
camel 行）。

### Bug 3（测试基建）：requires_postgres 测试不清理 → 污染生产表

`test_execution_source_db` 的 `_fresh_symbol()` 生成 `CN:99{uuid}` 直接插入真实 DB
且不清理——8-04 起累计 230+ 条 hash 行，掩盖了上面两个 bug 的可见性。已加 autouse
teardown 删除本模块插入的行（清理后表归零，实测无残留）。

## 已知问题（上游，非本次修复范围）

- **journal 里的 hash symbol**：`execution_decision_changes` 中约 40-75% 的
  BUY/ADD 行 symbol 是 `CN:99xxxx` 格式（上游 deriveActionCard/snapshot diff 写入的
  坏 symbol）。intake 现在正确拦截（out-of-scope skip），但上游写入方仍应修——
  记录待办，单独排期。

## 验证

- 后端 1370 passed / 2 skipped（唯一失败为既有 flaky）；前端 494 passed；tsc 干净
- 真实数据端到端：intake 建仓 CN:600000（ALPHA 来源）→ run_update 净口径平仓闭环
- 回测页 API 冒烟 200（归因空态正确返回 hint）

## 反模式确认（未做）

- ❌ 未做参数寻优（页面只展示敏感度；发布依据以 paper 实绩为准）
- ❌ 未改引擎行为（页面只消费 OPT-063 的 API）
- ❌ 未做真实交易 UI（纯分析页）
