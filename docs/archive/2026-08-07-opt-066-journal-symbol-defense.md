# OPT-066：journal 上游 symbol 防御层（遗留修复）

> **完成日期**：2026-08-07
> **目标**：OPT-064 清理了 967 条测试污染 journal 行，但写入路径本身无校验。补防御层让坏 symbol 永远进不了决策日志。

## 问题回顾

- 污染源：`test_execution_source_db.py` 的 requires_postgres 测试直接插 `CN:99{uuid}` 假 symbol 的 paper_trades / `manual-test` 快照 / `snap-agg`、`snap-bf` 假 id 行，8-04~8-07 累计 967+ 条
- 清理后暴露的另一面：**任何来源**（前端 snapshot 提交、外部 AI agent、alpha 通道）都能向 journal 写入任意格式 symbol——写入路径无校验

## 防御方案（双层）

| 层 | 改动 |
|----|------|
| **后端（权威）** | `is_valid_watchlist_symbol()`：`^(CN:\d{6}|HK:\d{1,5}|ETF:\d{6})$`（与 `trendok._symbol_to_ts_code` 同规则）；`_cards_by_symbol` 在 diff 前过滤非法卡片；`ingest_snapshot` 在**存储前**剥离非法卡片并返回 `rejectedCards` 计数（可观测：部分拒绝不会静默丢卡） |
| **前端（双保险）** | `buildExecutionSnapshotPayload` 构建卡片前跳过非法 symbol（`WATCHLIST_SYMBOL_RE` 同规则） |

## 验证

- 后端 1379 passed / 2 skipped（唯一失败为既有 flaky）；前端 495 passed；tsc 干净
- 实测：`ingest_snapshot(cards=[CN:600000, CN:9901ae04, HK:00700])` → `rejectedCards: 1`，坏卡不入库
- 测试：后端 3 个新测试（校验函数 10 断言 / diff 忽略坏卡 / split 计数）+ 前端 1 个（payload 跳过坏 symbol）

## 反模式确认（未做）

- ❌ 未修前端 localStorage 存量（registry 已验证 0 污染）
- ❌ 未做 symbol 规范化映射（拒绝而非猜测）
- ❌ 未阻塞合法提交（正常 CN/HK/ETF 不受影响）
