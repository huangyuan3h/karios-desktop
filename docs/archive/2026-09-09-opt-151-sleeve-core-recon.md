# OPT-151 · 核心腿 recon 对账闭环（2026-09-09）

> **一句话**：把卫星 H5 对账模式镜像到核心腿——`sleeve_paper_recon()` 复现 18:20
> sleeve 决策（pre-job paper 状态重建）→ 核对 paper 簿记了没有 + 用户实际做了没有；
> mismatch 发 webhook 高事件 + `sync_job_record` 失败行，UI PortfolioHealthCard 新增
> 「核心腿对账」横幅。ok 只看 paper 层（自动化必须镜像引擎），用户侧 informational。

## 交付

| 件 | 文件 |
|----|------|
| 对账服务 | `service/sleeve_paper_recon.py`（`sleeve_paper_recon(day=)`）：决策复现 = 重建 pre-job 持仓（open CN:/ETF: 行 + 当日 `sleeve_exit` 平仓行 − 当日 multi-sleeve 新开行），再跑同一台 `build_multi_asset_sleeve` 状态机 |
| API | `GET /api/backtest/sleeve-recon/latest`（`api/backtest_routes.py`，即时计算，无快照表） |
| 14:30 action brief | `service/trading_brief.py` `_sleeve_recon_section()` + markdown「核心纸账对账」；mismatch → `sleeve_recon_mismatch` 事件（dedupe/day） |
| 盘后 job | `scheduler/sleeve_paper_job.py` `_run_recon()`：18:20 镜像后对账，clean → success / mismatch → `job_failed`（进 notifications `lane=system`） |
| 可见性 | `sleeve_paper_auto` + `sleeve_paper_recon` 进 `SYNC_JOB_TYPES`（Scheduler 页）+ `TRADING_JOB_TYPES`（cron 失败通知） |
| UI | `PortfolioHealthCard` `SleeveReconBlock`（核心腿对账横幅：✅/🔴 + 应做动作 + 用户对齐徽 + 细节折叠）；query `useSleeveReconQuery` |

## 语义

- **expected**：BUY/ROTATE → 应买 pick ETF；SELL_TO_A_SHARE/SELL_TO_REPO/ROTATE(持有换靶) → 应卖 pre 持仓。
- **actual(paper)**：当日 `createdAt` 的候选 ETF 开行（BUY 实际成交 = next_open `entryDate`，行 `signalSnapshot.signalDate == day`）；当日 `sleeve_exit` 平仓行。
- **user 实际**：user_trades 候选 ETF 行；BUY 宽容到 next_open（`userAlignment = aligned/pending/missing/idle`，只展示不翻 ok）。
- **口径铁律**：决策复现刻意镜像 `_build_multi_for_paper` 的持仓形态（无 entryDate），
  复现结果与 18:20 job 完全同源。

## 验收

- 任意交易日三问：「核心该做什么动作 / paper 记了没有 / 实际做了没有」— action brief +
  Watchlist 横幅 + Scheduler 页三处可答。
- 测试：`tests/test_sleeve_paper_recon.py`（12 单测 + 1 requires_postgres 端到端，假 symbol
  `ETF:995131` + 双模块 CANDIDATE_SYMBOLS stub，teardown 清理）；`tests/test_sleeve_paper_recon_job.py`
  （job 挂钩 4 测）；trading_brief 既有 action-brief 测试补 `_sleeve_recon_section` stub。
- 全量：4211 passed；4 失败均为 HEAD 既有（alembic baseline ×2 / HK costs / webhook E7）。
- DB 基线：`db_rows_baseline check` OK（27 表零变化）。
- 前端：`tsc` 0 错，vitest 880 passed。
- Live 冒烟：`/api/backtest/sleeve-recon/latest` 200，当日 ROTATE→OIL 预期正确（18:20 job 未跑，
  如实报 missed + pending）。

## 顺手发现（不在本 OPT 修）

1. **真实账本脏行 ×2**：`ETF:513350` 存在 `close_date=2026-08-20 < entry_date`（08-25 / 09-07
   开仓）的 sleeve_exit 行，早于本 OPT（疑似早期测试污染）。对当日 recon 会误报缺卖；建议人工
   确认后 PATCH/删（OPT-150 账本修正机制可挪链）。
2. **paper 镜像的 trail8 盲区**：`_build_multi_for_paper` 构造 holdings 不带 entryDate →
   `_etf_trail_exit` 在 paper 路径永不触发（live 路径带 entryDate 会触发）。recon 刻意同构以避免
   假阳性；paper 与 live 的 trail 口径差异留待后续 OPT（会改变 paper 行为，需单独拍板）。
