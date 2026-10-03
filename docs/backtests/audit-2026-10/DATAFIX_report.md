# Karios 数据同步修复报告（2026-10-03，无人值守执行）

> 对象：`~/Projects/karios-desktop`（分支 `fix/data-sync-2026-10`，commit `e9305ec3`，未 push、未合 main）。
> 纪律：未下单、未连券商、未改策略参数、未改回测口径、未删除历史数据；只用项目现有同步代码 + 已配置 token 回补，未另起数据源；未用 Crimson 代码数据；未开可见浏览器；未读 .png/.jpg。
> 前置输入：`FINAL_REPORT.md`（§0.4、§0.6、§7）、`SUMMARY.md`（E）、`H2e`/`H2f`/`H2g` 数据部分。今日 2026-10-03（周六），国庆休市（10-01~10-07），最后交易日 2026-09-30。

## 1. 诊断表（修前 → 修后最大日期，2026-10-03 实测）

| 数据源 | 表/文件 | 修前最大日期 | 修后最大日期 | 行数 | 自动调度 | 失败原因/结论 |
|---|---|---|---|---|---|---|
| A股日线 | `daily`（CN 部分） | 2026-09-30 | 2026-09-30（无变化，健康） | 23994624 | 有（`close_sync` 每日 17:10 + `daily_full_sync` 周五） | 健康：09-30 为最后交易日，09-25 CN 0 行为中秋休市（日历 closed），非缺数 |
| 14:30 振幅 | `amp_1430` | 2026-09-30（5018 行/日，dense） | 同左 | 81608 | 有（14:30 面板 + 18:40 回填） | 健康：09-28/29/30 连续 5018/日，OPT-224 已修好 |
| 5分钟线 | `bar_5min` | 2026-09-30 | 同左 | 58821148 | 有（`bar_5min_close` 工作日 18:40） | 健康 |
| 套筒 ETF 日线 | `daily`（5 只套筒） | 2026-09-30 全 | 同左 | 518880:1393 / 513350:689 / 513110:859 / 513100:908 / 511260:1393 | 有（`sleeve_etf_daily_sync` 工作日 17:25，54/54 成功） | 健康；`513100` 908 行为 2023-01-03 起完整（上市后），非缺数 |
| ETF 月快照 CSV | `data/etf/etf_daily.csv` | **2026-09-11**（54715 行） | **2026-09-30**（55207 行，+492） | 55207 | **修前无（纯手动脚本），修后有（新增 `etf_snapshot_sync` 每月 2 日 19:30）** | 根因：`scripts/sync_etf_daily.py` 无调度、无节流；显示层靠 `merge_recent_db_closes` 续命。已回补 + 自动化 |
| 宽基份额 | `cn_etf_share` | 2026-09-30（4/日） | 同左 | 8056 | 有（`risk_state_sync` 工作日 18:50，16/18 成功） | 健康 |
| 指数日线 | `index_daily` | 2026-09-30（5 码） | 同左 | 20903 | 有（`index_daily_full_sync` 工作日 16:30） | 健康 |
| 市值/基本面 | `stock_dailybasic` | 2026-09-30 | 同左 | 7096279 | 有（`stock_daily_basic_sync` 工作日 17:20） | 健康 |
| 资金流明细 | `cn_moneyflow` | 2026-09-30 | 同左 | 4601985 | 有（`risk_state_sync` 内补漏） | 健康 |
| 北向总量 | `cn_moneyflow_hsgt` | 2026-09-30 | 同左（口径已修正） | 1350 | 有（`risk_state_sync`） | **口径断裂**：2024-08-19 后为成交额（100% 正），页面曾标「净买」。已改后端 + 前端标注，20 日累计在断裂处断开 |
| 北向个股 | `cn_hk_hold` | **2026-08-25** | **2026-09-30**（HK-only，1703411 行，+3888） | 1703411 | 有（按需补漏；日披露已停） | vendor 缺口：2025-08-23..2026-08-06 全空 + CN 日频自 2024-08-19 起仅季频脉冲；09-30 新增 1014 家全 HK。CN 日变化不可做（交易所停披，非代码可修） |
| 两融总量 | `cn_margin_total` | 2026-09-30（**仅 SSE 1 家，SUM 1.315 万亿，假跌 −49.6%**） | 2026-09-30（仍 SSE-only，但**聚合已剔除不完整日**，页面显示 2.58 万亿上期完整值 + 警告） | 3668 | 有（`risk_state_sync`，报 success 但实为部分发布） | 根因：SSE 比 SZSE/BSE 早发布约 T+1；同步照单全收且报成功。已加 `incomplete_dates` + 聚合过滤 + UI 警告；09-30 SZSE/BSE 重拉仍无（交易所未发布，节后补） |
| 两融明细 | `cn_margin_detail` | 2026-09-30（仅 SH 2001，缺 SZ） | 同左（重拉仍 2001，vendor 未发布） | 3565650 | 有（同上） | 同上，节后自动补（15 天窗口） |
| 散户聚合 | `cn_flow_daily` | 2026-09-30 | 同左 | 900 | 有（`risk_state_sync` 内聚合） | 健康 |
| ETF 资金流 | `market_etf_fund_flow_daily` | 2026-09-30 | 同左 | 5473 | 有 | 健康 |
| 自选池自动化 | `watchlist_automation_runs` | 152 次中 44 跳过（29%） | 逻辑已修（新增 20:30 补跑），历史行不动 | 152 | 有（17:30 + 18:05 watchdog + 18:40 post_5min + **新增 20:30 retry**） | 主因：`close_sync_not_ready` 24 次 = 17:30 跑在 close 落地前（09-30 close 落地 20:41）；`not_trading_day` 20 次 = 节假日，属预期。池最终靠 catchup/post_5min 建成，29% 高估了真实失败 |

## 2. 改了什么（文件 + commit，未 push 未合 main）

commit `e9305ec3`（分支 `fix/data-sync-2026-10`，仅含本任务 16 文件；基分支此前 `db-enhance` 的其他脏改动未动）：

- 两融不完整日：`db/cn_risk_state.py`（+`MARGIN_EXCHANGES`/`HSGT_BREAK_DATE`/`margin_exchange_counts`/`incomplete_margin_dates`/`is_hsgt_turnover_date`）、`service/cn_risk_state_sync.py`（`sync_margin_total` 返回 `incomplete_dates` 并告警）、`api/backtest_routes.py`（聚合只用 3 所完整日；`north20` 在断裂处重启；跨口径不向前填充；`/flow-series` 新增 `marginIncompleteDates`/`hsgtBreakDate` + 行级 `northIsTurnover`/`marginIncompleteDate`）。
- 北向标注：`apps/desktop-ui/.../FundFlowPanel.tsx`（「北向成交额 · 2024-08-19 后为成交额口径，非净买」、三路标题去「净流入」、footer 口径说明 + 部分发布日红字警告）、`lib/queries/backtest.ts`（`FundFlowRow/Response` 新增可选字段，后向兼容）。
- ETF 快照自动化：新增 `service/etf_snapshot.py`（与手动脚本同 UNIVERSE/口径，45 天窗口增量合并，0.4s 节流）、`scheduler/etf_snapshot_job.py`（每月 2 日 19:30，避开脆弱的 day-1 全量任务）、注册 `scheduler/__init__.py` + `api/sync_routes.py`（`SYNC_JOB_TYPES`）+ `packages/shared/.../scheduler.ts`（目录）。
- 14:30/17:30 跳过：新增 `scheduler/watchlist_retry_job.py`（工作日 20:30，仅 close 已落地且当日无 applied 池时补跑，幂等）+ 同上三处注册；`watchlist_automation` 目录文案注明 skip→补跑链。
- 健康检查：新增 `scripts/data_health_check.py`（见 §4），`service/weekly_review.py` 周报 +§4b 数据健康节。
- 测试：新增 `tests/test_margin_completeness.py`、`tests/test_datafix_etf_health.py`；更新 `tests/test_scheduler_jobs_extra.py`（新 job 注册断言）。
- 后端抽查：`PYTHONPATH=src pytest tests/test_margin_completeness.py tests/test_datafix_etf_health.py tests/test_etf_closes_freshness.py tests/test_api.py tests/test_weekly_review.py` → **34 passed**；`... + scheduler/risk_state/watchlist` 子集 → **176 passed, 1 skipped**；全量套件在 5 分钟内跑不完（回测重型用例），未强行全跑。前端 vitest 在本机无 node，不可跑（改动为可选字段 + 文案，后向兼容）。

## 3. 补了多少数据（备份先行，均用现有同步代码 + 已配 token）

- 备份：`~/Projects/wealth-ideas/karios-audit-2026-10/backup/` 内 `cn_margin_total/detail/hsgt/hk_hold/etf_share/flow_daily/moneyflow/market_etf_fund_flow_daily` 的 pg_dump（`pgAdmin` 内 `pg_dump 18.0`）+ `daily_sleeve_20261003.csv`（9 只 ETF 在 `daily` 的全量行）。`daily` 全表 24M 行未整表 dump（过大），只备了本次涉及的 ETF 行；其余表未写不备。
- ETF CSV：`refresh_window(20260912, 20260930)` → **+492 行，max 20260911 → 20260930**（41 码全）。
- `cn_hk_hold`：补 08-26/27/28 + 09-30 → **+3888 行，max 2026-08-25 → 2026-09-30**（全 HK；CN 仍 0，见残留问题）。
- 两融 09-30：重拉 `sync_margin_total` + `sync_margin_detail_for_dates(['2026-09-30'])` → 仍 SSE-only（`updated=1`/`2001` 行，`incomplete_dates=['2026-09-30']`），**0 行新增**（交易所/SZSE 尚未发布，非 token 问题；聚合侧已屏蔽假跌）。
- 其余（daily/amp/bar/index/moneyflow/etf_share/hsgt）：已在最后交易日，无需补。

## 4. 健康检查命令（已进周报）

```bash
cd services/data-sync-service
PYTHONPATH=src .venv/bin/python scripts/data_health_check.py
PYTHONPATH=src .venv/bin/python scripts/data_health_check.py --json
```

- 列出 15 张关键表：行数、最大日期、相对 `trade_calendar` 的滞后交易日数（>2 标红，exit 2）。
- 跳变：两融完整日 SUM 日环比 >30% 报警；两融 <3 所覆盖报警（2026-09-30 正在报）；daily SH 计数日环比 >30% 报警。
- 2026-10-03 实测：15 表 lag 全 0，CSV max 20260930，仅 1 条报警（09-30 两融部分发布），exit 2 符合预期。
- 周报：`weekly_review` §4b 每次自动带上 15 表快照 + 自查命令（fail-open，不影响原有 1–5 节）。

## 5. 还剩哪些问题需要用户（要人/要钱/要等）

1. **两融 09-30 SZSE/BSE 未发布**：交易所 T+1 + 国庆休市，节后（10-08 开市后）`risk_state_sync` 15 天窗口会自动补上，无需操作；若节后仍缺再查 token/限流。本次未删 SSE 已有行，未写假全量。
2. **北向 CN 日频永久缺**（交易所 2024-08-19 起停披 + vendor 2025-08-23..2026-08-06 全空）：代码修不了，只能做季度低功率或放弃日变化（H2f 结论不变）。如需恢复日频，要等交易所恢复披露或另购数据源（本次按要求未另起）。
3. **tushare token 配额**：`etf_daily_full`（~1000 只全量）曾因 200 次/分钟限流失败 3 次，新 `etf_snapshot_sync`（41 只 + 节流）已避开；若未来扩 UNIVERSE 或 token 过期/欠费，月快照会再次滞后——健康检查会标红，请续费/加 token。
4. **全量后端套件未跑完**（重型回测用例 >5 分钟）：已跑 évi 相关 176 + 34 通过；合 main 前请在空闲时跑一次全量 `pytest`。
5. **前端未跑测试**（本机无 node）：改动小且后向兼容，合 main 前请跑一次 `vitest` + `typecheck`。

## 6. 赚钱线索（profit-leads 规则执行）

- 本次为数据可靠性修复，未跑任何策略回测、未发现新的「扣费后每笔为正 / 明显优于随机」线索。
- 故 **未向 `~/Projects/wealth-ideas/profit-leads.md` 追加**（注明来源本次 DATAFIX，特此声明无新增）。

*证据版本：karios-desktop `fix/data-sync-2026-10` @ `e9305ec3`（2026-10-03）；DB `daily` 23994624 行至 2026-10-02（CN 至 09-30）、`amp_1430` 81608 至 09-30、`bar_5min` 58821148 至 09-30、`cn_hk_hold` 1703411 至 09-30、`cn_margin_total` 3668、`etf_daily.csv` 55207 行至 20260930；备份在 `backup/`；健康命令见 §4。*

KARIOS DATAFIX DONE
