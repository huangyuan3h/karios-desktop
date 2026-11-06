# East Money 数据源切换 / 降级补齐 · 归档于 2026-11-06

## 当时的目标（todo 链接）
- 对应 `docs/todo.md §4 数据源`（质量与覆盖）；工程入口 `docs/optimization-checklist.md` OPT-244。
- 用户原话：「我希望换」——2026-11-03 起东财 push2/push2delay/clist/push2his 大面积
  Empty reply / RemoteDisconnected，要求把 Sync 可见的失败腿切到非东财源，同时不许造假。

## 实际做了什么
1. **熔断按 host+path 分域**（`service/em_breaker.py`）：clist/push2his 连续 3 次失败
   只封禁自己，不再连坐 `push2/stock/get` 与 `reportapi/report/list`。旧全局 latch 保留为
   测试/应急覆盖，`breaker_status()` 四键契约不变；`/api/health/datasources` 新增
   `latchedScopes`。
2. **Tushare 实时行情恢复**：tushare 1.4.x 读 `$HOME/tk.csv`，旧代码写
   `~/.tushare/tk.csv` → `ts.realtime_quote` 每次抛 `TOKEN_ERR_MSG`。这是
   `satellite_live_panel`「no 14:30 prints」的真实根因（不是东财）。改为读取 tushare
   自己的 `TOKEN_F_P`；另加 Sina `hq.sinajs.cn` CN 兜底（仅补 Tushare 缺失代码，指数排除）。
3. **行业资金流非东财回退**（`service/industry_fund_flow_tushare.py`）：用现有
   `cn_moneyflow`（Tushare 个股 moneyflow）按申万一级聚合
   `(buy_lg+buy_elg)-(sell_lg+sell_elg)`，`source=tushare_moneyflow_sw_l1`。
   2026-11-05/06 与东财 bkzj 对照：Top5 行业名完全一致，数值差 ~2–3%。
   EM 空 → 回退；两者都不可用 → `ok=false` + `providerErrors`，不写零行。
4. **ETF 资金流历史修复**：`fd_share` 缺失的行会让 `last_date` 跳过该日；改为从最近
   120 天最早缺 `fd_share` 的日期补起，且不覆盖已有 EM 实时主力净流入行。新增
   `netFlowMetric` / `netFlow3dMetric`（`main_net_inflow` / `share_change_implied` /
   `mixed`），前端用 `*` + 脚注区分。
5. **Harbor 14:30 现价**：14:30 快照 universe 追加停车场 ETF
   （518880/513350/513100/513110/511260）与在场 CN paper 持仓，写入
   `bar_5min.live_1430`，让 harbor 盘中 trail 预警有真实 14:30 打印。

## 验证 / 数据
- 后端全量 `4622 passed, 3 skipped`；本次相关 610 passed；DB 行数检查（save→跑→check）OK。
- 前端 `tsc` 0 error；ETF/行业卡 vitest 16 passed；eslint 仅既有 warning。
- 实测：`fetch_realtime_quotes` 恢复 600000.SH/510300.SH/000001.SZ；
  `aggregate_for_date("2026-11-06")` 31 个 SW L1，Top5 = 公用事业/银行/美容护理/农林牧渔/煤炭；
  `510300.SH` 修复 10-30..11-05 `fd_share`/`net_inflow`。
- 详细矩阵/限制/重启验收：`.opencode-runs/em_source_swap_2026-11-06_report.md`。

## 后续影响 / 留给谁
- `option_iv_daily`（EM clist）与 `research_report_sync`（reportapi）仍无备用源；前者已
  degrade，后者靠熔断分域恢复（reportapi 本身健康）。
- 行业序列可能出现「EM 日 + Tushare 日」混排，每行有 `source`，UI 已标注；不做静默改写。
- `satellite_live_panel` 需下一个交易日 14:30 才能验证真实面板（2026-11-06 14:30 已过）。
