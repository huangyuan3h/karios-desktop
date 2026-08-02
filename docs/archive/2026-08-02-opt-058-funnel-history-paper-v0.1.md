# OPT-058：漏斗 N 日表格（TIP-002 收尾）+ Paper-trading v0.1 关闭条件 · 归档于 2026-08-02

## 当时的目标（todo 链接）
- todo §12 #20「漏斗 N 日转化率表格」— TIP-002 埋点已就绪，只差前端展示（0.5 天）
- todo §12 #21「Paper-trading v0.1 关闭条件」— +10% target / score 跌穿 / 池内剔除（1 天）
- 同批 review 确认的两个最高 ROI 项：度量闭环 + 正在积累的 paper 数据口径完整性

## 实际做了什么

### A. 漏斗 N 日转化率表格
- 后端：`db/watchlist_automation.list_recent_runs(limit)` — `DISTINCT ON (trade_date)` 每交易日取最近一次 ack 的 run（含 `meta.funnel`）
- 后端：`GET /watchlist/automation/runs?limit=N`（clamp 1..30）→ `{ok, runs, asOfDate}`；**必须注册在动态路由 `/watchlist/automation/{run_id}` 之前**（FastAPI 匹配顺序）
- shared：`AutomationFunnelSchema` / `AutomationRunHistoryRowSchema` / `FunnelHistoryResponseSchema`
- 前端：`lib/queries/funnel.ts`（`useFunnelHistoryQuery`，stale 2min / 轮询 5min）+ `components/watchlist/FunnelHistoryTable.tsx`（列：日期 | TV | 回撤 | TrendOK | +新增 | 转化率 | 兜底；转化率 = TrendOK/TV，空窗日按 fb 口径）挂在 WatchlistPage ImportDebug 下方
- `funnelFromMeta` 从私有改为导出（组件复用）

### B. Paper-trading v0.1 关闭条件
- `db/paper_trading.py`：`CLOSE_REASONS` 扩展 5 种；新阈值 `TARGET_PNL_PCT = 10.0`、`SCORE_FLOOR = 30.0`
- `db/watchlist_automation.py`：新增 `fetch_latest_score_since(symbol, since)`（watchlist_score_daily 最新 score）
- `service/paper_trading.run_update` 重构出 `_pick_close_reason()`，优先级：
  `stop_hit (−5%) > target_hit (+10%) > score_floor (<30) > pool_exit (不在 registry) > max_hold (5天)`
- **fail-open 纪律**：score 数据缺失 / registry 读取失败 → 不因该条件关闭（与"主线数据未就绪不误 TRIM"同哲学）
- `/v1/paper-trades` 的 `closeReason` description 同步 v0.1 口径

## 验证 / 数据
- 后端新增 15 测试（paper v0.1：target/score_floor/pool_exit/两处 fail-open/stop 优先级 + funnel：shape/limit/路由优先），`test_paper_trading.py` + `test_funnel_history.py` + `test_watchlist_automation.py` 共 50 passed
- 全量后端：1316 passed, 3 skipped, 1 failed（`test_tv_ego_lite` RecursionError —— **pre-existing flaky**，单独跑通过，与本次无关）
- 前端：FunnelHistoryTable 5 新测试全绿；`tsc --noEmit` 0 error；shared build 成功
- 前端全量 430 测试中 4 个失败（exec-attention ×2 / copy-ai-brief / queries/dashboard）—— stash 本次改动后复现，**pre-existing**，与本次无关

## 后续影响 / 留给谁
- ⚠️ 前端 4 个 pre-existing 测试失败：疑似与本地化工作（`073a49a` 前后）不同步，建议单独修
- paper v0.1 后：胜率 / 持有天数 / 关闭原因分布统计口径完整，可支撑 §12 #12 BacktestPage 重写的决策链
- `pool_exit` 依赖 automation 17:30 先于 update 17:45 跑（当天被清出池的票当天关闭）——调度顺序已满足
