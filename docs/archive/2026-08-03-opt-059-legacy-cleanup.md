# OPT-059 隐藏页 / legacy 清理（SimTrade + 旧回测框架退役） · 归档于 2026-08-03

## 当时的目标（todo 链接）
- `docs/todo.md §12 #19`：SimTradePage（1017 行）+ `/simtrade` API、BacktestPage（664 行）+ `testback/` 旧回测框架仍注册在 `main.py` 路由——nav 已注释隐藏但代码/API 仍在维护面内，退役或标 deprecated。

## 实际做了什么
- **前端**：删除 `SimTradePage.tsx`、`BacktestPage.tsx`、`lib/queries/backtest.ts`；从 `AppShell.tsx` 移除 dynamic import / PAGE_TITLES / render 分支；`SidebarNav.tsx` 移除注释入口。
- **后端**：删除 `api/simtrade_routes.py`（`/simtrade/*` 5 个 endpoint）与 `testback/` 整个包（engine / universe / strategies / db）；从 `query_routes.py` 移除 `/backtest/run`、`/backtest/result/{id}`、`/backtest/runs`、`DELETE /backtest/run/{id}` 及配套模型；`main.py` 注销 simtrade router。
- **DB**：`backtest_run`（2 行）/ `backtest_trade`（132 行）经 **Alembic 0017_drop_backtest_tables** 删除；`schema_baseline.py` 同步移除对应 DDL（fresh DB 不再建这两张表）。
- **杂项**：删除 7 个相关测试文件；`.dockerignore`、`AGENTS.md` 移除 testback 引用。

## 验证 / 数据
- 后端 `pytest`：**1247 passed / 3 skipped**，唯一失败 `test_trendok_industry_flow::test_t1_sniper...` 在干净 HEAD（stash 全部改动后）同样失败，为既有问题，与本任务无关。
- 前端：typecheck 干净；vitest **429 passed**；lint 0 errors（warnings 均为既有文件）。
- Ruff：改动文件全部通过。
- OpenAPI：141 个 path 中无 `/backtest*`、`/simtrade*`。
- DB：`backtest_*` 表已不存在，Alembic head = `0017_drop_backtest_tables`。

## 后续影响 / 留给谁
- §8 重启回测时按 `docs/todo.md §8` 前置条件做（paper-trading 先行、与 live Execution Gate 同口径），**不要**从 git 历史翻回 testback/ 复用——那是已被证伪的旧实现。
- hk_daily* 里"保留 5y bars 供长期回测"的注释保留不动，数据保留策略不变。
