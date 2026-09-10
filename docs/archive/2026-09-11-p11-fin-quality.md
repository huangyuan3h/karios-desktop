# P0-11 财务质量计划 · 归档于 2026-09-11

## 当时的目标（todo 链接）
- `docs/todo.md` §P0-11：三张原始报表入库，给 S-3 加一层防守型质量门（只剔除、不预测涨跌），财务看长期（20–60 天归因，不碰 3 天卫星腿）。

## 实际做了什么
- 数据：tushare `balancesheet/income/cashflow` 全量入库（2018Q1–2026Q2，5461 只，~16.5 万行/表，migration 0044；热字段 typed + 全行 JSONB）+ PiT 管线 `service/fin_panel.py`（单季拆分→TTM→行业中位数→市值快照）。
- 诊断 7 轮（22 季，预注册单假设、零网格、冻结 PASS 线）：F1 ROE（方向反）、F2 现金流含金量（弱）、F3 跳过（与 F2 同源）、F4 低杠杆（弱）、G1/G2/G3 市值中性重验（弱/反/反坐实）。
- 回放 1 轮：P18 价值×动量复合诊断 PASS（pooledIC +0.118）→ 三臂三窗回放（composite −8.5/−43/−43pt、mom_only +2/−44/−36pt）→ REJECT（83% 拦截是缺数 fail-closed → #7 覆盖 + #5 砍宽度；value 腿组合零增量）。

## 验证 / 数据
- 诊断脚本 `scripts/diag_fin_{f1,f2,f4,g1,g2,g3,p18}.py`；引擎门 `BacktestConfig.value_mom_gate` + 10 个测试全绿（默认 off，无害保留）。
- 结论档 `docs/backtests/factors/fin-{f1,f2,f4,g1,g2,g3,p18}*.md`；回放报告 `data/backtest_reports/walk_forward_p18_{composite,momonly}.json`。
- 方法论副产品：诊断 IC +0.12 到组合 −43pt 的落差——新因子先报门内覆盖 + 缺数率，再谈 IC。

## 后续影响 / 留给谁
- P15 质量 gate 方向整体关闭（`experiments-planned.md` §5 已留 P15 汇总行）；P18 gate 关闭（fail-open 变体需独立预注册）。
- 三张表 + 管线保留作数据资产；不补 OPT（诊断/回放链路本身无工程债）。
