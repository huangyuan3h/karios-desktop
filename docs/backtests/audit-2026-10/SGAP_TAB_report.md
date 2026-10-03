# S-gap 失效趋势 Tab 交付报告（2026-10-03）

## 做了什么
在回测页加了一个新 tab，名字叫「S-gap 失效趋势」，横轴全部是时间。
后端加了只读接口 `GET /api/backtest/sgap-decay`，数据是冻结文件
`services/data-sync-service/data/backtest_reports/sgap_decay.json`，
由新脚本 `scripts/generate_sgap_decay.py` 从
`~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2f_trades.json`
（1130 笔，不跑新 DB 查询）算出来。前端新组件
`SgapDecayPanel.tsx` 用现有 recharts 画 7 张图，顶上有一行算出来的中文小结
（不是写死数字）。共享层加了 `sgapDecay` Zod 校验。
顺手修了一个之前就坏的 typecheck：共享层缺 `SatelliteLast1430Response`
（回测页本来就引用它，HEAD 上 typecheck 就报错）。

7 张图：
1. 滚动 40 笔 / 60 笔平均每笔收益，零线，OOS2/train/valid/holdout 底色。
2. 滚动 40 笔胜率，50% 线。
3. 月信号数柱子，0 笔的月份就是 drought。
4. 滚动 40 笔随机分位，50 降档线 + 75 加仓线，底色是复活灯 0 红 / 10 黄 / 20 绿。
5. 拥挤：滚动 40 笔成交中位 + 市值中位（亿元）。
6. 大单边缘：滚动 40 笔窗内按 large_pct 中位分组，高减低（同日事后归因，不可交易）。
7. 交易空间累计 + 回撤（笔收益求和，非复利）。

方法（冻结，写在 `sgap_decay.py` 开头）：分位用 H2k 的 F long R1 正态近似
（每笔均值 +0.0178、标准差 1.2175，live 用 500 次同日随机精确值）；
复活灯是 K2（N40/X75，低于 50 降一档，两次连续确认才加，步进间隔 20 笔，
paper-20 和月频执行在回放里略去，实盘仍是 0%）；同日多笔保持文件顺序。
没碰任何 Crimson 代码。

## 怎么打开看
1. `cd ~/Projects/karios-desktop && pnpm dev`（后端要根目录 `.env` 里有 `DATABASE_URL`）。
2. 浏览器开 `http://localhost:3000`，点左侧「回测」，再点 tab「S-gap 失效趋势」。
3. 接口直查：`http://127.0.0.1:4330/api/backtest/sgap-decay`。
4. 重算冻结文件：`cd services/data-sync-service && PYTHONPATH=src python3 scripts/generate_sgap_decay.py`。
5. 静态预览图：`~/Projects/wealth-ideas/karios-audit-2026-10/sgap_decay_preview.png`
（同一 JSON 用 matplotlib 画的 7 张图英文版）。

## 从数据读到的关键数字
- 共 1130 笔，2021-08-09~2026-09-23，累计 +491.1 点，交易空间最大回撤 -24.9 点
（H2k 用 blotter 顺序是 -26.5，差 1.6 是同日多笔排序不同，月度数字完全一致）。
- 最新 40 笔：均值 -0.37 点/笔（合计 -14.8），胜率 30.0%，t -2.45，分位 2.2%，复活灯 0%（保持观察）。
- 滚动 60 笔最新：均值 -0.32，合计 -19.2。历史最差 40 窗合计 -16.0、60 窗 -20.4（都是历史最差，H2k blotter 口径 -15.4/-20.9，结论一样）。
- 月：2026-09 20 笔 -11.4（54 个月最差）、2026-08 36 笔 -9.2、2026-07 4 笔 +1.4、
2026-01 39 笔 +23.2；2026-07..09 三个月 -19.2（52 个 3 月窗最差）。
分窗合计 OOS2 +220.1 / train +62.5 / valid +3.1 / holdout -20.2，和 H2k 一致。
- 拥挤最新：成交中位 4.2 亿、市值中位 115 亿（历史 0.65 亿 / 34 亿，越挤越大）。
- 大单边缘最新约 +0.02（基本是零，2026 年高低组无差，和 H2f 的 t 0.78/0.17 一致）。

## profit-leads 规则执行
本轮没追加。tab 上全是衰减证据（最新为负、分位 2.2%、复活灯 0、大单边缘归零），
没有出现「每笔扣费后为正、或分位明显优于随机」的新指标；K2 灯是风控闸不是新 alpha，
H2k 已经判过不追加，这里延续不追加。

## 测试结果
- 后端新测试 `test_sgap_decay.py`：7 通过。
- 后端相关（sgap + backtest 路由 + combo_tiers）：72 通过。
- 后端大盘（非 DB）：4218 通过，1 个 `test_schema_parity` 失败——干净 HEAD 上也失败，
和本次改动无关（没碰 DB/schema 文件）；alembic 那几个也是 HEAD 上就坏。
- 共享层：93 通过（含新增 sgap 2 个 + last1430 1 个）。
- 前端相关（backtest 查询 + sgap 面板 + 回测页）：38 通过（含新增 6 个）。
- 前端全量：935 通过，2 个 watchlist（PortfolioHealthCard、TodayTodoCard）失败——
干净 HEAD 上也失败，和本次无关。
- 前端 typecheck：通过（修了之前缺的 SatelliteLast1430Response）。
- 前端 build：通过。

## 分支
`feat/sgap-decay-tab`，从 `refactor/portfolio-layer-step1`（59700298）建，
提交 e77228ce。没 push，没 merge。

KARIOS SGAP TAB DONE
