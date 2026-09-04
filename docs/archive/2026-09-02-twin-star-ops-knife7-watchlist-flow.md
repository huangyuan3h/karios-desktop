# 机会双子星第 7 刀：Watchlist 日流程 + sat/S-3 拆账  · 归档于 2026-09-02

## 当时的目标（todo 链接）
- `docs/todo.md` P0-0 刀 7
- `docs/designs/twin-star-ops-phase-2026-09-02.md` B2/B3/B5 + A5
- OPT-135

## 实际做了什么
- pick≠STOCK：所有 CN 持仓默认进卫星仓，A 股线不再画「股票篮应轮出」
- pick=STOCK：recipe/候选集合 = 卫星（body3 / −5%）；剩余 CN = S-3 篮（移动/金字塔只贴剩余）
- Watchlist Health 增加「今日顺序」：14:20 提醒 → 14:30 名单 → ①核心 ②卖卫星 ③缺口买
- S-3「缺 19 只」不再出现在双子星交易面；占用对照标成「不是交易铃」。未重写 `paper_vs_backtest_report.py`（S-3 统计 C4 仍等 20 笔）

## 验证 / 数据
- `PortfolioHealthCard.test.tsx` OIL 全卫星 + STOCK 日拆账 + 隐藏 recon
- `twin-star-trade-plan.test.ts` day flow / `isLiveSatelliteStock`
- `test_notifications.py::test_twin_star_stock_day_splits_sat_from_s3_leftover`

## 后续影响 / 留给谁
- 运营观察：任意交易日三句验收（核心该持什么、卫星 4 槽有谁、谁因涨停没买）
- 并行不抢：OPT-124 tushare 多 token
- 双子星逐笔统计 C4 仍等 paper `source=twin_star` 样本
