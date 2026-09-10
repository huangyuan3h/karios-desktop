# OPT-152 · Timeline 实盘口径主曲线（A 方案 · 2026-09-09）

> **一句话**：双子星 Timeline 的 190% 之谜 = `navSingle` 基准复利线按"100% 资金押当日择强"
> 计，不看 S-3 实际仓位。2026-06-08/09 建滔集团（00148.HK）两天 +15.45%/+25.77% 被全额放大
> （实际 deployedPct=10%），随后三个月无大阳线、复利线从虚高峰值回落（204.5→192.6）。
> 修法（用户拍板"我需要真实的"）：**实盘口径曲线为主曲线，100% 押注基准降级灰虚线对照**。

## 交付

| 件 | 文件 |
|----|------|
| builder | `service/pick_strong_track.py` `build_twin_star_timeline` 加 sim 参数（CN/HK nav_curve + 各自日历）→ rows 新增 `navSim`/`navSimMulti` 系列；STOCK 日核心收益 = S-3 实际权益（CN+HK 50/50 日再平衡联合，缺市日 forward-fill），ETF/REPO 日沿用基准路径（择强 100% 硬切即真实行为）；卫星占用日 50/50 切仓混合同现有公式；无 sim 时字段 None（向后兼容） |
| API | `api/backtest_routes.py` twin_star 分支传入 `run.nav_curve` / `run_hk.nav_curve`（引擎 `BacktestRun.nav_curve` 已存在，零引擎改动） |
| 前端 | `lib/twin-star-nav-series.ts`（`twinSimPct`/`coreSimPct` + `hasSimCurve`）+ `TwinStarNavOverlay.tsx`（主曲线 = 实盘口径双子星/核心，基准 = 灰虚线 + 图例%，说明文案切换）+ `queries/backtest.ts` 类型 |
| 测试 | `tests/test_twin_star.py` `TestSimProductCurve` 5 测（STOCK 用 sim / ETF 用基准 / 占用日混合 / 缺市日 forward-fill / 无 sim 回退）；`twin-star-nav-series.test.ts` 2 新测 |

## 数字（past_year 2025-09-09~2026-09-09 · 习惯 C1+14:30 Live 配方）

| 曲线 | 窗口累计 | 说明 |
|---|---|---|
| 双子星（实盘口径 navSimMulti） | **+96.8%** / DD 13.0 | 主曲线（重启后图上所见） |
| 核心（实盘口径 navSim） | **+98.55%** / DD 14.8 | S-3 实际权益（CN+HK 50/50 联合） |
| 卫星 | +45.4% | 不变 |
| 双子星（100% 押注基准，灰虚线） | +128.9% / DD 18.5 | 注意：T+2 进 HK 冻结口径后，HK 持仓路径变了，基准线自身从 192.6 → 128.9 |
| 建滔两天对照 | 基准 +32.2pt vs **实盘 +6.7pt** | 10% 仓位的真实贡献（当初的"一大截"） |

## 语义与边界

- **零策略改动**：择强/卫星信号、冻结报告（`opportunity_twin_star_v3_clip4_frozen.json` +194.9 等）
  语义不变——那些是"100% 押注基准"口径的历史定数；图上新增的是实盘口径并行曲线。
- 单轨 timeline（`strategy=pick_strong`）的 navSingle 基准语义保留不动（对照用途）。
- CN/HK 联合 = 50/50 日再平衡（本 OPT 声明的显示约定；live 双账户各半名义，若实际池子不同再调）。
- 双子星资金结构（用户拍板确认）：卫星占用日核心每票 = 10%×0.5 = **5% NAV**；闲置日核心用满
  ——机会模式冻结定案；静态 50/50 已 REJECT（state-bucket-algo §3.0，−70~−88pt）不重开。

## 验收

- 后端：`pytest` 全量 4216 passed（4 失败 = HEAD 既有：alembic ×2 / HK costs / webhook E7）；
  DB 基线 check OK（18:30 cron 写入的 market_top_inst_summary 属 live 调度，非测试污染）。
- 前端：tsc 0 错，vitest 883 passed。
- 端到端：进程内重建 twin timeline，数字如上表；**后端进程需重启**才能生效（timeline 无文件缓存，
  重启后首访会重建，约数十秒）。
