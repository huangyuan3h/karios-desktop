# 小资金敏捷轨（Small Agile Track）· 设计草稿

> **状态**：草稿 · 2026-08-30 提出 · **不碰** 现有 `S-3 + 择强单轨` 真值 · **2026-08-30 pivot：清零 score/RS 独立寻变量**
> **归属**：`todo §22` 外的独立探索轨，A 股专用，预算 ≤200万
> **入口**：本文件为唯一设计真值；回测计划见 `docs/backtests/small-agile-plan-2026-08.md`
> **拍板前不落地**：获用户确认后再改引擎/建表/上 paper

---

## 0. 为什么要新轨

现有策略 `docs/modules/strategy-params.md:8` + `docs/modules/pick-strong-track.md:1` 已验证 `OOS2 47.3 / train 34.1 / valid 38.7`，但本质是 **机构同场趋势跟随**：
- 宇宙 `全市场5226` + `min_avg_amount 0.7亿` 门槛 `strategy-params.md:34` 直接切掉机构嫌小的尾部
- `RS前50% + 主线Top3 + score≥65` 选的是机构必配的主升赛道，`exclude_boards 300` 排除创业板弹性
- 持有 `60天(主升45)` + `不止盈+8%回撤` = 让利润奔跑的机构尺度，未发挥“日级决策/时间可控/可100%空仓”的散户优势

**小资金优势（用户 2026-08-30）**：
1. 钱少（≤200万）→ 可控投入时间，机构的钱必须在场
2. 决策日级别 → 可等、可快切，不必持续暴露
3. 机构不会去的地方 → 低流动/小市值/非主线/低覆盖

新轨目标：**在机构容量禁区内，用日级择时换超额，而非用持仓时长换 beta**。

---

## 1. 设计约束（不可违背）

1. **隔离**：不改 `S3_CONFIG` / `pick-strong` / `walk_forward_baseline.json`；新引擎 `BacktestConfig scout_*` 独立命名空间
2. **预算硬约束**：单轨名义 ≤200万，全程 `cash≤1.0`，单票 `≤15%`，`max_positions 5-10`，滑点按低流动性加压 `0.15-0.25%` 单边
3. **日级 as-of 纪律**：所有信号以 `trade_date` 收盘后可见为准，禁止用 `stock_dailybasic.total_mv` 的未来值（需 `as_of` 口径）
4. **三窗铁律沿用**：`OOS2 2024-08-01~2025-08-01 / train 2025-08-01~2026-02-01 / valid 2026-03-01~2026-08-07` + `holdout 2026-08-08~` 只读；`>5pt劣化拒收`，`n<30` 降级为发现
5. **不做**：期货杠杆、外盘直连、高频分钟、融资融券空头

---

## 2. 假设分层（按机构痛点排序）

| # | 假设 | 机构为什么做不到 | 预期 alpha 来源 |
|---|------|------------------|-----------------|
| H1 | **流动性洼地溢价**：`0.2-0.7亿` 日均额区间存在未被定价的动量 | 容量/冲击/风控限 | 小市值流动性补偿 |
| H2 | **时间套利**：持仓 `3-10天` 的日级择时，机构持仓 `60天` 无法跟 | 必须在场/调仓慢 | 波段择时而非趋势持有 |
| H3 | **非主线/低覆盖**：非 Top3 行业、低研报覆盖票在主线轮动间隙补涨 | 只能配主线 | 轮动间隙 |
| H4 | **拥挤度反向**：避开 `RS Top10%` 最拥挤段，做 `RS 50-80%` 的次强 | 抱团 | 拥挤回撤规避 |

> **2026-08-30 pivot**：H1/H2 在 `score/RS` 下 `valid -0.1~-7.4` 全拒收（见 `small-agile-plan §2 R1/R2`），**清零 score/RS**，转 `R0-IC` 独立因子 IC 筛选（`amplitude/turnover_spike` 等），首轮结果见 `small-agile-plan §2 R0-IC`。

---

## 3. 数据盘点（现成可用）

- `daily.amount`（亿元级成交额，`services/data-sync-service/src/data_sync_service/db/daily.py:1`）→ `avg_amount_60` 自算
- `stock_dailybasic.total_mv/circ_mv`（`services/data-sync-service/src/data_sync_service/db/stock_dailybasic.py:1`，tushare `daily_basic`）→ 市值分层，已有 `BacktestConfig min_mv/max_mv` 钩子
- `trade_calendar` + `daily` 日历 → 持有期按交易日计 `backtest_engine.py:_calendar_days_between`
- `watchlist_score_daily` 仍可复用，但 Scout 允许 **不依赖 score**（避免与 S-3 同质）
- 缺口：`研报覆盖度/股东户数` 需评估后补，非首轮阻塞项

---

## 4. 实验设计（pivot 后）

### R0-IC 独立因子 IC 筛选（现行首轮 · 2026-08-30 已跑）

- 宇宙：`20-80亿`（`stock_dailybasic.total_mv/10000`）；因子：`turnover_spike/amplitude/gap/ret1/ret5/dist_high5/down_cnt/neg_mv`（全 `daily` 自算，无 score/RS）；前瞻 `5d/10d`；脚本 `scripts/scout_factor_ic.py:1`
- 判定：`IC>0.03 + IR>0.5 + 五分组单调 + 三窗同号`；首轮 `amplitude -0.10/IR-1.09` 与 `turnover_spike -0.08/-0.81` 三窗同负最稳（`data/backtest_reports/scout_factor_ic_latest.json`），`neg_mv/ret5` 衰减已排除
- 下一步：`amplitude + turnover_spike` 低波组合的 **极简回放**（`next_open +0.15%`，`Q1低振幅` 多头，`max_hold 5/10`），验证 `valid>0`

### R0/R1/R2（旧 · score/RS 锚定 · 已作废）

> 2026-08-30 前在 `score65/RS0.5` 下 `20-80×hold5-60` 三窗 `valid -0.1~-7.4` 全拒收（`small-agile-plan §2 R1/R2`），与 `P17 LIQ` 环境依赖同根，已封存不再引用。

---

## 5. 验收与风险

- **通过**：三窗 `0劣化` + 任一窗 `+5pt` 提升 + `holdout` 不跌 + `n≥30`；优先 `Calmar` 而非绝对收益
- **拒收**：单窗好看/其他窗崩、或长窗 `DD>60`、或 `amount<0.2亿` 导致 `n<20`（容量不足）
- **风险**：低流动性尾部在 2024 弱市年可能假好看（P17已现 `LIQ OOS2+44/train-28` 环境依赖）；需与 `regime` 分层对照

---

## 6. 落地路径（拍板后）

1. 本文件定稿 → 新建 `docs/backtests/small-agile-plan-2026-08.md` 实验记录页
2. 加脚本 `scripts/run_walk_forward_scout.py`（薄封装 `backtest_engine.BacktestData/BacktestConfig`，scout 预设）
3. R0 数据刻画脚本 `scripts/scout_universe_profile.py`（只读 `daily + stock_dailybasic`，不改引擎）
4. R1/R2 三窗跑分，结果回写 `small-agile-plan` + `data/backtest_reports/scout_*.json`
5. 通过项才进 `paper` 小资金影子池（≤20万），≥20笔后再谈是否并入择强

---

*起草 2026-08-30 · 下一步：建 `small-agile-plan` 实验页 + 跑 R0 刻画*
