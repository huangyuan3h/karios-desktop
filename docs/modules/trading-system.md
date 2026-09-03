# 交易系统逻辑真值（重建手册 · S-3 定案）

> **目的**：本文件自包含描述当前交易系统的完整逻辑——数据、信号、闸门、仓位、退出、
> 操作流程与验证证据。**即使系统全部丢失，凭本文 + 数据源即可重建。**
> 回测引擎实现见 `backtest_engine.py`（OPT-070/071/072/073）；策略演进见
> `docs/modules/backtest-strategy.md`（2026-08-09 归档，现行参数真值见 `strategy-params.md`）；作战计划见 `docs/todo.md §19`。

---

## 0. 系统总览

```
数据层 ──► 信号层 ──► 闸门层 ──► 执行层 ──► 复盘层
TV快照     score       regime       仓位模型      回测引擎
K线        RS排名      资金流        移动止损      评估框架
指数       行业归属    mainline      paper对照      walk-forward
资金流                 sentiment
情绪
```

- 市场：CN（A股）为主；HK/ETF 部分支持（符号前缀 CN:/HK:/ETF:）
- 周期：日线级别（收盘信号，非盘中）
- 风格：**趋势跟随**——低门槛上车、长持有、移动止损保护利润、弱市空仓

---

## 1. 数据层（依赖与起点）

| 数据 | 表 | 起点 | 回填方式 |
|------|----|------|---------|
| 个股日 K 线 | `daily`（PK ts_code, trade_date） | 1998-06 | tushare 全量 |
| TrendOK 分数 | `watchlist_score_daily`（PK symbol, trade_date） | **2025-08-01** | `scripts/backfill_watchlist_scores.py`（复用 `_trendok_one` 纯函数 as-of 回算；universe=TV 快照历史选中 ∪ score 历史 ∪ registry） |
| 指数 K 线 | `index_daily` | 2023-01 | `service/index_daily.py`（INDEX_CODES 含上证/创业板/沪深300/中证500/**科创50**） |
| 板块资金流 | `market_cn_industry_fund_flow_daily` | 2025-12-15 | eastmoney SW L1（31 行业） |
| 市场情绪 | `market_cn_sentiment_daily` | 2026-01-05 | 盘后 cron |
| 行业归属 | `stock_eastmoney_industry` | 全量 | EM 板块映射 |
| TV 候选池 | `tv_screener_snapshots` | 2025-12-21 | TV screener 每日 AM/PM 抓取（已剥离，仅历史） |
| 黄金/原油/美股ETF | `daily` `518880/513350/513100` | 2023-01 | `fund_daily` `backfill_target_etfs.py`（`518880 881b/513350 662b/513100 880b`） |
| 债券ETF | `daily` `511260/511010` | 2023-01 | `fund_daily` `511260 881b` |
| 美债/中债收益率 | `macro_daily` `US10Y 8.9k/CN10Y 6.1k/CN30Y/US30Y/VIX 2.5k` | 1990/2002/2016 | `akshare bond_zh_us_rate` + `yfinance VIX` `backfill_yields_ak.py` |

**as-of 纪律（不可违反）**：所有信号计算必须只用截至决策日的数据——
分数/红绿灯/资金流/RS/情绪全部 as-of 重算，禁止用未来数据（回测引擎强制）。

---

## 2. 信号层

### 2.1 score（TrendOK 分数 · 0-100）

- 计算：`service/trendok.py _trendok_one`（纯函数）+ `_compute_watchlist_score_v4`
- 因子：EMA5/20/60 排列、MACD(12,26,9)、RSI14、20 日新高、量能比、行业资金流加减分
- **语义警告**：score 是"买点评分"（偏爱低位刚启动），不是"趋势质量评分"——
  涨了 3 倍的高位强票 score 反而低（中际旭创一年 +337% 但 ≥85 仅 1 天）
- 用法：**趋势入场用 ≥65**（65 分窗口=主升段），不使用 85+ 追高

### 2.2 RS 相对强度（全市场排名）

- 计算：20 日收益（`daily` 窗口函数 `lag(close,20)`），按当日全市场有行情票排名，
  百分位 0-1（最强=1.0）
- 服务：`watchlist_automation.compute_rs_ranks`（按日缓存）；API `GET /watchlist/rs-ranks`
- 用法：**只买前 50%**（排除全市场最弱一半；walk-forward 证明 0.7/0.8 过拟合、0.5 稳健）

### 2.3 指数红绿灯 regime

- 计算：`market_regime.get_index_signals(as_of_date)` + `execution_gate.classify_market_regime`
- 规则：三指数（上证/创业板/中证500）全绿=**Strong**、部分绿=**Diverging**、全不绿=**Weak**
- API `GET /market/regime`（与回测同码）；as-of 模式跳过 HK 网络拉取（防前视）

### 2.4 行业资金流 / mainline

- 数据：`market_cn_industry_fund_flow_daily`（SW L1 31 行业净流入）
- **sectorOutflowBlock**：当日所有行业净流入都 ≤0 才挡（不是合计口径）
- **mainline 白名单**：5D 净流入 Top3 ∪ 动量突破（当日净流入 ≥20 亿 且 排名升 ≥10）
- 匹配：个股 EM 行业名 vs SW L1 名直接匹配（现状；名空间错配是已知局限）

### 2.5 市场情绪（恐慌保护）

- 数据：`market_cn_sentiment_daily.risk_mode`
- **挡开仓**：`risk_mode ∈ {no_new_positions, extreme_caution}`（与 live `_RISK_DEFEND` 同码）

---

## 3. 入场规则（S-3 · 全部满足才买）

| # | 条件 | 来源 | 备注 |
|---|------|------|------|
| 1 | **regime ≠ Weak**（Strong/Diverging 可开） | 2.3 | Weak 空仓（纪律核心） |
| 2 | **score ≥ 65** | 2.1 | 趋势入口 |
| 3 | **RS 排名 ≥ 前 50%** | 2.2 | 全市场相对强度 |
| 4 | **行业资金流不枯竭**（至少一行业正流入） | 2.4 | sectorOutflowBlock |
| 5 | **行业 ∈ mainline 白名单**（5D Top3 ∪ 动量突破） | 2.4 | 主线纪律 |
| 6 | **sentiment 无恐慌**（risk_mode 非 no_new_positions/extreme_caution） | 2.5 | 与 live 同码 |
| 7 | **恐慌冷却**：距最近恐慌日 ≥3 个交易日 | 2.5 | 恐慌后头 3 天买入是陷阱（验证窗 +7.8pt） |
| 8 | 未持有该票 且 持仓数未满上限 | — | 单票一次 |

数据缺失时的行为（按历史能力复刻）：regime 缺失=挡；资金流/mainline 数据缺失（2025-12-15 前）=**降级放行**；sentiment 缺失（2026-01-05 前）=**降级放行**；RS 缺失=挡（fail-closed）。

---

## 4. 持仓与退出规则

| 规则 | 值 | 说明 |
|------|-----|------|
| 固定止损 | **-5%**（净口径） | 硬保护 |
| 移动止损 | **-8%**（峰值回撤） | 利润保护（主退出机制） |
| 止盈 target | **+100%**（不主动止盈） | 让利润奔跑 |
| score_floor | **0（关闭）** | 评分回落≠趋势结束，不平仓 |
| 最大持有 | **60 天** | 到期强制平仓 |
| 滑点 | **0.05% 单边**（诚实口径） | 双边共 0.1%；0.1%+ 单边会吃掉 -31pt（收益对成本敏感） |
| 停牌/无报价 | 顺延持有 | 有报价后继续判定 |

退出优先级：固定止损/移动止损/target > 到期 > 窗口结束（回测专属）。

---

## 5. 仓位模型（2026-08-23 mp10 固化）

| 参数 | 回测口径 | paper/实盘口径 |
|------|---------|-------------|
| 单笔仓位 | 10% | **10%**（`S3_POSITION_PCT=0.10` paper与回测同口径，用户拍板 2026-08-11） |
| 同时持仓 | 10 笔（`mp10` `walk_forward_baseline 40ef4cd0` `OOS2 43.1/train 35.6/valid 43.3`） | **≤10 笔** |
| 名义上限 | 10%×10=100%（`cash≤1.0` 恰满，夏普恒定） | 同 |
| 单票上限 | — | 15%（红线） |
| 板块集中度 | — | 30%（红线） |

回测累计收益 = Σ(单笔 pnl × 仓位)；`position_pct 0.05→0.10` 已统一，paper 实绩可直接对照回测数字。金字塔加仓 `trigger 2.5%/0.5×/1次` 同步。

---

## 6. 操作流程

### 6.0 Copy Markdown = S-3 回测口径（2026-08-09 上线）

- 每次「Copy All / Watchlist Copy」输出顶部新增 **`## S-3 回测口径买入候选`** 区块：
  - 选股 = S-3 全条件（score≥65 · RS 前 50% · regime 非 Weak · 主线白名单 · 非持仓 · CN only）
  - 仓位 = **回测口径 10%/笔**（受 sleeve 上限约束，逐个累减）
  - 无候选时输出「空仓等待（纪律）」
- 实现：`execution-markdown.ts buildS3Candidates`（Dashboard 与 Watchlist 两处 Copy 共用）；
  RS 数据在导出时实时拉 `/watchlist/rs-ranks`

### 6.1 手动操作（当前模式 · L2 可视化已上线）

1. 打开 Watchlist 页 → 看顶部 **S-2/S-3 操作口径行**：非 Weak = ✅ 可开仓
2. 找 **score ≥ 65** 且 **RS 徽标"前X%"为绿色**（≥50%）的票
3. 确认行业在主线（mainline 标记）→ 买入（5-10% 仓位）
4. 持有：**移动止损 -8%**（涨上去的利润回撤 8% 就走）/ 固定 -5% / 60 天
5. 不主动止盈、不因分数回落卖

### 6.2 回测页复现（S-3 参数）

```
start=2025-08-01 · end=今天
score=65 · max_hold=60 · 止损=-5 · 移动止损=-8 · target=100（用 50 近似）
RS 过滤=0.5 · Diverging 仓位=1.0 · 仓位 10% · 持仓上限 20 · gates=full
恐慌冷却=3 天 · 滑点=0.05（回测页新增参数）
```

### 6.3 paper 实盘（已固化 2026-08-21 · 三窗 + past_year 验证）

- **paper 书**：`paper_trades` 表（`source S3/S3HK` 股票篮；`source=twin_star` 机会双子星卫星 4 槽 × 12.5%，body=3 收盘卖、无 −5%，`cron 17:43 paper_twin_star`，S-3 update 跳过这些行；`sleeve_pct` 闲置套筒，`CLOSE_REASON_SLEEVE_EXIT`），`cron 17:42 paper_s3_intake CN+HK` + `17:45 update` + `18:20 sleeve_paper_auto`（`service/sleeve_paper_auto.py`）
- **闲置套筒**：`T6` 单纳指 `513100>MA200` 三窗 `OOS2+3.9/train+15.3/valid+21.8/past+51.1`；**多资产轮动** `GOLD/OIL/NASDAQ/BOND10 mom60>0 top2 Nasdaq-first` 三窗 `OOS2+19.3/train+17.9/valid+14.4/past+38.1` 全过（`service/multi_asset_sleeve.py:52`），`portfolio_health multiAssetSleeve` + `GET /commodities/sleeve` 已上线，`watchlist` 与 `paper` 同码
- **脉冲高置信** `OIL RSI<25 90% n30 +3.92%/10d`（`commodity_pattern_scan.py`）`valid +28.5` 三窗全过，已进 `impulseSleeve`（`2×` 杠杆），`NASDAQ RSI>75 78%` 等 `R1-R5` 按 `todo §22.7` 分批固化
- **对账**：`C4 paper_vs_backtest_report.py` + `BehavioAudit` + `BacktestPage Timeline`（`GET /api/backtest/timeline?start=2025-08-01` 日级 `pick/navBase/navMulti/deployedPct` 分布，`TimelineCard` 色条 `GOLD/OIL/NASDAQ/BOND`），`paper ≥20 笔` 后出统计定论

**文档真值**：`strategy-params.md §1` 参数表 + `service/paper_trading.py:60` + `service/paper_s3.py` + `service/paper_twin_star.py` + `service/sleeve_paper_auto.py` + `service/multi_asset_sleeve.py` + `service/commodity_signals.py`

---

## 7. 回测验证状态（防过拟合纪律）

**方法论**：训练窗 2025-08-01~2026-02-28 调参 → 验证窗 2026-03-01~2026-08-07 一次性确认；
参数必须落在"业务自然截断"；样本 ≥100 笔/年才采信；淘汰方案存档不删除。

### 7.1 S-3 最终数字

| 窗口 | 年化 | 胜率 | 均净% | 夏普 | maxDD% | 交易 | 超额 vs 最强基准 |
|------|------|------|-------|------|--------|------|------------------|
| 训练窗 | 154.1 | 45 | 6.95 | 5.47 | 6.2 | 128 | +78.7 |
| 验证窗 | **96.6** | 48 | 8.77 | 6.81 | 7.8 | 48 | **+52.9** |
| 全年（合并） | **121.7** | 46 | 7.54 | 5.43 | 9.9 | 164 | **+54.5** |

（含全部保护：sentiment 挡 + 恐慌冷却 3 天；滑点 0.05% 单边诚实口径；无滑点时 123.4%）

基准（全年）：上证 +10.5% / 沪深300 +15.5% / 中证500 +28.0% / 创业板 +52.6% / **科创50 +67.2%**。

### 7.2 关键结论清单

1. **score 85 追高是最大错误**——趋势用 65；score 是买点分不是趋势分
2. **止盈是利润杀手**——target 10→50→100 单调提升（双窗一致）；score_floor 30→0 同理
3. **Diverging 开仓**（非 Weak 就参与）是最大单项贡献（49.5→97.4）
4. **RS 前 50%** 是唯一稳健阈值（0.7/0.8 验证窗劣化=过拟合）
5. **sentiment 恐慌保护**与 live 同码、训练窗零代价、验证窗 +9.3pt
6. **组合回撤熔断是负优化**（趋势小亏常态，熔断无法区分危机）——已弃用
7. **闸门价值**：行业资金流+mainline 把胜率从 43% 抬到 63%（2025-12-15 后段）
8. **策略是 beta 依赖型**：趋势段（Q2）+62% 超额、回调段（Q3）-22%——不是全天候策略

### 7.3 失效条件（触发即重新评估）

- 验证窗样本积累 ≥100 后：胜率 <35% 或年化 <0
- paper/实盘对照偏离回测 >50%
- 基准结构变化（如科创50 年化 <20% 时目标线重新校准）

---

## 8. 重建清单（系统丢失时）

1. 建表：`alembic upgrade head`（schema_baseline.py 全量 DDL）
2. 数据同步：tushare daily（1998 起）/ index_daily（含科创50）/ fund flow / sentiment
3. 回填 score：`scripts/backfill_watchlist_scores.py --start 2025-08-01`
4. 回测引擎：`service/backtest_engine.py`（S-3 参数见 §6.2）
5. Watchlist 可视化：RS 徽标（`/watchlist/rs-ranks`）+ regime 提示（`/market/regime`）
6. 验证：重跑 §7.1 数字误差 <10% 即重建成功
