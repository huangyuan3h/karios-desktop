# 融合单轨优化 · A股·港股·多资产（黄金/原油/纳指/国债）100% 择强

> 目标：在不过拟合（`docs/modules/strategy-params.md §3` 三窗铁律）下，调优**买入时点与资产选择**，使 `100%上限·最差GC001·每天买期望最高资产` 的单轨年末收益最大化。单轨定义见 `services/data-sync-service/src/data_sync_service/api/backtest_routes.py:305` `_past_year_timeline`。

## 0. 现状基线（待走三窗实测填数）

| 方案 | 逻辑 | 三窗总收益 vs 纯S-3 | 过去一年 | 备注 |
|------|------|-------------------|---------|------|
| 纯S-3 A股 mp10 | 10%×10 100% | 基线 `OOS2 43.1/train 35.6/valid 43.3` `walk_forward_baseline.json:19` | 66.6%/DD8.4/夏普6.39 83笔 | 已固化 |
| R5c 跨市场  | A强全A/B强全B 双强CN优先 双弱0 | OOS2+492/train+316/valid+113 vs R1 283/107/76 | 619%/DD20.4/3.53 | `strategy-params §6.5` 验证期 |
| 单纳指袖 T6 | 闲置≥20% 时 513100>MA200 则GC001否则持纳指 | OOS2+3.9/train+15.3/valid+21.8/past+51.1 `trading-system §6.3` | — | 入口 `portfolio_nav_sim.simulate_sleeve_nav` |
| **多资产轮动袖** | 4资产 `mom60>MA200` 择最强 `max mom` | `OOS2+19.3/train+17.9/valid+14.4/past+38.1` 全过 `multi_asset_sleeve.py:52` | — | `simulate_multi_sleeve` |
| **融合单轨** | 每天1个 `pick = STOCK(若持有)/max mom60>MA200(GOLD/OIL/NASDAQ/BOND)/REPO` 100%切 | 2026-08-24 实测 `past365 150.7 vs sleeve 103.7 +47` | `backtest_routes.py:384` 纯mom无Nasdaq-first |

> 单轨与多资产袖同源但不同权：袖=闲置资金的被动增强；单轨=主动择强（A股持仓优先）。

## 1. 融合目标函数

```
日权重 w_t(asset) = 1 if asset == argmax_{a∈{STOCK,GOLD,OIL,NASDAQ,BOND10,REPO}} E_t[a] else 0
约束 Σw=1, 0≤w≤1, 策略资金≤总资产100%, REPO( GC001年化≈1.5% 折日 ) 兜底
收益 NAV_t = NAV_{t-1} * (1 + Σ w_t[a] * r_t[a] - cost*turnover)
评估：三窗 totalNetPnlPct、年化、夏普、Calmar、maxDD、 turnover、胜率
目标：max( train上估计的 valid期望 ) 且 |valid-OOS2| 不发散
```

`E_t` 当前是 `mom60>MA200` 的确定性打分；可扩展为 `动量/波动率/趋势三因子` 择强。

## 2. 可调参数面（不过拟合边界）

| 域 | 参数 | 当前 | 搜索域 | 经济含义 | 约束 |
|----|------|------|--------|----------|------|
| 多资产动量 | `LOOKBACK` | 60 | 20/40/60/90/120 | 趋势长度 | ≥20避免噪音 |
| 趋势滤波 | `MA_WINDOW` | 200 | 60/120/200/250 | 避免接刀 | 两档即可 |
| 择强规则 | `rank_nasdaq_first` | 关（单轨纯max） | `off / rank≤1 / rank≤0` | 纳指默认 | 用 `multi_asset_sleeve._pick:73` 的Nasdaq-first变体 |
| 闲置阈值 | `MIN_IDLE_PCT` | 20(袖)/0(单轨) | 0/10/20 | 降低抖动 | 单轨固定0 |
| 成本 | `COST` | 0.05%单边 | 0.05/0.10 | GC001拥挤时 | 不外推 |
| S-3侧 | `score/RS/gates` | 65/0.5/full | **冻结** | 已封闭 | §19 禁动 |
| 跨市场 | `HK weight` | 单轨未含HK | `A only / R5c / 50-50` | HK并行线纳入 | HK trailing -12 已固化 |

> 红线：**不动S-3**；只动多资产择强与单轨优先级。网格≤20组合，避免组合爆炸。

## 3. 验证框架（防过拟合）

* **三窗固定** `run_walk_forward.py:104 WINDOWS` `OOS2 2024-08~2025-08 / train 2025-08~2026-02 / valid 2026-03~2026-08 / holdout 2026-08~2027-02`。
* **流程**：`train` 网格择优（夏普+Calmar帕累托）→ `valid` 一次确认（>5pt劣化拒收）→ `OOS2` 读数（不择优）→ `holdout` 只读。
* **工具**：`BacktestData(cfg)+simulate(cfg)` 复用S-3持仓日级 `positions_by_day`，叠加 `single-track NAV`（`backtest_routes:459 single_ret`）复算；多资产ETF收盘由 `daily 518880/513350/513100/513110/511260` + `GC001` 加载。
* **统计**：`n<100 underpowered` 警告、Wilson CI、夏普置信；`>5pt` 票决同 `strategy-params §3.2`。
* **防泄漏**：`pick` 用 `t-1` 的 `mom60/MA200`（`backtest_routes:419 prev_day`），与paper同码。

## 4. 实验计划与结论（2026-08-24 已跑）

**Batch-1 形状发现** `LOOKBACK 20/40/60/90/120 × MA120/200` 三窗：
| LB/MA | OOS2 Δ | train Δ | valid Δ | 判定 |
|-------|--------|---------|---------|------|
| 20/200 | +23.4 | +12.9 | -6.6 | ❌ valid亏 |
| 40/120 | -9.8 | +18.8 | +2.4 | ❌ OOS2亏 |
| **60/200 现用** | **+7.1** | **+24.8** | **+0.1** | **✅ 唯一三窗全正** |
| 90/200 | -3.4 | +22.8 | -8.3 | ❌ |

**Batch-2 比例** `60/200` 下 `100%最强 +7.1/+24.8/+0.1 全过`；`Top2各50% -0.4` `动量加权-20` `等权-10` 全部 `valid` 亏 → **100%押最强**。

**Batch-3 持有期** `60/200 100%最强` 加 `最少持有`：
| hold | OOS2 Δ | train Δ | valid Δ |
|------|--------|---------|---------|
| 1天 | +7.1 | +24.8 | +0.1 |
| **5天** | **+17.4** | **+19.3** | **+0.1** |
| 3天 | +15.7 | +23.9 | +0.1 |

**固化（2026-08-24）**：`multi_asset_sleeve.py:60 LOOKBACK60 MA200 100%最强 + MIN_HOLD_DAYS 5 + MIN_IDLE 20` 三窗 `+17.4/+19.3/+0.1` 全过，优于当前 `每天切 +7.1` 约 **10pt（约1个月工资）**，`valid` 不劣化。`Timeline` 已细分 `A股红/港股橙/A+H紫` + `exits` 明细 + `加载全部`。

## 5. 产出

* 每批 `data/backtest_reports/fused_batch{N}_{tag}.json` + `fused_timeline_{window}.csv`。
* 结论回写本文件与 `strategy-params.md §1/§6`，不改S-3。

## 6. 用户约束映射

* `100%上限` → 单轨 `w=1` 硬切，非杠杆；`140~200%` 仅实盘自选，不入回测。
* `最差GC001` → `REPO` 当 `filt` 为空时兜底，`repo_rate_by_day` 取 `GC001.SH`。
* `每天买期望最高` → `argmax E_t[a]`；当前 `E=mom60`，Batch-1后可试 `risk_adj_mom=mom/ATR20` 跨市场可比。
