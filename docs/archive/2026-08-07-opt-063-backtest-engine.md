# OPT-063：回测引擎 v0 —— 信号回放 + live 平仓逻辑同口径（todo §16 L3-P2 / §8 回测）

> **完成日期**：2026-08-07
> **目标**：L3-P2「回测引擎」——与 live 同口径的参数敏感度工具。数据深度实测：watchlist_score_daily（系统实际打的 TrendOK 分）2026-06-18 起、tv_screener_snapshots 2025-12-21 起、daily bars 1998 起。

## 架构决策

**信号回放（signal replay）而非全因子重算**：TrendOK 分用「系统当时实际记录」的 `watchlist_score_daily`，不存在重写规则的问题；平仓逻辑 100% 复用 live 的 `_pick_close_reason`。

```
watchlist_score_daily（当时实际 TrendOK 分）→ score >= threshold → 收盘建仓
      → 每日 mark-to-market → _pick_close_reason（LIVE 同码）→ 平仓（净口径）
```

## 同口径铁律的落地

| 复用点 | 方式 |
|--------|------|
| 平仓条件 | `_pick_close_reason` 直接复用；新增 `score` + 阈值 override 参数（回测注入 as-of 历史分 + config 阈值；live 传 None 行为字节级不变） |
| 成本 | `paper_cost_model.round_trip_cost_pct`（与 paper v0.2 同一模型，净口径） |
| 持仓天数 | `_calendar_days_between`（与 paper `_holding_days_for` 同语义） |

## 前视偏差防护（明确清单）

- score 一律 as-of 注入（当日记录值）；缺失 → score_floor fail-open
- `pool_exit` 无 registry 历史 → v0 关闭（fail-open 语义）
- 机构席位 / ETF 资金流 / 主线 SRV 无历史 → 不参与（已隐含在得分中）
- 窗口末尾强制平仓用 `end_of_window`（引擎专用 reason，live 永不产生）

## 参数敏感度（v0 网格 4×3×3=36 组合）

score_threshold ∈ {70,80,85,90} × max_hold ∈ {5,10,20} × stop ∈ {-3,-5,-8}。

**2026-06-18..2026-08-07 实测（仅敏感度参考，样本小）**：
- 全部 36 组合净期望为负（-0.5% ~ -2.7%）——该窗口纯 score 信号无 edge
- stop -8 普遍优于 -3（-3 时 90 分档最差 -2.71%）
- 90 分阈值反而最差（或反映高分信号追高段）
- **这正是敏感度分析的目的：回测不撒谎，结果为「再校准阈值」提供依据（不作为发布依据）**

## 交付物

| 件 | 说明 |
|----|------|
| `service/backtest_engine.py` | BacktestConfig / BacktestData（一次加载共享网格）/ simulate / run_sensitivity / 默认网格 |
| `db/daily.py::fetch_ohlcv_batch_between` | 窗口批量 OHLCV（引擎专用，同 tuple 形状） |
| `api/backtest_routes.py` | `GET /api/backtest/run`（单配置）+ `GET /api/backtest/sensitivity`（网格） |
| `scripts/run_backtest.py` | CLI：单配置 / --grid；输出 JSON 报告到 `data/backtest_reports/latest.json` + markdown 表格 |
| 测试 | `tests/test_backtest_engine.py` 13 个（入场阈值/净触发/score_floor as-of/停牌存活/max_hold/分桶/API） |

## 验证

- 后端 1365 passed / 2 skipped（唯一失败为既有 flaky，与本次无关）
- 真实数据端到端：单配置 21 笔交易、网格 36 组合 ~30s

## 反模式确认（未做）

- ❌ 未重写 TrendOK/规则（铁律）
- ❌ 未做参数寻优（只敏感度对比；发布依据仍以 paper 实绩为准）
- ❌ 未做 BacktestPage UI（§8 P2 / §12 #12 单独排期，等引擎数字稳定）
- ❌ 未做 HK/ETF 回放（v0 只 CN）
- ❌ 未做 TV 池回撤窗口过滤（v0.2：tv_screener_snapshots 2025-12 起可做 7.5 个月窗口）

## 下一步（L3-P2 v0.2 候选）

1. TV 池 + 回撤区间 [-15%,-5%] 过滤（宇宙从 score_daily 扩展到 2025-12）
2. 月度滚动窗口对比（市场环境变化）
3. BacktestPage UI（§8 P2）
