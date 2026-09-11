# 习惯卫星时钟统一到 14:30：诚实排序不算前视（2026-09-11 拍板）

> **一句话**：Live / 回测 / 审计此前是**三个不同的决策时点**（冻结引擎 T-1 收盘→T 开盘；
> 习惯回测 14:30 成交但按**全天振幅**排序；Live 14:20 快照按**当时振幅**排序），所以对账永远对不上。
> 把回测审计的排序改成 **14:30 可得振幅**（`bar_5min` 里 `trade_time<=14:30` 的 max high−min low / 1430 价，
> 零前视）后，三窗不降反升，**统一到 14:30 买+卖**。
> **关键词**：14:30 时钟 · rank amp_1430 · 零前视 · 审计对账

## 1. 问题：同一策略三个决策点

| 链路 | 决策时刻 | 排序依据 | 能否在 14:30 执行 |
|---|---|---|---|
| 冻结引擎 | T-1 收盘信号 → T 开盘买 | T-1 全天振幅（收盘已知） | 否（要开盘买） |
| 习惯回测/审计（旧） | T 14:30 成交 | **全天振幅**（15:00 才知道） | 否（前视） |
| Live 实盘（旧） | T 14:20 快照 → 14:30 买 | 快照到此刻的振幅 | 是 |

证据（2026-09-09）：`webhook_events.twin_star_reminder` 14:20 推送的买入名单
`300308/301046/300408/688035` = 实盘成交；而 behavior-audit 的引擎期望是
`000988/300308/300408/000823`（全天振幅 + 冻结 T-open book）。`sat_push_log` 因主键
`(trade_date, slot, ts_code)` + `ON CONFLICT DO UPDATE`，每分钟推送互相覆盖，只留 15:00 那版，
14:30 名单不可考。

## 2. 验证：14:30 可得振幅 vs 全天振幅（习惯口径）

口径：C1 3% + same_1430 买 + body=3 + 第3日14:30卖 + clip4 4×12.5% + 核心 trail8 + opp_50。
`rank_amp` = 全天振幅（旧）；`rank_amp_1430` = `bar_5min` ≤14:30 的振幅（诚实）。
脚本 `scripts/compare_sat_rank_1430.py`，报告 `data/backtest_reports/sat_rank_1430_2026-09-11.json`。

twin（核心+卫星）**收益 / Sharpe / 最大回撤**：

| 变体 | OOS2 | train | valid | past_year | aligned |
|---|---|---|---|---|---|
| rank_amp（全天，旧） | +85.3/2.72/16.9 | +50.7/4.10/6.3 | +84.4/3.05/17.8 | +131.5/2.48/17.8 | +136.3/2.57/18.2 |
| **rank_amp_1430（诚实）** | **+85.7/2.74/16.7** | **+59.7/4.47/6.3** | **+91.0/3.20/17.8** | **+152.3/2.71/17.8** | **+149.6/2.72/17.8** |
| Δ vs 全天 | +0.4/+0.02/−0.2 | +9.0/+0.37/0 | +6.6/+0.15/0 | +20.8/+0.23/0 | +13.3/+0.15/−0.4 |

对核心三窗：**+74.9 / +23.3 / +5.0**（全过），Sharpe 三窗都升。**前视根本不需要，14:30 排序反而更贴合 14:30 入场。**

## 3. 参考：三方时钟（`scripts/compare_sat_fill_clock.py`）

| 时钟 | OOS2 | train | valid | aligned |
|---|---|---|---|---|
| 冻结 T-1→T开盘 | +56.6/1.90 | +45.9/3.58 | +95.7/3.19 | +127.4/2.42 |
| 习惯 14:30（全天排序） | +85.3/2.72 | +50.7/4.10 | +84.4/3.05 | +136.3/2.57 |
| 14:30买→第3日收盘卖 | +59.0/2.01 | +31.7/2.50 | +74.0/2.79 | +108.9/2.20 |
| 核心 | +10.8/0.50 | +36.4/2.65 | +86.0/2.84 | +131.0/2.32 |

## 4. 落地（done 2026-09-11）

- 引擎新增 `rank_key="amp_1430"`（`state_bucket_track.replay_sgap_from_context`，实验门控，仅 same_1430）；
  `_load_bar5_hl` 预载 `bar_5min` ≤14:30 的 max high/min low。
- 审计/引擎书 `twin_star_daily._sat_book` 改用 14:30 时钟：`fill_mode=same_1430, exit_hhmm=1430,
  max_open_to_1430_pct=0.03, rank_key="amp_1430"` → 审计期望名单 = Live 可执行定义。
- Timeline 习惯线（`strategy=twin_star` + `sat_fill=same_1430`）同步用 `rank_key="amp_1430"`；冻结默认
  （next_open）不动。
- `sat_push_log` 只落 **14:20–14:35** 决策快照，收盘版不再覆盖。
- 行为审计（2026-09-11 二次）：① **真实书**改为从 `user_trades` 按 as-of 重建（BUY/ADD 开、SELL 平），不再用只会存当前态的 `watchlist_registry`（已卖出的持仓历史日仍能对账；无交易记录时回退旧 registry）；② 真实持仓若出现在**近 BODY 个交易日的实际推送名单**（`pushed_sat_ts`）里，不再算 `satExtra`；③ `satMissing`/`satExpected` 只在**被推送过的名字**里算——没推给你的票不算"该持没买"。09-10 复核：actual 4、satExtra 0、satMissing 0（此前 4/4 乱报）。
- `bar_5min` 采集加回 `1000/1330/1400`（`DECISION_TIMES`），让 14:30 代理振幅有输入。
- Live 侧本已 14:20 信号 / 14:30 买 / 第3日 14:30 卖，不变。

## 5. 数据回补（done 2026-09-11）

- 已回补 **09-04~09-10 的 291 只缺口票**决策时点 5 分钟线（`1000/1330/1400/1430` + 尾盘）：
  291/291 ok、+14,490 行；三天的时点从 7 根补到 10 根。
  ```bash
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/backfill_bar_5min.py --start 2026-09-04 --end 2026-09-10 --no-skip-covered
  ```
  （`--no-skip-covered` 必须加：这些天尾盘 bar 已存在，`skip_covered` 会跳过而不补早盘时点。）
- **09-11 由当晚 18:40 `bar_5min_close` 自动补**（该 job 现已用 `DECISION_TIMES`）。
- 全量历史已含 10 个时点（OOS2/train/valid 验证有效）。
- **残余**：审计代理用「采样 5 分钟 bar」聚合 ≤14:30 振幅，Live 用东财快照的**当日累计 high/low**；
  两者数据源不同，名字级不会逐只相同（edge 是均值口径，已三窗验证）。要**逐名对账**应改用
  `sat_push_log` 里 14:20–14:35 的实际推送名单，而不是重放代理。

## 6. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/compare_sat_rank_1430.py --save-report
PYTHONPATH=src:scripts python3 scripts/compare_sat_fill_clock.py --save-report
```
