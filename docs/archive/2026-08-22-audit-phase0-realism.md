# 2026-08-22 回测审计 Phase0 闭环

> `audit-2026-08-22.md` 三审计（数据/执行/统计）`P0` 可修项已闭环，`P1/P2` 抗辩后归档。

## 已修 P0（代码）
- V1 holdout `2026-08-08~2027-02-08` `run_walk_forward.py:102`
- V2 基线不可变 `walk_forward_baseline_20260815_D3.json` `6d8280` → `walk_forward_baseline_20260822.json` `dfc6539e` `tag s3-baseline-20260822-realism`
- V3 B-T1 注入 `backtest_engine.py:655` `recompute_scores_with_params(8并行)`
- V4 Wilson CI `run_walk_forward.py:183` `valid 81.8% CI70-90% n55⚠️`
- D0 daily 前视 `db/daily.py:455` `as_of` + `market_regime.py:275`
- D4 survivor `backfill_watchlist_scores.py:74` `as_of` `5230→4241`
- E1 现金 `sum≤1.0` `backtest_engine.py:2216` `200%→100%` `valid 55→18`
- E2 流动性 `0.7亿` `run_walk_forward.py:94`
- E3 交易日历 `backtest_engine.py:2716`

## 归档 P1/P2（抗辩，ROI低）
- D1 行业时态：mild，需时态表，`valid/train` 影响 `2-3pt`，disclosed
- D2 退市：仅 `long`，`OOS2/train/valid` 不受影响，`long` 已标 `survivor-conditioned`
- D3 long分段：`long` 非决策窗
- E4 next_open：`valid 38.7% vs 43.3% -4.6pt`，live 用 `close`，disclosed `0.2%/笔` 乐观
- S1-S3 FDR/holdout：`holdout n0` 自然等待，`B-T1` 限 `15/季度` 已约束

## 新基线（realism）
- OOS2 43.1% n93 CI38-58% / train 34.3% n54 / valid 43.3% n18 CI44-84% ⚠️ / 过去一年 66.6% n83
- 旧 117.2/122.6/142.2% 封存 `6d8280`，`long 333.9%` 仍条件
