# P18：价值 × 动量复合（P0-11 · 预注册 · 诊断 PASS → 待回放 2026-09-11）

> **来源**：P0-11 最后一个（P18 价值+动量复合，单假设 H-P18，零网格）。
> 权重冻结 0.5/0.5（spec 的 0.3/0.7 版不开）；无股息率腿（本地无分红表，诚实边界）。
> **一句话机制**：便宜 + 趋势确认 = 震荡市更稳（文献形态，非弱弱拼凑）。

## 1. 方法（跑前冻结）

* value = EP/BP/SP/FCF-yield 等权 rank（TTM 口径 + 信号日总市值，PiT）；
  composite = 0.5·value + 0.5·mom60（rank 空间）；市值 5 组内中性 pooled。
* PASS 线：pooled meanIC > 0；pooled Q-spread ≥60% 季；|corr(value, mom)| < 0.5。
* 脚本：`scripts/diag_fin_p18.py`（`fin_panel.value_panel` 新管线）。

## 2. 诊断电池（20 季）+ 消融

* pooledIC(composite) **+0.118** ✓；posQ **70%** ✓；max|v−m| = 0.443 ✓ → **机械 PASS**。
* 消融：mom-only **+0.116** · value-only **+0.064**。
  结论：value 腿自身为正（非装饰），但 composite 的强度 98% 来自动量腿；
  value 的增量在诊断层 ≈ 0（0.116 → 0.118）。
* 覆盖：四腿齐全仅 57.9%（FCF 稀疏），2025+ 季度 n=346–946（薄）。
* 窗口结构：正向块集中在 2021–2023（含 2022 熊市价值防守段）；
  窗内（2024-08+）n 薄且有负季（2025Q3/Q4）。

## 3. 方向：OPEN 回放（附严格消融框）

回放必须是三臂：composite vs mom-only vs base，三窗 + tot 翻正。
若 composite ≯ mom-only → value 腿是装饰，整条死 #2（动量维已被 RS 全吸收，
P1/P2/P9 先例）；若 composite > mom-only 且三窗一致 → value 腿转正。

## 4. 工程成本（开跑前明示）

回放需把季度 value 面板 + mom 排序接进回测引擎做 gate；
README 已记 `--param trendok_*` 注入当前 no-op（`recompute_scores_with_params`
待修）——引擎手术 + 测试约 1–2 天。是否开，由用户拍板。

## 5. 回放裁决：REJECT（2026-09-11 · 三臂三窗 · 用户拍板开跑）

接线：`BacktestConfig.value_mom_gate`（off/composite/mom_only，中位 0.5 分割，
缺数 fail-closed）+ `_load_value_comp_ranks` + 测试 10 个全绿（`tests/test_value_mom_gate.py`）。

| 臂 | OOS2 | train | valid | 笔数 |
|---|---|---|---|---|
| base（基线） | — | — | — | 237/123/55 |
| composite | −8.5pt | −43.0pt | −43.3pt | 87/39/9 |
| mom_only | +2.1pt | −43.9pt | −36.4pt | 85/40/16 |

* composite ≈ mom_only → value 腿在组合层面零增量（诊断层的 +0.064 没进组合）。
* 归因（OOS2 composite，`gated_blocks`）：`value_mom_missing` 58,465 vs
  `value_mom_gate` 12,059 —— **83% 的拦截是缺数 fail-closed**，只有 17% 是真·后半剔除。
* 死因：**#7 覆盖不足 + #5 砍宽度**（237→87 笔），不是方向问题。
  四腿 58% 覆盖 + mv/mom60 缺失把门饿死；且 S-3 候选本就是"新转强"票，其中期
  mom60 排名天然偏弱（P9 根因同构：M250 门清空 valid 池）。
* 处置：P18 gate 关闭。fail-open 变体（缺数放行）= 新假设，需独立预注册；
  其 mom-only 形态近似未测过的 M60-top50 门，可单开诊断但须先过与 RS 的相关门。
  报告：`data/backtest_reports/walk_forward_p18_composite.json` /
  `walk_forward_p18_momonly.json`。
