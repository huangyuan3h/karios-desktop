# 卫星条件持有天数：按风格分 2/3/4 天（2026-09-12 · REJECT）

> **一句话**：用户假设「有些票适合拿 4 天、有些 2 天，按股票风格找分界」。只读诊断（冻结 body=3 逐笔）先给结论：**没有任何入场前风格的「第 3 天」三窗全负 → 砍到 2 天无支撑**；`tier0` 等桶的「第 4 天」边际确实三窗全正，但把它写进引擎（带 4 槽占槽税）后 **OOS2 −10.1pt → REJECT**。**维持 body=3，Live 不动。**
> **关键词**：条件持有 卫星 body 3/4/2 stage_tier 占槽税 diag-sat-hold-days

**脚本**：
- 只读诊断：`services/data-sync-service/scripts/diag_sat_hold_days.py`（另 `diag_sat_hold_d4.py`）
- 引擎回放：`services/data-sync-service/scripts/compare_sat_hold_stage.py`
- 原始表：`data/backtest_reports/diag_sat_hold_days_2026-09-12.json` · `diag_sat_hold_d4_2026-09-12.json` · `sat_hold_stage_2026-09-12.json`

**口径**：习惯冻结腿（C1 3% · `same_1430` · `amp_1430` 排序 · clip4 4×12.5% · body=3 第 3 日 14:30 卖）；核心择强 trail8 + `opp_50`。三窗 OOS2/train/valid。分界 `stage_1430`（前若干交易日 + 入场日 14:30 print，零前视）。

---

## 0. 为什么测

双子星卫星腿一直固定持有 3 天。用户直觉：不同风格股票脉冲长度不同，按风格定 2/3/4 天可提收益/夏普。相关旧证据：`body=4` 占槽税 `aligned −16pt`（[sat-habit-clock](sat-habit-clock-2026-09-03.md)）；`body=1/2` 截右尾 REJECT（[sat-body1](sat-body1-2026-09-07.md)）；`sat-score-segment` 判死用分数当分界（D3/D4）。本页补上「入场前风格标签 × 持有天数」这条缝。

## 1. 只读诊断：同一批 body=3 成交，看第 3 / 第 4 天边际

对冻结 body=3 每一笔，用同日段 14:30 价算两个边际（往返成本在「延不延/砍不砍」里抵消）：
- `m23 = d3/d2 − 1`（第 3 天的价值；**负** → 该风格砍到 2 天）
- `m34 = d4/d3 − 1`（第 4 天的价值；**正** → 该风格延到 4 天）

**2 天：无候选。** 没有任何入场前风格桶的 `m23` 三窗全负——第 3 天在几乎所有桶都为正（与 [sat-hold-path](sat-hold-path-day2-2026-09-03.md) 的 d1<d2<d3 一致）。唯一接近的是 `amp1430=high`（valid −0.95%）但 OOS2/train 为正，mixed。

**4 天：一批候选（`m34` 三窗全正）**：

| 桶 | OOS2 | train | valid | n（OOS2/train/valid） |
|----|------|-------|-------|------------------------|
| `tier0` S2-advance × climax | +0.76% | +0.50% | +1.60% | 85/37/9 |
| `wein=S2-advance` | +0.33% | +0.41% | +0.60% | 146/73/13 |
| `dd60=at-high` | +0.40% | +0.48% | +0.56% | 114/62/14 |
| `runup5=climax` | +0.65% | +0.70% | +0.97% | 115/47/19 |
| `amp1430=low`（**不可信**，见下） | +0.90% | +0.99% | +0.74% | 115/45/14 |

`tier1`（只中一个）`m34` 打架（−0.08/+0.57/−0.18），`tier2`（都不中）打架（+1.46/+0.06/−0.61）。

**数据坑（重要）**：`amp1430=low` 里 47% 的 fill 是 `amp1430=0`——查 `bar_5min` 是**一字/锁板**（当日日线 `o=h=l=c`，如 2024-10-08 涨停潮），不是「安静股」，且实盘 14:30 常买不进。该候选作废，不作为分界。`stage` 维度（收盘序列）不受此坑影响。

## 2. 引擎回放：把条件 body 写进 4 槽机器（带占槽税）

`replay_sgap_from_context` 加研究门控 `body_by_stage_tier`（默认 None = 冻结；入场按 `stage_1430` 存档，退出按 per-position body）。变体只改该映射：

| 变体 | 含义 |
|------|------|
| `base` | body=3（冻结） |
| `extend_t0` | tier0 → 4 |
| `extend_t0t1` | tier0/1（S2 或 climax）→ 4 |
| `cut_t2` | tier2（都不中）→ 2 |

twin 总收益 / 夏普 / 最大回撤（括号 = 相对 base）：

| 窗口 | base | extend_t0 | extend_t0t1 | cut_t2 |
|------|------|-----------|-------------|--------|
| OOS2 | +82.4/2.64/16.6 | +72.3/2.43/16.8 (**−10.1**) | +72.3/2.46/16.4 (**−10.1**) | +83.0/2.66/17.0 (+0.6) |
| train | +58.3/4.36/6.3 | +55.7/4.23/6.3 (−2.6) | +59.3/4.63/6.3 (+1.0) | +59.5/4.49/6.3 (+1.2) |
| valid | +84.1/3.05/17.8 | +82.2/3.00/17.8 (−1.9) | +92.1/3.24/17.8 (**+8.0**) | +85.5/3.06/17.8 (+1.4) |

成交笔数（% base）：`extend_t0` 89/92/94 · `extend_t0t1` 81/77/89 · `cut_t2` 117/117/133。

| 变体 | 判定 |
|------|------|
| `extend_t0` | **REJECT/total**（OOS2 −10.1；夏普/回撤两窗更差） |
| `extend_t0t1` | **REJECT/total**（OOS2 −10.1；valid +8.0 属「单窗好看」——死因 #4 形状） |
| `cut_t2` | **no-op**（三窗 +0.6/+1.2/+1.4 均在噪声内，OOS2 回撤略差） |

## 3. 判定：占槽税再次胜出

- **第 4 天的 per-stock 边际是真的**（tier0 三窗全正），但把它延长只少了 ~10% 成交，就足够在 OOS2 吃掉 10pt——**4 槽脉冲机器付不起让最强票多占一天槽**。这与 `body=4`/`trail-after-body` 的占槽税同根（死因 #1）。
- 诊断口径（只比同一批票的两天价差）**天然忽略占槽税**，只有引擎回放能暴露——本页是「诊断过门 ≠ 能进 Live」的又一实例。
- **2 天无路**：没有入场前风格桶的第 3 天为负，引擎里 `cut_t2` 也只是噪声级。
- 选择偏差提醒：`stage_tier` 是在**同样这三窗**上选出的（H-SAT-RANK），`extend_t0` 的 valid n 仅 9——即便它好看也不独立。

**结论：维持 body=3；2/4 天条件持有方向关闭，不补网格。**

## 4. 定位与复现

- Live / paper / 通知零改动；`body=3`、C1、R-wide、`amp_1430` 维持。
- 引擎新增 `body_by_stage_tier`（实验门控，默认 None）保留给后续研究；`build_sgap_timeline` 透传；单测 `test_body_by_stage_tier_extends_tier0` / `..._keeps_other_tiers_at_body`。
- 只读诊断脚本不写库、不动 Live。

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_hold_days.py --save-report
PYTHONPATH=src:scripts python3 scripts/compare_sat_hold_stage.py --save-report
```
