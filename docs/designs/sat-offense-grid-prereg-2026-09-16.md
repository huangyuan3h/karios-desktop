# 预注册：星舰进攻网格（H-SAT-OFF · 2026-09-16）

> **状态**：用户授权执行（"都看一下做一个计划，然后给我结论"），跑前冻结。
> **目标**：进攻 mandate（max long 几何收益，约束只保生存）下找星舰 v2（A2_true）的增量。
> **范围纪律**：只动回放后资本配置（w 序列 / x_nav 混合），**不动**引擎fills/槽数/clip/闸门/C1
> （S-3/clip4/R-wide/C1 冻结结论全部保留）。up 加仓因无杠杆不可行，已排除（见 §1）。

## 1. 机制（每臂一句话）

- **Arm S（卫星 choppy 缩量）**：卫星在 choppy 市况是否亏钱？若亏，把 choppy 日卫星敞口按 k 缩小，
  腾出的钱停套筒（`w' = 1 − k·(1−w_true)`，复用 `_compose`  machinery）。
  标签 = 指数版 up/choppy/down（`diag_sat_regime` 同口径：000001 ret20 ±3%，只用当日及之前收盘，因果 ✓）。
  up 固定 1.0（无杠杆，加不了）；down 不动（R-wide 闸已拦，敞口本来就是 0）。
- **Arm P（停车条件化）**：卫星空仓日 = 弱广度日，套筒在弱广度日是否也被鞭打？
  若是，parking 只在 `breadth_1430 > 0.5`（冻结 R-wide 阈值，**不新设阈值**）停套筒，
  否则停 REPO。切换摩擦诚实计费（`bps × w_true × |Δflag|`，15bps 敏感性必跑）。

## 2. 诊断先行门（只读，任一触发对应臂直接 VOID，不跑网格）

- **D1（套筒 valid 事件分解）**：套筒 valid 最大回撤段的峰/谷日期、深度、持有 ETF、trail 次数。
  纯记录，不门控（ episode 归因）。
- **D2（Arm S 门）**：卫星日收益 × 指数市况，分窗求和。若 choppy-sum ≥ 0（不亏）→ Arm S **VOID**
  （缩量只能砍赢家，死因 #1）。
- **D3（Arm P 门）**：套筒日收益 × breadth（>0.5 vs ≤0.5），分窗求和。
  若低广度套筒-sum ≥ REPO 同期（不停比停强）→ Arm P **VOID**。

## 3. 网格（S4 结构通道；选参只看 OOS2+train，valid 只验，long 验证）

- Arm S：k_choppy ∈ {1.0（=A2_true 基线）, 0.75, 0.5}。选择 = OOS2+train 均值 total 最大，
  且两窗各自 Δ vs A2_true ≥ −5（地板）；valid+long 只验（valid Δ ≥ −8，否则降级）。
- Arm P：单变体（阈值借用，无网格）+ 15bps 敏感性臂。

## 4. 冻结裁决（进攻通道 v1，数值可调）

- **PASS-offense**：long Δ vs A2_true ≥ **+25pt** 且 单窗绝对 total ≥ **−15%** 且 long MDD ≥ **−40%**
  且 valid Δ vs A2_true ≥ **−8** 且 15bps 下 long Δ > 0。
- 不满足 → REJECT（若 long Δ > 0 但 valid 破 −8 → 条件PASS，需 holdout/paper）。
- 死因预判：S-choppy 若存活最可能死于 #2（被套筒收益共线吸收——缩量腾出的钱停套筒，套筒本身 +89 long）；
  P 最可能死于 #7（切换摩擦吃掉价差，A3 REPO −31.3 前科）或 #4（valid 单窗现象）。

## 5. L 门 tick

L1 宇宙冻结 habit（含 mv 门，OPT-147）✓ · L2 无新成交语义（后处理 only，fill 时间戳不动；新 w 序列
用 14:30 已知量：breadth_1430/指数收盘/w_true，全部因果）✓ · L3/L4 n/a（不碰数据源/基期）·
L5 report JSON + 本档 tag + 基线复用 §5 数字 ✓ · L6 CN（habit mv 门排 HK）· L7 一命令复现 §6。

## 6. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_sat_offense_grid.py --save-report
```

## 7. 与冻结结论的关系

- clip4（4 槽）、C1=3%、R-wide 0.5、 same_1430、C1 配方：全部不动。本实验若 PASS，
  改的是**资本配置层**（w 序列），引擎与 Live 卫星指令不受影响（星舰本就不进 Live）。
- 若 REJECT：sizing/停车条件化方向关闭，不碰引擎。
