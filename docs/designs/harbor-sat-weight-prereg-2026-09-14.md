# 预注册：港湾 × 卫星 曝露曲线（H-SAT-W · 2026-09-14）

> **单假设 · 门槛跑前冻结。** 问：在「港湾核心 + 卫星机会型混合」里，**存不存在一个低卫星权重 w**，既拿到卫星 long 增量，又把 valid 结构性稀释控制在产品可接受预算内？
> **关键词**：曝露曲线 卫星权重 opportunity blend 产品层 预注册

## 0. 一句话机制
混合 = 卫星无仓日 **100% 核心**；卫星有仓日 `核心 + w × (卫星 − 核心)`（沿用 `blend_nav_opportunity`，0 改动）。w 是**产品资本结构参数**，不是策略参数。

## 1. 为什么开（背景）
- 三策略 clean 审计（2026-09-14）：双子星 50/50 四窗 +149.7/+52.2/+29.1/+291.9，**REJECT 只因 valid Δ−21.2（K1/K2）**；OOS2/long 收益与 Sharpe 全面优于港湾。
- 诊断（H-SAT-DIAG）：valid 稀释 = **结构性 regime 依赖**——有仓 30 天核心同日 +45.8% vs 卫星 +14.3%；闸门无过（反事实常开仅 +2.3%）。→ 换个 w 不会让 valid Δ 变正，但可以**把稀释封顶在预算内**，这是产品风险偏好问题，需定量曲线。
- 已知锚点：w=0.5 → valid Δ−21.2；w=0.4/0.6 在同档（B12 clean 只报告未裁决）。

## 2. 口径（冻结）
- **核心腿** = 港湾 P1（`harbor.parking_replay`，2026-09-14 clean 日历/单源口径）；**卫星腿** = `replay_sgap_from_context`（`amp_1430` + C1 + same_1430 + `gate_1430` + raw 基期，30bps RT）；均为冻结 Live 口径，0 改动。
- **混合** = `service.ps_g50_blend.blend_nav_opportunity(sat_weight=w)`；核心 NAV 对齐卫星日历（前向填充）。
- **窗口** = OOS2 / train / valid / long（`WINDOWS`）；**holdout（2026-08-10 起，数据至 2026-09-11）只描述、不裁决、不调参**。
- 网格（**7 点，预定**）：`w ∈ {0.10, 0.15, 0.20, 0.25, 1/3, 0.40, 0.50}`。
- **不扫**：卫星/核心口径、窗口切分、gate、regime 条件化（H4 已拒）。

## 3. 判定（跑前冻死）
- **K1（保护）**：四窗每窗 Δtotal ≥ **−5pt**（vs 港湾核心；valid 结构性稀释上限）。
- **K2（目标）**：long Sharpe ≥ 核心 **+0.10** 且 long Δtotal ≥ **+25pt**。
- **K3（风控）**：long MDD 不劣于核心（ΔMDD ≤ 0）且 valid MDD 不劣化 > **2pt**。
- **裁决**：全部满足 K1–K3 的 w 中取**最大 w**（最大曝露）→ 该 w 为**产品候选**（落地仍需资本结构拍板 + paper 3/20）；无 w 满足 → **REJECT**（维持 Live=港湾、不引入卫星曝露）。
- 三档以上同向才算稳（单点不过不翻案，单点过也不代表 robust）。

## 4. Robustness（预声明，只报告不选参）
- 全网格 total/CAGR/MDD/Sharpe + Δ；worst-window Δtotal 随 w 的曲线。
- 两腿日收益相关（active 日/全窗）；卫星 active 占比。
- holdout 描述性一行。

## 5. 死因预判（跑前写死）
- 主：**#2 共线/regime 预算冲突**——valid 稀释 ∝ w，w 小则 K1 过 K2 不到；w 大则 K2 过 K1 挂。最可能的结局：**无 w 同时满足**。
- 次：**#7 样本**——long 前半（2021–23）是事后样本；valid 仅 50 笔/3 episode，曲线可能不单调。

## 6. 产出
- 脚本：`scripts/eval_harbor_sat_weight.py` → `data/backtest_reports/harbor_sat_weight_2026-09-14.json`
- 结论落 `docs/backtests/stable/harbor-sat-weight-2026-09-14.md` + SUMMARY + todo。

**结果（跑后补）**：

*冻结于 2026-09-14，跑数前。*
