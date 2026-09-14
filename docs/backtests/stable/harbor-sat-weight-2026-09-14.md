# 「港湾 × 卫星」曝露曲线（H-SAT-W）· 2026-09-14 · REJECT

> **何时看**：想知道「卫星要不要按低权重进组合」「为什么不做港湾×卫星产品」时。
> **何时不看**：Live 操作（Live=港湾）；参数候选（本档只裁决曝露权重）。
> **一句话**：预注册 7 档权重（w=0.10~0.50）全部不满足 K1–K3：**valid 稀释 ∝ w（≈−42×w）、long 增益 ∝ w**，K1 要 w≤0.12、K2 要 w≥0.15 → 死区，无解 → **REJECT（NONE）**；Live=港湾维持、不引入卫星曝露。
> **关键词**：曝露曲线 · valid 稀释线性 · K1/K2 死区 · 产品候选否 · H-SAT-W

**预注册**：[`harbor-sat-weight-prereg-2026-09-14.md`](../../designs/harbor-sat-weight-prereg-2026-09-14.md)（跑前冻结网格/判据）· **脚本** `scripts/eval_harbor_sat_weight.py` · **报告** `data/backtest_reports/harbor_sat_weight_2026-09-14.json`

---

## 1. 机制（一句话）

混合 = 卫星无仓日 100% 港湾核心；有仓日 `核心 + w × (卫星 − 核心)`（`blend_nav_opportunity`，0 改动）。w = 产品资本结构参数（7 档：0.10/0.15/0.20/0.25/1/3/0.40/0.50）。

## 2. 结果（Δ vs 港湾核心；含成本）

| w | OOS2 Δtot | train Δtot | valid Δtot | long Δtot | long Δsr | long ΔMDD | worst Δtot |
|---|---|---|---|---|---|---|---|
| 0.10 | +15.7 | +0.2 | **−4.4** | +17.6 | +0.08 | +2.0 | −4.4 |
| 0.15 | +24.1 | +0.2 | −6.6 | +26.6 | +0.13 | +2.0 | −6.6 |
| 0.20 | +32.9 | +0.2 | −8.7 | +35.6 | +0.18 | +2.0 | −8.7 |
| 0.25 | +42.1 | +0.2 | −10.9 | +44.6 | +0.22 | +2.0 | −10.9 |
| 1/3 | +58.3 | +0.2 | −14.4 | +59.8 | +0.30 | +2.0 | −14.4 |
| 0.40 | +72.2 | +0.1 | −17.1 | +72.0 | +0.36 | +2.0 | −17.1 |
| 0.50 | +94.5 | +0.0 | −21.2 | +90.4 | +0.45 | +2.0 | −21.2 |

- 基线：核心 = +55.2/+52.2/+50.3/+201.5（sr 1.73/3.01/2.14/1.00，long MDD −22.8）；卫星 standalone = +212.7/+40.3/+14.7/+463.6（active 67%/57%/27%/52%）。
- 两腿 active 日相关：OOS2 0.23 / train 0.08 / valid −0.05 / long 0.10；全窗 0.18/0.09/0.03/0.09 → 低相关（long Sharpe 提升的来源）真实。

## 3. 裁决（冻结 K1–K3）

| w | K1（worst Δ ≥ −5） | K2（long Δsr ≥ +0.10 & Δtot ≥ +25） | K3（long ΔMDD ≥ 0；valid ΔMDD ≥ −2） | pass |
|---|---|---|---|---|
| 0.10 | ✅（−4.4） | ❌（+17.6/+0.08） | ✅ | ❌ |
| 0.15 | ❌（−6.6） | ✅（+26.6/+0.13） | ✅ | ❌ |
| ≥0.20 | ❌（≤−8.7） | ✅ | ✅ | ❌ |
| — | K1 要求 **w ≤ 0.118** | K2 要求 **w ≥ 0.12~0.142** | 全档过 | **NONE** |

**chosen: NONE → REJECT**（维持 Live=港湾、不引入卫星曝露）。

## 4. 发现

1. **K1/K2 死区是结构性的**：valid 稀释 ≈ **−42.4 × w**（w=0.5 时 −21.2）；K1 要求 w ≤ 0.118；K2 的 Δsr ≥ +0.10 要求 w ≥ 0.12、Δtot ≥ +25 要求 w ≥ 0.142。**两条件无交集**——与预注册死因预判 `#2 共线/regime 预算冲突` 完全一致。
2. **风控不是瓶颈（K3 全档过）**：long MDD 每档改善 **+2.0pt**（−22.8→−20.8）、valid MDD 不动（−21.8）：低相关卫星在长窗确实是"风险对冲"，但**对冲的收益代价在 valid 被 regime 放大**。
3. **valid 是唯一约束窗**：OOS2/train/long 的 Δ 全为正（train 持平）；把 valid 拿掉，任何 w≥0.15 都是"收益+风险双改善"——这正是双子星 REJECT 的同一条结构性伤口（见 [诊断](../sat/sat-valid-shortfall-diagnosis-2026-09-14.md)）。
4. **holdout 描述（08-10~09-11，n 小仅记录）**：两腿均回撤（core −2.5%/sat −6.5%），w≥0.1 均拖累 core（w=0.25 时 total −3.4% vs core −2.5%、sr −2.18 vs −1.17）→ 近期 tape 卫星同样弱，与"regime 依赖"一致。
5. **预注册勘误（透明记录）**：K3 的 long 条件在预注册里写成 `ΔMDD ≤ 0`（符号写反；`max_dd` 为负数，"不劣于"应为 `ΔMDD ≥ 0`）。跑后发现并修正；**K3 在每档都为真、不 binding，裁决（NONE）不受影响**。

## 5. 备查菜单（非推荐，仅供参考价格）

若未来产品层决定**显式接受** valid 稀释作为预算，曲线即价目表：w=0.10 → long +17.6pt/Δsr +0.08、valid −4.4pt；w=0.15 → long +26.6/+0.13、valid −6.6pt。这些**均不满足本次冻结判据**，落地必须另起预注册（并在预注册里重新定义判据与资本结构）。

> **后续（2026-09-14）**：三腿版「母港 M50 + 卫星」（H-B3-SAT）已试并 **PASS（chosen=1/3）**——换基座后 valid 稀释斜率 −42.4×w → −14.6×w，本档的 K1/K2 死区消失。见 [`harbor-b3-sat-2026-09-14.md`](harbor-b3-sat-2026-09-14.md)。

## 6. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_harbor_sat_weight.py --save-report
```

## 7. 关联

- 预注册：[`harbor-sat-weight-prereg-2026-09-14.md`](../../designs/harbor-sat-weight-prereg-2026-09-14.md)
- 前置诊断（valid 短板 = regime 依赖）：[`../sat/sat-valid-shortfall-diagnosis-2026-09-14.md`](../sat/sat-valid-shortfall-diagnosis-2026-09-14.md)
- 卫星专档：[`sgap-habit-satellite-standalone-2026-09-14.md`](sgap-habit-satellite-standalone-2026-09-14.md) · 双子星重拟合（clean）：[`twin-star-parking-refit-2026-09-13.md`](twin-star-parking-refit-2026-09-13.md)
- 三策略审计：[`../audit-three-strategy-lookahead-2026-09-14.md`](../audit-three-strategy-lookahead-2026-09-14.md) · 港湾基线：[`etf-parking-baseline-2026-09-13.md`](etf-parking-baseline-2026-09-13.md)
