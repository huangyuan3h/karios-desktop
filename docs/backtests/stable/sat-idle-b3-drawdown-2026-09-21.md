# 星舰空闲现金停「风险预算组合」· 回撤口径重裁（H-SAT-IDLE-B3-R）· 2026-09-21 · **PASS**

> **何时看**：想知道「星舰闲置现金停 B3 分散组合」在**回撤口径**下是否正式成立、可否作产品候选时。
> **何时不看**：Live 操作（Live = 港湾）；星舰前置（paper 3/20 + 用户授权）判定。
> **一句话**：回撤口径重裁 **PASS**（K1–K4 全过）——**A4_true long +654.3% / CAGR 52.1% / MDD −6.8% / SR 3.67**，
> 相对现行套筒 A2_true **让 259pt 收益、换回 23.1pt 回撤（浅 4.4×）、Sharpe +1.56**；
> holdout 描述同向（A4 −2.2% vs A2 −4.7%）。**仅作产品候选，不进 Live。**
> **关键词**：星舰 · 闲置现金 · 停车 · 风险预算 · 回撤口径 · PASS · 产品候选

> 预注册：[`sat-idle-b3-drawdown-prereg-2026-09-21.md`](../../designs/sat-idle-b3-drawdown-prereg-2026-09-21.md)（跑前冻结）
> 脚本 `scripts/eval_sat_idle_b3.py --premise drawdown`（只读）· 报告 `data/backtest_reports/sat_idle_b3_drawdown_2026-09-21.json`
> **不覆盖**同日 returns-口径的 REJECT（[`sat-idle-b3-2026-09-21.md`](sat-idle-b3-2026-09-21.md)）；**Live 不动**。

---

## 1. 结果（total% / CAGR% / MDD% / Sharpe；Δ vs 星舰 standalone）

| 窗口 | 星舰 standalone | A2_true 套筒（现行） | **A4_true B3（主）** | A5_true 50%B3 | A6_true 50%套筒 |
|---|---|---|---|---|---|
| OOS2 | +207.2 / −4.4 / 6.38 | +239.2 (Δ+32.0, Δdd −4.8) | **+236.5 (Δ+29.3, Δdd −1.2)** | +221.0 (+13.8) | +223.5 (+16.3) |
| train | +36.2 / −8.6 / 3.05 | +65.3 (Δ+29.1) | **+49.6 (Δ+13.4, Δdd +0.3)** | +42.5 (+6.3) | +50.0 (+13.8) |
| valid | +6.5 / −7.3 / 0.91 | −0.3 (Δ**−6.8**, Δdd −28.6) | **+3.0 (Δ−3.5, Δdd −2.8)** | +4.8 (−1.7) | +4.2 (−2.3) |
| **long** | +468.3 / 43.4 / −5.1 / 3.45 | +913.2 / 61.6 / **−29.9** / 2.11 | **+654.3 / 52.1 / −6.8 / 3.67** | +556.2 / 47.7 / −5.9 / 3.62 | +681.4 / 53.2 / −15.9 / 2.88 |
| holdout*（描述） | −3.4 / −12.5 / −1.00 | −4.7 / −13.7 / −1.38 | **−2.2 / −12.5 / −0.58** | −2.9 / −12.6 / −0.83 | −4.2 / −13.2 / −1.24 |

\* holdout（2026-08-08+，n≈28 日）**只描述、不裁决**；A4 对 A2 Δtot **+2.5**、Δmdd +1.2、Δsr +0.80——同向。

**A4_true vs A2_true（long）**：Δtot **−258.9** · **Δmdd +23.1** · **Δsr +1.56**；
B3/卫星日相关 all 0.12 / idle −0.03（低相关，无 A1 港湾核心那种 regime 冲突）。

## 2. 裁决（冻结 K1–K4 · 回撤口径）

| 判据 | 阈值 | 结果 |
|---|---|---|
| K1 三窗 Δtot vs standalone | 全 ≥ −5pt | ✅ `[+29.3, +13.4, **−3.5**]` |
| K2 long Δtot | ≥ +100pt | ✅ +186.0 |
| K3 long MDD 比 A2_true 浅 | ≥ 15pt | ✅ **+23.1** |
| K4 long Sharpe | ≥ A2+0.5 且 ≥ standalone | ✅ 3.67 ≥ 2.61 且 ≥ 3.45 |
| **裁决** | | **PASS（产品候选）** |

## 3. 发现

1. **"套筒的深回撤是集中造成、不是停车造成"——实锤可修**：同一卫星腿，把停放资产从
   argmax 单腿换成 B3 分散组合，long MDD **−29.9% → −6.8%（浅 4.4×）**、Sharpe **2.11 → 3.67**。
2. **B3 停车是"近乎免费的收益"**：对 standalone 只加 **1.7pt 回撤**（−5.1→−6.8），
   加 **+186pt long 收益**、SR 3.45→3.67；捕获套筒收益增量的 ~42%、只付 ~7% 回撤增量。
3. **valid 与 holdout 都更稳**：valid A4 −3.5 vs A2 −6.8；holdout A4 −2.2 vs A2 −4.7。
   K1 在 valid 的 −3.5 是"弱窗卫星本身只 +6.5%、任何停放都有微稀释"，非 B3 机制问题。
4. **前沿（long）**：standalone +468/−5.1/3.45 → **A4_true +654/−6.8/3.67**（Pareto：收益↑、
   回撤≈、Sharpe↑）→ A5 +556/−5.9/3.62（更保守）→ A6 +681/−15.9/2.88（收益更高、回撤更深）→ A2 +913/−29.9/2.11。

## 4. 边界（诚实项）

- **过拟合披露**：本档在已知 returns-口径结果后开，存在"事后选判据"风险；缓解见预注册 §0
  （阈值按机制先验、holdout 描述、A6 独立中间档）。**不得**用它翻转 returns-口径裁决。
- A2_true long +913.2 与冻结档 +884.9 有差（成本口径万3/滑点 + 数据至 2026-09-18），方向一致。
- B3 前 60 日等权（冻结约定）、月内不再平衡、未建模冲击；卫星腿未调参。
- holdout n≈28（数据至 2026-09-18），**只描述**。
- **PASS ≠ 进 Live**：落地需用户拍板资本结构 + 新预注册（Live 口径）+ paper 前瞻；星舰前置不变。

## 5. 结论 / 下一步

- **A4_true（卫星 + 闲置现金停 B3）= 正式产品候选（星舰"稳健版"）**：long **+654% / MDD −6.8% / SR 3.67**。
- 候选配置：**A4_true（稳健，回撤优先）** / **A6_true（50% 套筒，收益优先，+681/−15.9）** /
  A2_true（现行，+913/−29.9，用户已选"痛苦"档）。
- 下一步（需用户拍板）：① 是否把 A4_true/A6 接进目录 + Timeline 展示（纯显示层）；
  ② 若进 Live，另起 Live 口径预注册 + paper 前瞻。

## 6. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3.py --premise drawdown --windows OOS2,train,valid,long,holdout --save-report
PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3.py --premise drawdown --windows long
```

## 7. 关联

- 同轮 returns 口径（REJECT）：[`sat-idle-b3-2026-09-21.md`](sat-idle-b3-2026-09-21.md)
- 前作：[`sat-idle-parking-2026-09-15.md`](sat-idle-parking-2026-09-15.md)（H-SAT-IDLE · REJECT）
- 风险预算基准：[`etf-benchmark-parking-2026-09-13.md`](etf-benchmark-parking-2026-09-13.md)（B13）
- 母港：[`harbor-riskbudget-2026-09-13.md`](harbor-riskbudget-2026-09-13.md) · 卫星 standalone：
  [`sgap-habit-satellite-standalone-2026-09-14.md`](sgap-habit-satellite-standalone-2026-09-14.md)
