# 预注册：港湾停车资产换 B3 风险预算（H-HARBOR-B3-PARK · 2026-09-21）

> **单假设 · 门槛跑前冻结。** 问：把 **Live 港湾**的闲置现金停车从 **argmax 集中套筒**换成
> **B3 逆波动率分散组合**，能否在**保留实质收益增量**的同时，把 **valid 窗的深回撤**收窄？
> **关键词**：港湾 · Live · 停车资产 · B3 · 风险预算 · 回撤 · 预注册

## 0. 机制（冻结）

港湾 = S-3 CN 核心（冻结，`S3_CONFIG`）+ 闲置现金 `idle_t = 1 − Σ position_pct`（T−1 因果）
停 **B3 风险预算**（`homeport.risk_budget_nav`：5 资产 `510300/510500/518880/513100/511260`、
60 日逆波动率、月频、5bp/边内部成本）：
`nav *= 1 + r_eng + idle_t × b3_ret_t − 5bps × |idle_t − idle_{t−1}|`。

## 1. 为什么开（背景）

- B11：港湾 P1（argmax 停车）把 long 从 +82.7 提到 +201.5，但 **valid MDD −6.1 → −21.8、Sharpe 3.27 → 2.14**
  （S-3 空仓=满仓 ETF + trail 无冷却）；归因同星舰：**回撤来自集中停车腿**。
- 星舰侧已证（H-SAT-IDLE-B3-R PASS）：换 B3 → long MDD −29.9% → −6.8%（浅 4.4×）。
  **本档问：同一个修复能不能用到 Live 港湾**（Live=港湾，影响真实产品）。
- 死因预判：#2 regime（S-3 空仓日=弱广度日，B3 是否同病——A1 港湾核心的教训）；#4（B3 收益低 → K2 挂）。

## 2. 臂（冻结）

- **H-B3（主判定）** = S-3 核心 + idle 停 B3（5bps/边转移 + B3 内部成本）。
- **P1** = 现行 argmax 停车（canonical，对照基准）。
- **P1_H2** = 产品 H2 停车（对照）。
- **V0** = 纯 S-3（收益基准）。
- 卫星腿不涉及（本档只动港湾核心的停车）。

## 3. 判定（跑前冻死）

- **K1（不劣化任何窗）**：H-B3 三窗 `Δtot` vs **V0** 全部 **≥ 0**（停车必须仍加收益）。
- **K2（收益实质）**：H-B3 long `Δtot` vs V0 **≥ +50pt**。
- **K3（回撤——本档目的）**：H-B3 **valid MDD 比 P1 浅 ≥ 5pt**。
- **K4（效率）**：H-B3 long **Sharpe ≥ P1 long Sharpe**。
- **裁决**：**PASS = K1∧K2∧K3∧K4**；否则 REJECT。P1/P1_H2/V0 为对照，不裁决。
- **锚点披露**：PASS 仅作**候选**；**Live 不改**，落地需用户拍板 + Live 口径预注册 + paper 前瞻。

## 4. 死因预判（跑前写死）

- 主：**#4 收益不足**——B3 CAGR ~8% vs 套筒 14%，long 增量可能 < +50。
- 次：**#2 regime**——S-3 空仓日若为弱广度日，B3 在这些日子同样不给钱 → K1 挂。
- 再次：**#7 样本**——holdout 只描述。

## 5. 产出

- 脚本：`scripts/eval_harbor_b3_parking.py`（只读）→ `data/backtest_reports/harbor_b3_parking_2026-09-21.json`。
- 结论落 `docs/backtests/stable/harbor-b3-parking-2026-09-21.md` + `SUMMARY.md`；**Live 不动**。

*冻结于 2026-09-21，跑数前。*

**结果（跑后补 · 2026-09-21）**：**REJECT**。K1 ❌ `[+12.9, +13.3, −4.5]` · K2 ❌ +42.3 · K3 ✅ +11.7 · K4 ❌ −0.08。
B3 停车把 valid MDD −21.8→**−10.1** 却把 valid 收益打到 **−4.5**（**#2 regime 实锤**：S-3 空仓日=弱广度日，
B3 不给钱）；且 long MDD 反而更深（−28.8 vs P1 −22.9）——**港湾的长窗回撤来自 S-3 核心（−32.7），不是停车腿**。
结论落 [`../backtests/stable/harbor-b3-parking-2026-09-21.md`](../backtests/stable/harbor-b3-parking-2026-09-21.md)。
