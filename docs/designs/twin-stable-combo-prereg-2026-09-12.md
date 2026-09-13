# 预注册：双子星 × 多资产稳健核心 · 组合层风险预算（H-COMBO · 2026-09-12）

> **单假设 · 结构选择 · 门槛跑前冻结。** B7 独立结构（B3 波动率倒数，Sharpe 1.47/CAGR 7.3%/MDD −7.3%）与双子星（clip4 opp_50）在**组合层**配比，目标**降回撤/提 Calmar**。
> **关键词**：组合层 风险预算 双子星 稳健核心 分散 预注册

## 0. 一句话机制（§四 S1）
双子星（高收益、高波动 alpha）与 B3 稳健核心（低波动、低相关）组合 → **分散红利**：组合 MDD 显著低于纯双子星，Sharpe/Calmar 改善或不大幅恶化。这是 `dh1-s3-hybrid` 指明的"两腿组合层配置"路线。

## 1. 与现有结构的关系
- 双子星 = 现有 Live（择强单轨 core trail8 + strict S-gap clip4，`blend_nav_opportunity` opp_50）。
- B3 = B7 独立结构（5 资产波动率倒数月度，2021+）。
- 本页**只在组合层做配比**，不改任一腿内部规则。

## 2. 口径（冻结）
- 两腿日 NAV 对齐到共同交易日；T=twin（clip4 opp_50），S=B3。
- 配比**月度再平衡**（每月首日），权重漂移后调回，成本单边 5bp（按 |Δw|）。
- 窗口：OOS2/train/valid/**long 2021-01-04~2026-08-07**（T 长窗有 survivor 注记，见 §5）。

## 3. 组合（预声明 5 个，A/B 对打）
| # | 配比 |
|---|------|
| T0 | 100% 双子星（基线） |
| T1 | 80% T + 20% S |
| T2 | 60% T + 40% S |
| T3 | 50% T + 50% S |
| T4 | 风险预算（月度，w ∝ 1/σ60，两腿） |

## 4. 指标 / 判定（冻结 · long 窗为准）
- 指标：CAGR、年化波动、Sharpe、MDD、Calmar、两腿相关性。
- **"略稳达标"**：存在组合满足 **MDD ≤ T0.MDD + 10pt**（即回撤至少浅 10pt）**且** Sharpe ≥ T0.Sharpe − 0.10 **且** CAGR ≥ 0.5 × T0.CAGR。
- 命中 → 记"可用稳健组合"（可议是否进产品）；不中 → 记"无稳健增益"。
- **不扫**配比/窗口/频率/成本。

## 5. 死因预判（跑前写死）
- 主：**双子星长窗数据有 survivor/口径注记**（见 strategy-params §1 长窗待重跑）→ 长窗结论**保守看待**，以 OOS2/train/valid 为交叉验证。
- 次：`#2 共线`（双子星核心本就含防守/多资产，可能与 B3 重叠）；B3 只有 2021+。

## 6. 产出
- 脚本：`scripts/diag_twin_stable_combo.py` → `data/backtest_reports/twin_stable_combo_2026-09-12.json`
- 结论落 `docs/backtests/stable/twin-stable-combo-2026-09-12.md` + SUMMARY 一行。

## 结果（跑后补）
**达标组合 = {T2 60/40, T3 50/50, T4 风险预算}**；两腿日相关 **0.273**。**T3 50/50** long：CAGR 8.95%（vs T0 9.42）/ Sharpe **0.72**（vs 0.49）/ MDD **−16.9%**（vs −39.9）/ Calmar 0.53（vs 0.24）。**T4 风险预算**（≈83% B3）：CAGR 8.53 / Sharpe 1.34 / MDD −5.76。三窗 Sharpe 全面≥T0、MDD 全面更浅；代价=valid 强市绝对收益。见 [`twin-stable-combo-2026-09-12.md`](../backtests/stable/twin-stable-combo-2026-09-12.md)。

*冻结于 2026-09-12，跑数前。*
