# 预注册：习惯双子星重拟合（停车场核心 · H-TWIN-PARK · 2026-09-13）

> **单假设 · 门槛跑前冻结。** 把双子星的**核心腿**从已作废的"择强单轨 trail8"换成**新基线停车场**（S-3 + P1，B11 已验），**卫星腿保持 Live 定义**（same_1430 strict clip4），重拟合 `opp_50`，检验卫星是否仍产生正增量。
> **关键词**：双子星 重拟合 停车场核心 卫星 same_1430 预注册

## 0. 一句话机制（§四 S1）
双子星 = **二元切仓**：卫星无仓日 100% 核心；卫星有仓日 `核心 + 0.5×(卫星 − 核心)`。卫星 edge = S-gap 开盘跳空 + 波动延续（3 日）；核心 edge = S-3 股票 + 闲置 ETF 停车 → 两腿低相关时改善风险调整收益。

## 1. 为什么重开（背景）
- v3.1 clip4 的"核心腿"数字来自 `build_nav_from_cache`（**含 OPT-177 trail8 前视**）→ clip4 全表（+64.7/+51.4/+157.2、Δcore +46.9/+10.7/+18.1）**作废**。
- 新基线已确立：S-3 + 停车场 P1（三窗 **+9.4/+6.0/+9.1**、long **+90.0**，含成本；`etf-parking-baseline-2026-09-13.md`）。
- 卫星审计：交易决策 causal（amp_1430 rank / 14:30 print fill / body-3 退出）；paper intake 有前视（执行层，OPT-178 ⑧）→ **逻辑待用新核心重验**。

## 2. 口径（冻结）
- **核心腿** = S-3 CN 引擎（`S3_CONFIG`，window-local）+ **停车场 P1**（闲置即停、close 代理 14:30、argmax mom60+MA200、因果 trail8、0.05%/边）；NAV 连续、起点 1.0。
- **卫星腿** = `replay_sgap_from_context(strict, skip_t1_limit=True, max_pos=4, position_pct=0.25, body=3, fill_mode=same_1430, fill_hhmm="1430")`；成本 30bps RT（`COSTS_ROUNDTRIP`，OPT-172 已含）；standalone 4×25%。
- **组合** = `blend_nav_opportunity(sat_weight=0.5)`（无仓日 100% 核心；有仓日含 body 退出日）。
- **窗口**：OOS2 / train / valid / **long（2021-08-01~2026-08-07）**（`bar_5min` 2021-01-04 起；卫星窗口内空仓起步）。
- 核心 NAV 对齐卫星日历（前向填充）。

## 3. 判定（冻结）
- **K1（主）**：三窗 Δtotal > 0（每窗，vs 停车场核心）。
- **K2（风险）**：三窗 ΔSharpe ≥ 0 **且** ΔMDD ≤ 0。
- **K3（长窗）**：long Δtotal > 0。
- 全过 → 双子星（停车场版）成立、可进产品讨论；K1 过 K2 不过 → "有增量但风险未改善"（记录，不 Live）；K1 不过 → REJECT。
- **不扫**：sat_weight/slot/body/C1（沿用 Live 定义）。

## 4. Robustness（预声明，只报告不选参）
- `sat_weight` 0.4 / 0.5 / 0.6 三档（报告三窗+long 的 total/sr/dd）。
- 卫星 standalone 指标 + fill 数 / 持仓占用 / active 天数占比。
- 两腿日收益相关（active 日 vs 全窗）。

## 5. 死因预判（跑前写死）
- 主：`#2 共线`（两腿都是 A 股 beta，卫星弱窗 valid +14.5）；`#7 样本`（strict S-gap 稀疏）。
- 次：停车场核心波动降低后 50/50 稀释收益；卫星在长窗早期（2021-2023）规则未验。

## 6. 产出
- 脚本：`scripts/eval_twin_star_parking.py` → `data/backtest_reports/twin_star_parking_2026-09-13.json`
- 结论落 `docs/backtests/stable/twin-star-parking-refit-2026-09-13.md` + SUMMARY + todo。

**结果（跑后补）**：**REJECT（K1/K2/K3 全 FAIL）**。50/50 双子星 vs 停车场核心：OOS2 **+36.5/+1.14sr/回撤改善**；train **−11.8/−0.16**；valid **−28.8/−0.84**；long **−55.6/−0.13 且回撤 −23.6→−48.3**。40/60、60/40 全不过。根因：卫星 standalone 年度 **2022 −34.8% / 2023 −48.0%**、long MDD **−80.6%**（boom-bust），旧 clip4 三窗恰是其 2024+164.7%/2025+77.4% 黄金段 + 含前视核心腿 → 旧"少收益换 Sharpe/回撤"双失真。见 [`twin-star-parking-refit-2026-09-13.md`](../backtests/stable/twin-star-parking-refit-2026-09-13.md)。

> ⚠️ **2026-09-14 修正（两处口径 bug + 双市场日历 · 以上结果作废）**：① §2 冻结的卫星调用缺 Live 习惯参数（`rank_key="amp_1430"`、`exit_hhmm="1430"`、C1 3%），实跑的是全天振幅旧前视键；核心也用了旧本地循环。② 更关键：`daily` 在 09-11 被重灌 qfq 而复权，`bar_5min` 是 raw——老口径「收盘卖」把 qfq 收盘价 vs raw 入场价平仓，制造系统性亏损与 −80% 假回撤。③ 引擎/卫星日历含 HK-only 日期（OPT-183），卫星假期强平、PnL 归零。**clean 重跑：卫星 long +463.6%/MDD −8.4%，双子星 OOS2 +149.7 / train +52.2 / valid +29.1（−21.2 vs 核心）/ long +291.9（Δ+90.4）→ 仍 REJECT，只挂 valid 单窗（K1/K2）+ train Δ 贴 0**。详见 [三策略前视审计 2026-09-14](../backtests/audit-three-strategy-lookahead-2026-09-14.md)。

*冻结于 2026-09-13，跑数前。*
