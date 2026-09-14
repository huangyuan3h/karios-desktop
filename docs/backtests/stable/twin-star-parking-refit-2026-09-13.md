# 习惯双子星重拟合（「港湾」核心）· H-TWIN-PARK · 2026-09-13

> ⚠️ **2026-09-14 修正（两处口径 bug，本档全部作废）**：① 实跑脚本 `eval_twin_star_parking.py` 未传 `rank_key="amp_1430"`（用了全天振幅旧前视键）、缺 `exit_hhmm="1430"`/C1 3%、核心用旧本地循环；② **卫星价格基期混用**——`daily` 在 09-11 被重灌为前复权（qfq），而 `bar_5min` 是原始价（raw），老口径「收盘卖」= qfq_close/raw_entry，系统性亏损 + 假回撤。**「boom-bust / 2022 −34.8% / 2023 −48.0% / long MDD −80.6%」全部作废**（主因是 ②）。
>
> ⚠️ **2026-09-14 三修（OPT-183 · 双市场日历）**：引擎/卫星日历混有 HK-only 日期（CN 假期）→ 卫星假期强平、PnL 归零 + 核心持仓天数加速。**最终 clean 口径**：卫星 long **+463.6%/MDD −8.4%**（各年重算见 standalone 档）；双子星 50/50 = OOS2 **+149.7** / train **+52.2**（Δ+0.0） / valid **+29.1**（Δ−21.2、Δsr−0.65） / long **+291.9**（Δ+90.4）→ **仍 REJECT（valid K1/K2 挂；train Δ 贴 0）**。详见 [三策略前视审计 2026-09-14](../audit-three-strategy-lookahead-2026-09-14.md) · 卫星专档 [`sgap-habit-satellite-standalone-2026-09-14.md`](sgap-habit-satellite-standalone-2026-09-14.md)。

> 核心腿 = 新基线 **「港湾」（Harbor, `harbor-p1-20260913`）**。预注册：[`twin-star-parking-refit-prereg-2026-09-13.md`](../../designs/twin-star-parking-refit-prereg-2026-09-13.md) · 脚本 `scripts/eval_twin_star_parking.py` · 报告 `data/backtest_reports/twin_star_parking_2026-09-13.json`
> **判定（本档旧口径，已作废）**：REJECT（K1/K2/K3 全 FAIL）——卫星腿在停车场核心之上不成立。

## 1. 做了什么
把双子星的**核心腿**从（OPT-177 后作废的）择强单轨换成**新基线停车场**（S-3 + P1），卫星腿保持 **Live 定义**（`strict` S-gap、`skip_t1_limit`、clip4 standalone 4×25%、body=3、`same_1430` 14:30 信号+成交、30bps RT 成本），按 `blend_nav_opportunity(sat_weight=0.5)`（无仓日 100% 核心、有仓日 50/50）重拟合三窗+long。

## 2. 结果（total / CAGR / maxDD / Sharpe）· 2026-09-14 clean 口径（OPT-182 + OPT-183）
| 窗口 | 停车场核心 | 卫星 standalone | **双子星 50/50** | Δcore total | ΔCAGR | Δ回撤 | ΔSharpe |
|---|---|---|---|---|---|---|---|
| OOS2 | +55.2 / 58.0 / −14.3 / 1.73 | +212.7 / 227.8 / −3.3 / 7.44 | **+149.7 / 159.3 / −9.2 / 4.54** | **+94.5** | +101.3 | 改善 5.1 | **+2.81** |
| train | +52.2 / 138.2 / −8.0 / 3.01 | +40.3 / 101.1 / −8.1 / 3.50 | **+52.2 / 138.0 / −6.8 / 3.98** | **+0.0** | −0.2 | 改善 1.2 | +0.97 |
| valid | +50.3 / 156.6 / −21.8 / 2.14 | +14.7 / 37.3 / −6.5 / 2.06 | **+29.1 / 80.5 / −21.8 / 1.49** | **−21.2** | −76.1 | 持平 | **−0.65** |
| **long** | +201.5 / 25.7 / −22.8 / 1.00 | +463.6 / 43.1 / −8.4 / 3.50 | +291.9 / 32.8 / −20.8 / 1.45 | **+90.4** | +7.0 | 改善 2.0 | +0.45 |

- **判定**：K1（三窗 Δtotal>0）FAIL（train +0.0 贴线、valid −21.2）；K2（三窗 ΔSharpe≥0 且回撤不劣化）FAIL（valid −0.65）；K3（long Δtotal>0）PASS（+90.4）→ **REJECT**（与 clean 前的"只挂 valid"结论一致）。
- **sat_weight robustness（预声明，只报告）**：40/60 → OOS2 +127.4/3.91、train +52.3/3.84、valid +33.2/1.63、long +273.5/1.36；60/40 → +174.0/5.16、+51.9/4.05、+25.1/1.34、long +310.4/1.53。**没有任何档位能过 valid**。
- 卫星占用不低：active OOS2 67% / train 57% / valid 27% / long 52%；`avgHeldDays` 3.0、long 1036 笔 → **不是"空槽稀释"，是策略本身。**

## 3. 根因（⚠️ 旧叙事已作废）
~~卫星是 boom-bust 腿，2022 −34.8% / 2023 −48.0%，旧验证窗恰是它的黄金期~~ → **该叙事由 qfq×raw 基期混用 + 旧前视键造成，已作废**（clean 后卫星每年正收益，见 [`sgap-habit-satellite-standalone-2026-09-14.md`](sgap-habit-satellite-standalone-2026-09-14.md)）。
- 现行 REJECT 的真实原因：**valid 单窗卫星只有 +14.7%（核心 +50.3%）** → 稀释核心（Δ−21.2 / Δsr−0.65），叠加上文 train Δ 贴 0。edge 有 regime 依赖，早段（2021–23）又是事后样本。
- 停车场核心已是低波/高 Sharpe（1.73–3.01）：卫星不再提供风险平滑，只贡献它自己的尾部风险。

## 4. 结论 / 下一步
- **卫星腿（现行 strict S-gap/amp_1430/body-3 配方）不进新基线**；Live 默认 `twin_star` 是否保留卫星需产品决策（见 OPT-178/新基线记录）。
- 若还要救卫星：必须**带全周期门槛**重新预注册（2022-23 压测），并先解决"为何只在 2024-25 有效"（regime 依赖/池子结构）。**不允许在现有配方上调参重扫。**
- 原计划下一步 **B1 ETF 买持基准**：改为对 **停车场基线（不含卫星）** 跑，并补 510050/588000。

## 5. 关联
- 新基线：[`etf-parking-baseline-2026-09-13.md`](etf-parking-baseline-2026-09-13.md)（B11）
- 旧双子星（作废数字）：`docs/backtests/core/state-bucket-algo-2026-08-31.md` §3.0; OPT-177 审计：[`audit-trail8-2026-09-12.md`](../audit-trail8-2026-09-12.md)
- 卫星时钟：[`sat/sat-clock-unify-1430-2026-09-11.md`](../sat/sat-clock-unify-1430-2026-09-11.md)
