# 预注册：星舰空闲现金停放（H-SAT-IDLE · 2026-09-15）

> **单假设 · 门槛跑前冻结。** 问：把**星舰（卫星 standalone）**的空闲现金（无仓日 100% + 未用槽），按 **T−1 收盘因果口径**停进 X，能否在「只追求收益」口径下提升整体收益？
> **关键词**：星舰 空闲现金 资金利用 因果停放 卫星锚 预注册

## 0. 一句话机制

卫星腿 100% 名义、4×25% 槽；无仓/空槽日现金收益 = 0。把 `w_t = 1 − 0.25 × satPositions[t−1]`（昨日收盘可观测）这部分停进 X：
`port_ret_t = sat_ret_t + w_t × x_ret_t − c × |w_t − w_{t−1}|`。
**不动卫星信号/参数**，只改闲置现金的去处（= 港湾停车场同一思想，作用对象换成卫星）。

## 1. 为什么开（背景）

- 星舰 standalone 是当前**收益最高**的腿（clean：+212.7/+40.3/+14.7/**+463.6**，SR 7.44/3.50/2.06/3.50，MDD −3.3/−8.1/−6.5/−8.4）；但 **idle 巨大**：无仓日占比 OOS2 33% / train 43% / valid 73% / long 48%（H-SAT-W §2），有仓日均填 3.3–3.8/4 槽 → long 口径平均空闲 ≈ 50% NAV-日——**从未有任何实验直接测过 standalone 的 idle 停放**。
- 已死 ≠ 本档：
  - **H-SAT-W**（锚=港湾，固定 w≤0.50 网格）REJECT：valid 稀释 ∝ −42.4×w、K1/K2 死区「无解」；其 §5 明确「更大 w / 其他资本结构 **必须另起预注册、重定义判据**」。本档 = **卫星锚 + 资本效率**（只动现金，不改曝露锚点），不是港湾加卫星。
  - **twin-idle（2026-09-12）**：50/50 blend 内卫星套筒 idle 仅 **2.4–5.7%**（active 日），闲置→core OOS2 −9.3、闲置→REPO ±0.0 → 那个问题太小且被 blend 吸收；**standalone 的 idle 不在此列**。
  - **DH-2 教训**「闲置资金入 beta = 结构性逆势」：那是 **S-3 自己判定市场差**时把现金投 beta；本档卫星主腿照旧、X 自带风控（港湾核心四窗全正，含 ETF 停车场），机制不同——但 valid 卫星 idle 73% 与 X 弱窗重叠仍是本档第一死因预判（见 §5）。
  - 参考价目（只读）：港湾核心 long **+201.5**；ETF 停车场单腿 long **+63.4**（B13）；REPO 0.7%/yr。

## 2. 口径（冻结）

- **卫星腿** = `replay_sgap_from_context`（habit：`amp_1430` + C1 3% + `same_1430` + `gate_1430` + body=3 + 30bps RT，100% 名义、4×25% 槽，0 改动）。
- **停放权重（因果）**：`w_t = clamp(1 − 0.25 × satPositions[t−1], 0, 1)`；**不使用同日 label**。同日 `satActive` 版（legacy `blend_nav_opportunity` 惯例）只作描述行 `A1_legacy`，**不参与裁决**。
- **转移成本**：`c = 5bps/边`（基准，对齐港湾停车场 0.05%/边口径）× `|Δw|`（停放本金进出）；敏感性 `15bps/边` 只报告。
- X 三臂（**冻结，不扫**）：
  - **A1 = 港湾核心**（`eval_twin_star_parking._parking_core_by_day`，canonical `parking_replay`，含内部成本）；
  - **A2 = ETF 停车场单腿**（mom60+MA200 argmax、因果 trail8、4 ETF、5bps/边）；
  - **A3 = REPO**（0.7%/yr，对照）。
- **窗口** = OOS2 / train / valid / long（`run_walk_forward.WINDOWS`）；**holdout 只描述、不裁决**。
- **不扫**：卫星/核心口径、w 网格、停放资产菜单、转移成本网格（只报 2 档）、窗口切分。

## 3. 判定（跑前冻死 · 收益口径，用户前提「只追求收益」）

- **K1（不劣化任何窗）**：OOS2/train/valid 三窗 `Δtotal` vs **星舰 standalone** 全部 **≥ 0**。
- **K2（实质量级）**：long `Δtotal` **≥ +100pt**。
- **K3（记录，不 binding）**：四窗 MDD/Sharpe/Δ 全报；long 与 worst-window 的 ΔMDD 披露（收益口径下不作为否决线，但必须明示）。
- **裁决**：仅 **A1** 参与判定；K1+K2 满足 → **PASS（卫星锚资本结构候选）**，否则 **REJECT**。A2/A3/A1_legacy 为**价目表**（只报价不裁决）。
- **锚点披露（写死）**：本档 PASS **不等于**给港湾加卫星（港湾锚 = H-SAT-W 已 REJECT）；A1 是「卫星锚 + 空闲现金入港湾」的**新资本结构**，落地需用户拍板 + paper 前瞻。

## 4. Robustness（预声明，只报告不选参）

- 14:30 起效的敏感性：转移成本 15bps/边下的四窗 Δ。
- 描述行 `A1_legacy`（同日口径）与主行差 → 量化「因果滞后」的价格。
- 每窗 idle 占比 / 无仓日占比 / active 日均槽位 / fills；两腿日收益相关（全窗 + 卫星 idle 日切片）。
- long 年度分解（2021–2026 逐年 Δ）。

## 5. 死因预判（跑前写死）

- 主：**#2 共线/regime 预算冲突**——valid 卫星 idle 73%，若港湾核心在**卫星 idle 日**（弱广度日）的收益为负，K1 在 valid 直接挂。
- 次：**#4 换手摩擦**——卫星 ~1.6 次/日槽位变动 → 转移成本 ≈ 2–5%/yr（5bps）/ 6–15%/yr（15bps），可能吃掉 long 增量。
- 再次：**#7 样本**——holdout 数据仅至 2026-09-11、n 小，只描述。

## 6. 产出

- 脚本：`scripts/eval_sat_idle_parking.py`（只读）→ `data/backtest_reports/sat_idle_parking_2026-09-15.json`。
- 结论落 `docs/backtests/stable/sat-idle-parking-2026-09-15.md` + `SUMMARY.md`；**Live 不动**。

**结果（跑后补 · 2026-09-15）**：**REJECT**。A1（主假设）K1 ✅ 三窗 +26.4/+10.2/+1.6、**K2 ❌ long −93.3**（MDD −8.4→−27.6）。死因 = 预判 **#2 实锤**：卫星空仓日 = 弱广度日，港湾核心在这些美元日 **≈ −4%/yr**（核心整体 +25.7%/yr），年度仅 2025 +34.2（2021/2022/2026 全负）。价目表：A2 套筒 long +152.6 但 OOS2 −1.6 / valid −4.1 / MDD −30.5、15bps 转移成本下 +51.1；A3 REPO −31.3（摩擦 1.6%/yr > 回购 0.4%/yr）；A1_legacy（同日 label）−161.9 更差。结论落 [`../backtests/stable/sat-idle-parking-2026-09-15.md`](../backtests/stable/sat-idle-parking-2026-09-15.md)。

*冻结于 2026-09-15，跑数前。*
