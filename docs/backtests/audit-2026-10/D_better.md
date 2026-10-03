# Karios 审计 D：有没有更好的策略或收益结构（只读对手审计，2026-10-03）

## 0. 结论在最前

- **没有确定更好的。** 在同一口径（扣费后、同期窗口、同 100% 名义上限）下：
  - **长期（long 2021-08~2026-08）+风险调整后：星舰 B 仍是最优**（+729.9%/MDD −5.5/SR 4.15/Calmar ~10.0），高于 H2-a25（+803.3 但 MDD −8.4/SR 3.73/Calmar 6.9，K3 FAIL）、纯 B3 组合（+713.4/−6.8/3.93）、港湾（+201.5/−22.8/1.00）、母港 M50（+116.3/−11.6/1.18）、星舰 H2 100%（+1040.4 但 MDD −29.3/SR 2.22）。
  - **最近段（valid 2026-03~08、holdout 2026-08-08 起）反过来：星舰 B 最弱之一**（valid +5.9/SR 0.83/holdout −19.8；本审计同 vintage 重跑 valid +3.5/SR 0.53/holdout −19.1），港湾（valid +50.3/holdout −2.5）、母港（valid +25.6/holdout −2.5）反而更抗跌。long 的 +729.9 靠 2024（+200）+2025（+141.8）两年主体，2026Q2 +3.1/Q3 +1.9 基本走平（`starship-b-oos-decomp-2026-09-29.md:22-62`）。
  - **所有“看起来更好”的候选都只是沿收益×风险前沿滑动**：H2-a25（多 73.4pt 收益，多 2.9pt 回撤）、m10（+702.4 但 Calmar 9.58→8.71，用户已不采纳）、top2（修 K3 但 valid 破）、90% 星舰 B +10% 港湾/B3/REPO（本审计 D1：收益同比 −10~−15%、Sharpe 不动）、波动率目标（D2）、回撤刹车（D3）、B 腿季频（D3b：−12pt）。无一在 valid+holdout 样本外同时改善收益、回撤、Sharpe。
- **组合层面唯一成立的是“买保险”**：50% 星舰 +50% B3（SR 3.78/MDD −3.6/long +193.5，`sat-capital-split-2026-09-15.md:37`）、母港 M30（+152.5/−16.4/1.10，`homeport-weight-tune-2026-09-16.md`）、星港 w=0.2（+169.0/−11.2/1.62，pre-dense）都是拿 ~20~55% 长期收益换一半回撤，不加收益。闲置停泊中 B 腿（3-ETF 逆波）已是最优，H2 套筒（MDD −28.7）、指数池（−39.0）、REPO（摩擦 1.6%/yr）都不如它。
- **小资金验证建议（仅审计意见，不改代码）**：维持 Live=港湾；星舰 B（含 H2-a25）继续 paper 3/20+风险授权前不进 Live；若要试“更好”，唯一值得小资金（≤50 万，见审计 C 容量）试的是 **星舰 B 90% + REPO/货基 10% 月再平衡**（本审计 D1：long +578.6/−5.0/4.14，valid +3.2，holdout −17.3，换手极低，成本 5bp/边）或 **母港 M30**（现货防守档，年化 21%、MDD −16.4），目标不是更高收益而是更浅回撤；波动率目标/回撤刹车不建议试（样本外无效）。

> 口径锚：`docs/modules/strategy-recipes.md:54-64,160-166`；唯一现状 `docs/backtests/rebaseline-dense-2026-09-29.md:18-31`（post-dense：星舰 B OOS2/train/valid/long +221.9/+71.4/+5.9/+729.9，MDD −5.5 SR 4.15 stress +114.7/−7.8 holdout −19.8；2026-09-28 前旧值 +669.6/SR 3.90、stress +124.0 作废）。
> 审计纪律：只读。未改 karios-desktop / karios-research 任何代码配置 DB；未下单未连券商；未用 Crimson 代码数据；未开可见浏览器；未读 .png/.jpg。跑代码均在 `~/Projects/wealth-ideas/karios-audit-2026-10/scratch/d_better.py`（D1/D2/D3/D3b，复用审计 B 的 `nav_*.json` + 3-ETF 面板小量只读），DB 除 D3b 面板外零新查询，导出 <5MB（<3GB 红线）。karios-research 的 `indicator_series` 与星舰 B 无关（见审计 A §8）。

---

## 1. 星舰 B 是什么（对手审计复述，证据链）

| 环节 | 现行定义 | 证据 |
|---|---|---|
| 卫星腿 | HABIT：4 槽×25%、S-gap open/pre−1>3%、`amp_1430` 升序前 1/3、`gate_1430` breadth>0.5、C1 14:30/open−1>3% 跳过、body=3 第 3 日 14:30 出、32.28bp/笔 | `strategy-recipes.md:54-64`；`state_bucket_track.py:33-52` |
| 闲置腿 | 100% `{511260 国债,518880 黄金,513100 纳指}` 逆波 60d 月频 5bp/边，`homeport.starship_b_run` | `strategy-recipes.md:162`；`homeport.py:162-213` |
| 合成 | `r_t=sat_ret+w_t·bRet−5bp·\|Δw\|`，`w_t=cashShare(T−1)` 因果 | `strategy-recipes.md:144`；`eval_starship_b.py:144-147` |
| 成本 | 卫星静态 32.28bp（未套 1-tick）、转移 5bp×\|Δw\|、B 腿月调仓 5bp×换手 | `strategy-recipes.md:74-83` |
| 窗口 | OOS2 2024-08~2025-08 / train 2025-08~2026-02 / valid 2026-03~2026-08-07 / long 2021-08~2026-08-07 / stress 22-23 / holdout 2026-08-08~ | `backtests/README.md`；`run_walk_forward.py:WINDOWS`；审计 B `b_baseline.json` |
| 实盘 | Live 仍=港湾；星舰 B 为研究/展示/人工默认档，进 Live 前置 paper 3/20+风险授权（未满） | `strategy-recipes.md:101-102`；`AGENTS.md:209` |

本审计复用基线（`scratch/b_baseline.json`，2026-10-03 vintage，同配方）：OOS2 +229.8/−4.7/7.26（293 fills）、train +71.6/−4.5/6.22（124）、valid +3.5/−10.0/0.53（52）、long +731.3/−5.5/4.15（1078）、stress +110.9/−7.8/2.71（380）、holdout −19.1/−19.8/−4.59（52 fills/37 天）。与冻结 post-dense 差 ±8pt（OOS2 +7.9、valid −2.4），方向有正有负，属数据版本噪声；下文 Δ 一律以同 vintage base 为准。sat/park 拆分 long：卫星 +512.8、停放 +46.7（`b_baseline.json:long`）。

---

## 2. 横向比较：谁更好（同口径：扣费、同期、同资金）

口径说明：扣费=卫星 32.28bp/笔 + 组合转移/B 腿 5bp/边（`strategy-recipes.md:74-83`）；同期=OOS2/train/valid/long/stress/holdout 同日期（`rebaseline:18-25`）；同资金=100% 名义上限（卫星 4×25%、港湾 S-3 10×10%、母港 50/50 名义、星舰系 cashShare 现金权重但上限 100%，`strategy-recipes.md:40,48,121,144`）。卡玛=年化/回撤（本审计由 total/MDD 推年化，n/252 年，见 `d_better.py:stats`）；换手=卫星 fills + 停车月调仓 turns（`d_rebalance.json:turns 57 月频/19 季频`；港湾 P1 trades 30/23/21/long127，`five-strategy-v2-scorecard:121-136`）。

### 2.1 主表（post-dense 为准，pre-dense 标作废）

| 策略 | OOS2 | train | valid | long | long MDD/SR/Calmar | 换手（long） | 裁决 |
|---|---|---|---|---|---|---|---|
| **星舰 B（默认）** post-dense | +221.9/−4.8/6.99 | +71.4/−4.5/6.15 | +5.9/−10.0/0.83 | **+729.9** | −5.5/4.15/**10.0** | 1078 fills + B 腿 57 turns | 研究/展示并列，不给 PASS（`starship-b-2026-09-24.md:94-95`） |
| H2-a25 post-dense | +225.8/−4.7/6.63 | +76.7/−4.5/6.28 | +4.0/−9.1/0.65 | +803.3 | −8.4/3.73/6.9 | 同卫星 + H2 换手（H2 年换手 −52% vs 0pt，`harbor-h2-unify`） | **REJECT（仅 K3 FAIL −1.6pt，阈值 −1.0）**（`sat-h2-a25-2026-09-24.md:40-47`） |
| 纯 B3 组合（卫星+100% B3 5-ETF，非 B3 standalone）post-dense | +225.2/−4.6/6.82 | +69.3/−4.5/5.89 | +3.5/−10.1/0.54 | +713.4 | −6.8/3.93/8.0 | 同上 | 对照（`rebaseline:18-25`） |
| 星舰 H2 100%（卫星+100% H2 套筒）post-dense | +224.3/−8.6/5.03 | +100.4/−6.6/6.25 | +1.3/−29.4/0.29 | +1040.4 | −29.3/2.22/2.2 | 同卫星 + 套筒高换手 | 激进对照，不进 Live |
| 港湾 Harbor clean（S-3+闲置 H2） | +55.2/1.73/−14.3 | +52.2/3.01/−8.0 | **+50.3**/2.14/−21.8 | +201.5 | −22.8/1.00/1.1 | 127 trades（S-3）+ 停车月调 | **PASS K1-K3**（`etf-parking-baseline-2026-09-13.md:23`） |
| 母港 M50 clean（港湾×B3 50/50 月再平衡） | +36.5/1.90/−9.0 | +35.0/3.52/−5.0 | +25.6/2.26/−11.4 | +116.3 | −11.6/1.18/1.5 | 月再平衡 5bp/边 | **PASS K1-K5**（`harbor-riskbudget-2026-09-13.md:31-37`） |
| 母港 M30 clean（70% 港湾） | +43.9/1.81/−11.0 | +42.0/3.25/−6.3 | +35.4/2.18/−15.7 | +152.5 | −16.4/1.10/1.3 | 同上 | **PASS，防守档**（`homeport-weight-tune-2026-09-16.md`） |
| 星港 0.2 pre-dense（母港 M50×卫星，作废绝对值） | +65.3 | +38.2 | +21.0 | +169.0 | −11.2/1.62/— | 名义 w overlay | 冻结 PASS（1/3），当前应为 0.2（`harbor-b3-sat-2026-09-14.md:77-80`；`rebaseline:51` 未复跑） |
| B3 standalone（5-ETF 逆波，非组合） | — | — | — | +46.2/−4.7/1.76 | Calmar ~1.7 | 月调仓 5bp | 对照（`sat-idle-b3-2026-09-21.md`） |
| B 腿 standalone（3-ETF 逆波，本审计 D3b 重算 long） | — | — | — | +46.7/−3.5/2.26 | 57 turns | 对照（`scratch/d_rebalance.json`） |

证据：主数字 `rebaseline-dense-2026-09-29.md:18-31`；港湾 `etf-parking-baseline-2026-09-13.md:10-25`；母港 `harbor-riskbudget-2026-09-13.md:8-37`；M30 `homeport-weight-tune-2026-09-16.md`；星港 `harbor-b3-sat-2026-09-14.md:19-45,77-80`；H2-a25 `sat-h2-a25-2026-09-24.md:22-47`；星舰 B `starship-b-2026-09-24.md:31-66`；五策略 `five-strategy-v2-scorecard-2026-09-16.md:12-46,121-136`。

读法（对手视角，先说坏消息）：
- **收益**：long H2-a25（+803.3）> 星舰 B（+729.9）> 纯 B3 组合（+713.4）> 星舰 H2（+1040 但回撤崩）。OOS2/train 亦 H2-a25 领先（+225.8/+76.7 vs +221.9/+71.4）。但 H2-a25 的 K3（long MDD 相对纯 B3 −1.6pt）FAIL，总 REJECT；且 valid（+4.0 vs +5.9）、stress（+103.5/−10.1 vs +114.7/−7.8）、Sharpe（3.73 vs 4.15）全输星舰 B。
- **回撤/Sharpe/Calmar**：星舰 B 全最优（−5.5/4.15/10.0 vs H2-a25 −8.4/3.73/6.9 vs 纯 B3 −6.8/3.93/8.0）。m10（pre-dense +702.4/−6.2/3.86，`starship-b-2026-09-24.md:43-57`）看似多 32.8pt，但 Calmar 9.58→8.71、Sharpe 3.90→3.86 变差，用户 2026-09-25 已不采纳。
- **最近段反转**：valid 港湾 +50.3、母港 +25.6、星港 0.2 +21.0，全碾压星舰 B +5.9；holdout 港湾 −2.5、母港 −2.5 vs 星舰 B −19.8（`five-strategy:12-21` + `rebaseline:25`）。OOS 拆解 2026Q2 +3.1/Q3 +1.9、2023Q3 −3.0（`oos-decomp:40-62`）。只引 long 是挑选窗口（审计 B §2.3 t 检验 valid p≈0.58~0.73，Bonferroni 后 1.0）。
- **换手**：星舰 B long 1078 fills（~225/年，每笔 25% 槽位，组合换手 ~112%/年）远高于港湾 127 trades；B 腿 57 turns（月频）vs 季频 19 turns，收益差仅 −12pt（见 §4 D3b）。成本敏感性：+15bps/笔 ≈ −40pt long（`loss-analysis-2026-09-24.md:308-313`），低价股 1-tick 未建模（审计 C P0-2）。

一句话：**要最高长期收益选 H2-a25（但 REJECT 且更抖），要熊市韧性+可复制（3 只 ETF）+最高 Sharpe 选星舰 B，要最近段抗跌选港湾/母港。没有全窗全优的“确定更好”。**

### 2.2 变体清点（都沿前沿滑动，无免费午餐）

- 臂 B amp≤1%（`loss-analysis §7`）：valid +5.4→+12.0、dd −8.2→−2.6，但 long −74pt/CAGR −3.2pt，K1/K2 FAIL=少下注换波动。
- A1 m25（`starship-b §7`）：long +750.8 但 dd −7.4 破 K2，REJECT；m10 规则 PASS 但不采纳（见上）。
- A2 扩池（`loss-analysis §7`）：base3 最优，扩池全面降质，REJECT。
- top2-a25（pre-dense `h2-sleeve-top2-2026-09-24.md:19-38`）：long +712.2/−6.7/3.69，K3 修好但 valid −0.3 破旧 K1，条件 PASS，不进 Live。
- H-STARB-COMBINE（pre-dense `h-starb-combine-2026-09-26.md:32-60`）：无臂超 long（max A4 c0.9 +626.3<+669.6），A1/A4 c0.9 逼近（long ≥600/MDD ≥−8.4/SR ≥3.50/valid ≥0/stress ≥100），让 6~9% long 换 valid +5.4→~+9.8。方向不变（post-dense 亦单调）。

---

## 3. 组合结构评估

### 3.1 低相关：真实但已被 cashShare 吃掉

本审计 D1（`scratch/d_better.py:exp_d1`，复用 `nav_*.json` 日收益）：

| 窗 | sat-park 相关 | starB-sat 相关 | starB-park 相关 | 均现金 w |
|---|---|---|---|---|
| OOS2 | −0.039 | — | — | 0.704 |
| train | −0.083 | — | — | 0.554 |
| valid | +0.154 | — | — | 0.751 |
| long | **+0.021** | — | — | 0.841 |
| stress | +0.015 | — | — | 0.745 |
| holdout | +0.411 | — | — | 0.270 |

证据：`scratch/d_blend.json`。long 相关仅 0.021（`python3 -c` 直算亦 0.021，见过程日志），valid 0.154，holdout 0.411（同跌时相关跳升，n=37 小样本，不确定是否稳定）。

对照档：双子星×B3 日相关 0.273（`twin-stable-combo-2026-09-12.md`）；港湾×卫星 active 日 0.08~0.23（`harbor-sat-weight-2026-09-14.md`）；星舰系 sat-capital-split long 相关 港湾 0.09/母港 0.10/B3 0.09/套筒 0.00（`sat-capital-split-2026-09-15.md`）。

结论：卫星 vs 停放腿确为低相关，分散化理论成立。但星舰 B 已用 `w_t=cashShare(T−1)`（均值 long 0.84、OOS2 0.70）把分散化吃满；再在外层配 10~50% B3/港湾/REPO（H-SAT-W 7 档、`sat-capital-split` 价目、`h-starb-combine`）收益前沿单调（c=1.0 纯星舰最高），只能买保险不能加收益（见 §3.3 D1）。

### 3.2 闲置停泊：B 腿已是最优，H2/指数/REPO 都不如它

- B 腿（3-ETF 逆波，月频，5bp）：standalone long +46.7/−3.5/2.26（本审计 `d_rebalance.json:park_monthly`），与 B3 standalone +46.2/−4.7/1.76（`sat-idle-b3-2026-09-21.md`）相当，但更少一只 ETF 好复制。
- H2 套筒：standalone +89.1/−28.7/0.68（`sat-idle-b3-2026-09-21.md`），long 高但 MDD −28.7 为复发型（2026-03~07 原油±10% 来回止损 5 次，`sat-offense-grid-2026-09-16.md`），valid 内 −28.7 自体回撤拖累星舰 v2 valid −0.3（`five-strategy:45-46`）。
- REPO：long +3.4（0.7%/yr），摩擦 1.6%/yr>0.4%/yr（`sat-idle-parking-2026-09-15.md:§2`），50% 部分停车 long +681/−15.9/2.88 中间档。
- 指数池（+OIL+恒科/有色）：OOS2 +14.4 但 train/valid/long −20.4/−2.7/−69.3，MDD −39.0，REJECT（`sat-idle-indexpool-2026-09-21.md`）；B3 菜单 +OIL/恒科/有色全面降质（`sat-idle-b3-menu-2026-09-21.md`）。
- 港湾侧换 B3：valid +50.3→+34.1（−16.2pt），REJECT（`harbor-b3-parking-2026-09-21.md`），因 S-3 空仓日=弱广度日 B3 不给钱（#2 regime）。

### 3.3 仓位管理：波动率目标、回撤刹车、水位节流全 REJECT，国家队闸休眠

- DH-2 真实指数+vt20（`a1-voltarget-beta-2026-09-11.md`）：中证 500 +7.3%/yr、300/500+vt20 +4.6%/yr/DD −46%/SR 0.33，无前视可交易正年化，但与 S-3 结合四窗不一致（OOS2/train 改善、valid/long 劣化，`dh1-s3-hybrid-2026-09-11.md:§5-6`），闲置入 beta 结构性逆势，REJECT。
- 方向指引收紧止损（同档 §7）：MA200 几乎不触发（S-3 regime 早空仓冗余），MA60/20 OOS2 劣化（46.5→38.4/31.6 砍右尾），REJECT。
- 水位节流 θ{7,10,12}×{half,pause}（`product-post-peak-drift-2026-09-09.md:§10`）：6/6 REJECT（long −13.9~−120.6pt，−12 档锁死 22 个月复苏段），死于解除动力学。
- 国家队闸（`risk-state-sensors-2026-09-09.md:§6.3`）：三窗 +0.0 惰性、long +20.0（CN 78.6→98.6），协议 PASS 休眠零成本，已冻结。
- 本审计 D2/D3（见 §4）：波动率目标与回撤刹车在 OOS2/train/long 几乎不触发或降收益，valid 全降，holdout 仅因少曝露少亏。结论与冻结档一致：**仓位管理只能沿前沿滑动，不能免费加 Sharpe。**

### 3.4 再平衡频率：月频已是最优，季频−12pt

本审计 D3b（`scratch/d_better.py:exp_d3_rebalance`，面板只读，`.venv/bin/python`，`d_rebalance.json`）：
- B 腿 standalone long：月频 +46.69/−3.5/2.26（57 turns）vs 季频 +43.86/−3.5/2.02（19 turns），Δ −2.83pt。
- 星舰 B 合成 long：月频 +731.33/−5.51/4.15 vs 季频 +719.29/−5.48/4.12，Δ −12.04pt。
- 对照：park lookback 42/78（审计 B `b_params_long.json`）：OOS2 227.7/230.0（≈base 229.8），valid 0.3/−2.0（vs base +3.5），60d 最优。

结论：月频 60d 逆波为真平台，降频省换手但丢收益，不构成更好结构。

---

## 4. 新思路小规模预实验（A 股可交易约束，样本内外+扰动）

可交易约束（先声明）：T+1（信号 T−1 收盘→T 日生效，本审计 vol/brake 均用 `scale[t−1]` for return t，`d_better.py:vol_scale/brake_scale`）、无杠杆（scale≤1.0）、无做空（只做多+REPO/货基）、100 股整数手与 >¥5M 冲击未建模（审计 C P0-3/P1-1，小资金 ≤50 万假设下可忽略，>200 万需另算）、成本 5bp×|Δscale|（与星舰转移同口径）、unanimous 因果（trailing 窗只用 ≤T−1）。

样本划分：选参窗=OOS2+train（in-sample），验证窗=valid（out-of-sample），纯样本外=holdout（未满 37 天，描述性），long/stress 为参考（复用偏差，不作选参据）。扰动要求：每个候选必须过阈值±33% 与窗口 20↔60d。

### D1 静态降曝露（90/80/50% 星舰 B + REPO/货基，月再平衡 5bp/边）

- 做法：`blend_two(nav, repo=1.0, wA, monthly)`（`d_better.py:blend_two`），REPO=货基代理（0 收益，T+1 可申赎）。
- 结果（`scratch/d_blend.json`，同 vintage base OOS2 +229.8/train +71.6/valid +3.5/long +731.3/stress +110.9/holdout −19.1）：

| 窗 | base | 90%+REPO10% | 80%+20% | 50%+50% | 90%+park10% |
|---|---|---|---|---|---|
| OOS2 | +229.8/−4.7/7.26 | +195.5/−4.2/7.27 | +164.3/−3.8/7.27 | +86.8/−2.4/7.27 | +198.4/−4.4/7.26 |
| train | +71.6/−4.5/6.22 | +63.1/−4.1/6.22 | +54.9/−3.7/6.22 | +32.1/−2.3/6.21 | +65.2/−4.1/6.34 |
| valid | +3.5/−10.0/0.53 | +3.2/−9.0/0.52 | +2.8/−8.0/0.52 | +1.8/−5.0/0.50 | +3.5/−10.0/0.54 |
| long | +731.3/−5.5/4.15 | +578.6/−5.0/4.14 | +453.0/−4.4/4.14 | +195.7/−2.8/4.12 | +604.3/−5.0/4.18 |
| stress | +110.9/−7.8/2.71 | +96.5/−7.1/2.71 | +82.9/−6.3/2.71 | +46.8/−4.0/2.71 | +98.0/−7.1/2.71 |
| holdout | −19.1/−19.8/−4.59 | −17.3/−18.0/−4.60 | −15.5/−16.0/−4.62 | −9.8/−10.2/−4.67 | −17.3/−18.1/−4.52 |

- 读法：收益严格单调（降 10% 曝露≈降 ~15% long total），Sharpe 几乎不动（7.27/6.22/0.52/4.14），MDD 同比浅，Calmar 微降（long 10.01→9.79→9.57→8.96）。90%+park10%（+604.3/−5.0/4.18）略好于 90%+REPO，但仍 <base。**样本外 valid/holdout 亦无 Sharpe 改善**，扰动 90↔80↔50 方向一致。裁决：**REJECT（买保险，非更好）**，与 `sat-capital-split`（收益前沿 c=1.0 单调）与 `h-starb-combine`（无超越仅逼近）一致。

### D2 波动率目标（target 15% ann，trailing 20/60d，只减不增，无杠杆）

- 做法：`vol_scale(nav, target, window)`，scale=min(1,target/realized_ann)，T−1 信号→T 生效，成本 5bp×|Δscale|（`d_better.py:vol_scale`）。target 15% 为事前常见值（非网格最优），扰动 10/20%。
- 结果（`scratch/d_voltarget.json`，摘 15%）：

| 窗 | base | vt15_w20 (avg_scale) | vt15_w60 | vt10_w20 | vt20_w20 |
|---|---|---|---|---|---|
| OOS2 | +229.8/−4.7/7.26 | +156.6/−4.7/7.08 (0.92) | +154.2/−4.7/7.00 (0.87) | +110.0/−3.6/6.84 (0.77) | +196.6/−4.7/7.23 (0.97) |
| train | +71.6/−4.5/6.22 | +61.0/−3.9/6.10 (0.86) | +60.4/−3.9/6.15 (0.87) | +44.4/−2.6/6.07 (0.66) | +68.6/−4.5/6.12 (0.97) |
| valid | +3.5/−10.0/0.53 | +0.2/−10.0/0.10 (0.83) | +0.5/−10.0/0.15 (0.78) | −1.2/−9.1/−0.17 (0.70) | +3.0/−10.0/0.48 (0.97) |
| long | +731.3/−5.5/4.15 | +596.0/−5.5/4.30 (0.97) | +574.9/−5.5/4.27 (0.97) | +479.8/−5.0/4.35 (0.92) | +644.0/−5.5/4.27 (0.99) |
| stress | +110.9/−7.8/2.71 | +90.6/−7.2/2.58 (0.92) | +92.8/−7.1/2.58 (0.92) | +65.1/−6.4/2.38 (0.77) | +102.6/−7.8/2.63 (0.98) |
| holdout | −19.1/−19.8/−4.59 | −11.8/−12.2/−3.70 (0.58) | −11.7/−12.1/−3.81 (0.55) | −9.6/−9.9/−3.31 (0.44) | −14.0/−14.5/−3.93 (0.71) |

- 读法：in-sample OOS2/train 全降收益（−73/−11pt）、Sharpe 全降；valid 更差（+3.5→+0.2/+0.5，t10 打负）；long Sharpe 微升（4.15→4.30）但 total −135pt、Calmar 10.01→8.99；holdout 少亏（−19.1→−11.8）仅因高波时 scale 0.55 被动降曝露，非择时 alpha。扰动 10/20% 同向（越紧越差）。**样本外未成立，参数扰动不稳健，裁决 REJECT。** 机制：星舰 B 日收益右偏厚尾（审计 B skew 1.39/kurt 11.95），vol 高时恰为卫星脉冲密集期，降曝露=截右尾（冻结档 #2 死法）。

### D3 回撤刹车（trailing 60d 峰值回撤>7%→次日半仓 0.5，否则 1.0；扰动 5/10% 与全停 0.0）

- 做法：`brake_scale(nav, threshold, hold_frac)`，峰值只用 ≤T−1，T−1 信号→T 生效（`d_better.py:brake_scale`）。A 股可执行：4 槽→2 槽，余款 T+1 可用前 park 在 REPO。
- 结果（`scratch/d_brake.json`）：

| 窗 | base | brake7 半仓 (frac) | brake5 | brake10 | brake7 全停 |
|---|---|---|---|---|---|
| OOS2 | +229.8/−4.7/7.26 | +229.8 (0.00) | +229.8 | +229.8 | +229.8 |
| train | +71.6/−4.5/6.22 | +71.6 (0.00) | +71.6 | +71.6 | +71.6 |
| valid | +3.5/−10.0/0.53 | +2.5/−10.0/0.41 (0.055) | +1.5/−10.0/0.29 | +2.5 | +1.5 (0.055) |
| long | +731.3/−5.5/4.15 | +731.3 (0.00) | +731.3 | +731.3 | +731.3 |
| stress | +110.9/−7.8/2.71 | +106.8/−7.8/2.65 (0.006) | +102.6 | +106.8 | +102.6 |
| holdout | −19.1/−19.8/−4.59 | −15.5/−15.9/−4.53 (0.757) | −11.9/−11.9/−3.68 | −15.5 | −11.9 (0.757) |

- 读法：OOS2/train/long 从不触发（MDD <阈值，frac 0.00），无保险价值；valid 触发 5.5% 天但收益 −1~−2pt、Sharpe 降；stress 微降；holdout 少亏 3.6~7.2pt 但 Sharpe 未改善（−4.59→−4.53/−3.68，Calmar 更差 −3.85→−4.30/−4.86）。扰动 5/10% 同向。**样本外未改善风险调整后收益，裁决 REJECT**，与 `product-post-peak-drift §10`（水位计死于解除动力学）一致：刹车只在崩后降曝露，躲不开首跌、错过 V 反抽。

### D3b 延伸：B 腿月频 vs 季频（已在 §3.4，结论 REJECT）

- 月频 +731.3 vs 季频 +719.3（Δ −12.0pt），B 腿 standalone +46.69 vs +43.86（Δ −2.83pt），turns 57→19。省换手不加收益。

**三新思路总判**：D1/D2/D3 在 in-sample（OOS2/train）已不加 Sharpe，在 out-of-sample（valid）全降收益，在 holdout 的“少亏”全可用“少曝露”解释（Sharpe/Calmar 未改善）。扰动 ±33% 方向一致。按 `validation-gates-v2-2026-09-16.md` G1-G3+L，无一达 PASS/条件 PASS，**全部 REJECT，不开 Live，不配小资金试错**（除 D1 90%+REPO 可作心理安慰型降波档，见 §0 建议）。

---

## 5. 对手视角：最可能推翻“星舰 B 最优”的三刀（都砍不死，但必须说）

1. **valid+holdout 已转弱**：valid +5.9→本审计 +3.5（−41% 相对摆动，n=52 fills，±1 笔大额即翻符号，审计 B §1）、holdout −19.8（35 天，两腿同亏卫星 −20.4/停放 −0.2）、2026Q2/Q3 +3.1/+1.9 走平。若未来 3 个月 holdout 不收复，long 的 Sharpe 4.15 会被稀释，H2-a25 的 +803.3 亦同跌。只引 long 是挑选窗口。
2. **幸存者+回退抬高 long**：M1（当前快照剔 ever-delisted，`state_bucket_track.py:107-124` OPT-211 P3 自认 survivor bias）+ M2（14:30 闸稀疏回退收盘，`state_bucket_track.py:509-558`）在 long 前段（无 dense、ST/退市多）同时向上偏，审计 A 已量化 ST 3.1%+退市 0.3% 行占比，OPT-212 纳入退市 +24.6pt。long +729.9 有上偏，不确定多大，写不确定。
3. **选参甜蜜点**：body=4 在 valid +15.9pt 但 OOS2 −73.6pt（审计 B §3）、r_wide 0.4/0.6 都“改善”valid 但 OOS2 −2.6/−59.8、gap 3%→2.1% OOS2 −44pt、park 60→78 valid 打成 −2.0。C1=3%、bucket_q=3 是真平台，body/r_wide/gap 不是。星舰 B 是有效前沿上一点（`archive/2026-09-24-starship-b-loss-anatomy.md:32`），降曝露才降回撤，无免费午餐。

---

## 6. 赚钱线索（profit-leads 规则执行）

- 本次审计的 D1/D2/D3/D3b 均为组合/仓位 overlay，无“扣费后每笔正收益”口径（卫星单笔仍 62% 胜率、全 `body_exit`、左尾 477/1080 亏，`loss-anatomy:19`），亦无“明显优于随机”（vol/brake/blend Sharpe 未超 base，placebo 未做，沿用审计 B DSR=1.0 long 显著但 valid p≈0.6）。
- 星舰 B 本身高收益为已知冻结结论（非新发现），park 独立腿 +46.7% 为组合成分非每笔口径，不满足追加条件。
- 故 **本次未向 `~/Projects/wealth-ideas/profit-leads.md` 追加**（规则要求注明来源本次审计 D，特此声明无新增）。若后人跑出 valid+holdout 同改善的 overlay，再按规则追加。

---

## 7. 待办（留给后人，需 DB 只读分批，<3GB）

1. 星港/双子星 post-dense 复跑（`rebaseline:51` 待办，catalog 仍 pre-dense，引用前先 `eval_harbor_b3_sat`/`eval_twin_star_parking` 重跑）。
2. M1 宇宙敏感性：`load_sgap_context(include_st=True,include_listed_history=True)` vs 冻结，同 habit 跑四窗+holdout（审计 A §10.1）。
3. M2 闸无覆盖跳过 vs 回退 close，同窗对照（审计 A §10.2）。
4. 港湾 vs 星舰 B 的 valid 归因：S-3 在 valid 有仓 30 天跑赢卫星 3.2×（`sat-valid-shortfall-diagnosis-2026-09-14.md`），需看 2026-03~08 的行业/风格暴露是否持续。

*证据版本：karios-desktop 2026-10-03 现状；卫星系数字以 rebaseline dense 2026-09-29 为准；archive 只读不回写；designs 预注册冻结不回写。复现：`python3 scratch/d_better.py --step all`（D3b 需 `.venv/bin/python` + `PYTHONPATH=src:scripts`，面板小量只读）。*

KARIOS AUDIT D DONE
