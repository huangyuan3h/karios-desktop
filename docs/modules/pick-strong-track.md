# 「港湾」（Harbor）核心腿真值

> **核心腿真值（2026-09-13 起）**：**港湾 = S-3 股票核心 + 闲置现金 ETF 停车场**（tag `harbor-p1-20260913`；[B11 实验档](../backtests/stable/etf-parking-baseline-2026-09-13.md)）。  
> 旧「择强单轨（全资产同权 100% argmax）」已被 OPT-177 证伪；旧「机会双子星」4×12.5% 路径已被 B12 拒收——两段仅作历史档，**不再作实盘默认**。当前默认 `starship_b`（星舰 B）是独立的稳健星舰研究/展示/人工操作档，不改变港湾 Live；`starship_robust`（H2-a25）仍可选。
> **Live 已于 2026-09-13 切换港湾**：前端 / 后端 / paper / 调度全走港湾；旧 `twin_star` 路径与 OPT-178 修复已落地；**2026-09-18 起停车统一到 H2（H-H2-UNIFY），冗余（`harbor_h2` 线 / H2 影子 / 旧 T6）已清理（OPT-226 / OPT-179 ✅）**。
> S-3 / 停车场 / 套筒等是**子组件**，不是并列的「主策略」。
> **稳健星舰边界（2026-09-24）**：`starship_robust` 的现行配方是卫星 + `cashShare(T−1)` ×（25% H2 套筒 + 75% B3），报告为 `sat_h2_a25_2026-09-24`；K3 风险未过，不能写成全门 PASS，也不接 Live 下单。

---

## 0. 一句话

**S-3 核心负责出海（选股 + 持有），闲置现金回「港湾」停泊**：空闲资金每天按 t-1 收盘在 `mom60+MA200` 的 ETF 里取 `argmax` 停一只，无候选就留现金/回购。**ETF 层只作用于 S-3 的闲置现金，不抢股票仓，也不是全资产 100% 硬切**。

停车场候选（同权，2026-09-13 P1 冻结口径）：

| 资产 | 来源 | 强度代理（现行） |
|------|------|------------------|
| **GOLD** | `518880.SH` | `mom60` 且收盘 ≥ MA200 |
| **OIL** | `513350.SH` | 同上 |
| **NASDAQ** | `513110` / `513100`（best） | 同上 |
| **BOND10** | `511260.SH` | 同上 |
| （无候选） | — | 留现金 / REPO（保守） |

规则（冻结口径 · **P1 闲置即停** · 2026-09-13 定案）：

1. 用 **t-1** 收盘算 ETF `mom60`；须站上 `MA200`（防前视）。  
2. 只在 **S-3 闲置现金**上执行：**14:30 口径**（回测用收盘代理，差 0.5h）、取 **`argmax mom60`** 停泊一只；**换仓需新目标领先现持 ≥2pt（H2 迟滞，2026-09-18 起，H-H2-UNIFY）**，否则维持原腿。
3. **因果 trail8**：ETF 峰值回撤 −8% 以 **t-1 收盘**触发 → 出场回现金；再入场无冷却；含 **0.05%/边**成本。  
4. **不做**「股票 vs ETF 谁 mom 强」的切换——那是旧择强 overlay：`ETF>STOCK` 门槛 long **+20.8 vs P1 +90.0（−69pt）**，落地应去掉。

> ⚠️ **2026-09-12 审计（OPT-177）**：trail8 的回测实现曾用**当日收盘**触发 trail、却把**当日收益记 0**（1 日前视）。实测 long 窗 `fusedPct 40.2%(含前视) → 0.8%(因果)`、valid `56.9% → 20.0%`——**文档中 trail8 的 `+75/+82pt` 增量几乎全为前视幻觉**。已把 `build_nav_from_cache`/`build_mom_compare_timeline` 改为 **t-1 收盘触发（因果）**；trail8 的**真实**增量仅 valid +8.5pt / long +7.5pt（见 [`audit-trail8-2026-09-12.md`](../backtests/audit-trail8-2026-09-12.md)）。**双子星相关过往前视口径数字作废、待重验**。
>
> ⚠️ **2026-09-13 新基线「港湾」（Harbor, `harbor-p1-20260913` · Live 已切 2026-09-13）**：ETF 层正确定位 = **闲置现金停车场**（闲置即停、14:30 口径、含成本）：三窗增量 **+17.2/+13.0/+11.6**、long **+118.8**（2026-09-14 日历修正 clean 口径；幻影日 + 决策单源）（[`etf-parking-baseline-2026-09-13.md`](../backtests/stable/etf-parking-baseline-2026-09-13.md)）。**"100% argmax 硬切"作废**；旧 4×12.5% 卫星腿重拟合 REJECT（B12 旧口径已作废 → [2026-09-14 修正审计](../backtests/audit-three-strategy-lookahead-2026-09-14.md)：仍 REJECT，只挂 valid 单窗）；Live 前视/账本修复与旧路径删除见 **OPT-178**（✅ 已完成）+ **OPT-182/183**；**停车统一 H2 + 旧 T6/影子清理 = OPT-226 / OPT-179（✅ 2026-09-18）**。

历史拒收（旧择强层，保留备查）：短/长 lookback、risk-adj、Top2、Nasdaq-first、袖侧 hold5 外推 —— 见 [`pick-strong-hardening-2026-08-29.md`](../backtests/core/pick-strong-hardening-2026-08-29.md)；trail8 旧绝对 NAV 证据 [`pick-strong-trail8-and-stock-pool-2026-08-29.md`](../backtests/core/pick-strong-trail8-and-stock-pool-2026-08-29.md)（valid +82pt / long +75pt；**已因 OPT-177 作废**）。

> **Live / Watchlist（2026-09-13 已切）**：Live 下单为港湾（S-3 核心 + 闲置停车场）；旧 `twin_star` 下单路径已删除（OPT-178 ✅）。Watchlist 另保留 `starship_robust` H2-a25 的研究/展示/人工操作卡，不写 Harbor paper 账本。

> ~~**机会双子星 v3.1（2026-09-02 · 实盘默认）**~~ **→ 已 REJECT / 历史（2026-09-13 · B12）**：卫星腿重拟合停车场核心 **OOS2 +36.5 / train −11.8 / valid −28.8 / long −55.6**（long 回撤 −23.6 → −48.3）；40/60、60/40 全不过。~~根因：卫星 standalone 2022 −34.8% / 2023 −48.0%、long MDD −80.6%（boom-bust）~~ **⚠️ 2026-09-14：以上数字与根因叙事作废**——B12 实跑用了全天振幅旧前视键、缺 C1/14:30 卖、核心为旧本地循环。修正口径（Live 习惯 + `gate_1430` + 单源核心 + **qfq/raw 基期统一** + **市场日历过滤**）重跑：卫星 long **+463.6%/MDD −8.4%**、双子星 50/50 **OOS2 +149.7 / train +52.2 / valid +29.1 / long +291.9**（Δcore +94.5/+0.0/**−21.2**/+90.4）→ **仍 REJECT，只挂 valid 单窗（K1/K2），K3 过**。卫星仍**不进新基线、禁止调参重扫**；要救须带全周期门槛另起预注册。修正真值：[`audit-three-strategy-lookahead-2026-09-14.md`](../backtests/audit-three-strategy-lookahead-2026-09-14.md)；B12 旧档（已加修正横幅）[`twin-star-parking-refit-2026-09-13.md`](../backtests/stable/twin-star-parking-refit-2026-09-13.md)、旧档 [`state-bucket-algo-2026-08-31.md`](../backtests/core/state-bucket-algo-2026-08-31.md)。  
> **Timeline 图口径（2026-09-09 起 · OPT-152 · 历史）**：双子星 Timeline 主曲线 = 旧实盘口径；100% 押注基准 = 灰虚线对照。卫星线随 B12 一并作废。见 `optimization-checklist/archive/2026-09-09-opt-152-timeline-product-curve.md`。

> **不是「100% argmax 择强」**：ETF 层只停闲置现金，不做"股票 vs ETF 谁最强"的仓位切换。  
> **不是「纯 S-3」**：S-3 生成 STOCK 候选/持仓；闲置现金由停车场增强（三窗 **+17.2/+13.0/+11.6pt**、long **+118.8pt** vs 纯 S-3——绝对 +55.2/+52.2/+50.3/+201.5；2026-09-14 日历修正后口径）。  
> **股票核心**：来自 **S-3**；**状态分桶 S-gap** 是并列的独立 A 股腿（可 Timeline `strategy=state_bucket` 单独回测），**不是**港湾替换件。**母港（Homeport）= 港湾 × B3 风险预算 50/50** 亦为 Timeline 选项（`strategy=homeport`，展示口径，非 Live；[B15](../backtests/stable/harbor-riskbudget-2026-09-13.md)）。

---

## 1. 与旧概念的关系

```text
港湾（Harbor · 唯一产品基线）
├── 股票核心 ← S-3 CN/HK 引擎（gate/score/RS/退出…）提供持仓篮
├── 闲置停车场 ← ETF mom60+MA200 argmax + 因果 trail8（只停 idle）
└── 无候选 → 现金 / REPO
```

| 旧名 | 地位 |
|------|------|
| S-3 | **股票核心引擎**（参数冻结，[`strategy-params.md`](./strategy-params.md) §1） |
| 择强单轨 / 100% argmax | **历史 · 已证伪**（OPT-177 后 long ≈ +0.8% / MDD −58%）；见 [`audit-trail8-2026-09-12.md`](../backtests/audit-trail8-2026-09-12.md) |
| 机会双子星 / 卫星腿 | **历史 · REJECT**（B12：long −55.6 / 回撤 −48.3；卫星 standalone 2022 −34.8% / 2023 −48.0% / MDD −80.6%） |
| 第三资产套筒 / 多资产袖 | **停车场前身**（闲置现金 ETF 增强 → B10/B11 升为新基线） |
| R5c / R5CS | 历史分层资金路由（未进 Live） |
| 融合单轨 | 旧设计稿名 → 历史 |

---

## 2. 历史：择强单轨过去一年验证（2026-08-29 · **作废 / 仅备查**）

> ⚠️ 本节数字来自 OPT-177 前的含前视口径（trail8 当日收盘触发），**不再作为任何决策依据**；保留仅为对照链。现行基线见 §0。

窗口：`2025-08-28 ~ 2026-08-28`（约 253 个交易日）  
脚本：`scripts/pick_strong_grid.py --batch E` · 报告：`data/backtest_reports/pick_strong_trail8_20260829.json`

| 口径 | 收益 | 最大回撤 | 说明 |
|------|------|----------|------|
| **择强单轨 `mom_compare`+trail8（旧定案）** | **+190.6%** | **12.6%** | ETF 峰值 −8%→REPO；三窗/长窗见 trail8 文档 |
| 对照：无 trail（旧 A0） | +93.6% | 28.3% | 仅硬切；已降级为对照 |
| 对照：`hard_stock`（有股票仓则锁 STOCK） | +110.8% | 32.0% | 旧 Timeline 偏置；**不作定案** |
| 对照：CN S-3 引擎单独 | +58.3% | 23.0% | 仅股票腿，现金≤100% NAV |

**结论（历史）**：旧定案吸收 trail8 后 past_year / valid / long 同向大幅改善，OOS2 持平——**该结论已因 OPT-177 前视作废**。STOCK 入池加闸（n≥2 / mom>0 / MA / CN-only）已拒收 —— 见同日 STOCK 池报告。

### 2.1 历史：过去一年三方：单轨 vs 旧双子星 vs clip4（2026-09-02 · **作废 / 仅备查**）

同引擎、window-local 空簿、strict S-gap、`opp_50`。数字：`data/backtest_reports/past_year_twin_vs_core_2026-09-02.json`。复现：`PYTHONPATH=src:scripts python3 scripts/compare_past_year_twin.py --save-report`。

表内为 total% / Sharpe / maxDD。past_year **不**当三窗拒收闸，只展示。**全部口径在 OPT-177 后作废**（核心腿含前视），仅作历史对照。

| 窗口 | 单轨择强 | 双子星 v3 15×5% | **clip4 4×12.5%** | Δ vs 单轨 | Δ vs v3 |
|------|----------|-----------------|-------------------|-----------|---------|
| 产品过去一年 `2025-08-28~2026-08-28` | +190.6 / 2.54 / 12.6 | +190.4 / 2.57 / 12.6（−0.2） | **+194.9 / 2.64 / 12.6** | **+4.3pt** | **+4.5pt** |
| 滚到今日 `2025-09-02~2026-09-02` | +197.6 / 2.58 / 12.6 | +201.7 / 2.65 / 12.6（+4.1） | **+204.0 / 2.69 / 12.6** | **+6.4pt** | **+2.3pt** |
| 协议 past_year `2025-08-01~2026-08-07` | +181.2 / 2.43 / 12.6 | +191.3 / 2.57 / 12.6（+10.1） | **+195.9 / 2.62 / 12.6** | +14.7pt | +4.6pt |

> 表定义见 [state-bucket-algo §3.0](../backtests/core/state-bucket-algo-2026-08-31.md)（同报告；本表多两个窗口）。

历史要点（已作废）：

- 旧产品窗上 15×5% 双子星略输单轨（−0.2pt）；clip4 把增量"翻正"——**B12 重拟合证明这是卫星黄金段 + 前视核心的双失真**。
- 滚到 2026-09-02 的数字同样作废。
- 当年实盘默认 clip4 的决策已被 2026-09-13 B12 REJECT 推翻：现行基线见 §0，卫星重拟合档 [`twin-star-parking-refit-2026-09-13.md`](../backtests/stable/twin-star-parking-refit-2026-09-13.md)。

---

## 3. 优化纪律（只动港湾的停车场层）

1. **冻结**：S-3 定案参数（[`strategy-params.md`](./strategy-params.md) §1）——除非审计级 bug；停车场 **P1 冻结**（tag `harbor-p1-20260913`）——再入场冷却/上限等候选改进须另起预注册。**trail 后冷却已测并 REJECT（B14，2026-09-13：k∈{1,2,3,5} 四档全挂、方向关闭，见 [`parking-cooldown-2026-09-13.md`](../backtests/stable/parking-cooldown-2026-09-13.md)）**；同族机制（全局冷却/上限/自适应冷却）不重开。  
2. **可调**：停车场入池代理（mom60 / MA200 / trail）、切换成本、再入场规则。  
3. **验收**：三窗 walk-forward（OOS2/train/valid）+ long；`>5pt` 劣化拒收；`n<100` 标 underpowered。  
4. **工具**：  
   - 停车场基线复现：`PYTHONPATH=src:scripts python3 scripts/eval_etf_parking_baseline.py`  
   - 卫星重拟合（REJECT）复现：`python3 scripts/eval_twin_star_parking.py`  
   - ETF 基准对照：`python3 scripts/eval_etf_benchmark_parking.py`  
   - 停车冷却复现（REJECT）：`python3 scripts/eval_parking_cooldown.py`  
   - 历史择强回放：`PYTHONPATH=src:scripts python3 scripts/fused_timeline_walk.py --windows past_year --mode mom_compare`  
5. **实验记录**：一律写回 `docs/backtests/`；新基线见 [`etf-parking-baseline-2026-09-13.md`](../backtests/stable/etf-parking-baseline-2026-09-13.md)、基准对照见 [`etf-benchmark-parking-2026-09-13.md`](../backtests/stable/etf-benchmark-parking-2026-09-13.md)。

> **提醒（2026-09-01 归档 · 再有此想法时直接读）**：**不要再松 S-3 gate**。`S-3 gate` 在择强内的 `full→regime→none` 松闸已 10 变体三窗实测全拒收（`gates_none valid -58pt / gates_regime valid -32pt / entry_score OOS2 -6pt`），唯一过线的 `no_exclude300` 亦被 `twin` 稀释且与 `strategy-params.md:29` 创业板三窗亏钱结论冲突，**该方向结案**。下次再想“择强里 S-3 太严”时，直接读 [`backtests/s3-gate-pickstrong-optimization-2026-09-01.md`](../backtests/core/s3-gate-pickstrong-optimization-2026-09-01.md) §1-§3，无需重跑（复现 `scripts/test_s3_pickstrong_gates.py:1`）。真要动 STOCK腿请改**强度代理**而非闸门。

---

## 4. 代码入口

| 用途 | 路径 |
|------|------|
| 停车场基线复现（P1） | `scripts/eval_etf_parking_baseline.py`（报告 `data/backtest_reports/etf_parking_baseline_2026-09-13.json`） |
| 卫星重拟合（REJECT） | `scripts/eval_twin_star_parking.py` |
| ETF 基准对照 | `scripts/eval_etf_benchmark_parking.py` |
| Live 停车场（**已上线 2026-09-13**） | `service/multi_asset_sleeve.py` · `service/sleeve_paper_auto.py` |
| 历史择强回放 | `scripts/fused_timeline_walk.py` (`mode=mom_compare`) |
| 历史三方对照报告 | `data/backtest_reports/past_year_twin_vs_core_2026-09-02.json` |

---

*创建 2026-08-29 · 2026-09-13 改名为「港湾」（Harbor）：S-3 核心 + 闲置现金 ETF 停车场；择强单轨/机会双子星降为历史（OPT-177 / B12）。*
