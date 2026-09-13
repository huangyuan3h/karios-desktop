# 审计：ETF trail8 前视 bug（2026-09-12 · OPT-177）

> **一句话**：双子星核心（择强单轨）的 **ETF trail8 出场**在回测里用**当日收盘**触发、却把**当日收益记 0**——1 日前视。修因果（t-1）后，核心 long 窗 `fusedPct 40.2% → 0.8%`、valid `56.9% → 20.0%`；**"习惯双子星"被大幅高估**（valid CAGR 135.6→39.3、long 9.42→4.04）。**结论反转**：修正后「稳健双子星」(T3) 在 long 窗连收益都反超它。
> **关键词**：前视 bug trail8 双子星 回测污染 OPT-177 审计

**脚本**：`scripts/audit_trail8_lookahead.py` · `scripts/pick_strong_grid.py`（修复）· `src/.../service/pick_strong_track.py`（修复）
**触发**：在 H-ADAPT（B9）自研 overlay 时抓到同类前视，遂回头审 Live 主链（用户 2026-09-12）。

---

## 1. Bug 机制
`pick_strong_grid.build_nav_from_cache` / `pick_strong_track.build_mom_compare_timeline` 的 ETF trail：
```python
close = mp.get(day)                 # 当日收盘
if close < etf_peak * (1 - 8%):
    hold_pick = "REPO"              # 当日切空
fused_ret = etf_ret[day] if ETF else 0.0   # 当日收益记 0
```
→ 用**当日收盘**决定当日持仓，并抹掉当日亏损 = **1 日前视**。
（择强核心 mom60/MA200 用 t-1、STOCK 腿用 t-1 持仓快照，均无前视。）

## 2. 量化：核心（long 窗 2021–2026）

| 配置 | OOS2 | train | valid | long | trailExits |
|------|------|-------|-------|------|-----------|
| trail8 现行（当日收盘，含前视） | 19.2 | 37.5 | **56.9** | **40.2** | 6 |
| **trail8 因果（t-1）** | 19.2 | 32.6 | **20.0** | **0.8** | 3 |
| 无 trail | 19.2 | 32.6 | 11.5 | −6.7 | 0 |

- **前视抬升**：valid **+36.9pt**、long **+39.4pt**（几乎全部）。
- **trail8 真实增量**（因果 vs 无）：valid +8.5pt、long +7.5pt、train 0、OOS2 0。**文档宣称的 `+82/+75pt` 几乎全为幻觉。**
- 6 个触发日：2026-04-08 OIL −10.0 / 03-10 OIL −9.5 / 03-31 OIL −7.6 / 04-21 OIL −5.6 / 2025-10-28 GOLD −3.6 / 2024-07-25 NASDAQ −2.9。

## 3. 量化：双子星 T0（core + clip4 opp_50）

| 窗口 | 修正前（含前视） | 修正后（因果） |
|------|------------------|----------------|
| OOS2 | 25.66 / 1.13 / −16.66 | 25.66 / 1.13 / −16.66（无触发，不变） |
| train | 41.01 / 1.52 / −17.53 | **36.13 / 1.37 / −18.99** |
| valid | **135.63 / 2.04 / −17.77** | **39.29 / 0.90 / −17.81** |
| long | 9.42 / 0.49 / −39.9 | **4.04 / 0.28 / −41.39** |

（CAGR% / Sharpe / MDD%，`compare_twin_stable_detail`。）

## 4. 连锁影响（结论反转）
- **H-COMBO（B8）**：修正后用因果 T 腿重跑，**T3「稳健双子星」long CAGR 6.24 / Sharpe 0.52 / MDD −17.9，全面反超 T0（4.04/0.28/−41.4）**——不再只是"降回撤换收益"，而是**连收益都更好**。
- **H-ADAPT（B9）**：修正后 **A2（沪深300×MA200）/ A3（软切）由 out 变 PASS**（此前"强市保留"是对着被前视抬高的 T0 打，天然过不了）。A1（T-NAV 择时）依旧失败。
- **S-3 主线（`run_walk_forward`）不受影响**：走引擎自带 ATR trailing，非本函数。
- **Live paper** 用真实条件单，执行上无前视；**问题在回测/展示口径**。

## 5. 修复
- `build_nav_from_cache`：trail 改 **`trail_causal=True` 默认**（t-1 收盘触发，参数保留 `False` 仅作考古）。
- `build_mom_compare_timeline`（Timeline 实盘口径）：`close = mp.get(prev)`。
- 未动 Live 决策代码（本就实时）。

## 6. 后续
- **凡引用过 trail8 旧数字的档**（`pick-strong-trail8-and-stock-pool-2026-08-29`、`pick-strong-track.md`、双子星各处）**须标注作废**，以本档为准。
- **重评"习惯双子星"**：修正后其在 long/valid 的优势大幅缩水，**是否仍值得作为 Live 默认、与稳健组合的关系**需重新拍板。
- **纪律**：这次的 bug 与 H-ADAPT 自研时抓到的 **同类**（当日价决定当日仓位）——"漂亮得离谱先查前视"。

*2026-09-12 · OPT-177*
