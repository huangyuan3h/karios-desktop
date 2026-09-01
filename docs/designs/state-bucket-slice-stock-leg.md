# 状态分桶 Slice 替 S-3 STOCK 腿 · 设计计划

> **状态**：实验已结案 · 结论迁出 · 2026-09-01  
> **结案真值**：`docs/backtests/state-bucket-algo-2026-08-31.md` **文首「纠结的点与口径铁律」+ §3.0 v3**。本文只留过程稿，不要当产品默认。  
> **一句话**：可执行最优 = 机会双子星（strict S-gap + 无仓回核）；slice 替 S-3 / PS-G50 静态半仓 / 扩池 **均拒收**。

---

## 0. 一句话

**四态各自独立槽回放 → 日收益加权合成（slice）→ 三窗 walk-forward vs S-3 找中间点 → 通过后接入 Timeline / pick-strong STOCK 腿实验分支。**  
**不做** union 共享槽 · **不做** argmax 直替 · **不做** 未经可执行口径的 +122.8% 叙事。

---

## 1. 动机（和用户记忆对齐）

| 记忆 | 文档事实 | 本计划 |
|------|----------|--------|
| 四态合成 vs S-3 效果不错 | R6/R8 union past_year +122.8 vs S-3 +58.3（**历史成交**） | 结构改为 **slice**（R10），口径改为 **可执行** |
| 不是 union 一锅烩 | R10：union ≈ S-limit 独裁 | **每态独立 10/15 槽**，再合成 |
| 替 S-3 进系统 | 今天 S-3 → `positions_by_day` → `build_mom_compare_timeline` | slice 合成 → 同形态 `positions_by_day` |

---

## 2. 四态与默认合成候选

| 态 | body | 槽/参数（R11/R12） | 默认进合成？ |
|----|------|-------------------|-------------|
| S-limit | 3 | bq2 · 10槽 | slice2/3 ✅ |
| S-gap | 3 | bq3 · 15槽 | slice2/3 ✅ |
| S-shrink | 15 | bq2 · 15槽 | slice3 ✅ |
| S-fresh | 15 | bq5 · 10槽 | **仅对照**（R10 删除） |

**合成变体（找中间点）**：

| 代号 | 态 | 权重 |
|------|-----|------|
| `L` / `G` / `S` / `F` | 单态 | 100% |
| `slice2_LG` | L + G | 50/50 |
| `slice3_LGS` | L + G + S | 1/3 each |
| `slice2_L70` / `L60` / `L40` / `L30` | L + G | 70/30 … 30/70 |

---

## 3. 口径铁律

1. **可执行**：`skip_t1_limit=True`（T-1 涨停候选剔除；与机会双子星卫星同口径）  
2. **Universe**：ST/BJ/退市过滤（`state_bucket_track._load_rows`）  
3. **闸**：R-wide `breadth>0.5`  
4. **成本**：`COSTS_ROUNDTRIP=0.003`  
5. **验收窗**：**三窗** OOS2 / train / valid（与 S-3 walk-forward 一致）  
6. **拒收**：相对 S-3 基线 **>5pt 劣化** → 该变体不进入 Phase 2  

---

## 4. 分阶段落地

### Phase 1 — 服务层 + 三窗实验（本迭代）

| 交付 | 路径 |
|------|------|
| 单态 + slice 合成引擎 | `service/state_bucket_slice.py` |
| 三窗 vs S-3 + 权重网格 | `scripts/compare_sliced_vs_s3.py` |
| 报告 JSON | `data/backtest_reports/sliced_vs_s3_YYYY-MM-DD.json` |
| 文档 | 本文件 + `state-bucket-algo` §3.0d（实验表） |

**不做**：Timeline API · UI · paper · 改默认策略  

### Phase 2 — 回测可切换（拍板后）

- `strategy=state_slice` 或 query `slice=slice2_LG|…`  
- `backtest_routes`：slice 合成 `positions_by_day` 替 CN S-3 simulate  
- BacktestPage 策略开关  

### Phase 3 — pick-strong STOCK 腿实验分支（三窗过线后）

- `pick_strong_track` 接 slice 持仓篮  
- HK 暂保留 S-3  
- walk-forward 固化基线  

### Phase 4 — Live / Paper（最后）

- `multi_asset_sleeve` / paper 并列候选  
- 需 holdout 只读 + 用户拍板  

---

## 5. 实验矩阵（Phase 1 必跑）

```
行：L, G, S, F(对照), slice2_LG, slice3_LGS, slice2_L70/L60/L40/L30
列：OOS2 / train / valid × (total%, dd, sharpe)
基准：S-3 CN（run_walk_forward.S3_CONFIG）
输出：Δ(total) vs S-3、最优 slice 代号、是否过三窗拒收线
```

**中间点判定**（需用户拍板，建议默认）：

1. 三窗 **total 均 ≥ S-3 −5pt**  
2. 三窗 **sharpe 加权均值 ≥ S-3**  
3. **dd 中位数 ≤ S-3**  
4. 与择强 core 日收益 corr ≤ 0.15（Phase 1 可选后算）  

---

## 6. 明确不做

- union 共享槽（已复现，仅作历史对照）  
- argmax(状态分桶 NAV) 替 STOCK 腿（R8 拒收）  
- 未过三窗即改 `getStrategyMode` 默认  
- 在 `docs/` 根目录另起计划 markdown（本文件在 `designs/`）  

---

## 7. 验收清单（Phase 1）

- [x] `compare_sliced_vs_s3.py` 三窗可重复跑，JSON 落盘 → `sliced_vs_s3_2026-09-01.json`
- [x] 单态 L/G/S 与 slice2/3 数字可复现（同引擎 service 层）
- [ ] pytest：`state_bucket_slice` 单态 smoke（待补）
- [x] 实验结论见 §8

---

## 8. Phase 1 结果（2026-09-01 · 可执行 skip_t1_limit）

**S-3 基线**：OOS2 +47.3 · train +34.1 · valid +38.7

| 变体 | OOS2 Δ | train Δ | valid Δ | 过线(−5pt)/3 |
|------|--------|---------|---------|--------------|
| **G（S-gap 单态）** | **+64pt** | **+4pt** | −23pt | **2/3** |
| slice2_L30 | +19pt | −8pt | −28pt | 1/3 |
| slice2_L40 | +5pt | −11pt | −29pt | 1/3 |
| slice3_LGS | +4pt | −20pt | −31pt | 1/3 |
| slice2_LG | −7pt | −15pt | −31pt | 0/3 |
| L（S-limit 单态） | −56pt | −32pt | −38pt | 0/3 |

**结论**：

1. **可执行口径下，四态 slice 合成不能整体替 S-3**——valid 窗全线输给 S-3（约 −23~−31pt）。  
2. **中间点落在 S-gap 单态**，不是 slice2/3；加 S-limit 权重越高 valid 越差（涨停可执行过滤）。  
3. 历史 union +122.8% **不可外推**到此口径；Phase 2「替 STOCK 腿」**暂不拍板**。  
4. 保留路径：**S-gap 作卫星/研究腿**；或 Phase 2 仅 Timeline 对照，不改默认。

**Phase 2 门槛**：需用户拍板 + 至少一组合三窗全过 −5pt 线（当前无）。

---

## 9. PS-G50 基线（G50 稳态半仓 · 2026-09-01 冻结）

> **正式名称**  
> - **ID**：`PS-G50`  
> - **中文**：**G50 稳态半仓基线**  
> - **英文**：Pick-Strong G50 Static Half Baseline  
> - **含义**：**G** = S-gap 优化卫星单态 · **50** = 与择强 trail8 核心 **静态 50/50** 日收益混合（非机会双子星动态切仓）

| 项 | 值 |
|----|-----|
| 卫星 | S-gap · bucket_q=3 · 15槽 · body=3（R11 G_opt） |
| 核心 | pick_strong trail8（mom_compare · E1 定案） |
| 混合 | 每日 `0.5×core_ret + 0.5×sat_ret` |
| 口径 | 历史 scout 成交（**无** skip_t1_limit） |
| 冻结 JSON | `data/backtest_reports/pick_strong_g50_baseline_frozen.json` |
| 复现 | `PYTHONPATH=src:scripts python3 scripts/reproduce_static_blend_baseline.py --save-baseline` |

**≠ 机会双子星**：后者是 satActive 动态 50%；PS-G50 是全天固定半仓混卫星日收益。

### vs 单轨择强（冻结窗）

| 窗口 | 单轨择强 | **PS-G50** | Δtotal | 备注 |
|------|---------|-----------|--------|------|
| past_year | +181.2% · sr2.43 · dd12.6 | **+177.2% · sr3.98 · dd6.4** | −4pt | Sharpe↑ dd↓ |
| aligned | +190.6% · sr2.54 | +164.9% · sr3.82 · dd8.1 | −26pt | |
| OOS2 | +17.8% | +102.3% · sr2.88 | +84pt | 弱市年卫星强 |
| train | +40.7% | +69.5% · sr5.48 | +29pt | |

**已知问题（待下一版对照）**：涨停买不进（无 skip_t1_limit）— 退出日 satActive bug **不适用**本基线。

---

## 11. PS-G50-X 计划（空槽回核 · 2026-09-01）

> **纠结**：历史 PS-G50 的风险形态（Sharpe↑、回撤砍半、弱市不惨）是对的；可执行后塌掉，是因为涨停买不进时半仓卫星在吃 0 收益。不想硬塞更差的 S-gap，也不想把强趋势年的单轨优势交出去。
> **目标**：可执行口径下，尽量保住 PS-G50 的 Sharpe/回撤，总收益相对单轨不要崩。

### 机制（拟合对象）

| 项 | 值 |
|----|-----|
| ID | `PS-G50-X` |
| 中文 | **G50 空槽回核** |
| 卫星 | S-gap · bq3 · 15槽 · body3 · **`skip_t1_limit=True`** · **无** rank-fallback |
| 空槽 | `idle = max(0, 1 − satSlots×10%)` 的卫星名义资金 **跟择强核心**（核心本身会切 STOCK/金/油/纳/债） |
| 占用 | `satSlots` = 隔夜持仓 + 当日 body 退出（退出日成本仍进卫星） |
| 混合 | `port = w·core + (1−w)·(sat_ret + idle·core)`；拟合 `w ∈ {0.5, 0.7, 0.8}` |

对照：`static_50`（可执行死扛半仓）· `opp_50`（机会双子星二元切仓）。

### 验收（相对择强单轨）

1. past_year **总收益差 > −15pt**（不能再塌 70pt）
2. **Sharpe 仍高于**单轨
3. **maxDD 不差于**单轨
4. OOS2 弱市 **不显著变差**

未过线 → 不改实盘默认，只留研究报告。过线 → 再拍板是否进 Timeline / opt-in。

```bash
PYTHONPATH=src:scripts python3 scripts/compare_ps_g50x.py --save-report
```

### 拟合结果（2026-09-01 · 可执行连续簿）

报告：`data/backtest_reports/ps_g50x_2026-09-01.json`

| 窗口 | 单轨 | 可执行死扛 50/50 | 机会双子星 | **x70 空槽回核** | x80 |
|------|------|-----------------|-----------|-----------------|-----|
| OOS2 | +17.8 / 0.72 / 18.0 | +60.6 | +63.0 | **+41.8 / 1.42 / 17.0** | +33.4 |
| train | +40.7 / 3.01 / 8.4 | +29.5 | +38.5 | **+38.3 / 3.20 / 7.4** | +39.1 |
| valid | +139.1 / 3.37 / 11.9 | +60.5 | +122.3 | **+135.1 / 3.43 / 11.9** | +136.5 |
| past_year | +181.2 / 2.43 / 12.6 | +92.9 | +164.4 | **+170.3 / 2.41 / 12.6** | +174.0 |
| aligned | +190.6 / 2.54 / 12.6 | +88.8 | +169.7 | **+177.2 / 2.50 / 12.6** | +181.7 |

- **空槽回核修好了塌方**：死扛半仓 past_year −88pt → x70 **−11pt**。
- **历史 PS-G50 的 sr≈4 / dd 砍半买不回来**：那是涨停虚成交；诚实卫星 past_year 只有 +26%，空仓日跟核心走 → 回撤贴回单轨 12.6。
- **推荐研究点 `x70`**（核心 70% / 卫星名义 30%，空槽再回流核心）。验收：total 过 −15pt 线；Sharpe 与单轨打平（不过「仍高于」）；OOS2 仍 +24pt。
- **不改实盘默认**（仍择强单轨）。x70 可作为 opt-in 候选，需用户拍板。

### 深入拟合（2026-09-01 · window-local · 可执行）

报告：`data/backtest_reports/ps_g50x_deep_2026-09-01.json`

把「涨停后买下一个」做成 **replace**（同数量低波可成交）对照 **strict**（顶桶跳过）和 **fallback**（全池）。五窗均 **window-local 空簿**（与择强单轨同协议）。

结论：

1. **replace / fallback 卫星质量更差**（past_year sat +34%/+34% vs strict +52%）→ 否决扩池。
2. **最优可执行 = `strict` S-gap + 机会双子星 `opp_50`**（无仓 100% 核心，有仓切 50%）。
3. 历史 PS-G50 sr≈4 **仍作废**（涨停虚成交）。

| 窗口 | 单轨择强 total/sr/dd | **机会双子星** | Δtotal | ΔSharpe | ΔmaxDD |
|------|---------------------|---------------|--------|---------|--------|
| OOS2 | +17.8 / 0.72 / 18.0 | **+62.9 / 1.86 / 15.7** | **+45.1pt** | +1.14 | **−2.3** |
| train | +40.7 / 3.01 / 8.4 | **+49.1 / 3.87 / 7.6** | +8.4pt | +0.87 | −0.8 |
| valid | +139.1 / 3.37 / 11.9 | **+146.9 / 3.62 / 11.9** | +7.9pt | +0.26 | 0 |
| past_year | +181.2 / 2.43 / 12.6 | **+191.3 / 2.57 / 12.6** | +10.1pt | +0.14 | 0 |
| aligned | +190.6 / 2.54 / 12.6 | **+190.4 / 2.57 / 12.6** | −0.2pt | +0.03 | 0 |

三窗 walk-forward **3/3 总收益与 Sharpe 均不低于单轨**。aligned 打平。

