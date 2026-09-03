# 状态分桶（State-Bucket）算法真值【2026-09-02 v3.1 · 4槽×12.5% NAV】

> **一句话**：**择强单轨（trail8）为主干，S-gap 卫星作"机会增强"**——无仓时 100% 跟核心；R-wide 开闸且候选**可执行**时切 50%；**退出日仍算卫星占用**（`satActive`）。卫星 **`skip_t1_limit` + `pool_mode=strict`**（涨停跳过，**不**顺位补更差的缺口票）。**仓位：4 槽 × 套筒 25% = 总资产 12.5%。**
> **2026-09-02 v3.1 clip**：15×5% NAV 太散；只砍槽不加大单票三窗全拒。`4×12.5%` 相对 v3 15×10% 三窗全正（OOS2 +1.8 / train +2.3 / valid +10.3）。冻结 `opportunity_twin_star_v3_clip4_frozen.json`。
> **2026-09-01 v3**：window-local 可执行对照结案——机会双子星 **三窗 walk-forward 全过**单轨。历史 15 槽×10% 表见 §3.0-legacy-clip。
> **2026-09-01 v2 记账修正仍成立**：v1 `satPositions>0` 退出日成本逃逸（aligned 虚高 ~25pt）。v2 表里 past_year/aligned「输核心」是**另一套窗口切法 + 连续簿**，不要和 v3 window-local 混读。
> **2026-09-01 涨停审计仍成立**：旧静态 50/50 假设一字板可成交已弃。
> **命名动机**：涨停可能买得进也可能买不进——卫星是**机会**，不是刚性半仓。
>
> **角色（勿混）**：
> | 角色 | 含义 | 入口 | 地位 |
> |------|------|------|------|
> | **机会双子星 v3.1** | 择强 + strict S-gap + 无仓回核 + **4×12.5%** | Timeline `strategy=twin_star` · `opportunity_twin_star_v3_clip4_frozen.json` | **实盘默认** |
> | **机会双子星 v3 15×5%** | 同上、旧仓位 | `opportunity_twin_star_v3_frozen.json` | 对照；已被 clip4 替代 |
> | **择强单轨** | 无卫星 | Timeline `pick_strong` | Settings 对照 / 核心腿 |
> | **PS-G50** | 静态 50/50 · **历史成交** | `pick_strong_g50_baseline_frozen.json` | 研究上限（Sharpe≈4），**不可实盘** |
> | **独立腿 / slice** | 单态或分态，替 S-3 | `compare_sliced_vs_s3.py` | 可执行 slice **未过**三窗 |
> | **作废虚高** | R7/R8 union +122.8%、v1 +205.9 | — | 禁止再引用为真值 |

---

## 纠结的点与口径铁律（2026-09-01 结案 · 勿再重跑一轮才想起）

### 我们到底想要什么

看见过「择强 + 半仓卫星」的风险形态（Sharpe 更高、回撤更小、弱市不惨），但诚实成交后数字塌掉。想要的是：**可执行**、涨停买不进时钱去**别的真资产**（核心会切金/油/纳/债/股票），总收益别崩，Sharpe/回撤仍更好。

### 不要再混的四件事

| # | 容易混 | 真值 |
|---|--------|------|
| 1 | **机会双子星** vs **PS-G50** | 双子星 = `satActive` 二元切仓（无仓=100%核心）。PS-G50 = **每天**死扛 50/50。PS-G50 好看是因为没过滤涨停。 |
| 2 | **历史成交** vs **可执行** | 可执行 = `skip_t1_limit=True`。审计：约 33% 一字/涨停开盘买不进。历史 sat past_year ~156% → 可执行 ~52%（window-local）或更低（连续簿）。 |
| 3 | **strict / replace / fallback** | **strict**（定案）：先取全体缺口低波 1/3，再丢掉涨停。**replace**：先丢掉涨停再取 1/3（等于补了更差的票）。**fallback**：整个可成交池。v3 实锤 replace/fallback 卫星质量更差。 |
| 4 | **window-local 空簿** vs **连续簿** | 与择强 walk-forward 对齐用 **每窗空仓起步**。从 2024-08 连续跑再切片，卫星 past_year 会矮一截（持仓路径依赖）。v2 冻结表 ≠ v3 表，不是又发现记账 bug。 |

### 实验路径（拒收记录）

| 尝试 | 结果 | 结论 |
|------|------|------|
| 四态 union 替 S-3 | 历史赢、可执行 valid 崩 | 禁止用 +122.8% |
| 四态 slice 加权 | 可执行 valid −23~−31pt | 不替 STOCK 腿 |
| 顺位买下一个 S-gap | past_year sat 更差 | **不要扩池** |
| 静态 50/50 可执行 | past_year −70~−88pt | 空卫星在吃 0 |
| 空槽按比例回核 x70 | 塌方修好，Sharpe 贴回单轨 | 接近但不如二元机会切 |
| **strict + 机会双子星** | 三窗全过；past_year +10pt；dd 持平 | **定案可执行** |
| 只砍槽到 10、单票仍 5% | 三窗全负 vs 15×5% | 欠配不是集中 |
| **4 槽 × 12.5% NAV** | 相对 15×5% 三窗全正 | **v3.1 冻结仓位** |
| 历史 PS-G50 sr≈4 / dd≈6 | 涨停虚成交 | **买不回来**，停止追 |

### 复现脚本

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/compare_sat_clip.py --save-report
PYTHONPATH=src:scripts python3 scripts/compare_past_year_twin.py --save-report
# 冻结仓位：data/backtest_reports/opportunity_twin_star_v3_clip4_frozen.json
# 过去一年三方：data/backtest_reports/past_year_twin_vs_core_2026-09-02.json
PYTHONPATH=src:scripts python3 scripts/compare_ps_g50x_deep.py --save-report
# → data/backtest_reports/ps_g50x_deep_YYYY-MM-DD.json
# v3 15×10% 对照：data/backtest_reports/opportunity_twin_star_v3_frozen.json
```

---

## 0. 为什么是"状态分桶"（动机与拒收史）

- **行业分池已结案拒收**（`industry-style-plan-2026-08-31.md:1`）：144 行业 80%+ 归一 amplitude 簇，三簇真回放全负（amplitude valid -5.8 / turnover valid -15.3 / neg_mv OOS2 -33.4）。结论：行业是**静态、与 alpha 正交**的维，不是区分维度。
- **真正区分行为的是两件事**：① 个股**当日所处状态**（涨停/缺口/换手/距新高/次新）；② **大盘宽度 regime**（breadth>0.5 是唯一 5pt 级转正闸，见冻结基线 `20-150 amp_q10 10d breadth>0.5`）。
- 状态是**逐日动态**的（同一只票今天涨停明天跌停，状态不同），分桶在**每日截面内**进行，非静态名单。

---

## 1. 算法完整流程（每日截面，信号取 t-1 收盘）

### 1.1 前置计算（每个交易日，基于 t-1 数据）
1. 取全 A 非 `ST`/非 `BJ` 个股当日 `open/high/low/close/pre_close/amount` 与 `total_mv`。
2. 计算每只票 `amplitude = (high-low)/close`、`turnover = amount/avg20_amount`、`gap = open/pre_close-1`。
3. 全市场截面分位数：`amplitude Q10/Q70`、`turnover Q30`。
4. 大盘宽度 `breadth = (close>MA20 股票数) / 总数`；`R-wide = breadth>0.5`。

### 1.2 状态标注（每只票至多属一个状态桶，互斥）
| 状态 | 判定（当日） | 角色 |
|------|-------------|------|
| `S-limit` 涨停系 | `close ≥ 涨停价×0.999`（一字/封板） | 游资情绪核心 |
| `S-gap` 缺口系 | `gap > 3%`（高开缺口） | 跳空动量 |
| `S-shrink` 缩量低波 | `amplitude ≤ Q10` 且 `turnover ≤ Q30` | **可选**（R10：真分散腿，valid slice3 sr5.50；默认 slice2 不含） |
| （已弃）`S-fresh` / `S-stress` / `S-breakout` | 次新 / 高位放量 / 放量突破 | R10/R4 实证无超额，弃（S-fresh fills≈0 + 候选海啸） |

### 1.3 信号生成（每个保留状态桶内）
- 只取桶内 **`amplitude Q1`（最低波动 20%）** 作为候选——因子方向由 R1 IC 定（热点态振幅负 IC 最强，低波尾即反转/承接区）。

### 1.4 闸控与入场
- **R-wide 闸**：仅当 `breadth>0.5` 才开仓；否则全天空仓（保下行）。
- 信号取 **t-1 收盘**标注、状态：**next_open** 买入。
- 单边滑点 **0.15%**；仓位 `position_pct=10%`、**每态独立 `max_positions=10`、两态资金等权**（总敞口 ≤100%）。
- **按态切分（R10 冻结）**：`S-limit` / `S-gap` 各自独立槽位、等权组合，无跨态抢槽。
  > 旧结构（R6 共享 10 槽 + 优先级 `S-limit(4)>S-gap(3)>...`，不满不踢）经 R10 槽位审计实锤为 **S-limit 独裁**（S-gap fills 0~12/窗），已废弃。

### 1.5 出场（吃鱼身）
- **主规则**：持有达到该状态 **body 长度**即出（不恋鱼尾）。
  - 热点态 `S-limit`/`S-gap` → **body=3 日**（涨停/缺口动量极短）。
  - 慢态 `S-fresh`/`S-shrink` → **body=15 日**（次新/缩量 grind 长）。
- 可选收益率优先级出场（R6-B，更稳但少赚）：达标锁利 `hot +6% / slow +12%` + 峰值回撤 `>8%` 截尾。定案用主规则（吃满 body）。

---

## 2. 定案参数表（冻结）

| 项 | 值 |
|----|----|
| 宇宙 | 全 A 非 ST/BJ（`stock_basic` delist_date IS NULL） |
| 卫星状态 | **`S-gap`（缺口 >3%，bucket_q=3、4 槽、body3）** · 可选 `S-limit`/`S-shrink` |
| 弃用状态 | `S-fresh` / `S-stress` / `S-breakout` |
| 选股因子 | 桶内 `amplitude` 低波 33%（低波尾） |
| regime 闸 | `R-wide`：`close>MA20 占比>0.5` 才开仓 |
| body hold | `S-gap=3` · 可选 `S-limit=3` / `S-shrink=15` |
| 入场 | next_open、滑点 0.15% 单边、**4 槽×套筒 25%（总资产 12.5%）** |
| 结构 | **机会双子星 v3.1 clip4（实盘默认）**：择强 trail8 + strict S-gap + 无仓回核 + 4×12.5% |
| 成本 | `COSTS_ROUNDTRIP=0.003`（单边 15bp，仅卫星计） |
| 容量硬上限 | ≤200 万（低波尾宽度有限，天然卫星） |

---

## 3. 表现（回测，非实盘）

### 3.0 冻结：机会双子星 v3.1 clip4（2026-09-02 · 4×12.5% NAV · `opportunity_twin_star_v3_clip4_frozen.json`）

相对 v3 15×5% 的 Δ 在末列。回测网格：`docs/backtests/sat-clip-concentration-2026-09-02.md`。

| 窗口 | 择强单轨 total/sr/dd | S-gap 卫星 (4×25%) | **机会双子星 clip4** | Δ vs 核心 | Δ vs v3 15×5% |
|------|---------------------|--------------------|----------------------|-----------|----------------|
| OOS2 | +17.8 / 0.72 / 18.0 | +118.3 / 3.92 / 7.2 | **+64.7 / 2.13 / 16.1** | +46.9pt | **+1.8pt** |
| train | +40.7 / 3.01 / 8.4 | +38.0 / 3.28 / 4.3 | **+51.4 / 4.05 / 7.8** | +10.7pt | **+2.3pt** |
| valid | +139.1 / 3.37 / 11.9 | +14.5 / 1.69 / 6.2 | **+157.2 / 3.71 / 11.9** | +18.1pt | **+10.3pt** |
| past_year | +181.2 / 2.43 / 12.6 | +51.7 / 2.42 / 4.7 | **+195.9 / 2.62 / 12.6** | +14.7pt | +4.6pt |
| aligned（产品过去一年） | +190.6 / 2.54 / 12.6 | +34.5 / 1.60 / 8.7 | **+194.9 / 2.64 / 12.6** | +4.3pt | +4.5pt |
| trailing `2025-09-02~2026-09-02` | +197.6 / 2.58 / 12.6 | +34.7 / 1.61 / 8.7 | **+204.0 / 2.69 / 12.6** | +6.4pt | +2.3pt |

> 口径同 v3（每窗空仓、strict、opp_50），只改 `max_pos=4`、`POSITION_PCT=0.25`。开闸日均约 3.6 只。Watchlist 每只总资产 **12.5%**。
> **产品**：实盘 Settings 默认 `twin_star`（clip4）；单轨可切回作对照。
> **过去一年三方**（2026-09-02 实跑）：产品窗 `2025-08-28~2026-08-28` 上旧 15×5% 双子星 **−0.2pt 输单轨**，clip4 **+4.3pt**；滚到今日仍 +6.4pt vs 单轨、回撤钉 12.6。报告 `past_year_twin_vs_core_2026-09-02.json`；产品真值 `modules/pick-strong-track.md` §2.1。

### 3.0-legacy-clip：机会双子星 v3 15×5%（2026-09-01 · `opportunity_twin_star_v3_frozen.json`）

| 窗口 | 择强单轨 total/sr/dd | S-gap 卫星(可执行 strict) | **机会双子星 15×5%** | Δtotal | Δsr | Δdd |
|------|---------------------|---------------------------|----------------------|--------|-----|-----|
| OOS2 | +17.8 / 0.72 / 18.0 | +111.4 / 2.56 / 14.1 | **+62.9 / 1.86 / 15.7** | **+45.1pt** | +1.14 | −2.3 |
| train | +40.7 / 3.01 / 8.4 | +36.4 / 3.03 / 5.6 | **+49.1 / 3.87 / 7.6** | +8.4pt | +0.87 | −0.8 |
| valid | +139.1 / 3.37 / 11.9 | +15.4 / 1.62 / 9.2 | **+146.9 / 3.62 / 11.9** | +7.9pt | +0.26 | 0 |
| past_year | +181.2 / 2.43 / 12.6 | +51.9 / 2.16 / 7.6 | **+191.3 / 2.57 / 12.6** | +10.1pt | +0.14 | 0 |
| aligned | +190.6 / 2.54 / 12.6 | +34.6 / 1.36 / 12.4 | **+190.4 / 2.57 / 12.6** | −0.2pt | +0.03 | 0 |

> 口径：每窗空仓起步；`opp_ret = core`（`satActive=false`）否则 `core + 0.5×(sat−core)`（含 body 退出日）；卫星 `skip_t1_limit` + **`pool_mode=strict`**。已被 §3.0 clip4 替代，勿再当实盘仓位。

### 3.0-legacy：机会双子星 v2 表（exit-day 修正后 · 连续/不同切窗 · 勿当 v3）

`core_satellite_frozen_2026-08-31.json` tag=`…-v2-exitday`。past_year 核心 +202.2、aligned 双子星 −10pt——**窗口定义与连续簿不同**，不是 v3 打脸。v1 +205.9 成本逃逸作废。

**2026-09-01 涨停可成交审计（旧 50/50 被替代的根因，仍成立）**：
1. 冻结窗 671 笔卫星入场中 **一字板 24.1% + 涨停开盘 8.9% = 33% 实际买不进**
2. 可执行口径卫星：aligned 126.3→**25.4%**
3. 旧 50/50 可执行口径 aligned 仅 **+96.7%** < 核心 → 稀释
4. 卖出端 body=3 跌停封死仅 0.1% → 可忽略
5. Universe 已排除 ST/BJ/退市

> 历史快照：v1 机会口径 aligned +205.9（成本逃逸，已作废）；旧 50/50 R12 +164.0（一字板虚高，已作废）。

### 3.0b 独立腿：可执行 S-gap vs CN S-3（公平对比 · 2026-09-01）

> **目的**：把状态分桶当作**并列独立 A 股腿**，与择强 STOCK 腿生成器 **S-3** 同六窗对比。  
> **口径**：`skip_t1_limit=True` + ST/BJ/退市过滤。**禁止**用 R7/R8 的 past_year +122.8% 当真值（涨停可成交审计前）。  
> **脚本**：`scripts/compare_sgap_vs_s3.py` → `data/backtest_reports/sgap_vs_s3_YYYY-MM-DD.json`

| 窗口 | S-gap total/dd/sr | S-3 total/dd/sr | Δ(sgap−s3) |
|------|-------------------|-----------------|------------|
| OOS2 | **+111.4 / 13.7 / 2.41** | +47.3 / 18.9 / 1.26 | **+64.1pt** |
| train | **+35.3 / 7.3 / 2.98** | +34.1 / 11.6 / 2.22 | +1.2pt |
| valid | +10.3 / 12.2 / 1.05 | **+38.7 / 10.7 / 2.40** | −28.4pt |
| past_year | +29.8 / 11.5 / 1.33 | **+58.3 / 23.0 / 1.79** | −28.4pt |
| aligned | +25.4 / 11.9 / 1.02 | **+58.3 / 23.0 / 1.74** | −32.8pt |
| long2y | **+151.3 / 13.7 / 1.90** | +115.8 / 23.0 / 1.44 | **+35.5pt** |

> 冻结：`data/backtest_reports/sgap_vs_s3_2026-09-01.json`（`skip_t1_limit=True`）。S-gap 卫星列与 §3.0 可执行卫星一致。

**结论（2026-09-01）**：可执行 S-gap 在 OOS2 / long2y 显著优于 S-3，但 **past_year / aligned / valid 输给 S-3**（约 −28~−33pt）。→ **保留为研究 / 双子星卫星材料，不升格为第二股票腿**；更勿用旧 +122.8% 叙事。S-gap 的价值仍是：与择强低相关、dd 常更浅、作机会增强而非独立主仓。

### 3.0c 四态 union 合成 vs S-3（历史口径复现 · 2026-09-01）

> **目的**：复现你记得的 R6/R8 实验——**四态 OR 合成一个策略** vs CN S-3，walk-forward **三窗**。  
> **引擎**：`scout_state_bucket_pickstrong.simulate_state_bucket`（S-limit/S-gap/S-fresh/S-shrink，共享 10 槽 + 态优先级，body 3/3/15/15）。  
> **口径**：**历史成交模型**（假设涨停开盘能买，**无** `skip_t1_limit`）。  
> **脚本**：`scripts/compare_union_vs_s3.py` → `data/backtest_reports/union_vs_s3_2026-09-01.json`

| 窗口 | union total/dd/sr | S-3 total/dd/sr | Δ(union−s3) |
|------|-------------------|-----------------|-------------|
| OOS2 | **+128.6 / 19.5 / 2.44** | +47.3 / 18.9 / 1.26 | **+81.3pt** |
| train | **+76.9 / 8.4 / 3.57** | +34.1 / 11.6 / 2.22 | **+42.8pt** |
| valid | **+46.8 / 3.6 / 4.84** | +38.7 / 10.7 / 2.40 | **+8.1pt** |

> 复跑与冻结 JSON（`state_bucket_pickstrong_latest.json` / `state_union_latest.json`）**逐窗 Δ=0.00pt** → **实验成立**（三窗 union 全赢 S-3，且 dd/sr 多数更优）。
>
> **但不可直接当实盘真值**：① R10 已证共享槽 union ≈ S-limit 独裁；② 涨停可成交审计后单独 S-gap 近窗输给 S-3（§3.0b）。下一步若要做诚实对比，应在 union 引擎上加 `skip_t1_limit` 或改跑 slice2（L+G 独立槽）。

### 3.0d 四态 slice vs S-3（可执行 · Phase 1 · 2026-09-01）

> 设计：`docs/designs/state-bucket-slice-stock-leg.md` · 引擎：`service/state_bucket_slice.py` · 报告：`sliced_vs_s3_2026-09-01.json`  
> 口径：**每态独立槽** + 日收益加权 · `skip_t1_limit=True` · 三窗 OOS2/train/valid

| 变体 | OOS2 Δ vs S-3 | train Δ | valid Δ | 过线(−5pt)/3 |
|------|---------------|---------|---------|--------------|
| **G 单态** | **+64pt** | **+4pt** | −23pt | **2/3** |
| slice2_L30 | +19pt | −8pt | −28pt | 1/3 |
| slice2_LG | −7pt | −15pt | −31pt | 0/3 |
| slice3_LGS | +4pt | −20pt | −31pt | 1/3 |
| L 单态 | −56pt | −32pt | −38pt | 0/3 |

**结论**：可执行口径下 **slice 合成不能替 S-3**（valid 全线输）；「中间点」≈ **S-gap 单态**（OOS2/train 赢、valid 输）。历史 union +122.8% **不可外推**到此口径。Phase 2 替 STOCK 腿 **暂不拍板**。

### 3.1 三窗 walk-forward（历史 · R10 slice2 / R6 union 已更替）
| 结构 | OOS2 sr | train sr | valid sr |
|------|---------|----------|----------|
| R6 union-body（四态 or） | 2.44 | 3.57 | 4.88 |
| R10 slice2（L+G 切分） | 2.89 | 3.57 | 5.21 |
| **R12 双子星 50/50** | **2.88** | **5.48** | **4.74** |

### 3.2 过去一年（aligned 2025-08-28~2026-08-28 · v3 window-local）
| 指标 | 择强单轨 trail8 | S-gap 卫星(strict 可执行) | **机会双子星 v3** |
|------|----------------|---------------------------|-------------------|
| 总收益 | +190.6% | +34.6% | **+190.4%** |
| 最大回撤 | 12.6% | 12.4% | **12.6%** |
| sharpe | 2.54 | 1.36 | **2.57** |

> v3 **打平**核心总收益、Sharpe 略高。v2 aligned −10.1pt 是另一套切窗/连续簿，勿回写。v1 +205.9 成本逃逸作废。

### 3.3 与对照基准对比
| 策略 | aligned | dd | sr | 实盘默认 |
|------|---------|----|----|----------|
| 择强单轨（mom_compare+trail8） | **+190.6%** | 12.6 | 2.54 | **是** |
| **机会双子星 v3（opt-in）** | **+190.4%** | 12.6 | **2.57** | 否 |


---

## 4. 回测演进证据（R1–R6，可指向各 `_latest.json`）

### R1 状态 IC（`state_ic_latest.json` · 选因子仅 OOS2+train）
热点态振幅负 IC 比全样本强 2–3 倍且三窗同号 → 状态分桶真区分 alpha：
- `S-stress` amplitude IR **-1.30/-1.40/-0.42**、`S-gap` -0.75/-1.00/-0.56、`S-limit` -0.43/-0.60/-0.47（均 🟢）
- `S-shrink` amplitude valid **翻号**（-0.01）→ 冻结基线 edge 是 regime 依赖非状态稳健

### R2 状态 Scout 回放（`state_scout_latest.json` · 桶内 amp Q1 + R-wide）
- `S-limit` valid **+0.342** sharpe4.43、`S-gap` **+0.343** sharpe4.35（三窗全正，3.4× 旧基线）
- `S-fresh` +0.145、`S-shrink` +0.117、`S-stress` +0.079；`S-breakout` train **-15.5%** 拒收

### R3 合成（`state_or_latest.json` · S-limit∪S-gap or）
- valid **+0.330%/天**（3.3× 旧基线 +0.100）、sharpe 3.86、dd 7.8

### R4 逐态留/弃（`state_tune_latest.json`）
- **留** `S-limit`/`S-gap`（核心）+ `S-fresh`（弱）+ `S-shrink`（边际，amp Q1 压线，gap 替代失效）
- **弃** `S-stress`（最强因子仍 <0.100 基线）、`S-breakout`（train 负，无因子可救）

### R5 吃鱼身（每态 body hold · `state_body_latest.json` + `state_hold_grid_latest.json`）
- 热点态 body=3、慢态 body=15 → 四态日均全部抬升（S-limit +0.418 / S-gap +0.412 / S-fresh +0.206 / S-shrink +0.209）

### R6 合成 union + 优先级（`state_union_latest.json`）
- **A) union-body（定案）** valid +0.418/day、dd 3.6、sharpe 4.88 → 确认超 R3
- B) +收益率优先级出场 valid +0.304/day、dd 2.7（更稳但少赚，定案取 A）

### R7 联合第三类资产择强（`state_bucket_sleeve_latest.json` · 2026-08-31）
> 把状态分桶的 idle 资金接「第三类资产袖」（金/油/纳/债 mom60+MA200 择强），或把状态分桶作股票腿、与 ETF/REPO 做 pick-strong 式 argmax 100% 切换。**关键修正**：袖须带 `trail8`（ETF 峰值−8%→REPO，同 `pick-strong-track.md:27`）；初版漏 trail8 致袖平、误判"无改善"，补上后结论反转。

| 配置 | past_year CAGR | dd | sharpe | valid CAGR |
|------|---------------|----|--------|-----------|
| **状态分桶（单独）** | **+122.8%** | 8.4 | 3.36 | +137.3% |
| 第三类袖（无 trail） | +0.0% | 1.3 | 0.03 | +0.1% |
| **第三类袖（+trail8）** | **+64.4%** | 13.6 | 1.67 | +174.1% |
| 联合-卫星（无 trail） | +120.6% | 8.4 | 3.28 | +136.1% |
| **联合-卫星（+trail8）** | **+165.7%** | 11.9 | 2.90 | +288.8% |
| 联合-argmax（忠实择强 100% 切） | +38.6% | 7.1 | 2.07 | +29.8% |

> **结论（修正后）：联合 +trail8 确实改善（按收益）。**
> 1. trail8 是袖的命门：漏了袖平（0%），加上袖 +64%（past_year）/ +174%（valid）→ 联合-卫星 +165.7% / +288.8% 明显 > 状态分桶单独 +122.8% / +137.3%。
> 2. **但风险调整仍劣**：联合 dd 升到 11.9、sharpe 降到 2.90（< 状态分桶 3.36 / dd 8.4）。→ 按收益联合赢，按 sharpe 状态分桶单独赢。
> 3. **argmax(100% 切换) 仍烂 +38.6%**：状态分桶收益脉冲状，60 日动量在两信号间回落 → argmax 误判走弱切去平/负 ETF/REPO，洗掉强 alpha。**必须用卫星结构（状态分桶核心 + 袖管 idle），勿用 100% 切换。**
> 4. **自洽 pick-strong +190.7%**：初版误判"袖平→+190% 全来自 S-3"；加 trail8 后袖 +64% + S-3 +58% ≈ +190%，对上。说明第三类袖本身确有 edge（trail8 之功）。
> 5. **手上算法排序（past_year）**：择强定案 +190.7% > 联合-卫星(+trail8) +165.7% > 状态分桶 +122.8% > S-3 +58.3%。状态分桶是远比 S-3 好的 A股 股票腿。
> 6. **真正 upside（待做）**：择强架构用状态分桶替 S-3 股票腿 + trail8 袖 → 预期 > +190.7% 且 dd 更低。需接 `fused_timeline_walk.py` 真实管线验证。

### R8 状态分桶替 S-3 作股票腿 + trail8 袖（`state_bucket_pickstrong_latest.json` · 2026-08-31）
> 核心验证：择强单轨的股票腿（S-3）换成状态分桶，非股票腿保持第三类袖（金/油/纳/债 mom60+MA200+trail8）+ REPO。两种分配结构：卫星（状态分桶核心 + idle 接袖）/ argmax（pick-strong 式 100% 切换）。跑 3 窗 + 长窗（past_year）。

| 窗口 | 状态分桶(替S-3) CAGR/dd/sharpe | 联合-卫星+trail8 CAGR/dd/sharpe | 联合-argmax CAGR/dd/sharpe |
|------|-------------------------------|--------------------------------|---------------------------|
| OOS2 2024-08~2025-08 | +127.1 / 19.5 / 2.44 | **+148.6 / 19.5 / 2.64** | +48.9 / 12.1 / 1.99 |
| train 2025-08~2026-02 | +207.3 / 8.4 / 3.57 | **+283.2 / 10.8 / 4.18** | +119.5 / 10.5 / 3.11 |
| valid 2026-03~2026-08 | +137.3 / 3.6 / 4.84 | **+288.8 / 11.9 / 3.25** | +11.4 / 29.0 / 0.47 |
| past_year 2025-08~2026-08 | +122.8 / 8.4 / 3.36 | **+165.7 / 11.9 / 2.90** | +17.4 / 33.6 / 0.62 |

> **结论**：
> 1. **联合-卫星+trail8 在全部 4 窗均优于状态分桶单独**（CAGR 全更高，3 窗 + 长窗全正）→ "状态分桶替 S-3 + trail8 袖"的卫星结构有效，过纪律关。
> 2. **联合-argmax 在 valid/past_year 崩**（dd 29~33%、sharpe<0.65）→ 100% 切换把状态分桶的脉冲收益洗掉（与 R7 一致）。**必须用卫星结构，不能用 argmax 直接替股票腿。**
> 3. **代价**：联合-卫星 dd 升到 11.9（vs 状态分桶 8.4），sharpe 略降（past_year 2.90 vs 3.36）——收益换来的 dd；train/valid 卫星 sharpe 反而更高（4.18/3.25）。
> 4. **vs 择强定案 +190.7%**：联合-卫星+trail8 past_year +165.7% **略低于** 择强定案。原因：择强定案用 **argmax**（适配 S-3 的平滑动量），状态分桶是脉冲收益、不适 argmax（argmax+状态分桶仅 +17.4%）。故"替 S-3"仅在**卫星结构**成立，且不超择强定案。

### R8 子方向：状态分桶适配 argmax（股票腿动量平滑）· 已测，拒收
> 假设：argmax 崩是因状态分桶 NAV 的 60 日动量在信号结束后骤降→argmax 误逃。若用更长回看（120/250 日）平滑股票腿动量，argmax 或能在信号间歇期留在 STOCK、捕获更多。测 `combine_argmax_momlb` lb∈{60,120,250}。

| 窗口 | argmax lb60 | argmax lb120 | argmax lb250 |
|------|------------|-------------|-------------|
| OOS2 | +124.6 / dd19.5 / sr2.46 | +110.4 / dd19.5 / sr2.27 | +125.3 / dd19.5 / sr2.41 |
| train | +116.9 / dd10.5 / sr3.03 | +157.2 / dd10.1 / sr3.40 | +147.3 / dd10.1 / sr3.29 |
| **valid** | +1.5 / dd31.9 / sr0.27 | **-2.3 / dd33.0 / sr0.19** | **-2.3 / dd33.0 / sr0.19** |
| past_year | +16.6 / dd33.6 / sr0.60 | +58.5 / dd27.8 / sr1.80 | +96.9 / dd10.1 / sr3.19 |

> **结论：子方向拒收。**
> - 长回看在 train/past_year 有帮助（lb250 past_year +96.9 vs lb60 +16.6），但 **valid（验证窗）全负**（lb120/250 = -2.3%、dd33）→ 样本内改善、样本外失败，典型过拟合。
> - 状态分桶收益本质是**脉冲状**（信号日集中、间歇期 flat），任何基于历史动量的 argmax 都会在其间歇期逃向 ETF/REPO 而错失下一信号 → **脉冲性与 argmax 根本不兼容**，平滑无法根治。
> - **最终定位（修正 R8.4）**：状态分桶是远比 S-3 好的**独立 A股 策略**（+122.8% vs S-3 +58.3%），但**不能替入择强的 argmax 股票腿**。二者是**不同角色**而非可互换：
>   - 择强（argmax + S-3 + trail8 袖）= 当前最高收益引擎（+190.7%），靠 argmax 适配 S-3 平滑动量。
>   - 状态分桶 + trail8 袖（卫星）= +165.7%，收益略低但 dd 更低（11.9 vs 12.6）、且纯 A股 容量受限。
>   - **真正 upside 不在"替 S-3"，而在：状态分桶作卫星 + 择强作核心的 core-satellite 组合**（互不替、各自最优结构），或等状态分桶 holdout 验证后实盘对照。

### R9 Core-Satellite 组合（择强核心 + 状态分桶卫星）· `core_satellite_trail8_latest.json` · 2026-08-31
> 两支引擎独立 NAV 按 CN 日历对齐后加权混合。择强 NAV 取自 `pick_strong_grid.build_nav_from_cache(trail_pct=8.0)`（**含 trail8 的定案 E1**，已双查可复现）；状态分桶 NAV 取自其 simulate。权重 core/sat ∈ {80/20,70/30,50/50}。
> ⚠️ **首版修正**：初版误用 `build_fused_nav`（**无 trail8**）→ 择强被低估（past_year +85 而非 +181/+190）。改用 `build_nav_from_cache(trail_pct=8.0)` 后精确复现 E1（OOS2 17.82 / train 40.66 / valid 139.07 全吻合冻结 `pick_strong_trail8_20260829.json`；past_year 181.16 vs 190.65 仅起始日差）→ **定案无数据漂移，可复现**。下表为修正版。

| 窗口 | 择强核心(trail8) CAGR/dd/sr | 状态分桶 CAGR/dd/sr | corr | 50/50 CAGR/dd/sr |
|------|------------------------------|----------------------|------|------------------|
| OOS2 | +17.7 / 18.0 / 0.72 | +127.1 / 19.5 / 2.44 | 0.08 | +67.4 / 16.4 / 2.28 |
| train | +95.8 / 8.4 / 3.01 | +207.3 / 8.4 / 3.57 | 0.07 | +150.1 / 5.8 / 4.51 |
| valid* | +610.7 / 11.9 / 3.37 | +137.3 / 3.6 / 4.84 | -0.02 | +332.5 / 6.0 / 4.58 |
| past_year | +175.6 / 12.6 / 2.43 | +122.8 / 8.4 / 3.36 | 0.02 | +155.7 / 6.4 / 3.70 |

> \* valid 仅 112 日，CAGR 年化失真（择强 total +139%、状态分桶 +46.8%）；短窗用 total 比较更公允。train 同理（total +40.7%）。

> **结论（修正版）**：
> 1. **两策略日收益相关极低（corr 0.02~0.08，valid 甚至 -0.02）** → 机制正交（择强=跨资产动量 argmax+trail8；状态分桶= A股 低波尾状态），**理想互补、非替代**（修正 R8"替 S-3"误判）。
> 2. **混合在所有窗口 sharpe 高于任一带**（past_year 3.70 / valid 4.58 / train 4.51 / OOS2 2.28），且 **dd 显著低于择强单独**（past_year 12.6→6.4、valid 11.9→6.0、train 8.4→5.8）→ 状态分桶卫星压低核心回撤、提升风险调整收益。
> 3. CAGR 为加权均值（介于二者）；因择强(trail8)本身已强（+175% past_year），50/50 混合仍达 +155.7% past_year CAGR、dd 仅 6.4。
> 4. **权重**：状态分桶 sharpe/dd 更优，组合宜偏卫星（≥30%）；50/50 已显著改善 dd 与 sharpe。
> 5. **caveat**：择强引擎不计交易成本、状态分桶计 0.3% 往返——加成本后择强略降，但 corr/dd 改善结论不受影响。
> 6. **最终定位**：状态分桶与择强单轨是**互补角色**；正确用法 = **择强(trail8)作核心 + 状态分桶作卫星**，组合显著降低回撤、提升 sharpe，是二者的最优结合结构。

---

### R10 按态切分（穿透分析）· `state_per_type_latest.json` / `state_sliced_latest.json` · 2026-08-31
> 质疑 union"四态一锅烩"：逐态独占 10 槽回测 + union 槽位争抢审计 + 等权切分对比。**冻结 union 实为"S-limit 独裁"**：每窗 S-limit fills 130~732，S-gap 仅 0~12（优先级 4>3 饿死）、S-fresh/S-shrink fills≈0（S-fresh 候选海啸 OOS2 89,313 个、fills 1）。

**逐态画像（独占 10 槽，CAGR/dd/sr）**：

| 状态 | OOS2 | train | valid | past_year | corr_择强 |
|------|------|-------|-------|-----------|-----------|
| S-limit | +115.7/19.5/2.28 | +201.0/7.1/3.57 | +137.3/3.6/4.84 | +121.1/7.1/3.38 | +0.02~+0.09 |
| S-gap | +168.3/12.2/**3.54** | +131.3/10.1/3.15 | +135.0/3.2/**4.94** | +92.4/11.7/3.12 | −0.03~+0.09 |
| S-fresh | +29.6/21.3/1.01 | +7.3/8.3/0.63 | −2.4/6.1/0.15 | +5.8/8.3/0.53 | +0.07~+0.20 |
| S-shrink | +57.2/18.5/1.48 | +71.4/12.0/1.53 | +60.7/5.9/**3.79** | +50.8/12.0/1.51 | −0.07~+0.02 |

**切分 vs 共享槽 union（sharpe）**：

| 结构 | OOS2 | train | valid | past_year |
|------|------|-------|-------|-----------|
| union（冻结） | 2.44 | 3.57 | 4.84 | 3.36 |
| **slice2 = L+G 50/50** | **2.89** | **3.57** | 5.21 | **3.45** |
| slice3 = L+G+S 1/3 | 2.83 | 3.51 | **5.50** | 3.37 |
| slice4 = 全四态 | 2.52 | 3.44 | 5.24 | 3.28 |

> **结论**：
> 1. **union 的共享槽+优先级是错误结构**：S-limit 先到先得占满槽，S-gap（OOS2 最强态 sr 3.54）被饿死，S-fresh 灌候选池噪声。union ≈ S-limit 单独 + 残余。
> 2. **S-fresh 实锤删除**：独占回测全窗 sr≤1.01（valid 为负），切分加它全窗劣化（slice4 < slice2）。
> 3. **slice2（S-limit+S-gap 等权切分，每态独立 10 槽）四窗 sharpe 全部 ≥ union、dd 全面 ≤ union**，代价：S-limit 独大窗（train/past_year）CAGR 让出（+165/+107 vs +207/+123），OOS2/valid CAGR 反超。符合"重稳不追绝对收益"偏好。
> 4. S-shrink 是唯一真分散腿（与择强 corr −0.07~+0.02）：valid 上 slice3 sr 5.50 / dd 2.3 最强；作为低波稳定腿保留在"可加"集合，但默认 slice2。
> 5. **core-satellite 重做（slice2 作卫星 + 择强 trail8 核心，`core_satellite_sliced_latest.json`）**：corr 0.01~0.09（更低）；50/50 全窗 sharpe 4.64/4.60/3.66/2.55 均高于择强核心，dd 全面压至 ≤14.8（OOS2 14.8、train 5.8、valid 6.0、past_year 6.4）；与 union 卫星版（R9）混合表现相当、卫星单独更优。
> 6. holdout（2026-08-08~）数据尚未到齐，待补。

---

### R11 逐态参数寻优 + 逐态×择强组合（`state_optimize_latest.json` / `state_pk_combo_latest.json` · 2026-08-31）
> 网格：bucket_q∈{2,3,5} × max_pos∈{5,10,15} × body∈{2,3,5}；**只在 OOS2+train 选参**（mean sharpe），valid/past_year/aligned 只验证。

| 状态 | 最优参数 | 选参窗 mean_sr | 验证结论 |
|------|----------|---------------|----------|
| S-limit | bucket_q=2, 10槽, body3 | 2.984（原 2.925） | OOS2 +126（原+116）、past_year +128（原+121）→ 稳健小幅提升 |
| **S-gap** | **bucket_q=3, 15槽, body3** | **3.894（原 3.345）** | train sr 4.65（原3.15）、past_year +156/sr4.00（原+92/3.12）→ **大幅提升** |
| S-shrink | bucket_q=2, 15槽, body3 | 3.476（原 1.505） | **valid 崩盘 sr 0.49（原 3.79）→ 过拟合拒收**（纪律案例：选参窗好看≠真 edge） |

**逐态 × 择强 trail8（core/sat 50/50 混合，CAGR/dd/sr）**：

| 窗口 | 卫星 slice2_old（R10 冻结） | 卫星 slice2_opt（L+G 优化） | **卫星 G_opt（S-gap 单独）** |
|------|----------------------------|------------------------------|------------------------------|
| OOS2 | +72.0/14.8/2.55 | +83.2/17.3/2.60 | **+101.1/18.2/2.88** |
| train | +131.7/5.8/4.64 | +167.1/5.4/5.07 | **+182.6/5.2/5.48** |
| valid* | +331.6/6.0/4.60 | +352.2/6.0/4.68 | **+372.4/6.0/4.74** |
| past_year | +146.0/6.4/3.66 | +164.1/6.4/3.87 | **+171.7/6.4/3.98** |
| aligned | +149.4/6.4/3.76 | +162.2/6.4/3.87 | **+164.0/8.1/3.81**\* |

> \* R11 当时 aligned=+167.3/6.4/3.93，2026-08-31 v2 重固化修正（见 §3.0 注）。

> \* valid 短窗，CAGR 年化失真，看相对差异即可。

> **结论（R11）**：
> 1. **逐态优化有效且非过拟合**（valid/aligned 样本外仍优）：S-gap bq3/15槽 提升最大；S-shrink 优化被 valid 崩盘拒收——选参窗 mean_sr 3.48 是过拟合幻觉，纪律拦截。
> 2. **逐态×择强组合：S-gap（优化）单独作卫星全窗最优**——它的 corr 与择强最低（0.00~0.08）、sr 全窗 3.14~4.65；**并入 S-limit 反而稀释**（L sr 更低、corr 略高）。
> 3. 最优组合 50/50：aligned +167.3%/dd6.4/sr3.93、past_year +171.7%/6.4/3.98、train +182.6%/5.2/5.48 → **全面超 R10 slice2 卫星版**（sr +0.13~+0.84）。
> 4. caveat：G_opt 单独 dd 较高（OOS2 22.1）；50/50 后 dd 全窗 ≤6.4（OOS2 例外 18.2，该窗择强本身 flat）。卫星只用 S-gap = 放弃 S-limit 的 alpha 换更纯的对冲腿，取舍待拍板。
> 5. holdout 未到齐，待补。

> **R12 拍板冻结（2026-08-31）**：基线 = **择强 trail8 核心 + S-gap(优化) 卫星，50/50**（`core_satellite_frozen_2026-08-31.json`；`scout_baseline_state_body.json` 更替为 S-gap 优化配置）。S-limit/S-shrink 降为可选腿。

---

## 5. 纪律、风险与待办

- **回测 ≠ 实盘**：所有数字为历史回放，含 0.15% 单边滑点、0.3% 往返成本假设；未含停牌、容量冲击（≤200 万上限即为此）。
- **待验证**：`holdout 2026-08-08~2027-02-08` 只读；截至 2026-09-01 partial（15 日）机会 vs 核心 −2.7pt，未满窗。
- **实盘默认（2026-09-02）**：机会双子星 v3.1 clip4；单轨择强为 Settings 对照。
- **与择强单轨互补**（core-satellite）：核心 50–100% 择强（跨资产高收益）；卫星 ≤200 万 / 开闸时 50% 状态分桶（纯 A 股低 dd alpha）。二者资产正交 + A 股内机制不同（动量强股 vs 低波尾特殊态），回撤时序错开。相关性需同窗 daily NAV 验证（待跑）。
- **冻结基线变更**：本算法冻结为 `scout_baseline_state_body.json:1`，**替换**旧 `20-150 amp_q10` 基线（旧基线归档保留，不动）。

---

## 6. 代码入口

| 用途 | 路径 |
|------|------|
| 定案回放（union-body） | `scripts/scout_state_union.py:1` |
| 过去一年验证 | `scripts/scout_state_pastyear.py:1` |
| R1 状态 IC | `scripts/scout_state_ic.py:1` |
| R2 状态 Scout | `scripts/scout_state_scout.py:1` |
| R3 合成 | `scripts/scout_state_or.py:1` |
| R4 留/弃 | `scripts/scout_state_tune2.py:1` |
| R5 body / hold 网格 | `scripts/scout_state_body.py:1` · `scripts/scout_state_hold.py:1` |
| 冻结基线 | `data/backtest_reports/scout_baseline_state_body.json` |
| **机会双子星 v3 冻结** | `data/backtest_reports/opportunity_twin_star_v3_frozen.json` |
| **v3 对照脚本** | `scripts/compare_ps_g50x_deep.py` |
| **双子星冻结（R12 / v2 连续簿）** | `data/backtest_reports/core_satellite_frozen_2026-08-31.json` |
| **独立腿 vs S-3（S-gap 可执行）** | `scripts/compare_sgap_vs_s3.py` |
| **四态 union vs S-3（历史复现）** | `scripts/compare_union_vs_s3.py` → `union_vs_s3_*.json` |
| Timeline 独立腿 | `GET /api/backtest/timeline?strategy=state_bucket` |
| 双子星 API | `GET /api/backtest/timeline?strategy=twin_star` · `GET /api/backtest/twin-star/action` |
| 服务层 S-gap | `service/state_bucket_track.py`（`build_sgap_timeline` / `build_state_bucket_timeline`） |
| 14:30 前提醒 | `scheduler/twin_star_reminder_job.py`（工作日 14:20 · webhook `twin_star_reminder` + 通知中心 `twin_star` 类型） |

*创建 2026-08-31 · 状态：R1–R12 跑完、R12 双子星冻结、holdout 待只读确认。*

---

## 7. 复刻指南（仅凭本文档 + pick-strong-track.md 可重建全部策略）

> 目标：无代码、无脚本也能逐字重建「双子星 (Twin-Star)」冻结策略。所有口径以本节为准（与代码 `scripts/scout_state_bucket_pickstrong.py` / `scripts/pick_strong_grid.py` 逐行对应）。

### 7.1 数据源（Postgres，tushare 导入）

| 表 | 字段 | 用途 |
|----|------|------|
| `daily` | `trade_date, ts_code, open, high, low, close, pre_close, amount` | 日线 + 成交量额 |
| `stock_dailybasic` | `trade_date, ts_code, total_mv` | 总市值（万→亿：÷10000）；**无 mv 的票当日剔除** |
| `stock_basic` | `ts_code, list_date` | 上市日（次新判断，已弃用但仍过滤） |

### 7.2 每日截面特征（对每只票，T 日）

- `amp = (high − low) / close`；`turn = amount / mean(前20日 amount)`；`gap = open / pre_close − 1`。
- 涨停板价：`pre_close × 1.095`（60/00 开头 SH/SZ）或 `× 1.195`（其余，创业板/科创）；`close ≥ 板价 − 1e-6` → `S-limit`。
- `gap > 0.03` → `S-gap`。
- 全市场分位（当日截面）：`amp_q10`、`amp_q70`、`turn_q30`。`amp ≤ amp_q10 且 turn ≤ turn_q30` → `S-shrink`；`turn > 2 且 amp > amp_q70` → `S-stress`（弃用）；其余状态已弃。
- `breadth = (close > MA20 的票数) / (有≥20日历史且有 mv 的票数)`；`R-wide = breadth > 0.5`。

### 7.3 S-gap 卫星引擎（冻结参数，R11 寻优 / R12 冻结）

- **状态**：仅 `S-gap`（gap>3%）。
- **候选**：当日 `S-gap` 桶内按 `amp` **升序**取前 **1/3**（`bucket_q=3`，最低波 33%）。
- **闸**：仅 `R-wide` 日开仓（用 T 日 breadth；候选信号用 **T-1 日** 状态）。
- **入场**：T 日 **open** 价；**单边滑点 0.15%** 含在 `COSTS_ROUNDTRIP=0.003`（0.3% 往返）内，出场时一次性计提。
- **持仓**：`body=3` 交易日——**入场日算第 1 天，第 3 日 close 出**（周一买 → 周三收盘卖）。**无保护止损**（`protect5` / trail-after-body 2026-09-03 三窗拒收；Live 已对齐）。`max_pos=4`、每槽 `POSITION_PCT=0.25` → **卫星套筒满仓 100%**（4×25%；合成后每只总资产 12.5%）。v3 的 15×10%（名义 150%）已由 v3.1 替代。
- **NAV**：`NAV = 1 + Σ已实现 + (Σ槽位市值 − 槽位数×clip)`；信号取 T-1 收盘 → T 开盘执行，无前视。
- 宇宙 = 全 A 非 ST/BJ/退市（`stock_basic` JOIN：`delist_date IS NULL`、`name NOT LIKE '%ST%'`、排除 `.BJ`）。

### 7.4 择强核心引擎（冻结，见 `pick-strong-track.md`）

- 候选 = {STOCK（S-3 CN+HK 当日持仓篮，等权，强度=mom60 均值，有仓即入池）} ∪ {GOLD/OIL/NASDAQ/BOND：`mom60 ≥ 0` 且收盘 ≥ MA200}。
- `argmax mom60`，**100% 硬切**，`min_hold=1`，切换成本 0（压力测试 5–10bp 仍稳）；空 → REPO。
- **trail8**：持有 ETF 期间从入场后峰值回撤 **8%** → 当日切 REPO。
- 参数：`LOOKBACK=60`、`MA=200`、`mom_compare` 口径，信号用 T-1 收盘。

### 7.5 组合结构（机会口径 · 2026-09-01 v3）

- **闲置**：`satActive=false` → 100% 核心日收益（卫星资金跟核心 = 「买不进就买别的资产」）。
- **占用**：`satActive=true`（隔夜持仓 **或** body 退出日）→ `opp_ret = core_ret + 0.5×(sat_ret − core_ret)`。
- **卫星选股 `pool_mode=strict`**：全体 S-gap 按 amp 升序取前 1/3，**再**剔除 T-1 涨停。禁止先剔除再补仓（replace/fallback 已拒收）。
- **禁止**固定每日 50/50（可执行口径下空仓吃 0，past_year 塌 70pt+）。

### 7.6 验证窗口与冻结数字（v3.1 clip4 · 见 §3.0 表）

> 复现：核心 pick_strong；卫星 `build_sgap_timeline(..., skip_t1_limit=True, pool_mode="strict")`（默认 4×25%）；合成 `build_twin_star_timeline(..., opportunity=True)`；对照 `opportunity_twin_star_v3_clip4_frozen.json`。

### 7.7 实盘执行映射（机会双子星 · 实盘默认）

- **默认实盘**：机会双子星 v3.1 clip4（Settings 默认 `twin_star`）。
- **执行**：
  - 无卫星持仓且今日不开新仓 → **核心 100%**。
  - 开闸且 **strict 可成交候选非空** **或** 持仓簿非空 → **核心 50% / 卫星 50%**。
  - 卫星每只 **总资产 12.5%**（套筒 25%），最多 **4 只**。
  - 涨停买不进 → **放弃该票**（不顺位补），空出来的钱留在核心。
  - body=3 收盘卖（入场日=第 1 天；持仓簿 `exitsDue`）。**不要**挂 −5% 券商止损、也不要看见 −5% 就卖。
  - 执行顺序：先按 `coreTargetPct` 调核心腿；再按持仓簿卖到期 / 开闸买 strict 候选。
  - 核心 pick=STOCK 时 S-3 篮仍是 **10×10%**（收到 5/4/3 只 2026-09-03 OOS2 拒收）。Watchlist top 5 只是展示。
- **对照**：Settings 切 `single_track` = 无卫星的纯择强。
- **讨论记录**：[`clip4-ops-decisions-2026-09-03.md`](./clip4-ops-decisions-2026-09-03.md)。

### 7.8 14:30 前操作提醒 + 卫星持仓簿

- **调度**：工作日 14:20（`twin_star_reminder_job`）→ webhook `twin_star_reminder` + 通知中心。
- **API**：`GET /api/backtest/twin-star/action` → core + sat(gate/candidates) + **`book.openPositions/exitsDue`** + `coreTargetPct`。
- **持仓簿**：引擎短窗回放 `build_sgap_timeline` 的 `openPositions`（非券商成交；手动执行仍需自行下单）。
- **Holdout**：`2026-08-08~2027-02-08` 只读；截至 2026-09-01 仅 partial（15 日）：核心 +2.7 / 机会 +0.0 / vs_core −2.7pt —— 未跌破灾难线，但未证明超额；待满窗再确认。
