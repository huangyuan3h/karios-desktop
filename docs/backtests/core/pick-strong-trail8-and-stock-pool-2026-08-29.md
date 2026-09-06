# 择强单轨 · trail8 吸收判定 + STOCK 入池实验规划（2026-08-29）

> 澄清：此前 **live / Watchlist** 已有 ETF 峰值 −8%→REPO；**定案回测 `mom_compare` 绝对 NAV 未吸收**。  
> 本轮在 fused NAV 上跑完 Q8，并启动 STOCK 入池单变量实验。

报告 JSON：`services/data-sync-service/data/backtest_reports/pick_strong_trail8_20260829.json`  
脚本：`scripts/pick_strong_grid.py --batch E`

---

## 1. Q8 结果：`mom_compare` ± trail8（绝对 NAV）

口径：LB60 · MA200 · hold1 · cost0 · 同权 argmax；trail 仅作用于 **ETF 腿**（STOCK 仍走 S-3 自身退出）。

| ID | trail | OOS2 | train | valid | past_year | long | 判定 |
|----|-------|------|-------|-------|-----------|------|------|
| A0 / E0 | 0 | 17.8 / 18.0 | 35.7 / 11.6 | 56.5 / 28.3 | 93.6 / 28.3 | 53.3 / 51.3 | 基线 |
| **E1** | **8** | **17.8 / 18.0** | **40.7 / 8.4** | **139.1 / 11.9** | **190.7 / 12.6** | **128.6 / 49.9** | ✅ |

相对 A0：

| 窗 | Δ收益 | ΔDD | trail 触发次数 |
|----|-------|-----|----------------|
| OOS2 | 0 | 0 | 0 |
| train | **+5.0pt** | −3.2 | 1 |
| valid | **+82.5pt** | −16.4 | 6 |
| past_year | +97.1 | −15.7 | 6 |
| long | **+75.3pt** | −1.4 | 6 |

铁律：OOS2 零劣化；train/valid/long 同向改善；故事 = ETF 峰值回撤时切 REPO，避免滞后 MA200 的尾段，并可把仓位让给当日更强的 STOCK/其他腿。

### 定案

**吸收 trail8 进择强单轨定案**（回测 SSOT + live 一致）：

- `pick_strong_track.TRAILING_PCT = 8`（Timeline / API）
- `pick_strong_grid` 默认对照基线改为 A0+trail8（后续 STOCK 实验以此为靶）
- 文档：`modules/pick-strong-track.md`

Live 路径此前已有，无需再开开关。

---

## 2. STOCK 入池规则 · 单变量实验规划（下一批）

### 2.1 现状（基线 S0）

有任意 CN/HK S-3 持仓 → STOCK 入池，强度 = 持仓股 `mom60` 均值；**无** MA 闸、无 n 门槛、无 mom 符号闸。

### 2.2 假设（每次只改一条）

| ID | 单变量 | 假设 |
|----|--------|------|
| **S0** | （对照）有仓即入池 | 新基线 = A0 + trail8 |
| **S1** | `n_positions ≥ 2` 才入池 | 单票噪音篮拉低 mom，误占 100% |
| **S2** | 篮 `mom60 > 0` 才入池 | 负动量股票篮不该进同权池 |
| **S3** | 篮内 ≥50% 标的 close≥自 MA200 才入池 | 与 ETF「站上均线」同构，滤下跌刀 |
| **S4** | 仅 CN 入篮（HK 不计入 mom/仓） | HK realism 弱，拖累择强 |
| **S5** | 仅当篮 mom ≥ 最强 ETF mom − 0（即仍 argmax，但 STOCK 必须 mom>0 且…） | 并入 S2；本批不单开「gap」避免双变量 |

预注册上限 **≤5**（S1–S4 + S0）；**不做** regime 闸（依赖标签、易前视）。

### 2.3 择优协议（不变）

- 靶：**S0 = mom_compare + trail8**
- train 择优；valid 确认（相对 S0 **>5pt 劣化拒收**）
- OOS2 只读（相对 S0 不差 >10pt）
- long / past_year 展示；`n` 过小标 underpowered

### 2.4 命令

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/pick_strong_grid.py --batch S \
  --windows OOS2,train,valid,past_year,long \
  --json data/backtest_reports/pick_strong_stock_pool_20260829.json
```

（`--batch S` 实现见同日脚本改动；基线自动带 `trail_pct=8`。）

### 2.6 实跑结果（2026-08-29 · 靶 = S0 trail8）

| ID | 闸 | OOS2 | train | valid | past_year | long | 判定 |
|----|----|------|-------|-------|-----------|------|------|
| S0 | 有仓即入 | 17.8/18.0 | 40.7/8.4 | 139.1/11.9 | 190.7/12.6 | 128.6/49.9 | 基线 |
| S1 | n≥2 | 3.9/21.7 | 40.7/8.4 | 55.1/11.9 | 88.5/19.6 | 51.9/40.3 | ❌ OOS2/valid |
| S2 | mom>0 | 16.9/18.0 | 39.4/8.4 | 129.9/11.9 | 177.2/12.6 | 126.1/50.4 | ❌ valid−9.2 |
| S3 | MA≥50% | 18.0/14.4 | 29.5/7.5 | 59.1/11.9 | 104.7/11.9 | 132.2/49.9 | ❌ train/valid |
| S4 | CN-only | −9.7/29.6 | 51.9/18.9 | 59.1/11.9 | 102.1/15.3 | 33.2/49.9 | ❌ OOS2/valid |

**结论：全部拒收。** 维持「有仓即入池」。S2 最接近但仍 valid −9pt；S4 train 好看但 OOS2/valid 崩（不可用）。  
下一刀收益：**不继续扫 STOCK 入池闸**；转向执行 realism（paper next_open / C4）或观察层脉冲天平。

JSON：`data/backtest_reports/pick_strong_stock_pool_20260829.json`

---

*创建 2026-08-29 · trail8 ✅ 吸收 · STOCK 池 ❌ 全拒 · 状态闭环。*
