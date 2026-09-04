# 行业 × 风格 分池优化计划（Industry-Style）【2026-08-31 启动 → 2026-08-31 结案·拒收】

> **定位**：全 A 无池，按 `行业` 切分后再在行业内做 `风格` 二切，验证"不同行业该被不同风格操作"的假设。
> **动机**：泛池（如 `20-150` 3794只）把 `消费低波` 与 `科技高波` 混同，稀释 `IC`；`行业` 是风格的载体，分行业后 `R0-IC` 可显各行业最稳风格。
> **纪律**：沿用 `docs/backtests/README.md:28` 三窗铁律（OOS2/train/valid + holdout只读，>5pt劣化拒收）；`n<80` 行业并簇、`n<150` 不进 Scout 回放。
> **数据优先**：先 `n` 足、`IR` 显著的行业风格才信，模糊假设一律让位数据。
> **状态 2026-08-31 结案**：**I1-I3 全量跑完，结论=行业不是 alpha 的区分维度，全方案拒收**；见 §3 回填与 §4 复盘。新方向为动态状态×regime 分桶（`docs/backtests/state-regime-plan-2026-08-31.md`）。

---

## 0. 基线与口径

- 窗口：`OOS2 2024-08-01~2025-08-01` / `train 2025-08-01~2026-02-01` / `valid 2026-03-01~2026-08-07` / `holdout 2026-08-08~2027-02-08`
- 宇宙：**全 A 无池**（去 `ST`、去 `BJ` 北交，`stock_basic.list_status='L'`），行业取 `stock_basic.industry`（申万一级约 28 个）
- 引擎：`services/data-sync-service/src/data_sync_service/service/backtest_engine.py:1` 的 `BacktestData/BacktestConfig`，复用 `SCOUT_CONFIG` 预设
- 执行：`next_open` 入场，单边滑点 `0.15%`（低流动加压），`position_pct 0.10 / max_positions 10 / cash≤1.0`
- 风格切维：`amplitude 10d` 低波 Q10 vs 高波、`turnover 20d` 低流动 vs 高流动、`total_mv` 小/中/大
- 判定：**三窗0劣化 + 单窗+5pt** 且 `holdout` 不跌；`n<80` 行业→并至 `周期` 簇；`n<150` 仅出 `IC` 不回放

---

## 1. 实验清单

| 轮 | 代号 | 假设 | 变量 | 状态 |
|----|------|------|------|------|
| I0 | `style_classify` | 全 A 可按 `行业×低波` 聚类出稳定风格 | `mv<50 & amp<0.05` 等 4 风格桶规模 | ✅ 已跑（`small_lowvol 1832` / `mid_lowvol 1663` / `large_lowvol 467` / `small_highvol 532`） |
| I1 | `industry_ic` | 各行业 `amplitude` 因子 `IC` 方向/强度不同 | 28 行业 × `amp 10d` 前瞻 5d/10d 三窗 Spearman IC | ✅ 已跑（`industry_ic_latest.json` 2因子 / `industry_ic_full_latest.json` 8因子） |
| I2 | `industry_style` | 每行业最稳风格不同（医药低波 vs 有色高波） | 每行业内 `低波Q10 / 高波 / 低流动` 三窗 IC 选最优风格 | ✅ 已跑（合并至 I1 full 8因子，见 §3） |
| I3 | `industry_scout` | 分行业最优风格 `Scout` 回放超泛池 `+0.100%/天` | 每行业 `amp_q10 10d breadth>0.5` `10%×10` 三窗 NAV | ✅ 已跑（`scout_cluster_real.py:1` → `per_cluster_real.json`，**三簇全拒收**） |
| I4 | `industry_or` | 跨行业 `or` 合成多风格 sleeve | 各行业信号 `or` 合成总 NAV vs 泛池 Scout | ⏸ 取消（I3 已证行业无区分力） |

---

## 2. 风格桶基线（I0 · 2026-08-31）

> 脚本 `scripts/scout_style_all.py:1` / `scripts/scout_style_opt.py:1`；全 A 去 ST 后 `mv 5535 / amp 8129 / 合并 8130`

| 风格桶 | 定义 | 数量 |
|--------|------|------|
| `ALL` | 全 A 无池 | 8130 |
| `small_lowvol` | `mv<50亿 & amp<0.05` | **1832** |
| `small_highvol` | `mv<50亿 & amp≥0.05` | 532 |
| `mid_lowvol` | `50≤mv<300亿 & amp<0.05` | 1663 |
| `large_lowvol` | `mv≥300亿 & amp<0.05` | 467 |

---

## 3. 结果回填（2026-08-31 结案）

### I1 行业 IC（8因子全量 · `industry_ic_full_latest.json` 1.1M）

> 144 行业，`n≥10` 有效 89 个；`h10` 三窗 `IR` 同号且 `|IR|` 最大的因子即该行业"最稳因子"

| 最稳因子 | 行业数 | 代表行业（avgIR） | 判定 |
|----------|--------|-------------------|------|
| `amplitude`（低波） | **48** | 软件服务 -0.64 / 电气设备 -0.50 / 食品 -0.48 / 化工原料 -0.46 / 医药 -0.45 | 🟡 主导 |
| `turnover_spike`（低换手） | 19 | 影视音像 -0.50 / 汽车配件 -0.43 | 🟡 |
| `neg_mv`（小盘） | 14 | 文教休闲 +0.59 / 家居 +0.51 / 造纸 +0.47 | 🟡 |
| `ret5 / dist_high5 / down_cnt / gap` | 8 | 散 | ❌ 弱 |

> 诊断：`amplitude` 三窗同负占 48/89（54%）+ 未归它的行业里 `amplitude` 仍多数同负 → **低波异象跨行业广谱**；仅 `元器件/通信设备/铜/路桥/农用机械` 5 行业翻号（周期高波失灵）。

### I3 分簇 Scout 回放（真引擎 · `scout_cluster_real.py:1`）

> 按"每行业最稳因子"聚类成员 → 簇内各自因子 `Q10 10d` `10%×10` 回放（**无 breadth 闸**，对照 20-150 基线的 breadth>0.5）

| 簇 | 因子 | 成员 | OOS2 total/daily | train total/daily | valid total/daily | 判定 |
|----|------|------|------------------|-------------------|-------------------|------|
| `amplitude` | 低波 Q10 | 8639 | +12.2% / +0.048% | **-7.1%** / -0.055% | **-5.8%** / -0.052% | ❌ 拒收 |
| `turnover_spike` | 低换手 Q10 | 977 | +92.1% / +0.363% | +21.3% / +0.167% | **-15.3%** / -0.136% `dd39.7` | ❌ 拒收（valid 崩） |
| `neg_mv` | 小盘 Q10 | 434 | **-33.4%** / -0.131% | - | - | ❌ 拒收（OOS2 即负） |

> **三簇全部拒收**。`amplitude 簇`即全 A 无池版，缺 `breadth>0.5` 闸从 `+11.3` 翻 `-5.8`——与 20-150 基线"满仓 98% -13.9 → breadth 压仓才转正"完全一致，再次确认 **regime 闸是唯一 5pt 级转正项**。

---

## 4. 复盘（为什么行业分桶失败 · 2026-08-31）

**对的**：
1. 数据优先、不预设——全 A 无池 IC，让数据说话
2. 负结果有价值：低波异象是 A 股**跨行业广谱现象**（游资拉高即跌），非行业特有
3. 坚持真回放不信 IC——回放直接戳穿"分行业最优因子"假象

**错的（关键）**：
1. **行业是错误的分类型维**——`stock_basic.industry` 144 细行业大多 <100 只（`n<30 underpowered`），且**静态、与 alpha 正交**；真实驱动小资金 alpha 的是 `市值×流动性×振幅` 风格因子，**跨行业适用** → 80% 归一类不是分类失败，是数据在说 **alpha 不按行业分**
2. **选因子有 look-ahead + multiple-testing**——用 valid 窗 IC 选"每行业最优因子"再回放同一窗 = 循环论证；89×8=712 假设，选出的"最优"多靠运气（回放即证：三簇无一 survive）
3. **只做静态截面，缺动态状态**——"某种情况下"是**动态状态**（涨停/连板/换手状态/距新高/次新）+ **regime**（大盘宽度，唯一转正闸）

**结案**：行业×风格方案拒收归档；新方向 = **状态 × regime 分桶**（`docs/backtests/state-regime-plan-2026-08-31.md`）。
