# 组合回测审计结论（2026-08-29）

> 执行计划：[`audit-plan-2026-08-29.md`](./audit-plan-2026-08-29.md)  
> 冻结：`git HEAD d7579233` · CN baseline sha256 `da6940b5…` tag `s3-baseline-20260828-nav` · HK baseline `b0acebb3…`（2026-08-13，**过期**）

---

## 0. 总判（给决策用）— 2026-08-29 P0 修复后

| 问题 | 修复后 |
|------|--------|
| M1 套筒净值口径 | ✅ `engine_nav_by_day`；基线=引擎 47.3/34.1/38.7；增量 +3.1/+8.4/+22.3 |
| M2 dual 崩溃 | ✅ 已跑通；R5CS vs R5C +3.3/+8.4/+13.5 |
| M3 HK 基线过期 | ✅ 重固化 31.3/1.9/60.7；旧 270% 封存 legacy |
| 文档/UI 漂移 | ✅ strategy-params · BacktestPage · watchlist/dashboard export 对齐 |

详见正文原审计；本段为修复闭环。

---

## 0b. 修复前总判（归档）

| 问题 | 结论 |
|------|------|
| **有没有「算错方向」的重大逻辑谬误？** | **有（套筒 / 联合层）**：用「固定成本仓位权重 × 日收益复利」重放净值，系统高估上涨段；与引擎真实现金 NAV **不是同一口径**。 |
| **S-3 CN 主线回测是否可信？** | **主链可复现、方向成立**。现行 NAV + `next_open` + 现金≤100% + 流动性 0.7 亿基线 **可精确复现**；关掉 env 5 件套后 OOS2/train 仍显著为正 → **核芯 alpha 不依赖 valid 偷看**。 |
| **文档数字是否还能当真值？** | **大量过期**（修复前）。 |
| **1+2 组合结论是否站得住？** | **修复前不能**；修复后套筒/R5CS 可引用上表增量。 |

---

## 1. 复现矩阵（本轮实跑）

| # | 文档承诺 | 实得 | 判定 | 根因 |
|---|----------|------|------|------|
| A1 CN realism 43.1/35.6/43.3 | **47.3 / 34.1 / 38.7**（与 `walk_forward_baseline.json` 持平） | ⚠ 文档过期 / ✅ 对现行基线 | 2026-08-28 已切 NAV + `next_open`，文档未同步 |
| A2 过去一年 66.6% | 本轮未重跑 past_year | ☐ 跳过 | 不影响主结论 |
| A3 HK qfq 270/27/61 | **11.9 / -3.2 / 61.4**（`next_open`）；`close` 亦仅 **27.1 / 3.2 / 54.4** | ❌ | HK 基线仍是 08-13 旧算术/高杠杆口径；与现行引擎不可比 |
| A4 旧 D3 117% | 封存 JSON 仍在 | ✅ 仅作历史 | 勿引用 |
| B1 套筒增量 +2.8/+23.1/+30.4 | **+3.2 / +15.7 / +31.0** | ⚠ 方向正 / ❌ 绝对水平 | 增量符号可复现；但「基线收益 84%」≠ S-3 NAV 47%（方法论洞） |
| B2 多资产 +19.3/… | 本轮未单独重跑 | ☐ | 同属 Layer B，需先修净值口径 |
| C1 R5CS +10.8/+17/+30.9 | **脚本 IndexError 崩溃** | ❌ | `simulate_sleeve_nav` trail8 包装返回 `rows=[]`，dual 取日收益越界 |
| C2 dual R1 表 | 未跑通 | ❌ | 同上 |
| G1 油 RSI>80 | 本轮未跑 | ☐ | 观察层，非组合决策关键 |

命令摘要：

```bash
PYTHONPATH=src python3 scripts/run_walk_forward.py --windows OOS2,train,valid,holdout
# → 47.3 / 34.1 / 38.7 / holdout n=0

PYTHONPATH=src python3 scripts/run_walk_forward.py --market HK --windows OOS2,train,valid
# → 11.9 / -3.2 / 61.4  vs 基线 -258pt

PYTHONPATH=src python3 scripts/sleeve_nav_sim.py
# → delta +3.2/+15.7/+31.0；summary holdDays/idleDays 恒为 0（包装丢字段）

PYTHONPATH=src python3 scripts/run_walk_forward_dual.py
# → IndexError: sleeve_nav list index out of range
```

Env 压力（关 neutral_block / auto / D2 / D3，panic=3）：

| 窗 | 全套件 | 仅核芯 | 解读 |
|----|--------|--------|------|
| OOS2 | 47.3 | **48.7** | 核芯不靠 env |
| train | 34.1 | 30.9 | env 小幅正贡献 |
| valid | 38.7 | 35.8 | env 小幅正贡献；valid n=16⚠️ |

---

## 2. 重大问题台账

### P0 — 会误导决策（必须先修或降级引用）

| ID | 问题 | 证据 | 影响 |
|----|------|------|------|
| **M1 套筒/联合净值口径谬误** | 用固定 `position_pct` × 日涨跌复利重放，≠ 引擎「现金 + 按入场价 MTM」NAV。OOS2：引擎 **47.3%**，同持仓套筒式重放 **84.1%**（DD 18.9→6.5） | `portfolio_nav_sim` / `sleeve_exit_variants.py`；本轮诊断脚本 | **夸大 S-3「基座」与套筒绝对收益**；delta 符号或仍参考，**不可引用绝对 pt** |
| **M2 dual 脚本崩溃** | R5CS 调 `simulate_sleeve_nav(..., positions_by_day=[])`，trail8 包装返回空 `rows`，`joint_stats` 索引崩溃 | `run_walk_forward_dual.py:331-339` + `portfolio_nav_sim.py:101-112` | **组合层文档数字当前不可复现** |
| **M3 HK 基线过期** | 基线 270%（08-13）vs 现行 ~12%；train 转负 | HK 实跑 | **HK「高收益 beta」叙事在 realism 下不再成立**；至少要重固化或标「旧口径作废」 |
| **M4 文档真值漂移** | params/SUMMARY 写 43.1 或 117；代码基线 47.3 NAV | 多份 md vs `walk_forward_baseline.json` | 人工/Agent 会被旧数误导 |

### P1 — 口径缝 / 展示 bug（不立刻否定方向）

| ID | 问题 | 说明 |
|----|------|------|
| P1a | `holdDays/idleDays/avgIdlePct` 被包装硬编码为 0 | `portfolio_nav_sim` 委托 trail8 后丢字段 |
| P1b | paper 入场用当日 close；回测已 `next_open` | `paper_trading.py:202` vs `S3_CONFIG entry_mode` — paper 可能仍偏乐观 |
| P1c | paper 注释仍写 `10%×20` | 常量已是 `max_positions=10`，注释漂移 |
| P1d | dual `rebuild_daily_pnl` 同属固定权重日收益复利 | 即便修崩溃，联合 NAV 仍可能偏乐观 |
| P1e | 行业表无时态 / 分数回填配方前视 | audit-2026-08-22 D2/D3，仍 mild；未本轮重测量级 |

### 已确认 intact（相对 08-22 审计）

| 项 | 状态 |
|----|------|
| 现金 `sum(position_pct)≤1.0` | ✅ 代码在 `simulate` 入口检查 |
| `min_avg_amount=0.7` | ✅ 写入 S3_CONFIG / 基线 |
| 持有期交易日 | ✅ `_calendar_days_between` 用 calendar |
| `fetch_last_ohlcv_batch` as_of | ✅ 已有界 |
| CN 基线可复现 | ✅ 三窗持平 |

---

## 3. 分层可信度评级

| 对象 | 评级 | 一句话 |
|------|------|--------|
| S-3 CN 核芯（score+RS+gates+trail+熔断等） | ★★★★☆ | 可复现；关 env 仍正；现行 NAV 口径诚实得多 |
| S-3 CN 现行（含 env） | ★★★☆☆ | 可复现；env 增量小；valid n=16 不可外宣 |
| S-3 HK | ★★☆☆☆ | 旧基线拒信；现行弱市窗接近翻车，需重固化后再谈 |
| 第三资产套筒（绝对收益/增量 pt） | ★★☆☆☆ | 增量符号或真，**绝对水平不可信**直至改用引擎 NAV |
| CN+HK+套筒联合 | ★☆☆☆☆ | 脚本坏 + 口径谬误；历史 R5CS 数字本轮拒信 |
| 文档作为「真值」 | ★★☆☆☆ | 多版本并存，必须以 JSON 基线 + 本结论为准 |

### 仍站得住

- A 股 S-3：**上升期过滤（regime/主线/RS/score）+ 纪律退出** 在 OOS2/train 上赚的是真 alpha 方向，不是纯 env 拟合。
- 执行 realism（现金上限、流动性、次日开盘、NAV 指标）方向正确，且已落到 CN 基线。
- 48+ 次信号拒收的方法论纪律仍然有效。

### 必须降级 / 拒信

- 任何 **117% / 333% / HK 270% / Sharpe>6** 旧口径对外叙述。
- **套筒「基线 84% + 增量 30pt」类绝对数字**（在修复 M1 前）。
- **R5CS 三窗增量**（在修复 M2 + 净值口径前）。
- **valid 高胜率 / 高夏普**（n=16，仅发现）。

---

## 4. 优化准入（修完再谈策略）

| 优先级 | 动作 | 完成标准 |
|--------|------|----------|
| **P0-1** | 套筒/联合 NAV **复用引擎 `nav_curve`（或等价现金+MTM）**，禁止固定权重日收益复利当主指标 | OOS2 套筒 base 与 S-3 `total_net_pnl_pct` 偏差 ≤2pt |
| **P0-2** | 修复 `run_walk_forward_dual`（rows 空 / 日历对齐）；重跑 R1/R5C/R5CS | 脚本退出 0；矩阵重填 |
| **P0-3** | HK 基线按现行引擎重固化或标作废；更新 params/SUMMARY 引用表只留 NAV 数 | 文档与 JSON SHA 一致 |
| **P0-4** | paper 入场与回测 `next_open`（或明确披露偏差） | 注释与行为一致 |
| P1 | 恢复 sleeve 统计字段；行业时态 / 分数前视披露 | 展示与审计一致 |
| **冻结** | 新信号、调 env、加杠杆、把金油脉冲升格为策略 | 直至 P0 闭环 + holdout 有样本 |

---

## 5. 直接回答用户质疑

1. **代码漏洞 / 过于乐观？**  
   CN 主引擎已明显收敛；**套筒与 dual 仍有乐观方法论 + 崩溃 bug**。paper close 入场相对回测 next_open 仍偏乐观。

2. **数据 / 基线不准？**  
   CN 现行基线 **准且可复现**；**文档与 HK 基线不准**。holdout 仍 n=0。

3. **策略 1 / 2 / 1+2？**  
   - **1 CN**：方向可信。  
   - **1 HK**：旧结论不可用，需重估。  
   - **2 与 1+2**：**不能基于现有文档数字做资金配置决策**，先修 P0。

---

*审计执行：2026-08-29 · 状态：结论已出，待 P0 修复后复跑矩阵。*
