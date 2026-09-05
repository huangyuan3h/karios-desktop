# 核心门控 C-gate：大部队避险时小分队不出门（2026-09-05 启动）

> **一句话**：卫星 3 天脉冲里能删的东西已连死三个（C3、CHURN、E-veto），改filt方向此路不通。换一条正交路：**用核心的嘴管卫星的手**——早盘核心切到避险/空仓（GOLD/BOND10/REPO）时，当日 14:30 卫星直接关门（strict 不补，空槽回核心）。门控信号复用冻结核心输出，零新参、不扫网格，选参只看 OOS2+train，valid 只验。
> **关键词**：习惯双子星 核心门控 零新参 预注册
> **状态**：计划已冻结，待跑。结论（PASS 或 REJECT 都留档）进本档 §4；任何情况下不自动进 Live。

**基线**：冻结习惯配方 `C1 3% + same_1430 + body=3 + 第3日14:30卖 + strict 4×12.5% + opp_50`（`sat-live-caliber-2026-09-04.md`）。
**预注册**：本档 §2–§3 即预注册（跑前冻结，跑中不改）。方法论沿 S4（`sat-churn`）：诊断电池一次跑完、双窗一致才进回放、最多只带一个进回放。

---

## 1. 为什么是这条路

- 卫星侧做减法已三连死：C3 组合冗余、CHURN 记候选、E-veto 方向证伪（20 天顶否决 3 天脉冲 horizon 错配）。继续在候选上加否决 = 在同一堵墙上撞第四次。
- R-wide（breadth>0.5）是“大盘宽度说不”，C-gate 是“核心持仓说不”——两者信息源不同（全 A 截面 vs 跨资产动量 + trail8），正交，有可能互补；也可能完全共线（核心避险日 R-wide 本来就关），那诊断会直接显示触发率≈0，按纪律关闭方向。
- 自由度=0 个新参数：门控只读冻结核心的每日 pick（STOCK/GOLD/OIL/NASDAQ/BOND10/REPO，`pick_strong_grid.build_nav_from_cache` mom60+MA200+trail8 定案口径），阈值全是现成标签，不引入任何数字。

## 2. 预注册冻结细则（跑之前定死，跑中不改）

- **门控定义**：入场日 D，早盘已知核心当日 pick（T-1 收盘算出，14:30 前完全已知，零前视；实现上与 Watchlist 早盘 live pick 同口径）落在关门集合 → 当日不买（记 `skip_coregate`，strict 不补，空槽回核心）。持仓中已买的不提前卖（只管进门，不管清仓，避免引入第二个开关）。
- **关门集合（诊断阶段看四桶，描述性）**：`STOCK` / `RISK_ETF(OIL/NASDAQ)` / `DEFENSIVE(GOLD/BOND10)` / `REPO`。回放阶段**最多只带一个**：两窗一致最差且机制说得通的一桶；若一致最差的是两桶合并（如 DEFENSIVE+REPO），只允许这一种合并（“避险”大类，跑前声明，不许现凑三桶组合）。
- **其余冻结**：C1 3%、skip_t1、bucket 1/3、R-wide 0.5、body=3、第 3 日 14:30 卖、clip4、opp_50、成本 0.3% 往返，全部不动。
- **窗口**：诊断只看 OOS2+train（valid 不碰）；回放三窗 OOS2/train/valid 为拒收闸；2021/22/23 熊市回放软闸；holdout 只读。
- **口径**：window-local 空簿、gross。

## 3. 拒收线（任一触发即 REJECT，关闭方向不补变体）

1. 诊断电池双窗不一致（最差桶两窗打架）→ 连回放都不进，直接 REJECT。
2. 回放任一窗 twin-tot 相对习惯基线差 `< −5pt`。
3. 任一窗 sharpe 相对基线差 `< −0.3`。
4. valid 相对**核心**转负（loses_core_tot）。
5. 关门触发率 <1%（门控几乎不开 = 与 R-wide 完全共线，无增量，关方向）或 >40%（关掉近半卫星 = 换策略不是门控，关方向）。

PASS 线：三窗 tot/sharpe 不差于基线且至少一窗 tot +2pt 以上才记候选；+2pt 以下只记 PASS-thin。**任何情况不自动进 Live**。

## 4. 执行步骤（一次一刀）

1. **暴露核心 pick**：`scripts/pick_strong_grid.py:build_nav_from_cache` 返回加 `pick_map`（加法字段，行为不变）——诊断与回放共用，与冻结 NAV 逐数一致。
2. **诊断电池**：新脚本 `scripts/diag_sat_coregate.py`（只读）：合格缺口 fills 按入场日核心桶切四桶，报均值/胜率/n + 触发率。双窗一致才进 3。
3. **三窗回放**：新脚本 `scripts/compare_sat_coregate.py --save-report`（复用 `compare_sat_churn.py` 结构），输出 twin tot/sr/dd vs 基线 + fills/skip 口径。
4. **熊市回放**：同口径重跑 2021/22/23。
5. **收尾**：本档补结果表 + 判定；`SUMMARY.md §1` 追一行。

复现（待第 2 步落地后填实测）：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_coregate.py
PYTHONPATH=src:scripts python3 scripts/compare_sat_coregate.py --save-report
```

## 5. 结果（2026-09-05 诊断电池实测 · REJECT，未进回放）

> **判定：REJECT/双窗打架**。没有一桶在两窗同时最差，门控无处下手。按预注册 §3.1 关闭方向，不进三窗回放。

**脚本**：`services/data-sync-service/scripts/diag_sat_coregate.py`（只读；口径同 S4；valid 未碰）
**前置改动**：`scripts/pick_strong_grid.py:build_nav_from_cache` 返回加 `pick_map`（加法字段，NAV 逻辑未动；诊断中 OOS2/train 两窗核心均正常跑出，行为一致）。

| 窗口 | STOCK | RISK_ETF(OIL/NASDAQ) | DEFENSIVE(GOLD/BOND10) | REPO |
|------|-------|---------------------|----------------------|------|
| OOS2 | −3.27%/28%/8795 | +0.82%/48%/193 | +1.02%/48%/1672 | +2.00%/72%/50 |
| train | +0.14%/45%/1375 | −0.51%/40%/1055 | −0.17%/43%/749 | −0.82%/47%/32 |

读法：

1. **每桶都翻面**：OOS2 最差的 STOCK（−3.27）到 train 变成唯一转正的（+0.14）；OOS2 最好的 REPO（+2.00）到 train 最差（−0.82）。DEFENSIVE 两窗一正一负。按 §3.1 任一桶都够不上“双窗一致最差”，回放无标的。
2. 允许的唯一合并 DEFENSIVE+REPO 也不行：OOS2 +1.05%（远好于平均 −2.50），train −0.20%（≈平均 −0.16）——关掉它等于在 OOS2 跳过赢家，方向反。
3. REPO 触发率 0.5%/1.0%（§3.5 <1% 线）：核心空仓日 R-wide 本来就关得差不多，门控几乎无增量；R-wide 开的日子里核心 STOCK 占 82%/43%——大部队和小分队本来就在同一边，轮不到门控说话。
4. **Live 不动**；引擎零改动（只有诊断脚本 + pick_map 加法字段）；核心冻结口径不变。
5. 教训记一条：核心 pick 与卫星 3 日收益的交互是 regime-不稳定的（STOCK 桶 −3.27 ↔ +0.14），任何“按核心持仓决定卫星做不做”的规则，先天站在流沙上——与 R-wide（宽度截面，OOS2/train 同向）的稳定性完全不是一回事。

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_coregate.py
```
