# 三策略前视审计与修正（习惯双子星 / 港湾 / 母港）· 归档于 2026-09-14

## 当时的目标（todo 链接）

- 用户诉求：周末发现前视问题、双子星被推翻、新基线「港湾」与「母港」建立之后，**对三套策略的实现做一次完整 review；发现 bug 立即停止比较并修复，然后在零前视、逻辑正确的前提下比较三者优劣**。
- 关联：`docs/todo.md` §10（2026-09-14 行）· 工程项 **OPT-182**
- 详细实验档（含逐项证据与复现命令）：[`../backtests/audit-three-strategy-lookahead-2026-09-14.md`](../backtests/audit-three-strategy-lookahead-2026-09-14.md)

## 实际做了什么（两轮审计 + 复核，共修 8 处）

### 第一轮：口径与前视（代码级）

| # | 问题 | 修复 |
|---|------|------|
| A | `eval_twin_star_parking.py` 卫星用**全天振幅旧前视键**（默认 `rank_key=None`）、缺 C1 3%、第 3 日按收盘卖；核心是旧本地循环 | 补 `rank_key="amp_1430"` / `exit_hhmm="1430"` / C1；核心换单源 `harbor.parking_replay` |
| B | B3/母港波动率窗负索引泄漏未来（`range(i-60,i)` @i=60 → `series[-1]`，train 命中） | 三处改 `range(max(1,i-60),i)`（riskbudget / b3_cap / benchmark） |
| C | 卫星 R-wide 闸用当日 15:00 收盘广度（14:30 前视） | 新增 `gate_1430`（`bar_5min` ≤14:30 面板 vs MA20，覆盖率 100%） |
| D | Live：trail peak 扫到 day 之后；`_pick` 忽略 `day`；平仓占位价永不回填；ROTATE 新腿 0%；recon 假 missedBuys | as-of 截断 / `as_of=day` 贯通 / `exitPendingOpenFill`+`patch_paper_exit_fill` / `parkPct` / recon 日期门槛 |

### 第二轮（用户质疑「数据真实吗」）：★ 卫星价格基期混用（qfq × raw）

| # | 问题 | 修复 |
|---|------|------|
| E | `daily` 于 09-11 被 `cn_reseed_qfq_tx.py` 重灌为**前复权**，`bar_5min` 仍是**原始价**；老口径「收盘卖」= `qfq_close/raw_entry` → 系统性亏损 + −80% 假回撤（2021 年两表比值 75% 个股差>1%、均值 +23.5%，拆股最大 7 倍） | `state_bucket_track` 新增 `_mark` / `_raw_ratio`：MTM/平仓/期末用 raw 15:00 记账，C1/涨停保护按当日因子缩放，广度 MA 用 raw；`next_open`/`same_close` 老口径不动 |

### 第三轮（复核「还有没有别的口径问题」）：★ 双市场日历污染（OPT-183）

| # | 问题 | 修复 |
|---|------|------|
| F | `daily` 表 CN/HK 同表；`backtest_engine._load_calendar` 与 `state_bucket_track._load_calendar` 未按市场过滤 → 日历含 **57 个 HK-only 日期**（CN 假期）：卫星假期强平无 mark 持仓（PnL 归零 48 笔）、S-3 持仓天数/结算被加速 | `_load_calendar(start, end, market)` 按 `config.market` 过滤（CN 非 HK / HK 仅 HK）；`state_bucket_track` 固定 CN-only；CN/HK 基线重固化 |

## 验证 / 数据（最终 clean 口径，含成本）

**三窗 + 长窗（total% / maxDD% / Sharpe）**

| 策略 | OOS2 | train | valid | long |
|---|---|---|---|---|
| 港湾 Harbor（Live） | +55.2 / −14.3 / 1.73 | +52.2 / −8.0 / 3.01 | +50.3 / −21.8 / 2.14 | +201.5 / −22.8 / 1.00 |
| 习惯双子星 50/50 | +149.7 / −9.2 / 4.54 | +52.2 / −6.8 / 3.98 | +29.1 / −21.8 / 1.49 | +291.9 / −20.8 / 1.45 |
| 母港 M50 | +36.5 / −9.0 / 1.90 | +35.0 / −5.0 / 3.52 | +25.6 / −11.4 / 2.26 | +116.3 / −11.6 / 1.18 |
| └ 双子星卫星 standalone | +212.7 / −3.3 / 7.44 | +40.3 / −8.1 / 3.50 | +14.7 / −6.5 / 2.06 | +463.6 / −8.4 / 3.50 |

**关键数据点**
- 卫星 long 年度（clean）：2021 +68.5（8–12 月）/ 2022 +43.8 / 2023 +15.3 / 2024 +43.7 / 2025 +32.6 / 2026 +5.8 —— **B12 的「2022 −34.8% / 2023 −48.0% / MDD −80.6% boom-bust」作废**。
- 数据真实性：long 窗 **1036/1036 入场、1017/1017 可核对出场**经因子换算后落在日线 [low, high] 内；单笔 −18.7%~+26.3%；退市股占 gap 事件 0.36%；另 19 笔无行情出场按 0 PnL（已知残余）。
- 判定：**港湾 K1/K2/K3 全过（Δ +17.2/+13.0/+11.6、long +118.8）**；**双子星仍 REJECT——K1 挂 valid（Δ−21.2、train Δ+0.0 贴线）、K2 挂 valid（Δsr −0.65）、K3 过（long +90.4）**；**母港 PASS 不变**。

## 后续影响 / 留给谁

- **Live 仍 = 港湾**；双子星不因「数字变好」上线（valid 单窗不过 + 早期样本未验证），要重开须新预注册 + valid 短板诊断；母港仍是产品候选（资本结构待拍板 + paper 3/20）。
- 文档同步：B12 横幅 / B13 / B15 / B16 修正注、SUMMARY 新增审计行 + 卫星 standalone 行、`pick-strong-track.md`、预注册 postscript、todo §10、**OPT-182 + OPT-183（双市场日历）**、卫星 standalone 专档 [`../backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md`](../backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md)。
- 测试：+21（raw 基期 MTM / C1 缩放 / gate_1430 / B3 负索引 / trail as-of / parkPct / 平仓回填 / recon 门槛 / **market-aware 日历 ×2**）；全量 **4212 passed**、`db_rows_baseline.py check` OK。
- 残余（已文档化）：卫星 raw 记账不计 3 日持仓股息（保守）；源数据退市记录不全；14:30 采样 bar 代理 Live 快照；2021–23 为事后样本。

## 文档地图（2026-09-14 本次涉及，按阅读顺序）

| 先看 | 文档 | 作用 |
|---|---|---|
| 1 | **本档** | 今日实验总览（结论级） |
| 2 | [`../backtests/audit-three-strategy-lookahead-2026-09-14.md`](../backtests/audit-three-strategy-lookahead-2026-09-14.md) | 详细证据 + 修复 + 复现命令 |
| 3 | [`../backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md`](../backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md) | 卫星腿 standalone 专档（三窗/long/年度/校验） |
| 4 | [`../backtests/SUMMARY.md`](../backtests/SUMMARY.md) | 实验总表（新增审计行 + 卫星行） |
| 5 | [`../optimization-checklist.md`](../optimization-checklist.md) OPT-182 | 工程修复清单 |
| 6 | [`../modules/pick-strong-track.md`](../modules/pick-strong-track.md) | 港湾产品真值（已同步修正） |
| — | B11/B12/B13/B15/B16/B17（`../backtests/stable/`） | 被修正的原始实验档（均加修正注） |
