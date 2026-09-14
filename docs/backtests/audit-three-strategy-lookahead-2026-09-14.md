# 三策略前视审计与修正（习惯双子星 / 港湾 / 母港）· 2026-09-14

> **背景**：2026-09-13「港湾」上线后，对三个策略的全部实现（回测评估脚本 + Live 服务层）做前视/逻辑复审。
> 审计共发现 **8 处问题**：3 处会改变回测数字（双子星评估口径、B3 波动率窗负索引、**卫星价格基期混用 qfq×raw**）、1 处卫星闸前视、3 处 Live 层账本/回放问题，以及 **F · 双市场日历污染（HK-only 日期）**；全部修复后重跑。
> **最终结论（clean 口径）**：港湾 Live 基线不变（P1 数字修正为 +55.2/+52.2/+50.3/+201.5）；**习惯双子星仍 REJECT（valid 单窗挂 K1/K2、train Δ 贴 0），B12 的「2022/2023 boom-bust、long MDD −80.6%」是污染口径产物、作废**；母港 PASS 不变（M50 = +36.5/+35.0/+25.6/+116.3）。
> **数据真实性**：三套数据都是真实行情（`daily` 为前复权、`bar_5min` 为原始成交价，同一日经因子换算 100% 落在日线高低区间内）；此前的异常数字来自**两表基期混用 + 双市场日历混用**，不是假数据。
> **关键词**：前视审计 · 习惯双子星 · B3 负索引 · gate_1430 · qfq/raw 基期 · HK-only 日历 · 修复重跑

---

## 0. 修复后三方对比（三窗 + long，含成本；总收益% / CAGR% / maxDD% / Sharpe）

| 策略 | OOS2 | train | valid | long |
|---|---|---|---|---|
| **港湾 Harbor**（B11 P1，Live 基线） | +55.2 / 58.0 / **−14.3** / 1.73 | +52.2 / 138.2 / **−8.0** / 3.01 | +50.3 / 156.6 / **−21.8** / 2.14 | +201.5 / 25.7 / **−22.8** / 1.00 |
| **习惯双子星 50/50**（修正口径） | **+149.7** / 159.3 / −9.2 / **4.54** | **+52.2** / 138.0 / −6.8 / **3.98** | +29.1 / 80.5 / −21.8 / 1.49 | **+291.9** / 32.8 / −20.8 / **1.45** |
| **母港 M50 = 港湾×B3 50/50** | +36.5 / 38.3 / **−9.0** / 1.90 | +35.0 / 85.9 / −5.0 / 3.52 | +25.6 / 69.4 / **−11.4** / **2.26** | +116.3 / 17.4 / **−11.6** / 1.18 |
| 双子星 Δ vs 港湾核心 | +94.5 / +2.81sr / +5.1dd | +0.0 / +0.97 / +1.2 | **−21.2 / −0.65 / +0.0** | +90.4 / +0.45 / +2.0 |

**读法**：
- **港湾**：不依赖卫星/被动腿的最简 Live 基线；valid 单窗绝对收益最高（+50.3），但 valid/long 回撤最深（−21.8/−22.8）。官方 S-3 基线（V0）同步修正为 **+38.0/+39.2/+38.7、long +82.7**。
- **习惯双子星**：修正后 OOS2/long 收益与夏普全面优于港湾（long +291.9 vs +201.5、MDD −20.8 vs −22.8）；**短板只剩 valid 单窗**（+29.1 vs 核心 +50.3，Δsr −0.65）+ train Δ 贴 0。按冻结 K1/K2/K3（三窗每窗 Δtotal>0、Δsr≥0；long Δ>0）→ **仍 REJECT，不改 Live**。
- **母港**：四窗回撤全部最浅（−9.0/−5.0/−11.4/−11.6）、valid 夏普最高（2.26）；代价是收益近半。**产品候选（PASS），不进 Live**（资本结构需拍板 + paper 3/20）。

> 历史版本说明：B12（09-13）因 A+E 双重口径 bug 得出「卫星 boom-bust、long −55.6」；本审计第一轮（09-14 上午）修 A 后发现 E（基期混用），第二轮补 F（HK-only 日历）。final 数字以本节为准。中间版本（+109.9/+65.5/+34.5/+300.3、+145.2/+50.5/+25.5/+329.1）均作废。

---

## 1. 审计发现与修复

### A. 习惯双子星评估口径 = 旧前视键 + 非 Live 时钟 + 旧核心

`scripts/eval_twin_star_parking.py`（B12，2026-09-13）：

| # | 问题 | 证据 | 修复 |
|---|------|------|------|
| A1 | 卫星未传 `rank_key="amp_1430"`，默认 `None` = **全天振幅排序** | `state_bucket_track.py` 注释：「None = full-day amplitude ascending (**the OLD lookahead key**)」；Live 定义用 14:30 可得振幅 | 传 `rank_key="amp_1430"` |
| A2 | 未传 `exit_hhmm="1430"` → 第 3 日按**收盘**卖（Live 是 14:30 卖） | `build_sgap_timeline` docstring 的 Live 习惯参数 | 传 `exit_hhmm="1430"` |
| A3 | 未传 `max_open_to_1430_pct=0.03` → 缺 Live C1 3% 过滤 | 同上 | 传 C1 |
| A4 | 核心用本地旧循环 `_parking_core_by_day`（幻影日强制清仓、与 B11 单源不一致） | OPT-180 后 `parking_replay` 是唯一实现 | 核心换 `service.harbor.parking_replay`（对齐 B11 最终 +62.0/+45.3/+50.3/+219.9） |

### B. B3/母港波动率窗负索引泄漏未来

`range(i - 60, i)` 在 `i == 60` 时 `j=0 → series[ts][j-1] = series[-1]` = **窗口最后一个收盘（未来）**注入 vol 估计；仅当 `cal[60]` 恰为月初触发（train 窗命中）。

| 文件 | 行 | 修复 |
|---|---|---|
| `scripts/eval_harbor_riskbudget.py` | 125 | `range(max(1, i - 60), i)` |
| `scripts/eval_b3_cap.py` | 80 | 同上 |
| `scripts/eval_etf_benchmark_parking.py` | 202 | 同上 |

**影响**：train B3 Sharpe **4.36→4.27**、MDD **−1.7→−2.2**；母港 M50 train Sharpe 3.32→3.30（其余窗/Long 不变）；**K1–K5 判定不变（PASS）**。B16 cap50 的 K4 一个子检查由「假过」变「真挂」，REJECT 不变。`service/homeport.py` 本就带 `max(1, …)` 守卫。

### C. 卫星 R-wide 闸用当日收盘广度（14:30 前视）

新增 `gate_1430=True`：用 14:30 可得面板算 MA20 广度，替代 15:00 收盘截面（Live 用 14:20 快照）。`bar_5min` 以 **bar 结束时间**标注（14:25–14:30 的 bar 标 `1430`），14:30 print 在 14:30 可得；合格域（mv∩非 ST/BJ/退市）14:30 覆盖率 100%。旧 `gate_1430=False` 保留（冻结数字可复现）。

### E. ★ 卫星价格基期混用（qfq × raw）——B12 数字的真正杀手

**发现**：`daily` 在 2026-09-11 左右被 `cn_reseed_qfq_tx.py` **重灌为腾讯前复权（qfq）**；`bar_5min`（vendor/baostock，`adjustflag=3`）仍是**不复权原始价**。卫星 `same_1430` 口径：

- 成交价（14:30 print）来自 `bar_5min` = **raw**；
- 记账/MTM/收盘卖来自 `daily` = **qfq**。

**量化**：两表同一天比值 `raw/qfq` 按年：2021 年 75% 的个股差 >1%、均值 **+23.5%**（最大 7 倍，拆股/高分红股）；2022 +20.1%、2023 +14.4%、2024 +9.2%、2025 +5.1%、2026 +2.5%。gap 候选股中位数偏差较小（2022H2 1.038、2025 1.006），但长尾个股（拆股）偏差巨大。

**后果**（全部经修复后重测确认）：
1. **B12 的卫星平仓**（未传 `exit_hhmm` → 用 `daily` 收盘）= `qfq_close / raw_entry` → 系统性亏损 + 巨大假回撤。这是「2022 −34.8% / 2023 −48.0% / long MDD −80.6%」的主因，**不是策略本身**。
2. 本审计第一轮的 `gate_1430` 用 **raw px vs qfq MA20** → 闸几乎恒开（fills 995→1721），不是有效的广度过滤。
3. C1 / 涨停保护用 raw px vs qfq open/pre_close → 误杀/误放。

**修复**（`state_bucket_track.py`）：`same_1430` 口径内统一用 raw 基期——
- `_mark()`：MTM / 平仓 / peak / 期末结算优先用 raw 15:00 收盘（`px_by_hhmm["1500"]`），缺失才回退 qfq；
- `_raw_ratio()`：同一日 `raw_close/qfq_close`，用于把 qfq 的 open/pre_close 缩放到 raw 基期（C1、涨停保护）；
- `_breadth_at_1430()`：MA20 的 19 根prior也改用 raw 收盘；
- `next_open` / `same_close` 老口径不动（本就 qfq 内部自洽）。

**真实性校验**（long 窗 1041 笔成交）：把 raw 14:30 成交价按当日因子换算回 qfq 后，**1041/1041 入场、993/993 出场全部落在当日日线 [low, high] 区间内**；年度收益 2021 +64.9%（8–12 月）/2022 +46.2/2023 +14.4/2024 +39.6/2025 +32.4/2026 +5.7（至 8/7）；单笔区间 −18.7%~+26.3%（20% 涨跌幅板），无异常值。数据真实，问题只在「两条价格序列的基期不一致」。

**卫星 long 分解（基期统一 + 日历修正后，25% clip standalone）**：

| 口径（逐步补 Live 参数） | total | MDD | fills | active |
|---|---|---|---|---|
| V0 旧口径（rank=None 全天振幅、无 C1、收盘卖） | +563.1% | −5.8% | 1160 | 53% |
| V1 + `amp_1430`（诚实排序） | +503.3% | −7.8% | 1065 | 53% |
| V2 + C1 3% | +493.4% | −9.1% | 1063 | 53% |
| V3 + `exit_hhmm=1430`（14:30 卖） | +492.9% | −9.0% | 1063 | 53% |
| V4 + `gate_1430`（全诚实 = 本审计口径） | **+463.6%** | **−8.4%** | 1036 | 52% |

→ 基期统一后，「旧口径更大回撤/更差收益」消失（旧 V0 的 +81%/−80.6% 是假象）；各口径收益都在 +460%~+560% 量级。B12 / SUMMARY 的「boom-bust 不可救」叙事**整体作废**。

### F. ★ 双市场日历污染（HK-only 日期 = CN 假期）

**发现**：`daily` 表 **CN 与 HK 同表**（HK 行约 2803 个 symbol），而两处日历加载器都没有按市场过滤：

| 文件 | 行 | 问题 |
|---|---|---|
| `service/backtest_engine.py::_load_calendar` | 890 | `SELECT DISTINCT trade_date FROM daily` = **CN∪HK 并集**；S-3 引擎把 HK-only 日期（CN 假期）当交易日 → `_calendar_days_between` 加速持仓天数/换手门槛/时间止损，T+N 结算与 cooldown 也被加速 |
| `service/state_bucket_track.py::_load_calendar` | 107 | 同上；卫星在这些日期对 CN 持仓 `held=999 → 强制平仓`，且当日无 CN 行情 → **PnL 归零、仓位直接消失**（long 窗 48 笔受影响） |

**证据**：long 窗并集 1270 日 vs `index_daily`（000001.SH）1216 日；差集 **57 日全部是 HK-only**（如 2021-09-20/21、2022-01-31、2023-01-26、2024-10-02…，`bar_5min` 0 行）；CN 日期与指数日历 **0 差异**。

**修复（OPT-183）**：`backtest_engine._load_calendar(start, end, market)` 按 `config.market` 过滤（CN `NOT LIKE '%.HK'` / HK `LIKE '%.HK'`）；`state_bucket_track._load_calendar` 固定 CN-only。CN/HK 基线分别重固化（`walk_forward_baseline.json` / `walk_forward_hk_baseline.json`，2026-09-14）。

**量化影响（clean vs 污染）**：

| 项 | 污染口径 | clean 口径 |
|---|---|---|
| S-3 CN 官方基线（V0） | +46.5/+34.4/+38.7、long +94.5 | **+38.0/+39.2/+38.7、long +82.7** |
| 港湾 P1 | +62.0/+45.3/+50.3/+219.9 | **+55.2/+52.2/+50.3/+201.5** |
| 港湾 P1 增量 Δ | +15.5/+11.0/+11.6/+125.4 | **+17.2/+13.0/+11.6/+118.8（仍 K1–K3 PASS）** |
| 卫星 standalone long | +439.0%（MDD −9.6） | **+463.6%（MDD −8.4）** |
| 双子星 50/50 | +145.2/+50.5/+25.5/+329.1 | **+149.7/+52.2/+29.1/+291.9（仍 REJECT）** |
| 母港 M50 | +39.5/+31.5/+25.6/+122.5 | **+36.5/+35.0/+25.6/+116.3（仍 PASS）** |
| HK 线官方基线（09-10 版） | OOS2 +21.6 / train +2.5 / valid +65.7 | **+21.8 / −0.2 / +65.7（重固化；差异含 09-10 后引擎变更）** |

→ **三策略判定全部不变**；冻结数字按 clean 口径更新。残留：卫星 long 仍有 **19 笔无行情出场**（退市/停牌日无 daily 行）按 0 PnL 计——已知生存偏差，量级 <1pt NAV。

### D. Live 层（纸账/复盘路径）前视与逻辑

| # | 问题 | 文件 | 修复 |
|---|------|------|------|
| D1 | trail8 peak 扫到 `day` 之后的 bar（历史回看/复盘假触发） | `service/multi_asset_sleeve.py::_etf_trail_exit` | as-of 截断 `d > day → break` |
| D2 | `_pick()` 忽略 `day`，用墙钟 today（历史 recon/展示前视） | `multi_asset_sleeve.py` | `_signal_series/_signal_closes/_pick` 贯通 `as_of=day` |
| D3 | 18:20 job 以 T 收盘占位记平仓且立即 closed，`run_update` 只回填 open 行 → **T+1 开盘实价永不落账**（OPT-178 ① 未完成） | `sleeve_paper_auto.py` / `db/paper_trading.py` / `paper_trading.py` | `_exit_fill` 标 `exitPendingOpenFill`；新增 `list_pending_exit_fills` / `patch_paper_exit_fill`，`run_update` 回填真实 open 并重算 gross/net |
| D4 | ROTATE `idlePct` 含被换掉的旧腿 → 新腿 `sleeve_pct=0%` | `multi_asset_sleeve.py` / `sleeve_paper_auto.py` | 新增 `parkPct = idle + parked`，新腿按 `parkPct` sizing |
| D5 | recon 把**所有** `multi-sleeve` 历史 BUY 腿当「当日新开」剔除、且把当日开盘已平仓腿当持仓 → 假 missedBuys | `sleeve_paper_recon.py` | `_opened_by_sleeve_job_today` 改按 signalDate/createdAt 日期门槛；`sold_days` 只含 exit exec day |

---

## 2. 判定（按冻结门槛，不移动球门）

| 项 | 结果 |
|---|---|
| **港湾 P1**（Live） | K1/K2/K3 全过（Δ +17.2/+13.0/+11.6、long +118.8） |
| **习惯双子星 50/50** | **K1 FAIL（valid −21.2、train Δ+0.0 贴线）· K2 FAIL（valid Δsr −0.65）· K3 PASS（long +90.4）→ REJECT**（维持不进 Live；valid 短板诊断已完成 = [regime 依赖](sat/sat-valid-shortfall-diagnosis-2026-09-14.md)，重开须新预注册） |
| **母港 M50** | K1–K5 全过（B3 负索引 + 日历修正后）→ **产品候选**；不进 Live（资本结构 + paper 3/20） |

**纪律提醒**：短窗好看（OOS2 +149.7）不构成翻案——**单窗（valid）不过就是不进 Live**；且卫星在 valid 窗只有 +14.7%（核心 +50.3），说明 edge 有 regime 依赖。与 2026-09-12「漂亮得离谱先查前视」互为镜像：「难看得离谱先查口径」。

---

## 3. 复现

```bash
cd services/data-sync-service
# 引擎/卫星日历已按市场过滤（OPT-183，默认生效）；基线重固化：
PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline              # CN 官方基线
PYTHONPATH=src python3 scripts/run_walk_forward.py --market HK --save-baseline  # HK 线基线
# 三策略 eval（本审计口径）：
PYTHONPATH=src:scripts python3 scripts/eval_etf_parking_baseline.py --save-report     # 港湾 P1
PYTHONPATH=src:scripts python3 scripts/eval_harbor_riskbudget.py --save-report        # 母港
PYTHONPATH=src:scripts python3 scripts/eval_twin_star_parking.py --windows OOS2,train,valid,long --save-report
PYTHONPATH=src:scripts python3 scripts/eval_etf_benchmark_parking.py --save-report    # B13 标尺
PYTHONPATH=src:scripts python3 scripts/eval_b3_cap.py --save-report                   # B16
```

测试：`tests/test_eval_riskbudget_causal.py`（负索引）、`tests/test_state_bucket_track.py`（gate_1430 / 14:30 广度 / raw 基期 MTM / C1 缩放 / **CN-only 日历**）、`tests/test_backtest_engine_loaders.py`（**market-aware 日历**）、`tests/test_multi_asset_trail.py`（as-of / parkPct）、`tests/test_sleeve_paper_auto.py`（平仓占位）、`tests/test_sleeve_paper_recon.py`（recon 日期门槛）、`tests/test_paper_trading.py`（平仓回填）。全量 pytest + `db_rows_baseline.py check` 见 OPT-182。

---

## 4. 残余（已文档化）

- **双市场日历**：已按市场过滤（OPT-183）；`daily` 仍 CN/HK 同表，新增日历读取点时须带市场过滤（Live 侧 `market_sentiment` cooldown / `watchlist_automation` RS 仍有未过滤的 `SELECT DISTINCT trade_date FROM daily` → 另计 OPT-184）。
- **卫星数据基期**：已统一为 raw（股息在 3 日持仓内不计，偏保守）；`daily` qfq 主要用于 gap/特征（比值不变）。若未来重灌 `daily` 或 `bar_5min`，须重跑本审计的因子校验。
- **生存偏差**：数据源 `stock_basic` 仅记录 10 只窗口内退市 CN 股（源数据不全）；退市股 gap 事件占比 **0.36%**；卫星 long 仍有 **19 笔无行情出场**按 0 PnL 计（退市/停牌日无 daily 行），对卫星影响小但无法完全消除。
- **bar_5min 粒度**：用采样 5 分钟 bar（14:30 前）代理 Live 的东财 14:20 实时快照高/低——名字级不逐只相同（均值口径，三窗已验）。
- **2021–2023 早期样本**：卫星 C1/amp_1430 规则是在 2024+ 窗口上验证的，长窗早期（+68.5%/+43.8%）属事后样本，regime 代表性存疑——valid 短板正是这一风险的体现。
- **Live 平仓 T+1 开盘**：由次日 `run_update` 回填；T+1 无 bar 则占位价保留至 bar 落地。
- **S-3 引擎 NAV 微观口径**：信号 T 日以 open(T+1) 为成本基准（总收益自洽），OPT-180 残余对三策略一致。

---

## 5. 关联

- 习惯 S-gap 卫星腿 standalone 记录：[`stable/sgap-habit-satellite-standalone-2026-09-14.md`](stable/sgap-habit-satellite-standalone-2026-09-14.md)
- 卫星 valid 短板诊断（同日核心对照 + 闸门反证）：[`sat/sat-valid-shortfall-diagnosis-2026-09-14.md`](sat/sat-valid-shortfall-diagnosis-2026-09-14.md)
- 港湾×卫星 曝露曲线（H-SAT-W，预注册：7 档全 REJECT）：[`stable/harbor-sat-weight-2026-09-14.md`](stable/harbor-sat-weight-2026-09-14.md)
- 三腿「母港×卫星」（H-B3-SAT，预注册：PASS chosen=1/3，产品候选增量）：[`stable/harbor-b3-sat-2026-09-14.md`](stable/harbor-b3-sat-2026-09-14.md)
- B12：[`stable/twin-star-parking-refit-2026-09-13.md`](stable/twin-star-parking-refit-2026-09-13.md)（顶部修正横幅）
- B11：[`stable/etf-parking-baseline-2026-09-13.md`](stable/etf-parking-baseline-2026-09-13.md) · B13：[`stable/etf-benchmark-parking-2026-09-13.md`](stable/etf-benchmark-parking-2026-09-13.md)
- B15：[`stable/harbor-riskbudget-2026-09-13.md`](stable/harbor-riskbudget-2026-09-13.md) · B16：[`stable/b3-cap-2026-09-13.md`](stable/b3-cap-2026-09-13.md)
- 时钟统一：[`sat/sat-clock-unify-1430-2026-09-11.md`](sat/sat-clock-unify-1430-2026-09-11.md) · Live 对账：[`audit-live-vs-backtest-2026-09-13.md`](audit-live-vs-backtest-2026-09-13.md)
- 工程清单：`docs/optimization-checklist.md` **OPT-182**（前视/基期）+ **OPT-183**（双市场日历）· 残余 **OPT-184**（Live 侧未过滤日历）
