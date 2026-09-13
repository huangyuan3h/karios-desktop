# P0-13 B11/B12/B13 · 新基线「港湾」（Harbor）· 归档于 2026-09-13

> **命名（2026-09-13 用户拍板）**：新基线 = **「港湾」（Harbor）**，tag `harbor-p1-20260913` —— S-3 核心 + 闲置现金 ETF 停车场（与"双子星/择强单轨"无关的独立基线名）。

## 当时的目标（todo 链接）
- `docs/todo.md` → **P0-13 B11**（B10 的正式化）+ **B12**（双子星重拟合）+ **B13**（ETF 买持基准）。用户定调："我们现在的基线是 S-3，然后再重新尝试把 ETF 层加进去作为基线，live 可能存在前视情况，需要分析清楚，然后确认一个方案。"
- 两个拍板：主假设 = **闲置即停**（非"仅空仓才停"）；执行时钟 = **统一 14:30（卫星口径）**。

## 实际做了什么
- **预注册** `docs/designs/etf-parking-baseline-prereg-2026-09-13.md`：P1 闲置即停（不设 floor、不设 ETF>STOCK 门槛）+ R1/R2/R3 三个 Live overlay 对照；门槛 K1（三窗增量≥−0.05）/K2（三窗合计>0）/K3（long>0）。
- **实验** `scripts/eval_etf_parking_baseline.py` → `data/backtest_reports/etf_parking_baseline_2026-09-13.json`：S-3 CN 引擎 + ETF（518880/513350/513110+513100/511260，`etf_daily.csv` close_adj）闲置停车，mom60+MA200 argmax，因果 trail8，0.05%/边成本，三窗+long。
- **Live 前视审计**（本次需求的一半）：逐步核对 `multi_asset_sleeve` / `sleeve_paper_auto` / `_etf_trail_exit` / `_signal_closes` 的数据日与成交日，并用真实 `paper_trades` 行验证。

## 验证 / 数据
- **P1 PASS → 新基线 = S-3 + 停车场**：三窗 **+15.5/+11.0/+11.6**、long **+125.4**（幻影日 + 决策单源修正）（vs V0 46.5/34.4/38.7/94.5；V0 三窗 = 官方基线 ✅）。
- **Live overlay 代价**：R1（`MIN_IDLE_PCT=20` 地板）≈0（long −1.7）；**R2（ETF mom60 > 股票篮）long +20.8 vs +90.0（−69pt）**、短窗 −2~3pt → 应去掉；R3（BOND 恒入池）+91.1 ≈ P1。
- **风险**：valid MDD −9.4→**−23.1**、Sharpe 2.44→**1.91**（机制：S-3 空仓时停车场=满仓，trail8 出场后**无冷却次日再进** → 噪声期反复 −8% 往返）；long 窗仍改善（MDD −23.6 vs −28.1、Sharpe 0.83 vs 0.64）。
- **Live 前视/账本审计结论**：
  1. **真前视**：`_etf_trail_exit` 用 T 收盘触发，但 paper 按 `pick["close"]`（T-1 收盘）记账 → 躲过触发日下跌（OPT-177 同类）。
  2. **口径混用**：`_etf_market_data` 用含 T 的 `_closes` 判 MA200/HOLD；`_pick` 用 T-1 `_signal_closes`。
  3. **`_pnl_for` 字段名错**（读 `entry_price`，行是 `entryPrice`）→ 套筒平仓 **pnl/days 恒 0**（真实 DB 3 条实证）。
  4. **`SELL_TO_A_SHARE` 成交价 0**：STOCK 腿 `pick` 无 `close` → `float(None or 0)=0.0`。
  5. **测试污染真实账本**：`tests/test_sleeve_paper_auto.py` 的 SELL 用例会关掉**全部候选 symbol 的真实腿**（`_open_sleeve_legs` 未 mock），写入 mock 的 `2026-08-20 / 2.25`（DB 3 条 OIL 腿 `closeDate < entryDate` 即此）。
  6. 旧 `third_asset_sleeve`（T6 单纳指）与 `multi_asset_sleeve` 在 health/brief 并存，需收敛。

## 后续影响 / 留给谁
- **新基线冻结** = S-3 CN + 闲置停车场（P1 口径）；后续组合实验从此出发（已写入 `strategy-params.md` 计划、SUMMARY）。
- **Live 修复清单**（待做，OPT-178）：① 18:20 job → 14:28 信号/14:30 执行；② trail 触发与记账同 print；③ `_pnl_for` 字段；④ SELL 价格来源；⑤ 测试隔离；⑥ 去 `MIN_IDLE_PCT`/STOCK 门槛；⑦ 双套筒实现收敛。
- **2026-09-13 追加审计（双子星/卫星）**：⑧ **`paper_twin_star` intake 真前视**——读 15:00 冻结快照选股/排序/C1、按 14:30 print 记账（`sat_push_log._in_decision_window` 守卫未用于文件缓存）；⑨ `state_bucket_track` replay/Timeline 的 R-wide breadth gate 用**当日收盘**（Live 14:20 screen causal → 交易不受影响，审计/Timeline 路径 PIT 不一致）；⑩ trail/protect 若在 14:30 fill 下启用会用当日收盘触发 → latent 前视，需 guard。核心 `pick_strong_track`/`pick_strong_grid` OPT-177 后已因果 ✅（入场=信号日收盘口径，非 next_open，但非前视）。
- **候选改进（须另起预注册，禁网格）**：trail 出场冷却 / 再入场门槛 / 停车上限（valid MDD 代价）。
- 数据局限：ETF 14:30 历史价不可得（`stk_mins` 1 次/小时、baostock 无 ETF、本地 vendor 无 ETF）→ 回测用 T 收盘代理（差 0.5h），Live 用实时 14:30 价无此问题。

## 关联档
- 实验：`docs/backtests/stable/etf-parking-baseline-2026-09-13.md`
- 预注册：`docs/designs/etf-parking-baseline-prereg-2026-09-13.md`
- 前作：`docs/backtests/stable/idle-sleeve-2026-09-12.md` · `docs/backtests/audit-trail8-2026-09-12.md`
- 脚本：`scripts/eval_etf_parking_baseline.py` · 报告 `data/backtest_reports/etf_parking_baseline_2026-09-13.json`

---

# B12 习惯双子星重拟合（停车场核心 × 卫星）· 同日追加

## 目标
把双子星核心腿从（OPT-177 作废的）择强单轨换成停车场基线，卫星腿用 Live 定义（strict clip4 / same_1430 / body=3），按 `blend_nav_opportunity(0.5)` 重拟合，回答"50/50 是否仍能用很少收益换 Sharpe/回撤"。

## 结果（REJECT）
- 50/50 vs 停车场核心：OOS2 **+36.5 / +1.14sr / 回撤改善**；train **−11.8 / −0.16**；valid **−28.8 / −0.84**；long **−55.6 / −0.13 且回撤 −23.6→−48.3**。40/60、60/40 全不过。
- 卫星 standalone long：total +81.2%、MDD **−80.6%**、active 52%、平均 3.48/4 槽、1139 笔（非空槽稀释）。
- 年度分解：2021(8-12) +11.1 / **2022 −34.8** / **2023 −48.0** / 2024 +164.7 / 2025 +77.4 / 2026 +2.4 → **boom-bust**。
- 根因：旧 clip4 三窗（2024-08 起）恰是卫星黄金段 + 核心腿含 OPT-177 前视 → "少收益换 Sharpe/回撤"**双重失真**。

## 结论 / 后续
- 卫星现行配方**不进新基线、禁止调参重扫**；Live 默认 `twin_star` 是否保留卫星 = 产品决策。
- 要救卫星须**带全周期门槛**另起预注册（先解释 2024-25 才有效的 regime 依赖）。
- 下一步 **B1 ETF 买持基准改为对停车场基线（不含卫星）跑**，并补 510050/588000。
- 档：`docs/backtests/stable/twin-star-parking-refit-2026-09-13.md` · 预注册 `docs/designs/twin-star-parking-refit-prereg-2026-09-13.md` · 脚本 `scripts/eval_twin_star_parking.py`。

---

# B13 ETF 买持基准 × 多方法拟合（停车场基线）· 同日追加

## 结果
- **收益：停车场基线四窗全胜**（所有单只 ETF + 等权/风险预算/60-40/择时）；事后最强单只仅 train 黄金 +49.7 反超，valid +25.7 vs +47.8、long +141.6 vs +184.5 落败。
- **风险：被动组合更强**：风险预算（300/500/金/纳/债）long **Sharpe 1.71 / MDD −5.0**（基线 0.83/−23.6）、train Sharpe 4.19；黄金 OOS2/train Sharpe 1.96/3.43。
- **最佳拟合 = 基线 × 风险预算 50/50**：四窗 Sharpe 全升（1.53/2.92/2.07/1.01 vs 1.31/2.28/1.91/0.83）、MDD 全面减半（−9.2/−5.3/−11.8/−12.2），代价=收益（long +114 vs +184）。
- 套筒单独 long +63.4/0.54 → **S-3 股票腿是收益主体，ETF 层是增强**。
- **510050/588000 早已在 `etf/etf_daily.csv`（2021 起全）** → 旧"数据缺口"作废。

## 后续
- 50/50（基线×风险预算）落地须**另起预注册**（2022-23 压测 + 再平衡成本敏感性）。
- 档：`docs/backtests/stable/etf-benchmark-parking-2026-09-13.md` · 脚本 `scripts/eval_etf_benchmark_parking.py`。

---

# 港湾上线（双子星退役）· 同日执行

## 做了什么（用户指令："全部改成港湾，删除双子星在系统里面的应用"）
- **后端退役**：删 3 个调度 job（`paper_twin_star`/`twin_star_reminder`/`twin_star_intraday`）+ 5 个服务/DB 模块（`paper_twin_star`/`twin_star_daily`/`twin_star_intraday`/`sat_hold_path`/`sat_push_log`）+ 2 个 API（`/twin-star/action|refresh`）+ timeline twin_star 分支 + sync/health/notifications 默认；`source=twin_star` 停写；Alembic `0050_remove_twin_star`（drop 表/列，`user_trades.leg` sat→parking）。
- **Harbor 上线**：`multi_asset_sleeve` 重写为 P1（闲置即停、无 floor/STOCK gate、无候选→REPO、trail8）；`sleeve_paper_auto` 统一 **T 收盘信号 → T+1 开盘成交**、修 `_pnl_for`/退出价/测试隔离；新增 `service/harbor.py` + `/timeline?strategy=harbor`（valid +47.8/base +38.7，与 B11 一致）。
- **前端**：默认 `harbor`、删双子星组件/系列/计划/合约与卫星指令；watchlist/回测/Webhook/设置改港湾（833 测试绿、tsc 干净）。
- **文档**：`pick-strong-track`/`watchlist`/`strategy-params`/`trading-system`/`README`/`todo`/`AGENTS` 改港湾口径；历史区（backtests/archive/designs）只读保留。
- **验证**：后端 `pytest --no-cov` **4171 passed / 3 skipped**；`ruff` clean；前端 833 passed + tsc clean；`linkcheck clean`。

## 残留
- 研究引擎 `state_bucket_track` / `pick_strong_track.build_twin_star_timeline` / `ps_g50_blend` 仅 scripts 引用（历史证据），不在运行路径。
- 旧 T6 `third_asset_sleeve` 仅展示（不再交易）→ **OPT-179** 收敛。
- 家族风险术语「卫星仓」（executionGate/dashboard）与策略卫星腿无关，保留未动。
- 档：OPT-178（完成）· OPT-179（新增）。
