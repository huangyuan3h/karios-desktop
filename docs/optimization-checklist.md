# Karios Desktop 优化 Checklist

> 工程执行栈（OPT-xxx）：架构 / 性能 / 兼容 / 工程债怎么做。编号 `OPT-001 ~ OPT-173`（重号见文末）。
> **正文 5 条未完成 + 3 条冬眠**；已完成 117 条按天归档 [`archive/`](archive/)（见文末索引表）。

---

## 如何使用

1. 未完成队列已清空（2026-09-06 H2 收官）；新 OPT 直接在"未完成"下开节，唤醒冬眠同理。
2. 每个任务开独立 Agent 会话，把对应章节整段粘贴给 Agent 作为 scope。
3. 完成后将 `[ ]` 改为 `[x]`，填写 **完成日期**，并按天归档：新档 `archive/YYYY-MM-DD-opt-NNN-slug.md`（仿 git 历史）+ 正文该节替换为索引行。
4. 若实施过程中方案有变，在本文件更新，不要另起文档。

### Agent 任务模板（见仓库根 AGENTS.md → Scoped optimization tasks；本文件只补一条）

> 开独立会话，把对应章节整段粘贴为 scope；只改列出的文件范围；更新状态；补测试；不扩 scope。

---

## 未完成（5 条 · 2026-09-11 工程盘点立）

> 来源：2026-09-11 工程健康盘点。**一 OPT 一会话，只改列出的范围，补测试，不扩 scope。**
> 顺序建议：A → B →（按需）C/D；E 策略实现准确度建议与 B 同期。

### A 仓库卫生（高杠杆低风险）✅ 全完成（OPT-160/161/162）

### B 质量护栏（棘轮，防回退）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-163 | [ ] | **pyright 警告棘轮**：`pyproject` 现把 `report{ArgumentType,OptionalSubscript,OptionalIterable,OptionalMemberAccess,GeneralTypeIssues,AttributeAccessIssue}` 降为 warning。先统计基线数，逐模块转 error（engine/routes/paper 优先）。验收：warning 数单向下行，CI 保持绿 |

### C 性能 / 数据（按需）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-167 | [ ] | **热表索引 + 慢查询审计**：`bar_5min`(6.5GB)/`daily` EXPLAIN 关键查询、补缺失索引；前端 bundle 体积分析。验收：慢查询清单 + 索引前后耗时对比 |
| OPT-168 | [ ] | **依赖刷新**：`pnpm outdated` + `uv lock`，分批升级 + 回归。验收：锁文件更新、全量测试绿、无 breaking |

### D 产品 / 可观测（最低优先）

| OPT | 状态 | 范围 / 验收 |
|-----|------|-------------|
| OPT-169 | [ ] | **UI 极简化美化**：按 `designs/ui-minimal-redesign.md`（拍板后从 designs 转 OPT）。验收：设计稿条目落地、前端测试绿 |
| OPT-170 | [ ] | **健康聚合 / 告警巡检补强**：聚合 job 失败/数据新鲜度/熔断为单一 health 视图，巡检缺口。验收：缺口表 + 告警用例 |
| OPT-178 | [x] 2026-09-13 · **Live 前视/账本修复 + 双子星退役 + 港湾上线**：① trail/记账统一为 **T 收盘信号 → T+1 开盘成交**（`_exit_fill`；不再用 t-1 收盘记账）；② `_pnl_for` 改读 camelCase（pnl/days 不再恒 0，新增回归测试）；③ `SELL_TO_A_SHARE` 0 元平仓路径**随双子星一并删除**；④ `tests/test_sleeve_paper_auto.py` 改假 symbol + patch 候选集（不再触碰真实腿）；⑤ `paper_twin_star`/`twin_star_daily`/`twin_star_intraday`/`sat_hold_path`/`sat_push_log` **删除**，3 个调度 job、2 个 API、`source=twin_star` 停写；⑥ 卫星 replay 仅研究脚本保留，非 Live；⑦ `multi_asset_sleeve` 重写为 **Harbor P1**（去 `MIN_IDLE_PCT`/STOCK gate；无候选→REPO；idle 即停），`/timeline?strategy=harbor` 新增（数字与 B11 一致：valid +47.8/base +38.7）；⑧ Alembic `0050_remove_twin_star`（drop `sat_push_log` + behavior_audit sat 列 + `user_trades.leg` sat→parking）；⑨ 前端默认 `harbor`、双子星组件/页面/契约删除（833 前端测试绿）。**残余（另计 OPT-179）**：旧 `third_asset_sleeve`（T6 单纳指，已不再交易，仅 health/brief 展示）与 Harbor 实现收敛 | — |
| OPT-180 | [x] 2026-09-13 · **Live=回测 决策单源归零**：`harbor.pick_parking`（mom60/MA200/别名/覆盖）与 `harbor.parking_replay`（trail 优先、TS 级换仓、幻影日过滤）成为 Timeline / Live / 评估脚本唯一实现；对账 **100%（473/473，三窗；含窗口首日）**。附带修复：Live `_pick` 曾用 A 别名 mom 却返回 B 标的（永远 513110）、回测别名换仓 peak 语义。**残余（已文档化）**：回测 NAV 用 prev 收盘成交代理、Live 实际 T+1 开盘（隔夜差未建模，决策级一致；如需 NAV 级可另开 OPT）。见 [audit-live-vs-backtest-2026-09-13](backtests/audit-live-vs-backtest-2026-09-13.md) | — |
| OPT-179 | [ ] | **旧 T6 `third_asset_sleeve` 收敛**：B10 已判 V1 单纳指 valid −3.7 出局；现仅 `portfolio_health`/`trading_brief`/`allocation`/`core_holding_audit` 展示引用。验收：健康/简报统一显示 Harbor `multiAssetSleeve`；`third_asset_sleeve` 移入 scripts 或删除；相关测试更新 | — |
| OPT-181 | [x] 2026-09-14 · **港湾 Live paper 执行修正（P0 审计发现）**：① `paper_sleeve_holdings` 统一读 camelCase（`sleevePct`/`entryDate`/`tsCode`）并把分数归一为百分数（S-3 0.10→10），`_build_multi_for_paper` 与 `/commodities/sleeve/paper`、recon 共用（原读 snake_case → idle 恒 100%）；② holdings 带 `entryDate` → trail8 在 18:20 job 真正生效（原 entryDate 缺失直接 return None，recon 按同盲区复刻）；③ ROTATE 原子化：任一旧腿无 exit fill 或平仓失败则不开新腿（原 continue 后无条件 insert，可双持仓）；④ `_resolve_fill_ts_code` 让 ETF `pendingOpenFill` 占位价可回填真实 T+1 open（股票平仓路径仍不收 ETF，避免通用止损误杀）；⑤ ETF 平仓按 0.05%/边×2 记 gross/net/costs（原只记 gross）。测试 +6（sizing/trail/rotate/fill/cost）；全量 4191 passed、`db_rows_baseline.py check` OK | — |
| OPT-182 | [x] 2026-09-14 · **三策略前视审计与修正（习惯双子星/港湾/母港）**：① `eval_twin_star_parking` 卫星口径补全（`rank_key="amp_1430"`——原默认 **全天振幅旧前视键**、`exit_hhmm="1430"`、C1 3%），核心换单源 `harbor.parking_replay`（原本地旧循环，B12 core=旧数字）；② B3/母港波动率窗负索引前视（`range(i-60,i)` @i=60 → `range(max(1,i-60),i)`；`eval_harbor_riskbudget`/`eval_b3_cap`/`eval_etf_benchmark_parking` 三处，train 窗命中）；③ 卫星 R-wide 闸用当日收盘广度（14:30 前视）→ `replay_sgap_from_context`/`build_sgap_timeline` 新增 `gate_1430`（`bar_5min` ≤14:30 print vs MA20；合格域覆盖 100%）；④ Live `multi_asset_sleeve._etf_trail_exit` peak as-of 截断（原扫到 day 后 bar）+ `_signal_series/_signal_closes/_pick(as_of=day)`（历史 recon/展示不再取墙钟最新）；⑤ ETF 平仓补完 OPT-178①：`_exit_fill` 标 `exitPendingOpenFill`，新增 `list_pending_exit_fills`/`patch_paper_exit_fill`，`run_update` 次日回填真实 open 并重算 gross/net（原占位价即最终价、永不回填）；⑥ ROTATE 新增 `parkPct`（新腿不再 0%）+ recon `_opened_by_sleeve_job_today` 日期门槛（历史 BUY 腿不再被剔除、当日 open 平仓腿不再算持仓）。⑦ **卫星价格基期混用（qfq×raw）**：`daily` 于 2026-09-11 被 `cn_reseed_qfq_tx.py` 重灌为前复权，`bar_5min` 仍为原始价——`same_1430` 老口径「收盘卖」= qfq_close/raw_entry（系统性亏损 + −80% 假回撤），`gate_1430`/C1/涨停保护跨基期比较失准。修复：`state_bucket_track` 新增 `_mark`/`_raw_ratio`（raw 15:00 记账、qfq open/pre_close 按当日因子缩放、广度 MA 用 raw），`next_open`/`same_close` 老口径不动。校验：1041/1041 入场、993/993 出场经因子换算后落在日线 [low,high] 内。**结论**：双子星 B12 叙事作废、基期统一后卫星 long **+439.0%/MDD −9.6%**；双子星 **+145.2/+50.5/+25.5/+329.1**（Δcore +83.2/+5.2/**−24.8**/+109.2）→ 仍 REJECT（只挂 valid）；港湾不变、母港 PASS；Live 仍=港湾。测试 +19（新 `test_eval_riskbudget_causal` + gate/raw-basis/C1 缩放/trail/parkPct/exit/recon）；全量 pytest + `db_rows_baseline.py check` OK。**注：本条数字经 OPT-183 日历修正后更新**（clean：卫星 long +463.6%、双子星 +149.7/+52.2/+29.1/+291.9），结论不变。见 [audit-three-strategy-lookahead-2026-09-14](backtests/audit-three-strategy-lookahead-2026-09-14.md) | — |
| OPT-183 | [x] 2026-09-14 · **双市场日历污染修复（HK-only 日期）**：`daily` 表 CN/HK 同表且无市场过滤 → `backtest_engine._load_calendar` 返回 CN∪HK 并集（long 窗多 57 个 HK-only 日 = CN 假期，`bar_5min` 0 行、`index_daily` 0 命中）→ S-3 持仓天数/换手门槛/结算/cooldown 被加速；卫星在假期强平无 mark 持仓（PnL 归零，48 笔）。修复：`backtest_engine._load_calendar(start, end, market)` 按 `config.market` 过滤（CN `NOT LIKE '%.HK'` / HK `LIKE '%.HK'`，BacktestData 传 market）；`state_bucket_track._load_calendar` 固定 CN-only。测试 +2（`test_backtest_engine_loaders::test_loader_calendar_filters_market`、`test_state_bucket_track::TestLoadCalendar`）。**重固化**：CN 官方基线 +38.0/+39.2/+38.7（long V0 +82.7）、HK 线 +21.8/−0.2/+65.7；港湾 P1 +55.2/+52.2/+50.3/+201.5（Δ +17.2/+13.0/+11.6/+118.8，K1–K3 仍 PASS）、双子星 +149.7/+52.2/+29.1/+291.9（仍 REJECT）、母港 M50 +36.5/+35.0/+25.6/+116.3（仍 PASS）；report JSON 全部重生成。见 [audit §F](backtests/audit-three-strategy-lookahead-2026-09-14.md) | — |
| OPT-184 | [ ] | **Live 侧日历过滤残余**：`market_sentiment.py`（panic cooldown 的 `SELECT DISTINCT trade_date FROM daily`）与 `watchlist_automation.py`（RS ret20 窗口）仍未按市场过滤，HK-only 日期可能进入 CN 冷却/排名窗口。验收：两处加 CN 过滤（或复用 OPT-183 的 shared helper），补测试 | — |
| OPT-185 | [x] 2026-09-14 · **策略族登记 + 星港/星舰 Timeline 组件**：前端 `strategy-settings` 扩为 4 档（港湾/母港/星港/星舰，**默认=星港**、v2 storage key + 旧键迁移）+ Settings 面板可选化（明确"仅影响展示、Live 恒港湾"）；后端新增 `service/starport.py`（`blend_starport_timeline`：母港 rows × 卫星 active 日 w=1/3 overlay + maxDD/activeDays/homeportPct 摘要，纯函数）+ `GET /timeline?strategy=starport|starship`（starship = 卫星 standalone 展示别名）+ warmup 增星港；前端 Timeline 四档切换 + 描述 + 默认档联动。测试：`tests/test_starport.py`（5）+ contract 2 + 前端 strategy-settings 重写/BacktestPage 默认档；前端 838 passed、typecheck/lint（0 error）/ruff 绿。冒烟（真实数据 2026-06-01~08-07）：`starport` → 星港 rows 48、summary `{fusedPct −6.08, homeportPct −5.87, satPct +6.63, activeDays 6}`；`starship` → 星舰 rows 49 | — |
| OPT-186 | [ ] | **Forward 数据收集（分腿 paper 记账）**：卫星腿 paper（14:30 信号/fills/滑点）+ B3 月再平衡 paper 记账；腿级账本（Harbor/B3/satellite）+ 事后混合重构（星港 w 任意档可算）。前置：OPT-178 卫星路径重接 + 执行审计电池（成本敏感 45/60/90bps、独立复现抽样、14:30 涨停接近度）。验收：三腿 paper 日更 + 腿级 NAV API + 与回测口径对账 | — |
| OPT-187 | [x] 2026-09-14 · **Backtest 页「策略总览」展示层（5 策略）**：顶部新 tab（默认落地）分 5 子 tab——港湾/母港/星港/星舰/双子星；每档展示三窗+长窗（total/CAGR/MDD/Sharpe，冻结 clean 口径）+ 结构 + 状态徽章 + 优/劣 + 真值档路径。**子 tab 下方联动 Timeline**（同一选中策略；双子星显示退役占位）——`StrategyCatalogPanel` 受控选择 + `TimelineCard` 受控 strategy/onStrategyChange，双向同步；**策略总览内嵌 Timeline 隐藏「资金流全景」**（宏观流向不属于策略汇总，TimelineCard `showFundFlow=false`；对比页保留）。后端 `service/strategy_catalog.py`（纯展示数据，5 行全带 doc/tag/status/timelineStrategy）+ `GET /api/backtest/strategy-catalog`；`packages/shared/schemas/strategyCatalog.ts` Zod + 导出；前端 `useStrategyCatalogQuery` + `StrategyCatalogPanel`。测试：后端 4 + shared 3 + panel 3 + BacktestPage（默认 tab/联动）全量更新；frontend 853 passed / typecheck / lint 0 error / ruff 绿 | — |
| OPT-188 | [x] 2026-09-14 · **Watchlist 策略视图切换（选中哪个策略就显示对应组件）**：顶部 `StrategyModeBar`（4 档，与 Settings 默认策略同源 `useStrategyMode`）；渲染映射——港湾：`HarborDecisionCard` + `PickStrongAlignBanner` + 旧提醒折叠；母港/星港：港湾决策卡（核心腿仍适用）＋ `StrategyStatusCard`（结构/长窗 SR/MDD/优/劣 + wiring 状态）；星舰：仅 `StrategyStatusCard`（卫星 standalone，Live 路径已退役）。**组件 strategy-aware**：`HarborDecisionCard(mode)` 标题/口径随策略（星港 → 星港·今日决策 + 组合腿注记 B3 月频/卫星最近交易日有仓状态；时间线切到对应 strategy）、`PickStrongAlignBanner(mode)`（星港日对齐 + 未接线说明）；未接线的腿（B3/卫星）在卡内明示 OPT-178/OPT-186 待办。测试：`StrategyModeBar` 2 + `StrategyStatusCard` 3 + 决策卡/对齐横幅 mode 用例；前端 849 passed / typecheck / lint 0 error | — |
| OPT-189 | [x] 2026-09-14 · **每策略「今日决策」面（decision-aid）**：后端 `service/strategy_today.b3_state`（B3 腿当前月目标/漂移/再平衡清单，60d 逆波动率、月锚点、纯函数可注入数据）+ `GET /api/backtest/b3-state`；前端 `B3LegBlock`（母港/星港：本月基准日、目标权重 chips、买卖清单/无需调整）+ `SatelliteLegBlock`（星港/星舰：最近交易日有仓 n/4 槽、闸、当日成交、OPT-178/186 待接说明）+ `SatelliteDecisionCard`（星舰 standalone 决策面）并入 WatchlistPage；shared `schemas/b3State.ts` Zod + 导出；冒烟（真实数据）：asOf 2026-09-11、本月锚 2026-09-01、targets 债 86.5%（逆波动率本性）、无调整。测试：后端 3 + shared 3 + 前端 B3Leg/Satellite/卡用例；前端 852 passed / ruff / lint 0 error | — |
| OPT-190 | [x] 2026-09-14 · **Timeline 卫星口径修复（星舰/星港）**：`build_state_bucket_timeline` 沿用 frozen(next_open) 研究口径（模块 docstring 原意），导致 `strategy=starship\|starport` 产品曲线与冻结 clean 数字不一致（滚动一年星港 27.31% < 母港 39.57% 的异常）。修复：`recipe="frozen"\|"habit"` 参数 + `FROZEN_RECIPE`/`HABIT_RECIPE` 常量（single source；habit = amp_1430 + same_1430 买卖 + exit 14:30 + C1 3% + gate_1430）；route：`state_bucket` 保持 frozen、`starship`/`starport` 走 habit；附带 `sgap_to_timeline_rows` maxDD 符号统一为负。验证（长窗 2021-08~2026-08）：星港 **198.29**（activeDays 630）/ 星舰 **463.57** / 母港 116.34 / 港湾 201.5 vs 冻结 198.1/463.6/116.3/201.5；滚动一年：星港 **39.79** vs 母港 39.57。测试：recipe 转发 + `state_bucket_track` 47 全绿 | — |
| OPT-191 | [x] 2026-09-14 · **双子星转并行对照档 + Timeline 接线（用户决策）**：不退役、去掉 REJECT 徽章——catalog status → `parallel_candidate`/「并行对照」、`timelineStrategy="twin_star"`、cons 保留事实（valid Δ−21.2/Δsr−0.65）去掉判决措辞；`blend_overlay_timeline` 泛化（星港 base=母港 w=1/3；双子星 base=港湾 w=1/2）+ route `strategy=twin_star`（habit 卫星）+ 前端 `TimelineStrategy` 5 档、总览内嵌曲线（隐藏资金流）；共享 schema 枚举加 `parallel_candidate`/`twin_star`。验证（长窗）：双子星 **291.94** / 港湾底座 201.5 / MDD −20.8 / activeDays 630（冻结 +291.9）。**默认数据收集档仍=星港（不动）**；5 套并行对比 | — |

### E 策略实现准确度（2026-09-11 新增）✅ 全完成（OPT-171/172/173）

> 把"前视/时钟/成本"那类事故（OPT-159 前视、14:30 时钟统一、OPT-154 滑点双计、OPT-157 qfq 混基准）从**人工审计**变成**机器护栏**。

---

### 刚完成（待归档）

| OPT-175 | [x] 2026-09-12 · **coverage 余量**：88.05% → **88.93%**（门 88 恢复安全垫）；新增 `test_fin_panel`(12)/`test_cn_fin_statements_sync`(11)/`test_commodity_signals`(17)；全量 4345 passed | — |
| OPT-176 | [x] 2026-09-12 · **散户关注度快照基建**：新表 `cn_xq_follow`（migration `0049`，雪球 `stock_hot_follow_xq` 全市场关注数）+ 每日 job `xq_follow_snapshot`（工作日 15:40 Asia/Shanghai）+ `scripts/sync_xq_follow.py`；SYNC_JOB_TYPES + `SCHEDULER_JOB_CATALOG`（factors 组）；`tests/test_xq_follow.py` 4 passed；无历史接口，只能向前每日积累（P0-13） | — |
| OPT-177 | [x] 2026-09-12 · **ETF trail8 前视 bug（双子星核心回测污染）**：`pick_strong_grid.build_nav_from_cache` 与 `pick_strong_track.build_mom_compare_timeline` 的 trail 用**当日收盘**触发、却把**当日收益记 0** → 1 日前视。实测 long 窗 `fusedPct 40.2 → 0.8`（因果）、valid `56.9 → 20.0`。**修复**：trail 改 t-1 收盘触发（`trail_causal` 默认 True；`build_mom_compare_timeline` 同步）；**连锁**：B8 修正后 T3 连收益都反超 T0、B9 A2/A3 变 PASS。审计档 [audit-trail8-2026-09-12](backtests/audit-trail8-2026-09-12.md) | — |
| OPT-171 | [x] 2026-09-12 · **PiT 前视护栏**：`build_panel` 拆出纯 `build_panel_from_px`；`tests/test_pit_no_lookahead.py` 钉死"上月流动性选集"契约 + 禁 `.shift(-` | — |
| OPT-172 | [x] 2026-09-12 · **配方 attestation 扩展**：`state_bucket_track.COSTS_ROUNDTRIP` 改为从 `paper_cost_model` 派生（单一源）；attest CN 30bps / HK 90bps、回测成本==live、engine 用共享模型 | — |
| OPT-173 | [x] 2026-09-12 · **回放黄金**：`tests/test_engine_golden.py` + `tests/golden/engine_summary.json` 冻结 `_summarize` 指标（`UPDATE_GOLDEN=1` 再生） | — |
| OPT-165 | [x] 2026-09-12 · **schema parity 护栏**：`tests/test_schema_parity.py`（纯 migration-coverage + PG 列超集）；**抓到真 drift**——`system_events` 无 migration → 补 `0048_system_events`（head 0048）；全量 4298 passed | — |
| OPT-166 | [x] 2026-09-12 · **死代码/omit 收敛**：`state_bucket_slice.py` 移出 `src/`→`scripts/`（无 src 引用，文档已更新），删 coverage omit；engine 6 候选行复核为防御 nil 守卫、保留 | — |
| OPT-174 | [x] 2026-09-12 · **main 变绿 hotfix**：修 ruff 7 + pyright 7（`ruff check`/`pyright` 均 0 error；全量 4295 passed） | — |
| OPT-164 | [x] 2026-09-12 · **CI 格式门**：一次性 `ruff format`（432 文件）+ `prettier --write`（251 源码）；`lint` 加 `ruff format --check`、root `format:check` + CI 步骤 | — |
| OPT-162 | [x] 2026-09-12 · 工作区 WIP 按主题切分提交（5 个 commit：OPT-160/161 工程 · sat 时钟统一 · trend_guide 钩子 · 数据一致性 · P0-12 研究+因子库；`git status` 干净） | — |

| OPT-160 | [x] 2026-09-12 · 运行产物去跟踪分类治理（111 文件 `git rm --cached`；`.gitignore` +4 规则 +`coverage_tmp.json`；README 再生来源表；报告测试 57 passed） | [2026-09-12-opt-160-artifact-untrack.md](archive/2026-09-12-opt-160-artifact-untrack.md) |
| OPT-161 | [x] 2026-09-12 · 文档死链清零（根因：`opt-151/152` 归档被误放 `docs/optimization-checklist/archive/` → `git mv` 到 `docs/archive/`；`linkcheck` clean 173 文件） | — |

| OPT-156 | [x] 2026-09-11 · stock_dailybasic 2023-12-29→2024-08-01 空洞回填（**实缺 2024-01~07**，140 个 A 股交易日/74.8 万行）+ trade_calendar SSE 补齐到 2005（+6575 行，4374 open days） | — |
| OPT-157 | [x] 2026-09-11 · CN `daily` 复权统一重建 + A 股日线回填 2007 + 财报回填 2007–2017 + 两融空洞补全（原 close 是标准 qfq/陈旧基准/未复权混合，跨除权收益跳变；重建后 2007–2026 单一基准，全市场一致性 99.8%→100%，脚本 `backfill_cn_daily_history`/`rebuild_cn_daily_qfq`/`fix_daily_outliers`/`backfill_cn_financials_history`，备份 `daily_backup_20260911`） | [data-consistency-2026-09-11.md](backtests/data-consistency-2026-09-11.md) |
| OPT-158 | [x] 2026-09-11 · 指数历史回补：`index_daily` 000300/000905/000001 等 5 指数经 `index_hist_extend.py --since 2005-01-01` 补 2005–2020（+20870 行），支撑真实市值加权 beta（DH-2） | [a1-voltarget-beta-2026-09-11.md](backtests/a1-voltarget-beta-2026-09-11.md) |
| OPT-159 | [x] 2026-09-11 · P0-12 pilot 前视审计：`strategy_a1_voltarget.build_panel` 用本月末成交额选本月票（前视 ~+20pt/yr）→ 改上月末 as-of；沉淀纪律"选池必须调仓日可得"；同批 `diag_tenbagger` 审计为 as-of 正确 | [a1-voltarget-beta-2026-09-11.md](backtests/a1-voltarget-beta-2026-09-11.md) |

| OPT-155 | [x] 2026-09-10 · ADV 冲击诊断（AUM ¥1000万 → 不 material，关闭；复活条件 ¥3000万+） | [2026-09-10-opt-155-adv-impact.md](archive/2026-09-10-opt-155-adv-impact.md) |

| OPT-154 | [x] 2026-09-10 · E5 滑点双计修正（回测成本 = live 成本模型，CN 30bps / HK 90bps） | [2026-09-10-opt-154-e5-slippage.md](archive/2026-09-10-opt-154-e5-slippage.md) |

| OPT-153 | [x] 2026-09-10 · 卫星腿判定修正（自选卫星单不再误报偏离 + 腿推断去 `===12.5`） | [2026-09-10-opt-153-sat-leg-audit.md](archive/2026-09-10-opt-153-sat-leg-audit.md) |

| OPT-152 | [x] 2026-09-09 · Timeline 实盘口径主曲线（A 方案：S-3 实际权益主曲线 + 100% 押注基准降虚线） | [2026-09-09-opt-152-timeline-product-curve.md](archive/2026-09-09-opt-152-timeline-product-curve.md) |

| OPT-151 | [x] 2026-09-09 · 核心腿 recon 对账闭环（sleeve_paper_recon + brief/job/API/UI） | [2026-09-09-opt-151-sleeve-core-recon.md](archive/2026-09-09-opt-151-sleeve-core-recon.md) |

| OPT-150 | [x] 2026-09-09 · 账本修正机制 PATCH 改腿 + 审计一键挪链 | [2026-09-09-opt-150-trade-correct.md](archive/2026-09-09-opt-150-trade-correct.md) |

| OPT-149 | [x] 2026-09-09 · user_trades 腿拆分 sat/s3 + 审计按腿分支（卫星对 paper 账） | [2026-09-09-opt-149-trade-legs.md](archive/2026-09-09-opt-149-trade-legs.md) |

| OPT-147 | [x] 2026-09-09 · R-wide breadth 池 HK 污染量级断言（flips 0/0/0 关项，引擎零改动） | [2026-09-09-opt-147-rwide-hk.md](archive/2026-09-09-opt-147-rwide-hk.md) |

| OPT-148 | [x] 2026-09-08 · paper HK T+2 settled 账本 + 平安最高佣金 90bps + swap 费用归位 | [2026-09-08-opt-148-hk-settle-ledger.md](archive/2026-09-08-opt-148-hk-settle-ledger.md) |

| OPT-124 | [x] 2026-09-06 · 多 token 轮换 + 配额看门狗 | [2026-09-06-opt-124-tushare-pool.md](archive/2026-09-06-opt-124-tushare-pool.md) |

---

---

| OPT-125 | [x] 2026-09-06 · DB 连接池 + 重试 + 慢查询护栏 | [2026-09-06-opt-125-db-pool.md](archive/2026-09-06-opt-125-db-pool.md) |
| OPT-126 | [x] 2026-09-06 · 东财出口探针 + 熔断可视化 | [2026-09-06-opt-126-em-probe.md](archive/2026-09-06-opt-126-em-probe.md) |
| OPT-145 | [x] 2026-09-06 · 外购分钟对拍 A 形式化 | [2026-09-06-opt-145-vendor-minute.md](archive/2026-09-06-opt-145-vendor-minute.md) |
| OPT-146 | [x] 2026-09-06 · 港股符号误标 + 结算语义 | [2026-09-06-opt-146-hk-symbol.md](archive/2026-09-06-opt-146-hk-symbol.md) |

---

---

## 冬眠（3 条 · 未关闭 · 可唤醒）

> 陈年 open / P2 储备，移出未完成队列减负。唤醒 = 移回"未完成"并排优先级。

| OPT | 状态 | 全文 |
|-----|------|------|
| OPT-045 | [ ] Phase A done · B/C 待开 | [2026-09-06-opt-dormant-open.md](archive/2026-09-06-opt-dormant-open.md) |
| OPT-075 | [ ] 2026-08-12 登记未处理 | 同上 |
| OPT-127 | [ ] P2 储备（用户判断暂无用；OPT-125 后重估） | 同上 |

---

## 已完成索引（108 条 · 按天归档 · 只读）

| 日期 | OPT | 标题 | 归档 |
|------|-----|------|------|
| 2026-06-18 | 031–040 | Sentiment 降载 / ensure_table / 指数批量 / 信号去重 / 退役串行 / Screener / Dashboard / RSS / Query 迁移 | [2026-06-18-opt-031-040-perf-queries.md](archive/2026-06-18-opt-031-040-perf-queries.md) |
| 2026-07-29 | 041–044 | HK 闸门 / ETF 通用化 / HK 日线 cron / watchlist 显示 | [2026-07-29-opt-041-044-hk-etf.md](archive/2026-07-29-opt-041-044-hk-etf.md) |
| 2026-08-01 | 045 Phase A | /v1 API 整圈（045+046+047 Phase A；正文 045 留 B/C） | [2026-08-01-opt-045-v1-api-surface.md](archive/2026-08-01-opt-045-v1-api-surface.md) |
| 2026-08-01 | 046–048 | /v1 业务 endpoint / explain 文档 / Tunnel 部署 | [2026-08-01-opt-046-048-api-v1.md](archive/2026-08-01-opt-046-048-api-v1.md) |
| 2026-08-01 | 049 | Paper-trading 启动 | [2026-08-01-opt-049-paper-trading.md](archive/2026-08-01-opt-049-paper-trading.md) |
| 2026-08-01 | 050 | 数据源质量审计 | [2026-08-01-opt-050-data-source-audit.md](archive/2026-08-01-opt-050-data-source-audit.md) |
| 2026-08-01 | 051 | API key 配额 openapi | [2026-08-01-opt-051-api-key-quota-openapi.md](archive/2026-08-01-opt-051-api-key-quota-openapi.md) |
| 2026-08-01 | 052 | Alpha Radar HK | [2026-08-01-opt-052-alpha-radar-hk.md](archive/2026-08-01-opt-052-alpha-radar-hk.md) |
| 2026-08-01 | 053 | DB 走向决策 | [2026-08-01-opt-053-db-direction.md](archive/2026-08-01-opt-053-db-direction.md) |
| 2026-08-01 | 055 | AI agent cookbook | [2026-08-01-opt-055-ai-agent-cookbook.md](archive/2026-08-01-opt-055-ai-agent-cookbook.md) |
| 2026-08-01 | 056 | Docker 一键起 | [2026-08-01-opt-056-docker-one-click.md](archive/2026-08-01-opt-056-docker-one-click.md) |
| 2026-08-01 | 057 | TV Capture 三轨（另一条 057 见 08-31 档） | [2026-08-01-opt-057-tv-capture-three-track.md](archive/2026-08-01-opt-057-tv-capture-three-track.md) |
| 2026-08-02 | 058 | 漏斗 N 日表格（另一条 058 见 08-31 档） | [2026-08-02-opt-058-funnel-history-paper-v0.1.md](archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md) |
| 2026-08-03 | 059 | legacy 清理（与 09-01 档的 059 涨停审计不同条） | [2026-08-03-opt-059-legacy-cleanup.md](archive/2026-08-03-opt-059-legacy-cleanup.md) |
| 2026-08-04 | 060 | Tauri 降级（另一条 060 见 09-01 档） | [2026-08-04-opt-060-tauri-deprecation.md](archive/2026-08-04-opt-060-tauri-deprecation.md) |
| 2026-08-04 | 061 | DB 备份迁移 | [2026-08-04-opt-061-db-backup-migrate.md](archive/2026-08-04-opt-061-db-backup-migrate.md) |
| 2026-08-07 | 062 | Paper v0.2 | [2026-08-07-opt-062-paper-v02.md](archive/2026-08-07-opt-062-paper-v02.md) |
| 2026-08-07 | 063 | 回测引擎 v0 | [2026-08-07-opt-063-backtest-engine.md](archive/2026-08-07-opt-063-backtest-engine.md) |
| 2026-08-07 | 064 | 卖出归因 + 回测页 | [2026-08-07-opt-064-exit-attribution-backtest-page.md](archive/2026-08-07-opt-064-exit-attribution-backtest-page.md) |
| 2026-08-07 | 065 | 周度复盘 | [2026-08-07-opt-065-weekly-review.md](archive/2026-08-07-opt-065-weekly-review.md) |
| 2026-08-07 | 066 | journal 防御 | [2026-08-07-opt-066-journal-symbol-defense.md](archive/2026-08-07-opt-066-journal-symbol-defense.md) |
| 2026-08-07 | 067 | 相关性防火墙 | [2026-08-07-opt-067-correlation-firewall.md](archive/2026-08-07-opt-067-correlation-firewall.md) |
| 2026-08-09 | 068–073 | 真实交易看板 / K 线源 / 入池闸门 / score 回填 / RS 过滤 / 持仓体检 | [2026-08-09-opt-068-073-engine-batch.md](archive/2026-08-09-opt-068-073-engine-batch.md) |
| 2026-08-12 | 074–087（无 075、077；075 未完成在正文，077 从未立项） | 健壮性审查 / 执行闸统一 / 体检提醒 / 对账 / BacktestPage / 通知 / 周报 / 条件单 / 信息层 / C4 框架 | [2026-08-12-opt-074-087-robust-batch.md](archive/2026-08-12-opt-074-087-robust-batch.md) |
| 2026-08-12 | 085–089 | S-3 信息层 + C4 对照框架 + 体检卡（当时总结） | [2026-08-12-opt-085-089-s3-info-layers.md](archive/2026-08-12-opt-085-089-s3-info-layers.md) |
| 2026-08-12 | 090–094 | webhook + 一键启动 + 红绿灯 | [2026-08-12-opt-090-094-webhook-lights.md](archive/2026-08-12-opt-090-094-webhook-lights.md) |
| 2026-08-13 | 095–110（无 101，从未立项） | 退出对齐 / HK 信号 / 蒙特卡洛 / 涨跌停 / ATR / 对账 / LLM 移峰 / Alpha 初验 | [2026-08-13-opt-095-110-align-batch.md](archive/2026-08-13-opt-095-110-align-batch.md) |
| 2026-08-14 | 111–118 | 对账横幅 / cron / 执行卡 / Webhook 目录 / Bark / Tunnel / 手机 UI / 网关 | [2026-08-14-opt-111-118-mobile-webhook.md](archive/2026-08-14-opt-111-118-mobile-webhook.md) |
| 2026-08-21 | 119–121 | 套筒模拟器 / 核心仓核对 / dual 联合验收 | [2026-08-21-opt-119-121-sleeve-dual.md](archive/2026-08-21-opt-119-121-sleeve-dual.md) |
| 2026-08-31 | 057–058 | dailybasic 停更修复 / 双子星 14:30 决策审计 | [2026-08-31-opt-057-058-dailybasic-twin-audit.md](archive/2026-08-31-opt-057-058-dailybasic-twin-audit.md) |
| 2026-09-01 | 059–060 | 涨停审计 / 双子星 v2 | [2026-09-01-opt-059-060-limitup-twin-v2.md](archive/2026-09-01-opt-059-060-limitup-twin-v2.md) |
| 2026-09-01 | 122–123 | 盘中近似信号 / v3 固化 | [2026-09-01-opt-122-123-twin-signal.md](archive/2026-09-01-opt-122-123-twin-signal.md) |
| 2026-09-02 | 128–135 | clip4 / 对齐 / Timeline / 占用 / blotter / 健康 / Zod / 日流程 | [2026-09-02-opt-128-135-twin-live.md](archive/2026-09-02-opt-128-135-twin-live.md) |
| 2026-09-03 | 136–137 | body=3 对齐 / 5 分钟线 | [2026-09-03-opt-136-137-sat-live.md](archive/2026-09-03-opt-136-137-sat-live.md) |
| 2026-09-04 | 138 | 红套件清零 | [2026-09-04-opt-138-red-suite-green.md](archive/2026-09-04-opt-138-red-suite-green.md) |
| 2026-09-04 | 139 | Scheduler 上报统一 | [2026-09-04-opt-139-job-guard.md](archive/2026-09-04-opt-139-job-guard.md) |
| 2026-09-04 | 140 | 模式口径统一 | [2026-09-04-opt-140-leg-split.md](archive/2026-09-04-opt-140-leg-split.md) |
| 2026-09-04 | 141 | 习惯口径引擎化 | [2026-09-04-opt-141-live-caliber.md](archive/2026-09-04-opt-141-live-caliber.md) |
| 2026-09-04 | 142 | 日历收敛 + Alembic | [2026-09-04-opt-142-calendar-api-migration.md](archive/2026-09-04-opt-142-calendar-api-migration.md) |
| 2026-09-04 | 143–144 | 可重放补强 / 外围抖动 | [2026-09-04-opt-143-144-replay-quiet.md](archive/2026-09-04-opt-143-144-replay-quiet.md) |
| 2026-09-06 | 124 | 多 token 轮换 + 配额看门狗 | [2026-09-06-opt-124-tushare-pool.md](archive/2026-09-06-opt-124-tushare-pool.md) |
| 2026-09-06 | 125 | DB 连接池 + 重试 + 慢查询护栏 | [2026-09-06-opt-125-db-pool.md](archive/2026-09-06-opt-125-db-pool.md) |
| 2026-09-06 | 126 | 东财出口探针 + 熔断可视化 | [2026-09-06-opt-126-em-probe.md](archive/2026-09-06-opt-126-em-probe.md) |
| 2026-09-06 | 145 | 外购分钟对拍 A 形式化 | [2026-09-06-opt-145-vendor-minute.md](archive/2026-09-06-opt-145-vendor-minute.md) |
| 2026-09-06 | 146 | 港股符号误标 + 结算语义 | [2026-09-06-opt-146-hk-symbol.md](archive/2026-09-06-opt-146-hk-symbol.md) |
| 2026-09-09 | 147 | R-wide breadth 池 HK 污染量级断言（关项） | [2026-09-09-opt-147-rwide-hk.md](archive/2026-09-09-opt-147-rwide-hk.md) |
| 2026-09-09 | 149 | user_trades 腿拆分 sat/s3 + 审计按腿分支 | [2026-09-09-opt-149-trade-legs.md](archive/2026-09-09-opt-149-trade-legs.md) |
| 2026-09-09 | 150 | 账本修正机制 PATCH 改腿 + 审计一键挪链 | [2026-09-09-opt-150-trade-correct.md](archive/2026-09-09-opt-150-trade-correct.md) |
| 2026-09-09 | 151 | 核心腿 recon 对账闭环（sleeve_paper_recon + brief/job/API/UI） | [2026-09-09-opt-151-sleeve-core-recon.md](archive/2026-09-09-opt-151-sleeve-core-recon.md) |
| 2026-09-09 | 152 | Timeline 实盘口径主曲线（A 方案：S-3 实际权益主曲线 + 基准降虚线） | [2026-09-09-opt-152-timeline-product-curve.md](archive/2026-09-09-opt-152-timeline-product-curve.md) |

> **历史重号说明**：OPT-057/058/060 在正文出现过两次（不同日期不同内容），OPT-059 的归档文件与正文条目主题不同。
> 一律不重编号，以"日期+标题"区分：057 = 08-01 TV Capture / 08-31 dailybasic 停更；058 = 08-02 漏斗 / 08-31 双子星 14:30 审计；
> 060 = 08-04 Tauri / 09-01 双子星 v2；059 归档 = 08-03 legacy 清理，正文 059（09-01 涨停审计）在 09-01 档。
