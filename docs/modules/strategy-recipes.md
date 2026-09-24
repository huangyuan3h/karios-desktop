# 五档策略现行配方（可重建 spec）

> **何时看**：想知道"某档策略现在到底怎么跑"、要改参数 / 改权重 / 改时点、或担心文档丢失需要重建时。
> **何时不看**：只想看回测数字与裁决过程 → [`backtests/SUMMARY.md`](../backtests/SUMMARY.md)；想看港湾产品叙事 → [`pick-strong-track.md`](./pick-strong-track.md)。
> **角色**：本文件是 5 档策略**当前生效逻辑**的唯一配方入口（universe → 信号 → 时点 → 仓位 → 出场 → 闲置现金 → 成本 → 闸 → 接线）。
> S-3 **参数数值表**不在此复制，真值在 [`strategy-params.md`](./strategy-params.md) §1；本文件负责"哪个机制在生效 + 代码在哪"。
> 冻结 tag 见 `service/strategy_catalog.py`（每档 `doc`/`tag` 字段）。
> 改任何参数 / 时点 / 权重 → **同步本文件 + `strategy-params.md`**；改口径先开预注册（`docs/designs/*prereg*`）。

---

## §0 共享底座（4 个引擎 + 调度）

### 0.1 S-3 股票核心引擎（港湾/母港/星港/双子星共用）

- **配置真值**：`scripts/run_walk_forward.py:41-115`（`S3_CONFIG`，Timeline 与 walk-forward 同源；`api/backtest_routes.py:1004` 导入）。
  只跑 CN；`strategy != "harbor"` 时才叠 HK 腿（`backtest_routes.py:1010-1032`）。
- **`S3_CONFIG` 全表（2026-09-17 生效）**：

| 字段 | 值 | 说明 |
|---|---|---|
| `score_threshold` | 65.0 | 入场分数（TrendOK `watchlist_score_daily`） |
| `rs_rank_min` | 0.5 | 20 日相对沪深300 收益的全市场百分位 |
| `gates` | `full` | regime + sentiment + flow + mainline 四闸 |
| `entry_mode` | `next_open` | T 收盘信号 → **T+1 开盘**成交 |
| `position_pct` / `max_positions` | 0.10 / 10 | 每腿 10%，最多 10 腿 |
| `max_hold_days` | 60 | 最长持有交易日 |
| `max_hold_env_shorten` | 45 | UPTREND 日入场的腿 45 日强平（D2） |
| `stop_loss_pct` / `trailing_stop_pct` | −5.0 / −8.0 | 固定止损 / 收盘峰值回撤 |
| `atr_stop_mult` / `atr_stop_strong_only` | 2.0 / True | Strong 日改用入场锁定 −2×ATR14%，让赢家跑（OPT-105） |
| `target_pnl_pct` / `score_floor` | 100.0 / 0.0 | 均等于关闭 |
| `neutral_block` | True | 真中性日 + 隐弱日禁开（TIP-014） |
| `entry_style` (+`rs_min`/`dip_min`) | `auto` (0.7 / 3.0) | uptrend 追动量 RS≥0.7；fan 日买回调 5d≤−3%；weak/neutral 禁开 |
| `env_position_scale` | `uptrend:1.25,fan:0.75` | 按**入场日** env 放大/缩小（D3） |
| `panic_cooldown_days` | 2 | 恐慌日后 2 日禁开 |
| `drawdown_circuit_pct` | −25.0 | 组合回撤熔断 |
| `national_team_gate` | True | 沪深300<MA200 且 4-ETF 份额 20 日 Δ≤0 时暂停开仓（TIP-017B） |
| `exclude_boards` / `min_avg_amount` | `"300"` / 0.7 | 排除创业板；60 日均额 ≥0.7 亿 |
| `pyramid_*` | 2.5 / 0.5 / 1 | 浮盈 2.5% 加一次半仓（每腿一次） |
| `slippage_pct` | **0.0** | 成本单源 = `paper_cost_model`（CN 静态 32.28bp 往返；引擎/paper 另按成交价套 1-tick 滑点下限，勿双计） |
| `diverging_scale` | 1.0 | Diverging 日仓位不缩 |

- **HK 腿差异**（`HK_S3_CONFIG`，`run_walk_forward.py:138-173`）：`gates="regime"`、`trailing_stop_pct=-12`、`rs_rank_min=0.6`、`max_positions=10`、`settle_lock_sessions=2`（T+2）、`min_avg_amount=0`。
  ⚠️ `service/reconciliation.py:34-96` 有一份副本（缺 `entry_mode="next_open"`、HK `max_positions=20`）——只用于对账，**真值以 `run_walk_forward` 为准**（见 §7 漂移 4）。
- **数据/信号**：分数 = `watchlist_score_daily`（TrendOK 落库值，引擎 `skip_live_score_lookup=True` 防读未来分）；价格 = `daily` **qfq**；regime = `get_index_signals` + `classify_market_regime`；主线 = SW L1 资金流 Top3 五日净流入 ∪ 当日≥20 亿且排名升≥10；情绪 = `market_cn_sentiment_daily.risk_mode`。
- **T+1 / 涨跌停**：涨停不买（主板 10% / 创科 20% / 北交所 30%；next_open 以**信号日**收盘为基准判定一字）；跌停不卖（当日 exit/add 决策顺延）；next_open 成交当日（holding≤1）的卖因作废。
- **出场优先级**：`stop_hit > target_hit > score_floor > pool_exit > max_hold`，trail 用收盘峰值回撤（`paper_trading.py:641-699`）。
- **现金约束**：总名义 ≤100%。
- **外闸**：panic / drawdown circuit / neutral_block / national_team_gate / light_red（CN paper）。
  ⚠️ `service/execution_gate.py`（ATTACK/WEAK_ATTACK/HOLD_ONLY/DEFEND）**不是** S-3 下单闸（仅 dashboard 展示，`dashboard.py:122`）。

### 0.2 卫星引擎（星港/双子星/星舰的进攻腿）

- **常量**（`service/state_bucket_track.py:33-52`）：`POSITION_PCT=0.25`、`MAX_POS=4`、`BUCKET_Q=3`、`BODY=3`、`R_WIDE_THRESHOLD=0.5`、`MIN_GAP_PCT=0.03`、`WARMUP_CAL_DAYS=120`、`COSTS_ROUNDTRIP=round_trip_cost_pct("CN")`（静态 32.28bp 单源；卫星腿未套 1-tick 下限，见 §0.4 注）。
- **两套 recipe**：
  - `FROZEN_RECIPE`（研究 next_open 口径）：`{skip_t1_limit, pool_mode="strict"}`
  - `HABIT_RECIPE`（**Live/产品口径**，星舰/星港/双子星展示都用）：`{skip_t1_limit, pool_mode="strict", max_pos=4, position_pct=0.25, body=3, fill_mode=same_1430, fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03, rank_key="amp_1430", gate_1430=True}`
- **Universe**：全 A 股 `daily` × `stock_basic`，剔北交所 / 退市 / ST。
- **信号**：S-gap `open/pre_close−1 > 3%`；振幅 `amp=(high−low)/close`；**14:30 振幅** `amp_1430=(14:30 前最高−最低)/14:30 价`（缺失排最后）；桶 = 振幅升序前 1/3（低波动优先）；strict pool 不补位。
- **过滤**：`skip_t1_limit`（14:30 print ≥ pre_close×(1+限幅−0.004) → 不买）；**C1**：`14:30/open−1 > 3%` 跳过；C2/C3/CHURN 已关。
- **成交/出场**：14:30 **raw print** 买入（无 print 当日不成交）；持有满 `body=3` → 第 3 个交易日 14:30 print 卖；日终 mark 用 15:00 raw 收盘；单笔扣静态 CN 32.28bp（`COSTS_ROUNDTRIP`）。天然满足 T+1（无同日买卖）。
- **闸**：`breadth_1430 > 0.5`（14:30 print 站上各自 20 日均线的家数占比）；闸关 = **当日不开新仓**，已有腿照常按 body 出场。
- **容量**：≤500 万（sat-execution-audit-2026-09-14，90bps 压力仍 +310%）。

### 0.3 ETF 停车引擎（单一实现 · 产品 = H2）

- **候选 4 腿**（`service/harbor.py:31-37`）：GOLD `518880.SH`、OIL `513350.SH`、NASDAQ `513110.SH`+`513100.SH`（alias 取优）、BOND10 `511260.SH`。
- **排名**：`mom60 = close(as_of)/60 交易日前 close − 1`；门槛 `close ≥ MA200`（BOND10 也需站上，`bond_ungated=False`）；**覆盖率 ≥3/4** 腿有 ≥200 根 bar 才允许出 pick，否则 None → **REPO（收益 0）**。
- **状态机（唯一一份）**：`harbor.parking_replay(..., hyst_band=)`，**Live / paper / Timeline / watchlist / recon 全部走它**（H-H2-UNIFY，2026-09-18）。① 先判现持 trail：收盘 < 峰值×(1−8%) → 当日转 REPO，**不当日再入**；② 换仓需 `want.mom60 − held.mom60 ≥ hyst_band`，否则**保留原腿**（不卖、无 REPO 空窗、`sides=0`、峰值续算；记录 `hyst_blocked`）；成本 5bp/边；`cooldown_days=0`。
- **`hyst_band` 口径**：**产品 = `harbor.HYST_BAND = 0.02`（2pt）**；`0.0` = 历史 canonical「任何变化就换」只作**研究参考**（`parking_replay` 默认 0.0，冻结实验可复现）。`service/parking_sleeve.py` 的 `hysteresis_parking_replay` 只是薄委托，无第二套逻辑。
- **基准**：展示/研究统一走 `harbor.load_etf_closes()`（CSV `close_adj` + DB 尾部按 anchor 比例拼接；`daily` 里 ETF 是 raw、`adj_factor` NULL）。

### 0.4 成本真值表

| 对象 | 成本 | 出处 |
|---|---|---|
| S-3 股票（CN） | 静态 **32.28bp** 往返 = 万3×2 佣金 + 0.1bp×2 过户费 + 0.541bp×2 经手/证管费 + 5bp 卖出印花 + 10bp×2 滑点**下限** | `paper_cost_model.py` |
| S-3 股票（HK） | 静态 **92.54bp** 往返 = 20bp×2 佣金 + 10bp×2 印花 + 1.27bp×2 微费 + 15bp×2 滑点**下限** | 同上 |
| 卫星每笔 | 静态 **32.28bp**（= CN 模型单源，未套 1-tick 下限） | `state_bucket_track.py:34-36` |
| ETF 停车 | 回测 5bp/边；paper 平仓记 0.1% 往返 | `harbor.py:27`、`sleeve_paper_auto.py:39-40` |
| 星舰卫星→停车转移 | 5bp×\|Δw\| | `state_bucket_track.py:1531-1545` |
| B3 月调仓 / 母港月复位 | 5bp × 换手 | `homeport.py:129-131, 158-179` |

> **成本口径（2026-09-21 严格化）**：`paper_cost_model.py` 为唯一源，已补 CN 万3 佣金 + 过户/经手/证管费、HK 微费（SFC+HKEX+AFRC+CCASS），并新增 **1-tick 滑点下限** `max(base_bps, tick/price)`。**引擎（S-3）与 Live paper 按成交价套 tick 下限**（低价股更贵）；**卫星/展示仍用静态 base**（未接价格，成本不敏感，见 sat-execution-audit）。最低佣金（CN ¥5、HK$100）按 `REFERENCE_CAPITAL=¥1M × 10% 槽位` 折算，在该参考规模下不触发（残差已声明）。**未建模**：>¥5M 冲击成本、HK FX、电话佣金 0.25%。

### 0.5 调度与 Live 记账链（Asia/Shanghai）

| 作业 | 时间 | 作用 |
|---|---|---|
| `satellite_live_panel` | 14:30 | 卫星现场面板（只读）+ 按所选策略 Bark 推送；**不补跑**，过期 API 标 `stale` |
| `close_sync` | 17:10（补跑 `*/10 17-23`） | 收盘同步 |
| `watchlist_automation` | 17:30 | 池构建 + S-3 intake（`close_sync` 未成功则 skip） |
| `sleeve_etf_daily_sync` | 17:25 | 5 只套筒 ETF 收盘进 `daily` |
| `paper_s3_intake` | 17:42 | S-3 买入镜像 |
| `paper_trading_update` | 17:45 | S-3 卖出/更新（当晚 bar 未到用 T 收盘占位 `pendingOpenFill`，次日修正） |
| `sleeve_paper_auto` | 18:20 | 港湾停车镜像到 paper 账本 |
| `bar_5min_close` | 18:40 | 存当日 5min bars + 刷新卫星池（17:30 时面板未出，OPT-219） |

- 启动补跑 `catchup_missed_eod_chain`（`scheduler/__init__.py:641-715`）：按守卫时刻补 17:30 后的各步；**skip ≠ done**（判据 `automation_applied_on`，OPT-220）。
- 唯一自动实盘记账 = paper book（S-3 + 港湾停车）；真实账户人工执行。前端"所选策略"只决定推送文案（`api/settings_routes.py`），不是下单开关。
- **所选策略（2026-09-24 用户拍板固化）= 稳健星舰 H2-a25**：`app_settings.strategy_mode='starship_robust'`；代码默认 `settings_routes.DEFAULT_STRATEGY_MODE` 与前端 `strategy-settings.DEFAULT_STRATEGY_MODE` 同步。它是唯一研究/展示/人工操作口径；**Live 下单仍=港湾**；H2-a25 进 Live 前置 = paper 3/20 + 风险授权（未满）。报告裁决为 K3 风险下的 REJECT，不能写成全门 PASS。

---

## §1 港湾 Harbor（Live · 日落）

| 项 | 现行定义 |
|---|---|
| 腿构成 | S-3 股票核心（§0.1）+ 闲置现金停 ETF（§0.3 `parking_replay`，**产品 = H2 迟滞 2pt**，统一 2026-09-18） |
| Idle 口径 | `idle = 1 − min(1, Σ position_pct)`（T−1 快照）；Live 停车按复权基准，trail 峰值含 fill 日前一交易日 |
| 时点 | 股票 T 收盘信号 → T+1 开盘成交；停车 T 收盘信号（18:20 job）→ T+1 开盘成交 |
| 出场 | 股票 = S-3 全规则；ETF = trail8 → `SELL_TO_REPO`（优先于换仓）；无候选/跌破 MA200 且无仓 → 持现金 |
| 闸 | 股票 = S-3 全闸；停车**无** regime/panic/circuit 闸、无 idle 下限、无冷却；**唯一约束 = H2 迟滞 2pt（换仓门槛）** |
| 代码入口 | `service/harbor.py`（timeline）、`service/multi_asset_sleeve.py:427-557`（Live 卡片）、`service/sleeve_paper_auto.py`（paper 镜像）；路由 `?strategy=harbor` |
| 真值指针 | 产品叙事 → `pick-strong-track.md`；回测 → `backtests/stable/etf-parking-baseline-2026-09-13.md`（B11）；Live 对账 → `verify_harbor_live_vs_backtest.py` 必须 100% |
| 验证线（已删除） | `?strategy=harbor_h2`（OPT-215）与 `harbor_h2_shadow`（OPT-216）随统一退役（OPT-226，2026-09-18）——H2 即港湾本身 |

## §2 母港 Homeport（产品候选 · M30 展示档 / M50 冻结档）

- 腿构成：`w_harbor × Harbor + (1−w_harbor) × B3`；**B3** = 5 资产逆波动率月频（`510300.SH / 510500.SH / 518880.SH / 513100.SH / 511260.SH`，60 日 `pstdev(日收益)`，`w∝1/σ`，当月首个交易日调仓；月内随价漂移不再平衡）。
- **现行权重裁决（2026-09-16 起）**：展示档 = **M30（w_harbor=0.7）**，`timelineStrategy=homeport_m30`，tag `h-mix-m30-20260916`；**M50（0.5）保留为冻结审计快照**（`?strategy=homeport` 路由默认仍是 0.5）。
- 成本：B3 换手 `5bp×Σ|Δw|/2`；月复位 `5bp×|w_harbor漂移|`。
- 状态：**display only**（无 paper / 无 Live 接线）。代码 `service/homeport.py`；证据 `backtests/stable/homeport-weight-tune-2026-09-16.md`。

## §3 星港 Starport（产品候选增量 · 默认数据收集档）

- 腿构成：底座 = 母港（`?strategy=homeport`，**M50 底座**）；卫星 = habit（§0.2）。
- 混合公式：卫星 active 日 `base + w×(satellite − base)`；idle 日 100% base。
- **现行权重裁决**：展示默认 **`STARPORT_DEFAULT_WEIGHT = 0.2`**（当前数据下 K1 过关的最大值，2026-09-16 用户拍板）；`SAT_WEIGHT = 1/3` 作为 H-B3-SAT **冻结审计参考**（1/3 在当前数据下 K1 −7.6 失败，挂 K1）。
- 成本：卫星 30bp/笔；底座原样继承。
- 状态：display only（Live 仍港湾）。代码 `service/starport.py`；证据 `harbor-b3-sat-2026-09-14.md` §8 / `harbor-sat-weight-2026-09-14.md`。

## §4 双子星 Twin Star（并行对照 · 不进 Live）

- 腿构成：底座 = **港湾**（含 canonical 停车）；卫星 = habit；`TWIN_STAR_WEIGHT = 0.5`。
- 定位：用户 2026-09-14 决定保留为并行对照档（catalog `parallel_candidate`），default 数据收集档是星港。
- 成本/闸/时点：卫星与港湾各自规则（§0.2 / §1）。
- 代码 `service/starport.py:155-169`；证据 `twin-star-parking-refit-2026-09-13.md` + `audit-three-strategy-lookahead-2026-09-14.md`。

## §5 星舰 Starship v2 + H2（激进档 · display/backtest + 14:30 面板与推送）

- 腿构成：卫星 standalone（4×25%）+ **闲置现金停 ETF**（H2 迟滞版）。
- 组合公式：`r_t = sat_ret_t + w_t×sleeve_ret_t − 5bp×|w_t−w_{t−1}|`，`w_t = cashShare(T−1)`（真实现金权重，因果）。
- 停车：§0.3 **H2**（换仓需领先 ≥2pt；被挡保留旧腿）。tag `starship-h2-20260916`。
- 14:30 现场面板（OPT-222/223）：`service/satellite_live.py` 生成（报价注入 → pre-injection replay 取 `exits/heldLegs` → 信号），覆盖率 ≥50% 才落盘；`satellite_live_panel_latest.json`；API `GET /api/backtest/satellite-signals/live-panel`；按所选策略 Bark/站内通知（`satellite_actions.py` 单源文案）。
- 前置（**未满**）：paper 20 笔 + 用户风险授权；90bps 执行审计通过（`sat-execution-audit-2026-09-14.md`）。
- 证据：`sat-idle-parking-2026-09-15.md`（v2 口径）+ `sleeve-tune-2026-09-16.md`（H2 采用，含 2026-09-17 修正数 +242.7/+76.8/−0.3/+975.0）。

## §5b 星舰稳健版 H2-a25（现行 canonical · 研究/展示/人工操作 · 不进 Live）

- **定义**：卫星 standalone（§0.2）+ 闲置现金 `w_t=cashShare(T−1)` 停 **`0.25 × H2 套筒 + 0.75 × B3`**。
  H2 = `mom60+MA200` 趋势 ETF，挑战者领先现持 ≥2pt 才换仓；转移 5bp/边。
- **身份**：`starship_robust` 是稳健研究/展示配方之一。**默认选择档 2026-09-24 起改为 `starship_b`（星舰 B，§5b2）**。canonical 只表示系统口径统一，不表示研究门全 PASS。
- **报告**：`sat_h2_a25_2026-09-24.json`；H2-a25 = OOS2/train/valid/long **+238.4/+55.9/+3.2/+738.5**，MDD **−8.4**，SR **3.50**。
- **裁决**：K1/K2/K4/K5 PASS；K3 FAIL（long MDD 相对纯 B3 **−1.6pt**，门槛 −1.0pt）→ 总状态 **REJECT（仅 K3 风险）**。
- **边界**：只用于研究、展示和人工操作；Live 自动执行仍为港湾，前置为 paper 20 笔 + 用户风险授权。
- **历史对照**：0pt `a25` 报告（+728.4/−6.9）保留为历史，不作为 H2-a25 现行数字。

## §5b2 星舰 B（稳健备选 · 研究/展示/人工操作 · 不进 Live）2026-09-24

- **定义**：卫星 standalone（§0.2）+ 闲置现金 100% 停 **`{国债 511260, 黄金 518880, 纳指 513100}` 逆波动率**（60d，月频再平衡，5bp/边；3 腿各 1/3 预热）。复用 B3 数学（`homeport.starship_b_run`）。
- **身份**：`starship_b` 是研究/展示/人工操作备选档（与 H2-a25 并列），不接 Live 自动下单。
- **数字**：OOS2/train/valid/long **+231.7/+51.5/+5.4/+669.6**；long MDD **−5.5**、SR **3.90**；**stress(22–23) +124.0 / −7.4**。
- **vs H2-a25**：long 少 69pt、回撤浅 2.9pt、Sharpe 高 0.40、stress 强 20pt；停放腿只 3 只 ETF，**实盘好复制**。
- **组件真值**：`homeport.starship_b_run/starship_b_nav/load_starship_b_closes`；`state_bucket_track.STARSIP_B_PARKING_MODE`；Timeline `strategy=starship_b`。报告 [`starship-b-2026-09-24.md`](../backtests/stable/starship-b-2026-09-24.md)。

## §5c 星舰激进对照（研究/展示 · 不进 Live）

- **定义**：卫星 standalone（§0.2）+ 闲置现金 100% 停 H2 套筒；`w_t=cashShare(T−1)`，5bp/边。
- **现行 H2 数字**：OOS2/train/valid/long **+240.7/+75.9/−0.6/+962.7**，long MDD **−30.5**、SR **2.14**。
- **定位**：高收益、高回撤对照；不与 H2-a25 混称，也不改变 H2-a25 的 canonical 身份。

## §6 现行口径裁决总表（双真值 + 默认差异，2026-09-17）

| 项 | 现行值 | 冻结/参考值 | 说明 |
|---|---|---|---|
| 母港权重 | **M30 = 0.7**（展示/产品） | M50 = 0.5（审计快照 + 路由默认） | `?strategy=homeport_m30` vs `homeport` |
| 星港权重 | **0.2**（展示） | 1/3（H-B3-SAT 审计参考） | 1/3 在当前数据下挂 K1 |
| 星舰停车 | **H2（2pt）** | 0pt canonical sleeve（历史对照） | Live/paper/Timeline/watchlist 统一 H2；0pt 仅复现冻结实验 |
| 星舰稳健版 | **H2-a25 = 25% H2 套筒 + 75% B3** | 0pt a25（历史对照） | 唯一现行稳健研究/展示档；K3 风险，总裁决 REJECT |
| 星舰 B | **100% 停 {国债+黄金+纳指} 逆波动率** | — | 稳健备选（2026-09-24）；收益略低/回撤浅/熊市强/好复制，不进 Live |
| 星舰激进对照 | H2 纯套筒 | 0pt A2_true（历史对照） | 研究/展示对照，不进 Live |
| 港湾 Live 停车 | **H2（2pt）· 统一 2026-09-18** | canonical `hyst_band=0`（研究参考） | H-H2-UNIFY；`harbor_h2` 线/影子账本已删除（OPT-226） |
| 双子星 | 并行对照（50/50） | 曾判 REJECT | 不是实盘方案，勿引 scorecard §2 |
| 星港默认底座 | 母港 **M50** | — | 若改 M30 底座需重新预注册 |

## §7 已知漂移 / 待裁决（重建前必须知道）

1. **Live 港湾执行时钟**：B11 预注册写"14:28 出信号 / 14:30 执行"；2026-09-18 把 S-3 腿字面改成
   `next_1430` 实测 **REJECT**（三窗 Δ −38.7/−11.1/−12.2，见
   [`s3-clock-1430-2026-09-18`](../backtests/s3/s3-clock-1430-2026-09-18.md)）→ **引擎维持
   `next_open`（T 收盘 → T+1 开盘）**；`entry_mode="next_1430"` 仅作实验变体保留。
   卫星腿（2026-09-11）与停车腿（B11 T 收盘代理）**已是 14:30 口径**，不受影响。
   若 Live 仍按 14:30 人工执行 = 相对冻结基线有量化跟踪误差（valid 窗 ~−12pt 量级），必须显式记账。
2. **`docs/modules/trading-system.md` 已陈旧**（自称"重建手册"）：panic 3 vs 2、slippage 0.05 vs 0、mp20 vs mp10、缺 `neutral_block`/`entry_style`/D2/D3/national_team/circuit/Strong-ATR。按 `AGENTS.md` 规则应整篇迁 `archive/modules-legacy/`（本文件已在 §0.1/§1 覆盖现行）。
3. **卫星 paper 账本缺失**：星舰"paper 20 笔前置"没有落账代码（`paper_trades` 无 satellite source）；`satellite_signals.py` 注释所称 paper 层 30bp 未落地。
4. **对账副本漂移**：`reconciliation.py` 的 `S3_CONFIG` 缺 `entry_mode="next_open"`、HK `max_positions=20`（真值 10）——展示不受影响，但重建时勿抄它。
5. **代码内未文档化的微观规则**：`pick_parking` 覆盖率门（≥3/4 且 ≥200 根）、NASDAQ alias 切换重置峰值、`entry_date ≥ 当日` 不计入 deployed（已补进 §0.1/§0.3，但只有本文件与代码）。
6. **`execution_gate.py` 不是 S-3 闸**（见 §0.1 末）；`paper_s3.py:18` docstring 仍写"5% sleeve"（实际 10%）。
7. **旧 Live 叙事残留**：`backtests/SUMMARY.md` 头部"实盘默认=机会双子星 v3.1 clip4"、`state-bucket-algo-2026-08-31.md` §3.0/§7.7、`pick-strong-track.md` §2.1——靠横幅纠偏，引用前先看日期。
8. **14:30 闸的样本口径（OPT-224，2026-09-17 修复）**：`bar_5min` 是**定向存储**（18:40 job 只拉当日缺口股 + paper 持仓），所以 14:30 现场面板的快照现在会落库（`live_1430`）供回放用；回放的 `_breadth_at_1430` 有覆盖率守门（<20% 当日票数 → 退全市场收盘广度），`derived_1500_marks` 补全市场 raw 15:00 mark。**不要**再让任何调用方采信薄样本闸（历史冻结窗与 09-10 及以前全市场覆盖不受影响，09-11..09-17 的 Timeline/holdout 行是回退口径）。

## §8 文档全丢时的重建路径

1. **参数**：`strategy-params.md` §1（S-3）+ 本文件 §0（引擎/卫星/停车/成本）+ §1-§5、§5b/§5c（每档配方）。
2. **代码真值坐标**：`run_walk_forward.py` `S3_CONFIG`/`HK_S3_CONFIG` → `backtest_engine.py` → `harbor.py`/`homeport.py`/`starport.py`/`state_bucket_track.py`/`parking_sleeve.py`/`sleeve_paper_auto.py`/`multi_asset_sleeve.py`。
3. **验证**：`scripts/run_walk_forward.py`（三窗）→ `scripts/verify_harbor_live_vs_backtest.py`（必须 100%）→ `pytest`（`test_harbor*`/`test_sleeve*`/`test_satellite*`/`test_engine*`）→ `scripts/db_rows_baseline.py check`。
4. **口径**：S-3 闸冻结在 `docs/modules/strategy-params.md` §1；策略裁决历史在 `docs/backtests/SUMMARY.md`；改动先预注册。

---

*维护：改任一机制的生效值 → 更新本文件对应小节；被拒变体不要回写进"现行"列。*
