# Karios 星舰 B 审计 C：数据质量与交易现实

> 只读审计，未改 karios-desktop / karios-research 任何代码、配置、数据库与实盘设置；未下单、未连券商。
> DB 均为只读查询（`default_transaction_read_only=on`），无导出大表，无临时表写入。
> 未使用 Crimson 代码/数据，未开浏览器，未读任何 .png/.jpg。
> 视角：试图推翻星舰 B 的对手审计员。每条结论附证据；不确定的写不确定。

## 0. 审计对象速览（证据链）

- 星舰 B 定义：卫星腿（冻结 `HABIT_RECIPE`：4 槽×25%，gap>3%，`amp_1430` 升序取前 1/3，`gate_1430`，C1 3%，body=3 第 3 日 14:30 出）+ 闲置现金 `w_t=cashShare(T−1)` × 100% {国债 511260 + 黄金 518880 + 纳指 513100} 逆波动率（60d 月频，5bp/边）。证据：`docs/modules/strategy-recipes.md:52-64,160-166`；`services/data-sync-service/src/data_sync_service/service/state_bucket_track.py:79-104`；`service/homeport.py:157-213`。
- post-dense 真值（唯一现状口径，2026-09-28 dense `amp_1430` re-baseline 后）：OOS2 **+221.9/−4.8/6.99**、train **+71.4/−4.5/6.15**、valid **+5.9/−10.0/0.83**、long **+729.9/−5.5/4.15**、stress(22–23) **+114.7/−7.8/2.78**、holdout(2026-08-08 起) **−19.8**。证据：`docs/backtests/rebaseline-dense-2026-09-29.md:18-31`。2026-09-28 前旧值（long +669.6/SR 3.90、stress +124.0 等）全部作废，证据同档 §2 作废清单。
- 身份：研究/展示/人工操作默认档；**Live 仍=港湾**，星舰 B（含 H2-a25）进 Live 前置为 paper + 用户风险授权（未满）。证据：`AGENTS.md:205-212`；`docs/modules/strategy-recipes.md:87-102`；`app_settings.strategy_mode='starship_b'`（DB 只读查询，2026-10-02）。
- karios-research：与 karios-desktop 同源镜像（AGENTS/README/docs 结构一致），`research/` 下为指标序列等外围研究；未发现星舰 B 独立的实盘/数据源（grep starship/星舰仅命中镜像文档与 OPT 记录）。结论：星舰 B 真值仍以 karios-desktop 为准，research 无增量证据。

## 1. 问题清单（按严重程度排序，每条附对收益的估计影响）

### P0-1 前瞻段本身已转弱：valid +5.9 / holdout −19.8，用户最初 4 笔卫星实盘全亏、均 −5.6%（最严重：不是 bug，是现实）

- 证据：
  - `docs/backtests/stable/starship-b-oos-decomp-2026-09-29.md:22-36,67-86`：收益高度集中于 2024（+200.0）与 2025（+141.8）；2026 YTD +37.7 但段内回撤 −10.1；valid +5.9/SR 0.83；holdout 35 天 −19.8/SR −4.97；2026Q2 +3.1、2026Q3 +1.9 基本走平；2023Q3 甚至为 −3.0。
  - 引擎为算术净值 `satNav = 1 + realized + MTM`（`state_bucket_track.py:1318`，转引自 rebaseline §0），各段独立重跑、不可切片复利——long +729.9 不等于"每年可拿"，2024–2025 两年是主体。
  - 用户真实卫星 journal（只读 `user_trades`，leg='satellite'，2026-09-22~29，全部 strategy_mode='starship_b'）：4 笔已平仓全部亏损——603019 84.77→82.16（−3.08%）、300990 108.56→104.53（−3.71%）、002982 12.54→12.42（−0.96%）、603125 24.54→20.94（−14.67%）。均值 **−5.60% 毛/笔，扣 32.28bp 后约 −5.93%/笔**。n=4，样本小但方向与 holdout 一致。
- 对收益的估计影响：**headline +729.9 对前瞻无指导意义**。若以后视段为镜，策略当前处于"valid 微弱 + holdout 大亏"区；任何"按 long 年化外推"的实盘期望都应作废。定量：holdout 35 天 −19.8% 已是实际发生的样本外−相对回撤（post-dense 口径），不是假设。
- 反方自检：OOS 拆解同时证明"不是单个窗口撑起"（6 个历年全正，后 2 年贡献大于前 3 年），见 oos-decomp §4.1。转弱≠证伪，但"引用时不要只引 long"（rebaseline §1 结论行）。

### P0-2 成本与滑点：回测用静态 32.28bp/笔，live 超支 15bp 即吃掉约 −40pt long；卫星腿未接 1-tick 下限，低价股成本被系统性低估

- 证据：
  - 成本单源 `service/paper_cost_model.py:86-120,185-208`：CN 静态往返 32.28bp（万3×2 + 过户 0.1bp×2 + 经手/证管 0.541bp×2 + 卖出印花 5bp + 滑点 10bp×2 下限）；1-tick 下限 `max(base_bps, tick/price)` 仅"引擎（S-3）与 Live paper 按成交价套"，**卫星/展示仍用静态 base**（`strategy-recipes.md:74-85` §0.4 注明"卫星腿未套 1-tick 下限"）。
  - 成本敏感性（卫星 standalone long，pre-dense）：30bps +474.4 / 45bps +434.3 / 60bps +394.2 / 90bps +313.9，即 **每 +15bps ≈ −40pt long**。证据：`docs/backtests/stable/starship-b-loss-analysis-2026-09-24.md:308-313` §7 C1（方法见 `scripts/audit_sat_execution.py:57-69`）。
  - 入场在 14:30 print，高溢价：14:30/今开溢价中位 4.65%（同上 C1 行；定义见 `audit_sat_execution.py:91-139`）。
  - 最低佣金按 `REFERENCE_CAPITAL=¥1M × 10%` 折算不触发（`paper_cost_model.py:42-49`）——**10 万本金以下小账户会触发¥5/笔下限**，模型未覆盖，方向为低估小账户成本。
- 对收益的估计影响：**live 若比模型多付 15bp/笔往返（如冲击、追价、网络延迟），long −40pt（约 −5.5% 相对）；多付 60bp 则 −160pt（约 −22% 相对）**。低价股（<¥10，tick 0.01 即 >10bp/边）方向同为低估，量级不确定（低价 fills 占比未在本次只读范围内统计，记为不确定，上界估算方法见附录 §5）。小本金（≤10 万）另加最低佣金残差。

### P0-3 最小交易单位 100 股：回测按名义权重分数股成交，高价股在小本金下根本买不进

- 证据：
  - 引擎按 `position_pct=0.25` 名义权重记账（`state_bucket_track.py:36-42,1318-1342`），全链无整数手取整（grep lot/shares/100股在引擎与 paper 模型中零命中；整数手仅存在于展示层下单辅助，不在回测路径）。
  - paper 实际持有反例（只读 `paper_trades`，twin_star 2026-09-07~10）：CN:688766 @397.29、CN:301666 @420.85——100 股即 ¥39,729/¥42,085；若本金 ¥100k、槽位 25%=¥25,000，**连 1 手都买不起**，回测却按 25% 权重全额计入。
- 对收益的估计影响：10 万本金下，高价信号笔（>¥250 即 1 手超槽位）直接缺席，单笔权重误差可达 100%（有/无）；50 万本金下误差收敛到约半手/槽位（≈2–5% 权重误差/笔）。方向双向（错过赢家也错过亏货，如 loss-analysis §4 最差单笔 688198 −5.96%），但**小本金复制精度显著低于回测假设**。10 万/50 万/200 万分档见 P1-1。

### P1-1 资金容量：100 万轻松、500 万可用、2000 万过大；冲击成本 >¥5M 明确未建模

- 证据：loss-analysis §7 C1 行——100 万规模单槽占 ADV 中位 0.29%（轻松）；500 万中位 1.44%；2000 万中位 5.7%、p90 达 0.77（过大）。`paper_cost_model.py:22` 明确"未建模 >¥5M 冲击成本、HK FX、电话佣金"。
- 对收益的估计影响：**10 万/50 万：容量折价≈0；200 万：p90 参与度已需警惕，冲击视个股流动性扣减（未建模，记不确定）；2000 万：不可照搬，回测收益不可实现**。卫星腿平均 60 日均额门槛仅 S-3 有 0.7 亿（`S3_CONFIG min_avg_amount=0.7`），卫星腿本身无流动性门（见 §0.2），容量约束全靠事后审计而非事前过滤。

### P1-2 星舰 B 没有系统化 paper 账本：实盘损耗无法量化本身就是最大风险之一

- 证据：
  - `strategy-recipes.md:197` §7.3："星舰 paper 20 笔前置没有落账代码（`paper_trades` 无 satellite source）"；`docs/optimization-checklist.md:80` OPT-228 `[ ]` 未做（卫星 forward paper 账本缺失）。
  - 只读计数：`paper_trades` 61 行中 satellite/starship 来源 **0 行**（S3HK 50、twin_star 8、S3 2、ALPHA 1）；twin_star 8 笔 2026-09-07~10 全 `body_exit`、7/8 亏（明细：−1.86/−1.28/−5.89/−1.57/−1.73/−6.50/−5.66/+1.02，均值约 −2.93%/笔，n=8 噪声大）。
  - 用户卫星 journal（OPT-239 接入，inception 2026-09-18）：4 笔已平仓（上文 P0-1），前置约 4/20，远未达标。
  - S-3 系 recon：HK valid 对齐笔 paper 比回测 −0.32/−0.33/−1.58%（`backtest_paper_recon` id 6/7 只读）；CN recon 无对齐（0/0）。
- 对收益的估计影响：**星舰 B 的"回测→实盘损耗"目前无系统数据，只能拼凑**：同 recipe 的 twin_star paper 小样本约 −3%/笔（n=8，不确定）；HK S-3 每笔 −0.3~−1.6%；港湾决策级 100% 对账但 NAV 级用 prev 收盘代理（见 P2-7）。在 paper 20 笔前置完成前，任何 live 收益承诺都是无本之木。

### P1-3 跌停卖出假设：冻结口径默认跌停可卖（`exit_skip_limit_down=False`），尾部会高估

- 证据：`state_bucket_track.py:353-363,820,1011-1031`——`_limit_down_locked_px` 存在但"仅当 `exit_skip_limit_down` 开启才跳过"，默认 False；HABIT_RECIPE 未设置该键（同文件 79-91），故冻结数假设跌停 print 可卖出。代码注释自带实例：2026-09-28 的 603125（正是用户 journal 里 −14.67% 的那只）。
- 对收益的估计影响：日常小（ audit 历史：近涨停仅 2 笔量级；跌停 occurrence 有计数但冻结文档未披露总数，记不确定），**极端周（如 2026-09-28 前后）单笔可差 −10% 级**。方向为高估组合收益，频率低、幅度大。建议（仅审计意见，不改代码）：未来敏感性可开一次 `exit_skip_limit_down=True` 重跑 long/stress 看 Δ。

### P1-4 历史费率静态化： whole-window 用现行 5bp 卖出印花 + 万3佣金，早段小幅高估

- 证据：`paper_cost_model.py:13,86-99`——CN 卖出印花 5bp（注"since 2023-08"，即承认历史为 10bp）却全窗口统一 5bp；佣金用万3（2026-09-21 由万2.5 上调，`strategy-params.md:163` 版本行）。无时间分段费率表（grep 全仓无按日期切换费率逻辑）。
- 对收益的估计影响：粗上界——若约 400 笔卖出落在 2023-08-24 前，每笔多付 5bp × 25% 槽位 ≈ **+0.5pt long 高估**（假设见附录；blotter 日期分布未逐笔核对，记为上界估计）。佣金万2.5→万3方向相反（早段多计约 1bp/边），部分对冲。**合计对 +729.9 的影响 <1pt，可忽略**，但方法论上"历史费率变化"项应记为未建模。

### P2-1 幸存者偏差：默认剔除 ever-delisted（有偏），但已量化且方向反直觉，ST 排除正确

- 证据：
  - `_universe_where`（`state_bucket_track.py:107-124`）默认 `sb.delist_date IS NULL`（用当前快照剔除 ever-delisted → 幸存者偏差，docstring 自认 "survivor bias"），PIT 正确开关 `include_listed_history` 存在但默认关（=冻结口径）。
  - DB 只读：`stock_basic` 10649 行中 delisted 23（CN 退市在列如 600193 退市创兴、605081 退市太和）；`daily` 2399 万行零重复键。
  - 项目已量化（`docs/optimization-checklist.md:64` OPT-212 ③）：纳入退市票上市段 habit long **+24.6pt**（OOS2 +4.5/train +8.3/valid +0.9），"方向反直觉是组合效应"，不翻转结论；ST 纳入则跑出 **+30pt 幻觉**（5% 板在 10/20% 模型下零振幅霸榜、不可成交）——反证 ST 必须继续排除，真修正需时点 ST + 5% 板建模（另起 OPT）。
- 对收益的估计影响：当前冻结 long +729.9 相对 PIT 正确口径**约 −25pt（偏保守约 −3.4% 相对）**，三窗合计约 −13.7pt。ST 侧若有人误纳入则 **+30pt 幻觉**，所幸冻结口径已排除（卫星 `_universe_where` 默认 `sb.name NOT LIKE '%ST%'`；DB 现存 ST 211 只）。

### P2-2 新股/次新股无过滤：2322 只 2021-08 后上市进入宇宙，上市首 5 日无涨跌幅未建模

- 证据：DB 只读 `stock_basic WHERE list_date>='2021-08-01'` CN（ex HK/BJ）**2322 只**；引擎宇宙无 `list_date` 上市年龄过滤（grep `list_date` 在 `state_bucket_track.py`/`backtest_engine.py` 零命中）；涨停模型仅 10/20/30% 三档（`backtest_engine.py:94-105`），注册制下主板上市前 5 日无涨跌幅未建模。
- 对收益的估计影响：**不确定**。gap>3% 天然偏好上市初期剧烈波动票；首日/前 5 日效应可正可负。未逐笔核对 fills 中上市<60 天占比（记不确定）。对手方注记：若次新贡献为正，剔除次新即砍收益；若为噪音，则引入不可复制（上市首日散户买不到足量）风险。需一次 fills×list_date 分布诊断（只读，未做）。

### P2-3 停牌/缺失与异常值：重复 0，主板 >21% 跳变 102 笔，零量行主要在 HK，卫星缺行即跳过

- 证据（DB 只读）：
  - `daily` 总 23,994,624 行（1998-06-01~2026-10-02），(ts_code,trade_date) 重复 **0**；2021-08 后 11,038,311 行中 vol=0 或 NULL 641,898、amount=0 或 NULL 657,576——抽样 2026 年零量行皆为 HK（如 02362.HK），系 HK vol 缺失而非停牌。
  - 主板名义（60/00 开头）`|ret|>21%` 的日 bar **102 笔**（2021-08 起）——超出 10% 板的跳变，应为停牌复牌/除权残留/数据口径 mix，占比极小（102 / 百万级）。
  - 覆盖率：近期每日 CN 行 7690~7720（2026-09-23~30），2024 起每日 min/max 6540/7719，无整日缺数。
  - ST 5% 未建模（`backtest_engine.py:94-96` 注释"ST 5% is not modeled (no ST flag in the daily table)"），但卫星宇宙已剔 ST（P2-1），风险敞口仅 S-3 侧（见下）。
- 对收益的估计影响：停牌日无 bar → 候选自然跳过（无幻觉买入）；持仓停牌期间 MTM 取不到价按权重冻结（`_nav_for_day` ratio 1.0 fallback，`backtest_engine.py:1117-1126`），方向中性偏保守。**合计影响 <1pt**（不确定：卫星停牌票 body 到期 fallback 用收盘价的细节未逐行核对）。

### P2-4 复权方式：qfq 两次全量重灌已固化，ETF raw+锚点拼接 bug 已修，DB 的 513100 截断不影响生产口径

- 证据：
  - CN/HK qfq 全量重灌（2026-08-10/11，341 万行/317.7 万行，0 失败）+ 每日增量同口径，见 `strategy-params.md:147-150` 版本行；`daily` CN 股票 `adj_factor` 全有（0/3/6 开头零 NULL，DB 只读），ETF（5/1 开头）全 NULL = raw（符合 AGENTS "daily 存 qfq、ETF 存 raw"口径）。
  - 2026-09-25 loader 事件：研究脚本 DB-only 取 513100（DB 只到 2023-01，本次只读复核 908 行、2023-01-03 起、adj NULL）导致 base long 假成 +644.0；生产 `load_starship_b_closes()`（复权面板 + DB 尾）复跑 +669.6（当时），教训已沉淀为"停放腿必须生产同源 loader"（`starship-b-loss-analysis-2026-09-24.md:14-22`；`eval_starship_b.py:54-80,112-120` 注释钉死）。
  - ETF 尾部拼接基期断裂 bug（raw 接复权致 513100 −80% 幻觉）已修：按锚点缩放（`docs/optimization-checklist.md` OPT-218 ②）。
  - qfq 依赖未来分红→冻结窗可漂移，已声明为数据版本事项 L5（`lookahead-inventory-2026-09-24.md: B7` 行）。
- 对收益的估计影响：**当前数字无复权口径误差**（生产/回测同源已验证）。残余风险仅 qfq 历史漂移（分红口径变化时冻结窗小数点后漂移，<0.5pt 量级，记不确定）。

### P2-5 时区/交易日历：双市场并集污染已修（OPT-183），卫星固定 CN-only，近期覆盖完整

- 证据：`backtest_engine.py:946-973` 按 `market` 过滤日历（CN 取 `NOT LIKE '%.HK'`）；卫星 `_load_calendar` 取 CN-only（`state_bucket_track.py:204-240` 及 OPT-183 行）；`trade_calendar` SSE 2005-01-01~2026-10-27 共 7970 天；2024-01-01~2026-09-30 开市 666 天；调度均为 Asia/Shanghai（AGENTS 调度表）。
- 对收益的估计影响：修复前幻影交易日曾致港湾三窗虚增减（audit-live 档修②：2025-10-02/03/06/08 等假期强制清仓又追回），**当前口径干净**。残余：`trade_calendar` 只有 SSE 单表，HK 节假日靠 daily 行反推（卫星 CN-only 不受影响）。

### P2-6 涨停买不进：已建模（10/20/30% + 信号日基准 + 40bp 锁），卫星 audit 零一字；S-3 侧 ST 缺口是小洞

- 证据：
  - S-3：`_board_limit_pct/_at_limit`（`backtest_engine.py:94-141`），`base_day` 取信号日（2026-09-17 审计修），涨停阻入场、跌停顺延出场。
  - 卫星：`skip_t1_limit`（`_limit_locked_px` 限幅−40bp，`state_bucket_track.py:346-350,393-394`）+ C1（14:30/open−1>3% 跳过）+ 缺 print 当日不成交（fail-closed，`strategy-params.md:307-318` fail-open 表 #4）。
  - 实证：1084 fills rangeBad 0、一字板 0、近涨停 2（loss-analysis §7 C1）。
  - 小洞：ST 5% 板两引擎均不建模；卫星靠剔 ST 免疫，**S-3 的 `exclude_st` 默认 False 且 `S3_CONFIG` 未设置**（`backtest_engine.py:364`；`run_walk_forward.py` 无 exclude_st 行），ST 票若进入 S-3 池将按 10% 板误判。paper  overlap 0 ST（DB 只读），实际触发概率低。
- 对收益的估计影响：卫星腿涨停约束**已充分建模，影响≈0**（audit 支撑）。S-3 侧 ST 洞对星舰 B 合成无直接影响（星舰 B 不含 S-3 腿），仅 H-STB-COMBINE 类 S-3 混配实验的远端风险。

### P2-7 成交价假设：卫星 14:30 print、S-3 次日开盘、港湾停车 T+1 开盘；回测 NAV 用 prev 收盘代理，隔夜差未建模

- 证据：
  - 卫星 habit：14:30 raw print 买入，无 print 当日不成交；第 3 日 14:30 print 卖，缺 print 回退收盘（`strategy-recipes.md:52-64`；`state_bucket_track.py:1011-1041`；fail-open 表 #4/#5）。
  - S-3：`entry_mode=next_open`（T 收盘信号→T+1 开盘），中位差≈信号日收盘 0.00%（`backtests/core/core-entry-execution-2026-09-10.md`，SUMMARY 行引用）。
  - 港湾停车 Live 为 T 收盘信号→T+1 开盘，而回测 NAV 以 prev 收盘代理（`audit-live-vs-backtest-2026-09-13.md:36-38` 残余项明示"隔夜差未建模"）。
  - 星舰合成层转移成本 5bp×|Δw|（`eval_starship_b.py:84-109 blend_b` 月频；`_compose` 5.0bp）。
- 对收益的估计影响：14:30 print 非 VWAP、非收盘——大额下单按 print 全额成交假设在 500 万内可接受（P1-1），2000 万则 print 本身会被吃掉（记不确定）。港湾腿隔夜差：S-3 中位 0.00% 证据下≈0；卫星 14:30→15:00 mark 差未单独披露，隐含在 fills 实现里。**合计小个位数 pt（不确定）**。

### P3-1 口径陷阱（对手方特别提醒）：算术净值 + dense 重定基，旧数字仍在外流传

- 证据：oos-decomp §0（算术净值不可对 long 切片；连续 long 切出 OOS2 +61% vs 独立重跑 +200%）；rebaseline §2 作废清单（catalog/UI/测试已更新，`designs/*-2026-09-2{4,6}*` 预注册与 `archive/*` 冻结为历史不回写；starport/twin_star 未复跑仍标 pre-dense）。
- 对收益的估计影响：误读口径可造出 **3× 级幻觉**（61% vs 200% 例）。本审计一律用 post-dense 独立重跑数；凡引用 2026-09-28 前卫星系数字的外部材料一律视为作废。

## 2. 回测 vs 实盘对照（同一交易日成交差异，量化实盘损耗）

| 对照项 | 回测 | 实盘/ paper | 差异（损耗） | 证据 |
|---|---|---|---|---|
| 港湾停车决策 | Timeline pick | Live 决策链 PIT 重放 | **100%（473/473 三窗）**，决策零损耗；NAV 级隔夜差未建模 | `audit-live-vs-backtest-2026-09-13.md:5-21,36-38` |
| 星舰 B 卫星腿 | habit replay fills | 系统 paper **无账本**（OPT-228 未做；`paper_trades` satellite 0 行） | **无法量化**；代理样本：twin_star 同 recipe 8 笔均值约 −2.93%/笔（n=8）；用户 journal 4 笔 −5.60% 毛/−5.93% 净（n=4，2026-09-22~29，含 603125 −14.67% 跌停实例） | DB 只读计数 + §1 P1-2 |
| S-3 HK | walk-forward | paper recon 对齐笔 | **−0.32/−0.33/−1.58% 每笔**（2 笔样本）；CN 对齐 0 笔 | `backtest_paper_recon` id 6/7 只读 |
| 成本模型 | 静态 32.28bp | 真实佣金/印花/滑点 | 敏感性 +15bp→−40pt long（§1 P0-2）；tick 下限卫星未接 | loss-analysis §7 C1 |
| 前瞻损耗 | valid +5.9 | holdout −19.8（35 天） | **−25.7pt 落差**，策略层面已实现 | rebaseline §1 |

结论：星舰 B 的系统性"实盘损耗"数字目前不存在——不是因为损耗为零，而是因为 paper 前置（20 笔）远未完成。零散代理样本一致指向**弱市段实盘体验差于回测均值**，与 holdout −19.8 同向。

## 3. 数据质量 checklist 逐项结论

| 项 | 结论 | 证据 |
|---|---|---|
| 幸存者偏差（退市） | 默认剔 ever-delisted，小幅保守（long 约 −25pt vs PIT）；ST 排除正确 | P2-1 |
| ST 处理 | 卫星剔除✓；S-3 `exclude_st=False` 小洞（对星舰 B 无直接影响） | P2-1/P2-6 |
| 停牌/缺失 | 缺行跳过、MTM 冻结；重复 0；覆盖完整 | P2-3 |
| 新股 | 无上市年龄过滤，2322 只次新在宇宙，首 5 日无限幅未建模，不确定 | P2-2 |
| 复权 | qfq 重灌 + ETF 锚点修复；生产口径免疫 DB 截断 | P2-4 |
| 异常值 | 主板 >21% 跳变 102 笔（可忽略）；零量主因 HK 缺 vol | P2-3 |
| 重复行 | (ts_code,trade_date) 重复 0 | P2-3 |
| 时区/日历 | OPT-183 已修；SSE 单表够卫星用；近期覆盖全 | P2-5 |
| 涨停买不进 | skip_t1_limit + C1 + 缺 print 不买；1084 fills 零一字 | P2-6 |
| 跌停卖不出 | 默认假设可卖，小尾部高估（603125 实例） | P1-3 |
| 成交价 | 14:30 print / 次日开盘 / 停车 T+1 开盘；NAV prev 收盘代理残余 | P2-7 |
| 费率 | 静态现行费率全窗，历史变化影响 <1pt | P1-4 |
| 滑点 | 10bp/边 + tick 下限（卫星未接后者）；+15bp→−40pt | P0-2 |
| 100 股 | 未建模；小本金高价笔不可执行 | P0-3 |
| 容量 | 100 万 OK / 500 万可用 / 2000 万过大；>¥5M 冲击未建模 | P1-1 |
| T+1 | 卫星 body=3 天然满足；S-3 holding==0 跳过出场（OPT-212） | §1 内文 |

## 4. 赚钱线索（profit-leads 规则执行）

本次审计未发现新的"扣费后每笔正收益且样本外可信、或明显优于随机"线索，不追加 `profit-leads.md`。说明：

- `amp_1430` Q1 +1.09%/74%（`diag_sat_entry`）是真截面信号但为样本内诊断，且引擎收紧回放已 REJECT（long −74pt），见 loss-analysis §3e/§6，不构成新 lead。
- H-STB-COMBINE A4 条件化（同 c +13.5~25.4pt）含真实 timing 但撞择时类、幅度小、任何窗不超基准（`h-starb-combine-2026-09-26.md`），不构成可落地的 lead。
- 用户 journal 4 笔与 twin_star paper 8 笔皆为负样本，无正 lead。

## 5. 附录：只读 SQL 与复现（均未写库、未导出大表）

```sql
-- 宇宙与退市/ST（2026-10-02 只读）
SELECT count(*), count(*) FILTER (WHERE delist_date IS NOT NULL),
       count(*) FILTER (WHERE name LIKE '%ST%') FROM stock_basic;
-- 10649 / 23 / 211
-- daily 范围/重复/零量（2026-10-02 只读）
SELECT min(trade_date), max(trade_date), count(*) FROM daily;  -- 1998-06-01~2026-10-02, 23994624
SELECT count(*) FROM (SELECT ts_code, trade_date, count(*) c FROM daily GROUP BY 1,2 HAVING count(*)>1) t;  -- 0
-- CN 股票 adj 全有、ETF 全 NULL（2026-10-02 只读，2021-08 起）
-- 513100.SH: 908 行，2023-01-03 起，adj NULL（raw）
-- amp_1430: 81608 行，2024-01-02~2026-09-30；bar_5min 2024 起 35,512,080 行（仅计数，未导出）
-- paper/user 计数（2026-10-02 只读）
-- paper_trades 61（S3HK 50 / twin_star 8 / S3 2 / ALPHA 1），satellite 0 行
-- user_trades 67；leg='satellite' 8 行（BUY 4 / SELL 4，2026-09-22~29）
-- backtest_paper_recon 5 行；watchlist_automation_runs 152 行
```

复现（只读，需 Postgres；结果以 post-dense 为准）：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_starship_b.py
PYTHONPATH=src:scripts python3 scripts/decompose_starship_b_oos.py --quarters
PYTHONPATH=src:scripts python3 scripts/audit_sat_execution.py
```

## 6. 总判（对手方一句话）

星舰 B 的数据工程是诚实的（大 bug 都已修并留档、敏感性公开），但 headline +729.9 由 2024–2025 两年驱动、前瞻段（valid +5.9 / holdout −19.8 / 用户前 4 笔 −5.6%/笔）已转弱；执行假设在 50 万内大致成立（容量 OK），小本金受 100 股门槛、大本金受冲击成本、所有规模受滑点超支（+15bp→−40pt）侵蚀；且星舰 B 尚无系统 paper 账，进 Live 前置未满。维持"研究/展示/人工档、不进 Live"的现状判断。

KARIOS AUDIT C DONE
