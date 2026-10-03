# Karios 星舰 B 审计 A：未来函数 / 信息泄漏（只读对手审计，2026-10-03）

## 0. 结论在最前

- **现行冻结口径（HABIT_RECIPE + post-dense amp_1430 + 生产同源 park 面板）下：严重 0 条。**
- **中等 4 条**（M1 宇宙幸存者偏倚仍冻结未修／M2 14:30闸稀疏回退收盘／M3 全样本选参固化／M4 amp采样代理→dense位移）。
- **轻微 4 条**（L1 raw缺失回退qfq／L2 算术板限10/20%+跌停默认成交／L3 研究脚本loader口径不一致残留／L4 qfq/ETF复权快照版本局限）。
- **未发现（通过）6 条**（P1 月频权重因果／P2 cashShare真T-1／P3 B腿无未来argmax／P4 日历前填因果／P5 satellite池非回测未来／P6 14:30主链因果）。
- **历史严重已修 4 条**（H1 全天振幅排序旧键／H2 qfq×raw基期混用／H3 CN/HK并集日历／H4 trail当日收盘记0收益），数字作废，不计入现行。
- 一句话：星舰 B 的 +729.9%/−5.5%/4.15（long，post-dense）不是靠“偷看未来”涨出来的；残留偏差以“幸存者+稀疏回退+选参”为主，方向是抬高绝对收益、不是虚构因果。最近段转弱（valid +5.9/SR0.83、holdout −19.8）是真实前瞻状态，引用只引 long 会误导。

> 口径锚：`docs/modules/strategy-recipes.md:54-64,160-166`；唯一现状 `docs/backtests/rebaseline-dense-2026-09-29.md:3-5,18-25`；四窗 OOS2/train/valid/long +221.9/+71.4/+5.9/+729.9，MDD −5.5 SR4.15 stress +114.7/−7.8 holdout −19.8。2026-09-28 前旧值（+669.6/SR3.90、stress+124.0）全部作废。

审计纪律：只读。未改 karios-desktop / karios-research 任何代码配置DB；未下单未连券商；未用 Crimson 代码数据；未开可见浏览器；未读 .png/.jpg。跑代码均在 `~/Projects/wealth-ideas/karios-audit-2026-10/scratch/`（复制 `homeport.py/harbor.py/eval_starship_b.py` 副本，`e1/e2/e3` 三实验），DB 只做 SELECT（`daily` 23994624 行等计数），导出 <10MB（<3GB 红线）。karios-research 的 `indicator_series` 与星舰 B 无关（见 §8）。

---

## 1. 星舰 B 完整规则（重建用，文件+行号）

| 环节 | 现行定义 | 证据 |
|---|---|---|
| Universe | 全A `daily×stock_basic`，剔 BJ/退市/ST（当前快照） | `strategy-recipes.md:58`；`state_bucket_track.py:107-124` |
| 卫星常量 | POSITION_PCT 0.25／MAX_POS 4／BUCKET_Q 3／BODY 3／R_WIDE 0.5／MIN_GAP 0.03／WARMUP 120／COSTS 32.28bp | `strategy-recipes.md:54`；`state_bucket_track.py:33-52` |
| Recipe | HABIT `{skip_t1_limit,strict,max_pos4,pct0.25,body3,fill_same_1430,fill1430,exit1430,max_open_to_1430 0.03,rank amp_1430,gate1430 True}` | `strategy-recipes.md:56-57` |
| 信号 | S-gap `open/pre_close-1>3%`；`amp=(h-l)/c`；`amp_1430=(1430前最高-最低)/1430价`缺失排最后；桶=振幅升序前1/3低波优先；strict不补位 | `strategy-recipes.md:59` |
| 排序 | `amp_1430` 升序，唯一来源 `amp_1430` 表（dense真≤1430，2026-09-28定案）；`bar_5min` 4点代理仅缺失回退 | `strategy-recipes.md:60`；`state_bucket_track.py:461-495` |
| 买入 | 14:30 raw print 买，无print当日不成交；天然T+1 | `strategy-recipes.md:62`；`state_bucket_track.py:1258-1265`（见子代理复核，行号以 exploration 为准，不确定±5行） |
| 卖出 | 满body=3第3交易日14:30 print卖；日终mark用15:00 raw；单笔扣32.28bp | `strategy-recipes.md:62` |
| 仓位 | 4槽×25% | `strategy-recipes.md:54,57` |
| cashShare | `w_t=cashShare(T-1)` 因果；组合 `r_t=sat_ret+w_t*bRet-5bp*\|Δw\|` | `strategy-recipes.md:144`；`state_bucket_track.py:1655-1657`；`eval_starship_b.py:144-147` |
| 闲置ETF腿 | 100% `{511260国债,518880黄金,513100纳指}` 逆波动率60d月频5bp/边，3腿1/3预热，复用B3数学 `homeport.starship_b_run` | `strategy-recipes.md:162`；`homeport.py:157-213` |
| B3对照 | 5资产 `510300/510500/518880/513100/511260` 60日 `pstdev` `w∝1/σ` 月首调仓月内漂移 | `strategy-recipes.md:121`；`homeport.py:96-143` |
| 费用 | 卫星静态32.28bp；星舰转移5bp×\|Δw\|；B3月调仓5bp×换手；ETF回测5bp/边 | `strategy-recipes.md:74-83`；`homeport.py:129-131,158-179` |
| 涨跌停 | `skip_t1_limit`：14:30 print≥pre_close×(1+限幅-0.004)不买；C1 `14:30/open-1>3%` 跳过 | `strategy-recipes.md:61`；`state_bucket_track.py:346-350,376-394` |
| 复权 | `same_1430` 统一raw：`_mark` 优先raw1500缺才退qfq；`_raw_ratio=raw1500/qfq_close` 把open/pre缩到raw做C1/涨停；广度MA前19根也用raw | `audit-three-strategy-lookahead-2026-09-14.md:72-76`；`state_bucket_track.py:952-965` |
| 闸 | `breadth_1430>0.5`（14:30 print站上各自MA20占比）；关=当日不开新仓，已有照body出 | `strategy-recipes.md:63`；`state_bucket_track.py:509-558,977-983` |
| ETF面板 | 展示/研究走 `harbor.load_etf_closes()` CSV close_adj+DB尾anchor拼接；`daily`里ETF raw adj NULL | `strategy-recipes.md:72`；`harbor.py:45-126` |

数据来源：`daily`（qfq，23994624行，1998-06-01..2026-10-02）×`stock_basic`（10649）×`bar_5min`（58821148行，2021-01-04..2026-09-30，≤1430部分18708676行/1392天）×`amp_1430`（81608行，2024-01-02..2026-09-30，666天，5027名）×`data/etf/etf_daily.csv`（月快照，4.3M，面板三腿均2021-01-04起）。回测方式：卫星 `replay_sgap_from_context(**HABIT)` 算术净值 `satNav=1+realized+MTM`（不能切片取比，逐期独立重跑，`rebaseline:14`），park腿 `starship_b_run` 月频因果，再 `compose_parked_rows` 以真实现金权重合成。实盘记录：Live仍=港湾，星舰B为研究/展示/人工默认档，进Live前置 paper 3/20+风险授权（未满），`strategy-recipes.md:101-102`。

历史修正三件套：① 2026-09-25 loader修正（DB-only丢513100 2021-22史，base long +644.0→+669.6 pre-dense，stress +124.0→+114.7，`archive/2026-09-25-starship-b-data-corrections.md:11-25`）；② B22左尾解剖（亏损=卫星3日左尾477/1080全body_exit，S-gap挡跌=砍反弹，维持原配方，`archive/2026-09-24-starship-b-loss-anatomy.md:19-32`）；③ 2026-09-29 dense rebaseline（4点代理→真≤1430，long +669.6/SR3.90→+729.9/4.15，train +17.9、OOS2 −7.0但SR+0.40，`rebaseline:18-25,27-31`）。

---

## 2. 历史严重已修（作废，不计现行，但审计必须列）

### H1 当日close/high/low决定当日买入排序 ——历史严重，现冻结无
- 证据：旧键 `state_bucket_track.py:1092-1093 ranked=sorted(gap, key=amp)` 其中 `amp=(high-low)/close` 来自 `_day_features:289-291` 当日日线（15:00才知道）；`scripts/compare_sat_rank_1430.py:4-6` 原文“FULL-DAY amplitude (known only at 15:00) — lookahead”；现 `eval_starship_b/decompose/eval_sat_*` 全部显式 `rank_key="amp_1430",gate_1430=True`（如 `eval_sat_idle_parking.py:80-81`），`replay:929-936` 对 `same_1430+rank None/gate False` 打 OLD look-ahead 警告，需 `allow_lookahead=True` 才放行。
- 修前修后：属旧臂；现 habit 键零前视。引用旧 `stage/rank None` 收益必须换 `stage_1430/amp_1430` 重跑（`state_bucket_track.py:1096-1122` stage含决策日close，stage_1430才是prior+print零前视）。

### H2 qfq×raw基期混用 ——历史严重，现主链已修
- 证据：`first-principles-2026-09-05.md:128` “qfq永远不能直接对raw限价做等值比较，必须 raw=qfq×adj_latest/adj 重建”；`audit-three-strategy-2026-09-14.md §E` daily 09-11重灌qfq vs bar_5min raw致假亏损，`_mark/_raw_ratio` 修复（`state_bucket_track.py:952-965`，`satellite_signals.py:67-78`同式，比值同日adj抵消原理对）。
- 修前修后：V0 +563.1%→V4 +463.6%旧假象消除（`audit-three §E`，以文档为准）；现主链无，缺数日回退见L1。

### H3 CN/HK并集日历污染 ——历史严重，现已修
- 证据：`lookahead-inventory-2026-09-24.md:17` OPT-183 `backtest_engine._load_calendar` daily CN/HK同表并集污染；修复按market过滤，sat固定CN-only（`state_bucket_track.py:107-124`已有 `.BJ/.HK` 显式过滤）。文档数：S-3 +46.5/+34.4/+38.7 long+94.5→+38.0/+39.2/+38.7 long+82.7等（`audit-three:94-117`，引用档值）。

### H4 trail/同日进出/回填覆盖 ——历史严重，现已修
- 证据：`lookahead-inventory:15,18-20` OPT-177 trail当日收盘触发记0收益改t-1；OPT-212 同日进出幽灵交易+首日吞收益+5min回填覆盖live（改holding==0跳过+MTM at-cost+`on_conflict nothing`）；OPT-224薄样本闸（见M2）；B1 `portfolio_nav_sim:168-190` 同日收盘定持仓记当日收益已修+幽灵探针；B6 `ext_minute_csv:246` 回填update改nothing。

---

## 3. 中等（现存，影响绝对数，需声明+敏感性）

### M1 成分/universe用今天名单回看 ——中等（已声明未修，冻结值）
- 证据：`state_bucket_track.py:107-124 _universe_where` + `127-146` docstring原文“OPT-211 P3: exclusions read CURRENT snapshot (survivor bias — delisted-while-listed history never counts). include_st/include_listed_history (both default False=frozen)”；`sat/sat-score-segment-2026-09-08.md:25` D4原文“回填宇宙=registry未来热门+daily存活全集，退市票永不计入——长窗天然幸存者偏倚”。
- 量化（本审计只读SQL，2026-10-03）：
  ```sql
  SELECT count(*) FROM stock_basic; -- 10649
  SELECT count(*) FROM stock_basic WHERE delist_date IS NOT NULL; -- 23
  SELECT count(*) FROM stock_basic WHERE name LIKE '%ST%'; -- 211
  SELECT count(*) FROM daily d JOIN stock_basic sb ON sb.ts_code=d.ts_code WHERE sb.name LIKE '%ST%'; -- 733196 / 23994624 ≈3.1%
  SELECT count(*) FROM daily d JOIN stock_basic sb ON sb.ts_code=d.ts_code WHERE sb.delist_date IS NOT NULL; -- 77752 ≈0.3%
  SELECT count(*) FROM daily WHERE ts_code LIKE '%.BJ'; -- 347370 ≈1.4%（已剔）
  ```
  另 `karios-research/.../BRIEF.md:54-56` 承认2010年DB约1773 vs 实际约2000，缺失退市/合并名，残留偏倚。
- 修掉重跑实验：未做全量重跑（需 `load_sgap_context(include_st=True,include_listed_history=True)` 同habit跑OOS2/train/valid/long，数据量1.68M行量级，内存吃紧分批，本次未跑以免超红线）。预期：3日脉冲单笔影响小，long绝对收益/胜率被抬高，不确定多大，写不确定。优先序最高（§7）。

### M2 14:30闸稀疏回退收盘 ——中等（有意折中，dense后缩小）
- 证据：`state_bucket_track.py:509-558 _breadth_at_1430` 覆盖不足 `tot<0.2*eligible` 时 `return None`（`MIN_BREADTH_COVERAGE=0.2`）；`977-982 if b1430 is not None: breadth=b1430 else: decision_unavailable_days.add(day)` 并保留close-basis breadth做门。close>MA20用15:00收盘，是14:30决策时的未来。AGENTS称“historical parity path, never thin sample”（OPT-224）。
- 量化：`bar_5min` 全市场日约5000名 vs `daily` 日约9000-10000名（本审计抽样 2024-06-03 bar5_1430 5058 vs daily 9122；2026-06-03 5207 vs 10250），覆盖约50-55% >20%守门，故现闸多走14:30；但 `amp_1430` 仅gap名单（抽样日 40/85/89行 vs 5000），long前2.5年（2021-08~2023）零dense行，100%代理/回退时代。`_mark:952-957` 同日MTM用raw1500缺退qfq，仅记账不进决策。
- 修掉重跑实验：未跑全量（改2行：`b1430 is None→当日不开门/跳过` vs 现回退close，跑三窗+long+holdout）。预期dense后差缩小，不确定需实测。`sat-live-replay-amp-drift §1-3` 已证两边都只读≤1430、桶成员约40%不同（代理偏差非前视）。

### M3 参数阈值用全样本统计量定 ——中等（时点无，选择有）
- 证据：`BUCKET_Q=3,MAX_POS=4,POSITION_PCT=0.25,BODY=3,R_WIDE=0.5,C1=0.03` 在 `state_bucket_track.py:36-53,73-91` 为固定常数；`1146-1159` 分桶 `qn=len(ranked)//bucket_q` 是当日截面分位数，非全样本分位数。无“T日用未来均值定阈值”。但 `C1 grid/ampcap bq4/cap1.0%`（`eval_sat_ampcap.py:7,37-54 max_amp_1430_pct=0.010`）是全样本网格选后固化，属选择偏差/过拟合风险，`first-principles §二.4-5` 已列单窗死因。
- 修掉重跑：嵌套式（阈值只在OOS2定锁死看train/valid）或沿 `validation-gates-v2` G1-G3+L重裁，不新调参。本次未新调参，不确定具体阈值在哪窗定的，写不确定。

### M4 amp采样代理非前视但换桶 ——中等边界（已re-baseline，方向已知）
- 证据：`sat/sat-live-replay-amp-drift-2026-09-28.md §1-3` 两边都只读≤1430，桶成员约40%不同、头名94%相同；`bar_5min`多为4点采样，51.5% fill采样窗内全平（proxy amp=0）。`dense-amp-1430-2026-09-28.md:32-40` 单变量A/B：OOS2 −7.0/SR+0.40/DD−0.2、train +17.9/DD−4.0/SR+1.96、valid +1.4/DD−2.2/SR+0.55、past_year +22.4、aligned +21.1、long2y +11.9，6/6 DD降SR升，PASS采纳。`_load_bar5_hl:461-495` 以amp_1430为准bar_5min仅回退。
- 修前修后差：即上表（pre-dense 4点 vs dense真≤1430）。星舰B long +669.6→+729.9（`rebaseline:20-25`）含此位移+loader修正叠加，不可单归因，写不确定拆分。

---

## 4. 轻微（现存，需声明，不推翻结论）

### L1 qfq/raw缺失回退 ——轻微
- 证据：`_raw_ratio` 缺任一侧即None→不缩放（`959-965`），`_mark` raw1500缺即退qfq（`952-957`），广度MA prior缺raw即退qfq（`544-547`）。早期 `derived_1500_marks` 覆盖不确定。`hotmoney_lib.py:12-29 latest_adj/raw_price` 只用于游资解剖，不必硬套卫星（等价逻辑已在卫星内）。
- 实验：`audit_sat_execution.py` 已有 `skipNoPrint/limitDownExitDays` 分布；敏感性应跑“强制有raw才成交/记账”，本次未跑，写不确定。

### L2 板限/一字/跌停成交假设 ——轻微
- 证据：卫星不读 `stk_limit` 表（本机无 `public.stk_limit`，`to_regclass` NULL，算术板）；`_limit_locked_px:346-350 px>=pre_close*(1+lim-0.004)`，`lim=0.20 if prefix(3,68) else 0.10`；`_t1_limit_locked:323-343` 同限T-1收盘判可执行；`same_1430:1277-1288 one_word=False` 只看print≥限幅-0.004跳过，不用日线high==low（避免全天信息），正确；`same_close/next_open` 才用high==low==close/open（成交时刻已知）。出场跌停 `1022-1025` 仅 `exit_skip_limit_down=True` 才顺延，默认False=跌停print照成交但 `limitDownExitDays` 始终审计；`sat-live-replay §7.3` 实测2年521笔收盘跌停0笔，trailing audit=1（603125 09-28）。
- 缺口：只有10/20%两档，无ST 5%（universe已剔今ST，历史ST整段剔除而非按日5%建模，见M1）、无BJ 30%（已剔）。预期影响小，写不确定。
- 修掉重跑：`exit_skip_limit_down True vs False` 重跑long/valid即可，本次未跑。

### L3 loader口径不一致残留 ——轻微（影响复现，不构成B前视利好）
- 证据：`harbor.load_etf_closes/homeport.load_risk_closes` 经 `merge_recent_db_closes`（`harbor.py:45-126`，只追加CSV末日后DB行，冻结不动，按anchor缩放，ETF adj NULL走重叠比，无重叠factor1.0告警）。不一致残留：`eval_twin_star_parking:59-75`、`eval_etf_parking_baseline:54-70`、`eval_etf_benchmark_parking:56-72`、`eval_harbor_riskbudget:54-70` 直接读CSV未调merge（窗口止于CSV末日前不受影响，近期窗滞后达1月）；`eval_harbor_b3_parking:68-69` 混用CSV定cal+合并尾算B3，把DB尾日期丢掉。`eval_starship_b:54-80` 已自首DB-only缺史并切 `load_park_panel`（`54-80,118-120`）。
- 本审计实验E2（scratch/e2_loader.py，只读）：面板三腿均1393/1392行2021-01-04..2026-09-30；DB-only 513100仅908行2023-01-03起；独立park腿full 49.16%，common cal面板46.28% vs raw44.32%差1.96pt。方向与2026-09-25修正一致（long understated，非前视利好）。

### L4 复权快照版本局限 ——轻微（方法论，需声明）
- 证据：`data/etf/etf_daily.csv` 样例 `close_adj=close*adj_factor`（如510050 adj1.4478）；月度快照把截至快照日未来分红回溯进历史，属qfq总收益标准做法，日收益不变（anchor线性相消，`harbor.py:97-100`），权重 `w∝1/σ` 不变，仅绝对价/图表基期动。`rebuild_cn_daily_qfq:3` 已声明qfq未来函数（ledger B7），ETF同理应声明为已知局限。`merge anchor` 尾部红利除权日会比历史多记一次价跌（现金红利未补），三腿分红频率不确定，预期年化<0.1%未实测，写不确定。
- 实验：取两版不同月份CSV重算权重L1，若无归档则在复现节补“快照版本相关，frozen窗以台账为准”。本次无旧归档，未跑。

---

## 5. 未发现（通过，有证据）

### P1 月频权重只用过去 ——通过
- 证据：`homeport.py:87-93 _vol_at range(max(1,i-lookback),i)` 只到i-1；`178-185 starship_b_run /96-119 risk_budget_run` 月首权重由上月末及以前60d算出；`195-212` NAV先用旧权赚prev→day再切新权下一日生效。
- 实验E1（scratch/e1_causality.py，纯合成）：Feb-01权重基线vs注入Feb-10 +20%未来尖峰，L1=0.0000000000，PASS因果。E2源码断言 `range(max(1, i - lookback), i)` 排除i，PASS。E4月滞后敏感性：引擎当日收旧权+记turnover，次日才吃新权，与“延迟1日生效”差约1日收益<0.1%/年量级，PASS口径稳定。

### P2 cashShare真T-1 ——通过，附结算假设
- 证据全线 `w[t]=cash[t-1]`：`state_bucket_track.py:1655-1657 if i>0: w=cash[i-1]`；现金定义 `1322-1329 cash=1+realized-len(pos)*clip`，realized含当日closed_today（先算to_close再pop再realized+=，`980-1068`），即cashShare[t-1]含t-1当日14:30出场款，t日开盘前已知可执行；`eval_sat_idle_parking:263-265 w_true=cash[i-1]`；`eval_starship_b:144-147` 同；`eval_sat_park_mix:75-76 w=[0]+cash[:-1]`；`compose_parked_rows:1637-1665` 同。`_true_cash_share:100-135 from_rows=True` 读Timeline四舍五入cashShare与产品round4对齐。
- 实验E3合成（scratch/e1_causality.py §E3）：causal nav1.121554 vs leaky同日nav1.131801 premium0.010246，证同日会虚高，现口径无偷看。假设：14:30卖出款T-1日内可用于T日停车调仓记账（单账户resize，`compose:1637-1642` one-sided正确）；实盘T+1交收冻结另计，不属回测前视。

### P3 B腿无未来argmax，H2因果，benchmark事后行勿引 ——通过/提示
- 证据：星舰B无argmax只有逆波动加权；H2 `harbor.py:313-322,364-367 want=pick_parking(prev)` 信号用prev收盘，`pick:168-242 bisect_right-1定i mom=close/mp[ago] gate MA200 covered<3回REPO`，`held_mom:245-265` 同，`trail8:337-347` 先判prev破峰破则当日REPO不再入。唯一事后排序 `eval_etf_benchmark_parking.py:244-245 best_ts=max(...series[-1]/series[0])` 明确标注事后仅展示未进组合，勿当策略引。

### P4 日历对齐/前填 ——通过（信息级）
- 证据：`homeport._series_on_cal:76-84` 前填、`eval_sat_idle_parking._align_day_nav:148-157`、`eval_starship_b:132-139 calB取PARK_B[0]与卫星日交集再last前填` 都只用≤d过去值，无future fill。`_vol_at` 跳falsy，前填后首段除外无None，节假日平收益轻微压低vol非前视。`blend_b` 用债日历定park日历，债停牌而金/纳指交易则park当日stale一日偏保守。`eval_sat_idle_b3_menu:76-92` 已用merge+因果动态宇宙（≥60 sessions才纳入），long早期等权得当。

### P5 satellite池/compute/refresh ——非回测未来，通过
- 证据：`watchlist_automation:595-621 compute_satellite_pool(day-200重放, decisionAvailable False→None fail-open)`；`1143-1166 refresh_satellite_pool(post_5min,18:40才有当日1430 panel,失败no-op不删)`。这是展示/持仓镜滞后一天（OPT-219/220），不是回测用未来。查 `watchlist_automation_runs(applied非skipped)` 审计链即可。

### P6 14:30主链因果 ——通过
- 证据：`amp_1430: _load_bar5_hl:476-477 trade_time<='1430'`；`backfill_amp_1430:39,113-114 MAX_HHMM 1430跳过`，`_gap_codes:60-69` 用当日open/pre_close>0.03定gap（09:30已知）；成交/出场 `1258-1265 same_1430无print不成交`、`1037-1041 exit1430`；C1/涨停 `1195-1206 px(1430 raw) vs open/pre×_raw_ratio` 因果。

---

## 6. 修掉泄漏重跑小实验总表（本审计实跑 vs 引用档）

| # | 实验 | 修前 | 修后 | Δ | 状态 |
|---|---|---|---|---|---|
| E1合成因果 | 未来尖峰是否改月首权重 | 注入Feb-10+20% | Feb-01权重L1 0.0 | 0 | 本跑PASS（scratch/e1_causality.py） |
| E3合成cash | w=cash[T] vs cash[T-1] | leaky 1.131801 | causal 1.121554 | premium 0.010246（合成例，非实盘） | 本跑，证T-1无偷看 |
| E2 loader | park独立腿 DB-only vs 面板 | rawDB common 44.32% | 面板common 46.28% | +1.96pt；full面板49.16%（含2021-22） | 本跑只读（scratch/e2_loader.py），方向复现2026-09-25修正（base long +644.0→+669.6 pre-dense，stress124.0→114.7） |
| M4 dense | 4点代理 vs 真≤1430 | 4点 sat%（OOS2 207.24/train36.19/valid6.50/past48.70/aligned44.36/long2y265.80） | dense（200.22/54.09/7.91/71.07/65.52/277.73） | OOS2−7.0/train+17.9/valid+1.4等，6/6 DD降SR升 | 引用 `dense-amp-1430:32-40`，星舰B long+669.6→+729.9含此位移 |
| M1宇宙 | 当前快照 vs PIT（含ST/退市史） | 冻结（ST 733k行3.1%+退市77k0.3%剔除） | 不确定 | 不确定 | 未跑全量（1.68M行，内存分批，列优先序§7） |
| M2闸回退 | 回退close vs 无覆盖跳过 | 现回退 | 不确定 | 不确定 | 未跑全量，dense后预期缩小 |
| H2基期 | qfq/close混用 vs raw重建 | V0 +563.1% | V4 +463.6% | 假象消除 | 引用 `audit-three §E` |
| H3日历 | 并集 vs market过滤 | S-3 long+94.5等 | +82.7等 | −10pt量级 | 引用 `audit-three §F` |

全量重跑命令（只读，需DB，未跑，留后人）：
```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_starship_b.py
PYTHONPATH=src:scripts python3 scripts/decompose_starship_b_oos.py --quarters
```

---

## 7. 对手视角：最可能推翻星舰B的三刀（都砍不死，但必须说）

1. **holdout/valid已转弱**：post-dense valid +5.9/SR0.83、holdout −19.8（`rebaseline:23-25,31`），OOS拆解2024+200.0/2025+141.8主体、2026Q2+3.1/Q3+1.9、2023Q3−3.0（`oos-decomp:22-29,40-62`）。long +729.9靠2024Q4+70.9等前期，后段+341.2>前段+279.8但valid起衰减。只引long是挑选窗口。
2. **幸存者+回退抬高long**：M1+M2在long前段（无dense、ST/退市多）同时向上偏，绝对值不确定。若M1敏感性跑出−20pt以上，long仍+700%量级不死，但Calmar/SR需重算。
3. **选参前沿滑动**：B22已证所有挡跌/amp上限/早出/混H2/扩池/regime闸都沿收益×风险前沿滑动无免费午餐（`loss-anatomy:32`），A1 m10 PASS但用户不采纳（Calmar9.58→8.71 Sharpe3.90→3.86刚过+牺牲好复制）。星舰B是甜蜜点不是圣杯，降曝露才降回撤。

---

## 8. karios-research 说明（按 prompt 要求已读）

- `~/Projects/karios-research` 现为 karios-desktop 镜像+独立 `research/indicator_series`（单指标视频系列框架），与星舰B引擎无代码复用。`BRIEF.md:8-10` 硬约束“只写本目录，不导入services/apps/packages”；`13-16` DB只读（`default_transaction_read_only=on`，只SELECT/COPY，URL不打日志）；`19` 无前视（T close决策只用≤T bars）；`35-38` raw=`qfq×adj_ref/adj_factor`仅限价用，比率/收益尺度不变因果；`40-56` universe PIT（上市250天+20日均amount≥70000+ST代理+退市整理期15 bars，承认2010年缺失）；`58-69` T close信号→NEXT bar open成交+限价阻塞滚动+T+1；`74` 成本往返0.30%；`184-190` no-lookahead截断+垃圾替换测试。
- 结论：方法论可作参照（T→NEXT open、raw限价、PIT宇宙），但星舰B是14:30 intraday+月频ETF合成，不可直接套用数字。本审计未用其数据代码。

---

## 9. 赚钱线索（profit-leads 规则执行）

- 本次审计未发现新的“扣费后每笔正收益/明显优于随机”线索。星舰B本身高收益已知且已在档（非新发现）， park独立腿 full 49.16%为组合成分非每笔口径，不满足追加条件。
- 故 **本次未向 `~/Projects/wealth-ideas/profit-leads.md` 追加**（规则要求注明来源本次审计，特此声明无新增）。

---

## 10. 待办（按优先序，留给B/C审计或后人，需DB只读分批）

1. M1宇宙敏感性：`load_sgap_context(include_st=True,include_listed_history=True)` vs 冻结默认，同habit跑OOS2/train/valid/long+holdout（COPY分批，<3GB）。
2. M2闸无覆盖跳过：`gate True且b1430 None→不开门` vs 现回退close，同窗对照。
3. L2跌停顺延：`exit_skip_limit_down True vs False` long/valid。
4. L3 loader三态冻结：同一窗口三loader（合并尾/纯CSV/DB-only）对照+CSV快照版本钉死（L5）。
5. L4红利基差：三ETF除权日清单+除权周归因；`verify_harbor_live_vs_backtest` 保持100%。

*证据版本：karios-desktop 2026-10-03现状；卫星系数字以 rebaseline dense 2026-09-29 为准；archive只读不回写；docs/designs预注册冻结不回写。*

KARIOS AUDIT A DONE
