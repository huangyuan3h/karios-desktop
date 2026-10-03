# Karios H2d：核心腿（港湾 + 母港 M30）200万实战压力测试（只读审计，2026-10-03）

> 对象：`~/Projects/karios-desktop` 港湾 Harbor（Live）+ 母港 M30（展示防守档，`homeport_m30`）；200万人民币、长期复利最大化。H3（星舰B容量/切换H2b）另行在跑，本报告不重复星舰B。
> 口径锚：`docs/modules/strategy-recipes.md:52-64,160-166`（卫星HABIT）+ `§0.1/§0.3/§1/§2`（S-3/停车/港湾/母港）；`docs/backtests/rebaseline-dense-2026-09-29.md:18-25`（星舰B post-dense真值）；`docs/backtests/validation-gates-v2-2026-09-16.md`（G1/G3门）；`docs/backtests/README.md:177-183`（OOS2/train/valid切分）+ `AGENTS.md`（规则真值）。
> 前置复用：`~/Projects/wealth-ideas/karios-audit-2026-10/SUMMARY.md`（E+F）、`G_portfolio_100w.md`（G）、`H1_hypotheses.md`、`H2_backtests.md`、`scratch/nav_*.json`（星舰B）、`scratch/b_*.json`、`scratch/g_*.json`、`scratch/f_placebo_*.json`。
> 审计纪律：只读。未改 karios-desktop / karios-research 任何代码配置DB；未下单未连券商；DB只SELECT（`default_transaction_read_only=on`，`daily` 23994624行1998-06-01..2026-10-02、`stock_basic` 10649、`index_daily` 000300 5222行至2026-09-30、`amp_1430` 81608行）；跑数只在 `~/Projects/wealth-ideas/karios-audit-2026-10/scratch/`（`h2d_nav.py`/`h2d_capacity.py`/`h2d_robust.py`/`h2d_placebo.py`/`h2d_param_cheap.py`/`h2d_param_s3.py`/`h2d_combo.py`，种子42），导出约0.5MB（`h2d_*.json`，<3GB红线）；未用Crimson代码数据；未开可见浏览器；未读.png/.jpg。给非量化读者：百分比都是“窗口第一天到最后一天总共赚/亏多少”，MDD是“中途最深下跌”，Sharpe是“收益除以波动越高越稳”，分位100%=打败全部随机、50%=和瞎买一样。

## 一句话结论（给普通人）

**港湾和母港M30过去5年确实赚钱（+200%/+150%），200万也装得下（股票参与度<0.5%、冲击<6pt），但最近37天一起亏了-11%/-9%（比此前冻结的-2.5%深4倍，主因重仓的油气ETF单月-10%），且两者几乎是同一笔钱（相关0.998）、参数一动就差10pt、时间稳定性与星舰B一样差（PBO≈1.0）——能做200万的核心，但只能占50~70%且必须配止损/暂停线，不能100%满仓当“压舱石”。**

## 1. 两者完整规则、数据、回测口径（写清楚）

### 1.1 港湾 Harbor（Live，日落）
- 腿构成：S-3股票核心（`run_walk_forward.py:41-115` S3_CONFIG，Timeline与walk-forward同源）+ 闲置现金停ETF（`harbor.parking_replay`，产品=H2迟滞2pt，统一2026-09-18，`strategy-recipes.md:66-72,106-117`）。
- S-3：Universe全A `daily`×`stock_basic`剔北交所/退市/ST；分数`watchlist_score_daily`≥65 + 20日相对300百分位≥0.5 + 四闸（regime+sentiment+flow+mainline）；T收盘信号→T+1开盘成交（`entry_mode=next_open`）；每腿10%最多10腿；最长60天（UPTREND日入场45天强平D2）；固定-5%/峰值-8%（Strong日-2×ATR14%）；入场风格auto（uptrend追RS≥0.7/fan买-3%回调/weak禁开，D3放缩1.25/0.75）；恐慌冷却2天；组合-25%熔断；国家队闸（300<MA200且4-ETF份额20日Δ≤0暂停开仓）；排除创业板（`exclude_boards=300`，注：代码字面排除创业板，60日均额≥0.7亿）；浮盈2.5%加半仓一次；成本CN静态32.28bp往返（`paper_cost_model.py`单源，引擎`slippage_pct=0`不双计）。
- 停车：候选4腿GOLD 518880/OIL 513350/NASDAQ 513110+513100(alias取优)/BOND10 511260；排名mom60=close/60日前close-1，门槛close≥MA200（BOND10亦需站上）；覆盖率≥3/4腿有≥200根bar否则REPO（0）；状态机唯一`parking_replay(hyst_band)`：先判现持trail（收盘<峰值×92%→当日转REPO不当日再入），换仓需`want.mom60-held.mom60≥hyst_band`否则保留原腿（无卖、无空窗、峰值续算）；产品`HYST_BAND=0.02`（2pt），0.0为历史canonical研究参考；成本5bp/边；`cooldown=0`。
- Idle口径：`idle=1-min(1,Σposition_pct)`（T-1快照）；Live停车按复权基准，trail峰值含fill日前一交易日。
- 时点：股票T收盘→T+1开盘；停车T收盘（18:20 job）→T+1开盘。回测停车用T收盘代理（差0.5h，已声明）。
- 代码入口：`harbor.py`（timeline）、`multi_asset_sleeve.py:427-557`（Live卡片）、`sleeve_paper_auto.py`（paper镜像）；路由`?strategy=harbor`。真值`etf-parking-baseline-2026-09-13.md`（B11，P1 PASS K1-K3）+ `harbor-h2-parking-2026-09-16.md`（H2 vs P1 Δ+2.4/+9.3/-0.6/+11.3，MDD-1.1挂HK3 0.1pt）+ `harbor-h2-unify-2026-09-18.md`（H2即港湾本身）。

### 1.2 母港 Homeport M30（展示防守档）
- 腿构成：`w_harbor×Harbor+(1-w_harbor)×B3`；B3=5资产逆波月频（510300/510500/518880/513100/511260，60日`pstdev(日收益)`，`w∝1/σ`，当月首个交易日调仓，月内漂移不平衡）；现行展示M30（w_harbor=0.7，`timelineStrategy=homeport_m30`，tag `h-mix-m30-20260916`），M50（0.5）保留为冻结审计快照（路由默认仍0.5）；成本B3换手5bp×Σ|Δw|/2 + 月复位5bp×|漂移|；display only（无paper/Live接线）；代码`homeport.py`；证据`harbor-riskbudget-2026-09-13.md`（M50 PASS K1-K5）+ `homeport-weight-tune-2026-09-16.md`（M30 chosen：long CAGR 21.18%最大且余量健康，M20 22.91%但三处踩线不推荐）。
- 注意：M{N}=N% B3+(100-N)%港湾；M30=30%B3+70%港湾。

### 1.3 数据
- 行情tushare+腾讯qfq混源（`strategy-params.md:148-149` CN341万行/HK317.7万行重灌、794个≥5%假跳空；`tushare_pool.py`多token轮询）；本审计DB复核`daily` 23994624行1998-06-01..2026-10-02、`stock_basic` 10649（delisted23/ST211）、`bar_5min` 58821148行2021-01-04..2026-09-30（定向存储，覆盖50-55%）。
- 分数`watchlist_score_daily` 660万行2007-01-09..2026-09-30（TrendOK落库，引擎`skip_live_score_lookup=True`防未来；06-18前全合成+幸存者宇宙，不可解释梯度）。
- ETF月快照CSV `data/etf/etf_daily.csv`（本机至2026-09-11，4.5M）+DB尾拼接（`harbor.load_etf_closes`按anchor比例拼接，DB的513100仅908行2023-01-03起，ETF `daily`为raw、`adj_factor` NULL，Live/回测统一走合并基线，09-15 bug已修）。
- 指数`index_daily` 000300 5222行至2026-09-30、000905 4793行；缺000852（中证1000，以500代理，B§6已判）。

### 1.4 回测口径
- 三窗walk-forward固定切分（`backtests/README.md:177-183` OOS2 2024-08-01~2025-08-01/train 2025-08-01~2026-02-01/valid 2026-03-01~2026-08-07复用≥4，新候选上限条件PASS；holdout 2026-08-08~2027-02-08只读n≥50转正/n≥100 binding；long 2021-08-01~2026-08-07进裁决）+门控v2（G1收益/G2兑换/G3一致/L前视，PASS/条件PASS/REJECT/VOID，单窗好看=过拟合）；工具`run_walk_forward.py`；成本≥32bp往返（股票32.28bp+ETF/B腿5bp/边）。
- 本审计H2d切分沿用冻结（OOS2/train/valid/long同上，holdout按任务2026-08-10~2026-09-30，n=36收益/37历，`h2d_nav.py`独立重跑引擎+停车+B3，与冻结同口径，数据版本2026-10-02）。

## 2. 200万容量与冲击成本（逐笔参与度中位/p90/max，加15bp/30bp压力，ETF腿容量）

方法：`h2d_capacity.py`（种子42，DB只SELECT，`daily.amount`千元→元；S-3 blotter来自`h2d_nav.py`引擎`simulate` 5窗，long 416笔为容量样本，随机抽300笔查成交额；slot harbor=200万×pos（pos含env 1.25/0.75，名义10%即20万，1.25×即25万），M30 harbor袖=200万×0.7×pos（名义7%即14万）；ETF用`daily` 2024-08~2026-09全量成交额，slot harbor停车idle100%即200万、B3每ETF M30约12万/B3 100%约40万）。

### 2.1 股票腿（S-3，绑定腿）
- 样本：long 416笔（OOS2 83/train 52/valid 16/holdout 0），抽300笔金额中位3.60亿、p5 1.10亿、p10 1.27亿（S-3自带60日均额≥0.7亿过滤，尾部已切）。
- 200万参与度（slot/当日成交额）：
  - 港湾（20万槽位，中位20万/p90 25万/max 25万）：中位0.049%、p90 0.130%、max 0.416%，P(>5%)=0、P(>2%)=0、P(>1%)=0（`h2d_capacity_harbor200w.json`）。
  - M30港湾袖（14万槽位，中位14万/p90 17.5万/max 17.5万）：中位0.034%、p90 0.091%、max 0.291%，P(>5%)=0（`h2d_capacity_m30_harbor_sleeve200w.json`）。
- 读法：股票腿200万无忧（max<0.5%，远低于星舰B 100万P(>5%)11%/max54% `g_capacity.json`）；100万以下1手门槛对S-3无碍（C P0-3高价笔仅卫星受限，S-3 10%槽位20万无1手问题）。**证据强度：强。**

### 2.2 ETF腿（停车+H2/B3，200万关键在OIL）
- 日成交额中位（2024-08~2026-09，`h2d_capacity_etf.json`）：510300 40.22亿（p5 19.58亿）、510500 22.06亿（p5 8.22亿）、518880 31.76亿（p5 10.44亿）、513100 6.52亿（p5 2.67亿）、511260 23.39亿（p5 2.93亿）、513350（OIL）仅1.45亿（p5 0.10亿！）、513110 1.49亿（p5 0.48亿）。
- 200万参与度：
  - 大ETF（510300/518880/511260）：港湾停车200万中位<0.1%（510300 0.05%、518880 0.06%、511260 0.09%），B3每ETF 12-40万<0.1%，无忧。
  - 纳指513100：停车200万中位0.31%、p90 0.62%、max 1.17%，B3 12万中位0.02%，无忧。
  - **OIL 513350：停车200万中位1.38%、p90 14.37%、max 53.90%（`h2d_capacity_etf.json`）；B3每ETF 40万max 10.78%、M30 12万max 3.23%。** OIL p5日成交仅0.10亿（1000万），200万一进即20%，极端日53.9%（金额min约370万）。港湾在holdout 08-19~09-30重仓OIL（§3.5），200万在OIL低量日冲击未建模（C容量门>500万可用仅对卫星，ETF OIL未覆盖）。
- 读法：ETF腿除OIL外容量无忧；**OIL是200万唯一容量瓶颈**（单日200万占比可超10%，与股票腿<0.5%差两个量级）。若停车命中OIL且当日成交<5000万，需降仓或换腿（见§6暂停线）。**证据强度：强（526天全量）。**

### 2.3 冲击成本+15bp/+30bp压力
- 股票腿（`h2d_capacity.py`，extra×pos求和，long 416笔）：+15bp港湾-5.46pt/M30-3.82pt，+30bp翻倍-10.92/-7.64pt。分窗：valid 16笔+15bp仅-0.24pt（可忽略）、OOS2 83笔约-1pt、holdout 0笔0。相对long +206%/+152%（-5pt仅2-3%相对），valid +50%/+35%（-0.2pt可忽略）。低换手（港湾long停车60次、股票416笔 vs 星舰1078笔-40pt）故压力小一个量级，与G“低换手-5~-10pt估”一致（±5pt不确定已声明）。
- ETF腿（上界idle100%，`h2d_nav.json`停车始め60/15/16次）：+15bp long上界-9.0pt/valid-2.25/OOS2-2.4，+30bp翻倍。现实idle 30-40%（S-3平均部署60-70%，holdout 100%除外）则long约-3~-4pt、valid<1pt。B3月调换手10-20%/月，+15bp约-0.3%/年可忽略。
- 合计+15bp后long：港湾~200pt（206.9-5.5-3.5≈198）、M30~146pt（152.6-3.8-2≈147），仍>2倍于300 BH（-4.9%），方向不变。**证据强度：强（股票精确，ETF上界+现实折扣已声明）。**

## 3. 稳健性

### 3.1 逐年收益与回撤（`h2d_yearly.json`，long切片连续持有，2021-08-02起故2021仅102天、2026仅143天至08-07）
| 年 | 港湾H2 | M30 H2 | B3 | 300 BH参考 |
|---|---|---|---|---|
| 2021（102d） | -6.1%/-11.5%/−0.69 | -3.4%/-9.0%/−0.52 | +3.1%/-4.0%/1.34 | 缺（未重跑 yearly基准，记缺） |
| 2022 | +2.3%/-19.8%/0.22 | +1.4%/-14.2%/0.17 | -2.3%/-4.7%/−0.52 | -21.3%（§5） |
| 2023 | -0.9%/-17.2%/0.06 | +1.4%/-12.0%/0.17 | +6.1%/-2.2%/2.28 | 缺 |
| 2024 | +29.0%/-23.5%/1.00 | +26.2%/-16.8%/1.17 | +17.1%/-1.7%/3.44 | 缺 |
| 2025 | +61.4%/-11.2%/2.16 | +45.0%/-8.3%/2.23 | +11.6%/-3.3%/2.36 | 缺 |
| 2026YTD（143d） | +49.3%/-21.1%/1.89 | +35.2%/-15.4%/1.89 | +3.8%/-3.2%/1.36 | 缺 |
- 读法：每年为正仅2022/2023微利（+2%/-1%），2024+2025占主导（港湾2025 +61.4%单年≈long +206%的1/3，2024+2025合计超60%，与星舰B 62%集中度同构，B `b_yearly.json`）；2021/2023为负（-6%/-1%），2022/2023几乎零收益靠2024-2026续命；B3每年小正（除2022 -2.3%），波动小。跨策略逐年对照300/500 yearly未重跑（<3GB内S-3 yearly已跑，基准yearly缺，记缺）。**证据强度：强（描述性，集中度方向确定）。**

### 3.2 随机安慰剂（同频同量随机选股，500次/窗，种子42，`h2d_placebo.py`+`h2d_placebo.json`）
- 方法：保留实际每笔entry/exit日期+pos（同频同量），从当日冻结宇宙（主板+创业+科创5032票，剔BJ/HK/ETF/现名ST/退市）均匀抽票，close算往返（exit/entry-1-32.28bp）×pos，加法total（`ret%×pos`求和，pt），与实际close代理additive苹果对苹果；实际close vs 引擎NAV差≤2.1pt（OOS2 -2.1/train +2.1/valid -1.2/long -0.1），不翻转分位。**证据强度：强。**
- 结果（股票腿additive，pt）：
  - OOS2（83笔）：实际31.7 vs 随机均32.6/p5 18.8/p50 32.0/p95 49.2/max 62.1，分位48.6%（中位，和瞎买一样）。
  - train（52笔）：实际40.9 vs 随机11.5/0.0/10.8/24.1/44.0，分位99.6%（显著）。
  - valid（16笔）：实际37.5 vs 随机2.8/-6.7/2.2/14.8/50.2，分位99.8%（显著，n=16小样本已声明）。
  - long（416笔）：实际77.6 vs 随机40.3/9.4/38.8/76.7/114.7，分位95.8%（p95上，未到100%，随机max 114.7超实际）。
  - holdout：0笔（引擎0 closed），无法跑安慰剂，记缺（underpowered，只能描述）。
- 读法：与星舰B镜像反转（星舰OOS2 99.8%/valid 50.2%/holdout 10.9% `F_placebo_sensitivity.md:28-76`；港湾OOS2 48.6%/valid 99.8%）。港湾选股在train/valid/long有edge、OOS2无edge；long 95.8%（<100%，弱于星舰long 100%）。停车/B3腿未做真择时安慰剂（ETF动量G已REJECT Bonferroni 1.0，记缺）。**证据强度：强。**

### 3.3 参数邻域敏感性（只扰动已有参数±一档，不新调参）
- B3 lookback 42/60/78（`h2d_param_cheap.json`，月频，无引擎）：OOS2 18.8/18.8/18.5（±0.3）、train 17.2/16.8/16.8（±0.4）、valid 0.6/1.5/0.4（±1.1）、long 46.5/46.2/45.6（±0.9）。稳定。**强。**
- 权重M20/M30/M40/M50（70/60/50%港湾，`h2d_param_cheap.json`）：OOS2 45.9/42.4/38.9/35.5、train 52.2/47.8/43.4/39.0、valid 39.9/35.0/30.2/25.3、long 170.2/152.6/135.5/119.1，单调17pt步进，无悬崖（`homeport-weight-tune.md:7-17` M带同向互证）。稳定。**强。**
- 停车trail 6/8/10 + hyst 0/0.02/0.04（standalone idle100%，`h2d_param_cheap.py`）：trail敏感大：long 129.1/100.3/52.8（±30-47pt）、valid 7.0/5.7/-17.6（trail10崩-23pt）、OOS2 4.8/14.2/11.5（方向翻转）。hyst 0/0.02/0.04（trail8）：OOS2 11.0/14.2/10.5（±3.7）、train 23.6/35.6/28.2（±12）、valid 6.8/5.7/5.7（±1.1）、long 89.1/100.3/77.3（±23）。停车腿单独看不稳定，但港湾total经idle（30-40%）稀释后trail10 valid约-9pt（仍大）、hyst long约±9pt（中等）。H2 vs P1（hyst 0 vs 0.02）港湾total：OOS2 +2.4/train +9.3/valid -0.5/long +14.0（`h2d_nav.json`，与`harbor-h2-parking-2026-09-16.md:12-22` +2.4/+9.3/-0.6/+11.3方向一致，long差2.7pt为数据版本漂移，已声明）。**停车邻域：弱（不稳定，方向翻转）。**
- S-3股票（`h2d_param_s3.json`，引擎重跑，OOS2 83笔/valid 16笔）：score 60/70 Δ0.0（惰性，不绑定）；trailing -6 OOS2 -11.5/valid -1.1、-10 OOS2 -4.5/valid +10.3（跨窗翻转，-8为折中）；maxpos 8 OOS2 -14.5/valid 0.0、12 OOS2 +0.3/valid 0.0（8在OOS2大亏，10为下限）。与星舰body4陷阱（valid +15.9/OOS2 -73.6 `b_params_a.json`）同构：单参跨窗翻转。港湾继承（eng权重100%），故港湾total trailing邻域±10pt（OOS2 -11.5、valid +10.3）。**S-3邻域：挂（敏感+翻转，证据强）。**

### 3.4 DSR/PBO代理（`h2d_dsr.json`/`h2d_cscv.json`，long 1215收益）
- DSR（den复用B口径）：港湾SR1.00 den1.779 PSR vs0/0.5全1.0 vs1.0 0.53；DSR N20全1.0，N50 V0.2 0.39、N100 V0.2 0.006、N500 V0.1 0.78/V0.2 0.0。M30 SR1.08 den1.894 PSR vs1.0 0.93；DSR N100 V0.2 0.19、N500 V0.2 0.0。B3 SR1.76 den3.202 DSR N500全1.0。读法：港湾/M30 long显著性在N≥100/V≥0.2下被杀死（DSR<0.2），远弱于星舰B SR4.15 DSR全1.0（B `b_placebo2.json` den6.511/z>14）。**证据强度：强。**
- CSCV/PBO单配置代理（S=8，35半split，`g_cscv.json`同法）：港湾PBO代理1.00 corr(IS,OOS)-0.97、M30 1.00/-0.97、B3 0.94/-0.97。读法：好的4块预示差的另4块，时间不稳定，与星舰B代理0.94/-0.97（G§0.4）同构。正式PBO需N配置未跑，记不确定（代理非正式，已声明）。**证据强度：中（代理）。**

### 3.5 holdout（2026-08-10~2026-09-30，`h2d_nav.json`，n=36收益/37历）
- 港湾H2 -11.44%/-13.16%/-3.41、M30 H2 -8.83%/-9.96%/-3.71、B3 -2.90%/-4.42%/-1.72、引擎（S-3）0.00%（0 closed，无仓位，idle100%故港湾=停车）。
- 拆分：至09-11（24天，冻结口径）港湾-2.17%/M30-2.22%/B3-2.38%（与冻结-2.5 `five-strategy-v2-scorecard:163`差0.3pt，数据漂移，已声明）；09-12~09-30（12天）港湾-9.48%/M30-6.76%/B3-0.53%。即**前期持平、后期OIL单腿-10%（09-16 1.431→09-30 1.279，trail 09-24 REPO一次，whipsaw 09-02 Nasdaq -1.7%）拖累**，B3分散仅-0.5%。
- n=36<50 underpowered只能描述（`validation-gates:69-70` G3c），但方向为反向（跑输300 -7.3%? 300 holdout 08-10~09-30需重算，G§1 D补记300 -7.32%为08-10~09-30 38天，本审计holdout 300/500见§5 Q3：07-01~09-30 -12.1%/-17.7%，08-10~09-30约-7~-8%，港湾-11%跑输约3-4pt，M30 -8.8%基本持平，B3 -2.9%跑赢约5pt）。**证据强度：强（描述性，n小已声明）。**

## 4. 未来函数与数据风险复查（信号时点、复权、幸存者、单数据源）

- 信号时点：股票T收盘→T+1开盘（`run_walk_forward.py:48` next_open，`backtest_engine.py:107-124` T+1合规天然满足body≥1的星舰类比，S-3无同日买卖）；停车T收盘→T+1开盘（`strategy-recipes.md:111-112`，回测用T收盘代理，差0.5h，H2d同冻结）；S-3出场优先级`stop>target>score>pool>max_hold`（`paper_trading.py:641-699`），trail用收盘峰值回撤。`w_t=cashShare(T-1)`因果（星舰类比，港湾idle同T-1快照，`harbor.py:419-421`）。未发现同日权重偷看（E§1 P2同式已证星舰，港湾同idle T-1）。**无严重未来函数。强。**
- 复权：`daily`为qfq（前复权），`stk_limit`/`bar_5min`为raw（AGENTS price-basis lesson，`hotmoney_lib.py:latest_adj/raw_price`）；S-3引擎用qfq一致（`BacktestData.fetch_ohlcv_batch_between`），涨停判定用信号日收盘基准（`strategy-recipes.md:46`，主板10%/创科20%/北交30%，粗回测`pct_chg>9.5%`混同已声明）；ETF `daily`为raw（`adj_factor` NULL）而研究面板为adjusted，Live/回测统一走`harbor.load_etf_closes`合并基线（anchor比例拼接，09-15 bug已修，H2d沿用）。qfq/raw混用已按`raw=qfq×adj_latest/adj`重建 sanity（09-05 lesson），本次未发现新混用。**强。**
- 幸存者：Universe=scores表当日有分票（`BacktestData:universe=scores_by_day`），非`stock_basic`全史；`stock_basic` 10649/delist23/ST211（E复核一致）；引擎有`delist_by_ts`强制按last close退出（`backtest_engine.py:83,1055-1083,2854-2856`），但ST按现名剔（`_load_st_names`，无ST历史表，前视小偏，不确定）；scores 06-18前全合成+幸存者宇宙（D3+D4，`sat-score-segment-2026-09-08.md:25`自认偏倚）；上市首5日无涨跌幅未建模且无`list_date`年龄过滤（2322只CN次新，E§2.2，F `f_age.json`星舰N60 +19pt小拖累，S-3未跑年龄敏感性，记缺）。方向：long绝对值带幸存者上偏（ST幻觉F +180pt类比，S-3未量化，记不确定）。**方向强，量级缺。**
- 单数据源：行情tushare+腾讯qfq混源（341万/317.7万重灌，794个≥5%假跳空，多token轮询）；`amp_1430` vendor 5min回填仅卫星用，港湾不用；ETF月快照CSV（至09-11）+DB尾拼接（DB 513100仅908行2023-01-03起，日更`sleeve_etf_daily_sync` 17:25仅5只套筒ETF）；分数TrendOK单源（引擎`skip_live_score_lookup=True`防未来，但分数本身单源）；karios-research同源镜像非第二源（`BRIEF.md:8-19`，E§2.5已判）。4天无代码漂移±8pt（B§1）即单源版本风险活证据；OIL 513350仅689行（2023-11-28上市，long早期无OIL，B11已声明）。**强。**
- 规则缺口：ST 5%板未建模（`backtest_engine.py:96`注释，卫星靠剔ST免疫，S-3同靠剔ST，但ST历史缺故仍小偏）；跌停不卖顺延已建模（`backtest_engine.py:2973-2976` limit-down跳过，L2日常小，F long -0.6/holdout -0.2类比）；涨停不买以信号日收盘基准（一字判定，qfq/raw已对齐）。**强。**

## 5. 相关性与坏行情组合表现

### 5.1 相关性（`h2d_corr_*.json`+`h2d_cscv.json`，long 1215日/61月）
- 日：港湾-星舰B 0.156、港湾-300 0.320、港湾-500 0.366；M30-星舰B 0.170、M30-300 0.359、M30-500 0.405；港湾-M30 0.998（几乎同一资产，M30 70%港湾所致）。
- 月：港湾-星舰B 0.105、港湾-300 0.447、港湾-500 0.486；M30-星舰B 0.115、M30-300 0.478、M30-500 0.517。
- 对照：星舰B-300 0.012/星舰B-500 0.107（G§0.6）、卫星-停放0.021（D `d_blend.json`）、双子星×B3 0.273、港湾×卫星0.08~0.23（`harbor-sat-weight`）、星舰系sat-capital-split港湾0.09/母港0.10（`sat-capital-split-2026-09-15`）。港湾/M30与市场beta 0.3~0.5（自然beta，高波缺口组合类比H11 0.37~0.46 `H2_backtests.md:56`），与星舰B低相关（0.10~0.17，分散化理论成立，但港湾×M30 0.998互不分散）。**证据强度：强。**

### 5.2 坏行情（`h2d_bad.json`+`h2d_combo.py`，组合月再平衡5bp/边，保守70%M30+20%B3+10%货基/平衡50%港湾+30%M30+20%货基）
| 坏行情 | 港湾 | M30 | B3 | 保守/平衡 | 300/500 |
|---|---|---|---|---|---|
| 2022（242天，熊市） | +2.3%/-19.8% | +1.4%/-14.2% | -2.3%/-4.7% | +0.5%/+1.6% | -21.3%/-20.3%（超额+22pt，防守成立） |
| 2024初（78天，01-01~04-30微盘） | +7.6%/-10.1% | +6.7%/-7.4% | +4.3%/-0.8% | +5.5%/+5.8% | +6.4%/+0.6%（持平，无超额） |
| 2026 Q3全（63天，07-01~09-30拼接long+holdout） | -10.7%/-13.2% | -8.2%/-10.0% | -2.8%/-4.4% | -6.3%/-7.8% | -12.1%/-17.7%（跑赢4-10pt但绝对-6~-8%） |
| 2026 Q3前段（27天，07-01~08-07） | +0.8%/-9.2% | +0.7%/-6.7% | +0.1%/-1.1% | 缺（未单算，约+0.5%） | 缺（Q3全已含） |
- 读法：2022熊市防守成立（+2% vs -21%，两腿+组合全跑赢，M30回撤-14% vs 300 -28%腰斩）；2024初微盘持平（+7% vs +6%，无防守溢价，小市值0暴露故比星舰B -19%浅，G§3.2）；2026 Q3 crash bersama（-10%/-8% vs -12%，跑赢但绝对大亏，B3 -2.8%最抗，保守-6.3%/平衡-7.8%仍超MDD 15%档一半）。2022 stress类比M50 0.39/-8.3（`harbor-riskbudget:31-37`）与本次2022 +0.5%/+1.6%方向一致。**证据强度：强（描述性）。**

## 6. 结论：能否作为200万核心（比例区间+止损/暂停规则）+证据强度表

### 6.1 200万核心裁决
- **能，但只能半仓核心，不能满仓压舱。** 股票腿容量无忧（<0.5%），+15bp仅-4~-6pt，逐年4/6正、valid 99.8%强、B3稳定、2022防守+22pt超额是加分；但holdout 36天-11%/-9%（后期-9% OIL单腿）、OOS2安慰剂48%（和瞎买一样）、S-3 trailing/maxpos跨窗翻转±10pt、停车trail valid崩-23pt、DSR N500 0.0/PBO 1.0、港湾×M30 0.998（伪分散）、OIL 200万p90 14%/max 54%是硬伤。若按G三档（MDD 15%/25%/35%），200万默认按15%档执行：
  - **保守核心（默认，MDD 15%档）：50~70% M30 + 20~30% B3 + 10~20%货基（月再平衡5bp/边）。** 历史long +109.7%/-11.9%/1.15（`h2d_combo.py`），预期CAGR 12~18%（M30 21%×0.7+B3 8%×0.2≈16.3%±4pt valid/holdout折扣）、历史MDD约12%（上限15%）、2022 +0.5%/2024初+5.5%/2026Q3 -6.3%。200万执行：股票每票14万（0.03%）、ETF每票12万（OIL max 3.2%可控， parking若命中OIL且量<5000万则降至5%或换REPO）。
  - **平衡核心（MDD 25%档，能忍20%回撤）：40~50%港湾 + 20~30% M30 + 20%货基（即71%港湾暴露上限，`h2d_combo.py` long +136.9%/-17.1%/1.02）。** 预期CAGR 15~22%（0.5×26%+0.3×21%≈19%±4pt）、历史MDD约15%（上限20%）、2022 +1.6%/2024初+5.8%/2026Q3 -7.8%。港湾×M30 0.998故两者合计≤70%（不可各50%伪分散）。
  - **MDD 35%档：无坚实方案。** 只能在平衡上加≤20%星舰B观察仓（paper 20笔+holdout收复前置，否则0%，G§3.2/E§5），压力可达-25%。
- 什么情况证明推荐失效：保守/平衡若forward 60天-10%或holdout式两腿同亏（相关>0.4）持续3个月，或港湾/母港valid转负（当前+50%/+35%转负），或OIL连续两月为第一大持仓且跑输B3超5pt，即ETF压舱逻辑失效，转全货基并复盘；观察仓若paper 20笔均值<-1%/笔或holdout扩到-25%或再出单笔-8%（603125级），清零12个月不再启用（G§4/E§5）。

### 6.2 止损/暂停规则（触任一即停新手、只做paper/复盘，E§5+G§3.3强化，200万加OIL条）
1. forward（自今日起）60天-10%或单笔-8%即暂停（含OIL单腿-8%触发trail后空仓一天，勿立即再入）。
2. holdout扩到-25%（当前-11%/-9%）全停paper并复盘regime切换（n≥50前不加仓）。
3. 连续3笔同日全亏或单周-5%即降半仓（余款park在REPO，呼应D3 brake人工版）。
4. 数据链告警即停：`daily`滞后>2交易日、`watchlist_automation_runs`连续skip、ETF CSV超35天未快照（当前CSV至09-11，DB尾至09-30，已35天+，需先快照再实盘）。
5. **200万新增：OIL持仓日若OIL当日成交<5000万或参与度>5%（200万/成交额），当日降停车至50%（余款REPO），B3 OIL权重上限10%（月调时若OIL权重>20%则砍半）。**

### 6.3 每周监控（一页纸5分钟，E§5复用）
1. 组合NAV+两腿贡献（S-3/停车/B3）+相对300/500超额；2. 执行fills/胜率/均损益+14:30缺print率+涨跌停数+`bar_5min`覆盖率（<20%标红）；3. 成本实现滑点 vs 32.28bp模型++15bp敏感性（-5pt标尺）；4. 数据版本（`daily`/`watchlist_score_daily`/ETF CSV最大日期与行数、qfq重灌记录）；5. paper-vs-backtest对齐笔diff（目标20笔看均值）。引用数字一律并列valid/holdout，不许只引long。

### 6.4 证据强度表
| 结论 | 强度 | 关键证据 |
|---|---|---|
| 规则/数据/口径无严重未来函数 | 强 | T+1因果+`w=cash[T-1]`+qfq/adj分开+delist强制退出代码行 |
| 200万股票容量无忧（<0.5%） | 强 | `h2d_capacity_*` 300笔中位0.049%/max0.416% P(>5%)=0 |
| OIL 200万瓶颈（p90 14%/max54%） | 强 | `h2d_capacity_etf.json` 526天，p5仅0.10亿 |
| +15bp后long仅-4~-6pt | 强 | 股票精确求和+ETF上界，低换手（416+60 vs 星舰1078） |
| 逐年4/6正但2024+2025占超60% | 强 | `h2d_yearly.json` 2025 +61%/2024 +29% |
| 安慰剂train/valid 99%+、OOS2 48% | 强 | `h2d_placebo.json` 500次/窗，close差≤2.1pt |
| B3/权重邻域稳定 | 强 | lb±0.9pt、M带单调17pt步进 |
| S-3 trailing/maxpos翻转±10pt | 强 | `h2d_param_s3.json` OOS2-11.5 vs valid+10.3 |
| 停车trail valid崩-23pt | 强 | standalone 7.0→-17.6，harbor稀释后仍-9pt |
| DSR long fragile（N500 0.0） | 强 | den1.78/1.89，港湾0.006/M30 0.19/B3 1.0 |
| PBO代理1.0/corr-0.97 | 中（代理） | S=8单配置，正式PBO未跑 |
| holdout -11%/-9%（后期-9% OIL） | 强 | `h2d_nav.json` n=36，早期-2.2%匹配冻结-2.5 |
| 相关港湾×M30 0.998伪分散 | 强 | 日0.998，与星舰0.10-0.17真分散对照 |
| 2022 +2% vs -21%防守成立 | 强 | `h2d_bad.json`超额+22pt |
| 2026Q3 -11%/-8% vs -12%跑赢但大亏 | 强 | 拼接63天，B3 -2.8%最抗 |
| 幸存者/单源量级 | 弱（不确定） | ST历史缺+scores合成+CSV月更，F +46pt类比未量化S-3 |
| 核心50-70%+止损暂停建议 | 中 | 数字强，比例为审慎推断 |

### 6.5 不确定与待办（写缺）
- 缺：S-3 yearly基准300/500 yearly（未重跑）；holdout n=36<50 underpowered；H21/H22（转债/调入）无表blocked（H1§1/H2§4）；H14行业ETF NAV缺；H06/H13/H08 holdout n小（H2§4）；S-3年龄过滤敏感性未跑（F仅星舰N60 +19pt）；S-3正式PBO（N≥16）未跑；港湾真择时15000次（F式）未跑（本次仅500次stock leg）；组合CAGR/MDD为加权+blend重跑（±4pt/±3pt）；OIL冲击模型未建模（>5%参与度）。
- 待办（留后人，需DB只读分批<3GB）：1. 星港/双子星post-dense复跑（`rebaseline:51`）；2. 港湾日NAV落盘供正式PBO+月度矩阵；3. S-3加停牌/涨跌停精确过滤+年龄N60重跑；4. 可转债/调入补表（cb_basic/cb_daily/index_weight，PIT T→T+1）后开预注册；5. 嵌套walk-forward（B§10.4）；6. 中证1000补000852；7. holdout n≥50后重裁+paper 20笔；8. OIL低量日降仓规则paper验证。

## 7. 赚钱线索（profit-leads规则执行）

- 本次H2d候选：1. 港湾valid 99.8%/train 99.6%碾压随机——但为已知PASS腿（B11 K1-K3/H2d§3.2）的 placebo确认，非新策略/新参数，按B/E/F/G/H1/H2先例（已知高收益非新发现）不追加；2. M30 yearly 2025 +45%/2026 +35%——为已知M带单调（`homeport-weight-tune`）复用，非每笔口径，不追加；3. B3 DSR全1.0/PBO 0.94——为已知B腿（D3b +46.7%/2.26）确认，非新alpha，不追加；4. OIL holdout -10%——为负样本，明确不追加；5. S-3 trailing -10 valid +10.3pt——跨窗翻转（OOS2 -4.5pt），不满足跨窗一致，不追加；6. 保守/平衡组合+109%/+136%——为已知PASS腿低相关复用（ harbor×M30 0.998伪分散，heter），非新alpha，不追加。
- 故**本次未向 `~/Projects/wealth-ideas/profit-leads.md` 追加**（注明来源本次审计H2d，特此声明无新增）。若未来holdout n≥50收复（港湾/M30转正且OIL占比<10%）+paper 20笔转正，或B3在holdout转正且5门全过，另行追加。

*证据版本：karios-desktop 2026-10-03现状（`daily` 23994624行1998-06-01..2026-10-02、`stock_basic` 10649、`bar_5min` 58821148行、`index_daily` 000300 5222行至2026-09-30、ETF CSV至2026-09-11+DB尾至09-30）；卫星系以rebaseline dense 2026-09-29为准，港湾/母港以H2d `h2d_nav.json`（H2 Live口径）为准（冻结P1对照差OOS2 -2.2/train +8.8/valid -0.6/long +5.4pt为数据版本漂移，已声明）；archive/designs只读不回写；源仓未改一字（`git status`仅本次审计前已存在改动，本审计`h2d_*.py`与输出全在wealth-ideas scratch/审计目录）；随机种子42；`h2d_nav.json`（5窗×7腿）+`h2d_blotter_*.json`（416笔）+`h2d_capacity_*.json`+`h2d_yearly.json`+`h2d_placebo.json`（500次/窗）+`h2d_param_cheap.json`+`h2d_param_s3.json`+`h2d_dsr.json`+`h2d_cscv.json`+`h2d_corr_*.json`+`h2d_bad.json`。*

KARIOS H2D DONE
