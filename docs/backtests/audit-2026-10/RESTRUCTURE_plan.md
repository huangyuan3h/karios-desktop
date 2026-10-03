# Karios 重整方案（2026-10-03）：1.星舰B保留，是live和唯一基线，其他都跟它比（只写方案不改代码）
2.真正赚钱只有三台发动机：E1港湾/母港选股、E2 ETF逆波B腿、E3星舰B短线，其余十几个证伪归档。
3.资金分三档自动选：A100-150万进攻、B150-200万过渡、C200万+保守，月末看总资产+缓冲带切换。
4.回测页只留星舰B+三发动机+当前档组合+随机/沪深300对照，必须并列valid/holdout不许只看长回测。
5.双子星/星港是组合层（E1+E3拼装），合并到新组合层不再独立展示；切换/国家队/资金开关全部归档。
6.星舰B变体（容量版/H2-a25/纯B3/H2100%等）收为星舰B的参数档位，不再算独立策略。
7.所有参数冻结不许改；停用/复活写死，星舰复活等并行H2k，当前按paper20笔+holdout收复执行。
8.迁移分4步小PR，先只加不删（新组合层+主页面），再归档旧页面，每步可回退，不碰实盘下单逻辑。
9.默认按你真实资金100-120万显示真实成本/容量口径；数据健康检查已修好（ETF快照/两融/北向标注）进周报。
10.Yuan只需拍5个问题（默认A25+三档+阈值不动，详见§5），其他照方案做即可；本文件只写方案不改代码不改参。

# Karios 系统重整方案（RESTRUCTURE_plan，2026-10-03）

> 地位：本方案服从 Yuan 2026-10-03 硬性决定（最高优先级），与 FINAL/H2j 不一致处以本方案为准。
> 硬性决定复述：①星舰B必须保留，在找到更好方案前就是 live 策略和基线；星舰B默认 post-dense 配方及其页面、调度、14:30 执行链、模拟/实盘流水一律保留，不得归档、不得降级；所有其他发动机和组合都以星舰B为对照基线展示（主页面同时显示 vs星舰B 与 vs随机）；审计风险（holdout −19%、复活规则 H2k）照实写在旁边作提示，但不改变保留与 live 地位。②除星舰B以外由本方案规划去留和角色，并给出候选取代星舰B的预注册升级规则。③回测页大幅简化，只展示真正可用（星舰B+E1/E2/E3+当前资金档组合+随机/沪深300），其余折叠/隐藏，默认按真实资金（可配置，当前约100–120万）和真实成本/容量口径，并列 valid/holdout。
> 纪律：只读规划，只写本方案文档；不改 karios-desktop / karios-research 任何代码配置 DB、不改参数、不下单不连券商、不下载、不 push；永不用 Crimson 代码或数据；未用 read 打开 .png/.jpg；未杀他人进程。
> 前置已读：H2j_capital_tiers.md（全文192行）、FINAL_REPORT.md（§0–§6）、H3_review.md（全文91行）、DATAFIX_report.md（全文76行）；抽读 H2_backtests / H2b / H2d / G / SUMMARY 前60–80行；盘点 karios-desktop（前端 hash-SPA、后端单 service、56 个调度 job、scripts 27+ 脚本）与 karios-research（只读，同源镜像非第二源）。

## 1. 盘点表：每个策略/页面/调度/脚本 → 归属与去留

归属代码：E1=S-3选股（港湾/母港股票腿）、E2=ETF逆波（B腿/B3）、E3=S-gap短线（星舰B全族，live/基线）、C=组合层、D=数据层、X=已证伪/观察归档、T=工具/基础设施。
去留代码：保留=继续在主路径展示与调度；合并=收为某发动机的参数/档位或新组合层的权重，不再独立；归档=移到 archive/ 或加 feature-flag 隐藏，不删除不改逻辑。

### 1.1 策略/发动机盘点（27+变体全覆盖）

| # | 策略（审计名） | 归属 | 去留 | 角色与理由（证据出处） |
|---|---|---|---|---|
| S-01 | 港湾 Harbor clean（S-3+停车） | E1（主）+E2停车 | 保留 | E1 主引擎，唯一可上组合的选股腿。long+206.9%/valid+50.3%（H2d §3.2 99.8分位），B腿外唯一 5 门全过（H3 §1）。Live 名义已切港湾（OPT-178），本方案保留为 E1，M30 包装的基础。 |
| S-02 | 母港 M30（70%港湾+30%B3）/ M50 | E1×70%+E2×30%（包装） | 保留（M30默认，M50归档为历史预设） | 与港湾 long 相关 0.998 同源（H2j §1 h2j_corr.json，H2d §5.1），不是独立源，是 E1 的分散包装。保留 M30 为 E1 默认包装（w_harbor=0.7冻结），M50 收为历史参数预设。两者合计≤70%防伪分散（FINAL §0.3注）。 |
| S-03 | B腿 3-ETF逆波 60d月频 / B3 5-ETF | E2 | 保留 | 唯一多重检验后仍显著、最扛跌（holdout −0.2%~−2.9%，H2d §3.3 DSR N500全1.0）。long+46.2%/MDD−4.7%/SR1.76–2.26。保留为 E2，B3 为替代预设（lb42/60/78 ±0.9pt，取中60）。 |
| S-04 | 星舰B 默认 post-dense（100%停3腿，Yuan live/基线） | E3（live/基线） | 保留（最高优先级，不得归档降级） | Yuan 硬性决定：live+基线。配方 S-gap>3%+amp前1/3+C1跳过+body=3+4槽+T+1开第3日收+32.28bp（recipes 52-64,160-166；rebaseline 2026-09-29 OOS2/train/valid/long +221.9/+71.4/+5.9/+729.9 MDD−5.5 SR4.15）。风险照实并列：holdout −19.1%~-19.8%（历史0分位、placebo R1 10.9%/R0 2.1%反向，F §1/G §0.7）、valid 50分位、PBO代理0.94/corr−0.97，但不改变地位。页面/调度/14:30链/流水全保留。 |
| S-05 | 星舰容量版（20/50/100M过滤+市值30/50/100亿+6/8槽+sqrt k150bp） | E3参数档 | 合并 | 不是独立策略，是 E3 在 200万下的执行版。50M long 341.1/kept592/1078/max part1%/P(>5%)=0（H2b §2.2），200万 4×25%单票5万，100→500万355→341→312缓坡（H2b §2.5）。合并为星舰B“容量/过滤/槽位”参数档（默认50M+4槽，A35用8槽），不再独立展示。 |
| S-06 | H2-a25 post-dense（25%H2+75%B3） | E3参数档 | 合并（REJECT底） | K3 FAIL −1.6pt（rebaseline:30），long+803%但 valid+4.0%/holdout−20.1%。合并为星舰B历史预设，默认不启用。 |
| S-07 | 纯B3组合 / 星舰H2 100% | E2对照 / E3激进对照 | 合并/归档 | 纯B3= E2 对照（+713%/holdout−19.3%）；H2 100%= E3激进对照（+1040%/MDD−29.3%/holdout−22.7%，证伪不进Live FINAL §1）。收为基线对照预设，不进主页面，归档到研究折叠区。 |
| S-08 | 星港 0.2 pre-dense（母港M50+卫星 w=0.2） | C（E1+E2+E3拼装） | 合并 | 本质是组合（底座母港M50+卫星），冻结 PASS 1/3 待复跑（rebaseline:51，H3观察）。数据收集默认档。合并到新组合层 C/A 原型，不再独立页面（harbor-segments 注释 Overlay 行即星港，保留解析但隐藏入口）。 |
| S-09 | 双子星 Twin Star（港湾×卫星 50/50） | C（E1+E3拼装） | 归档（feature-flag隐藏，不删除） | 不是独立源，是固定比例组合（recipes §4并行对照，不进Live；OPT-178已退役双子星，alembic 0050_remove_twin）。H2j 相关港湾×星舰 long 0.15真分散但双子星本身无独立alpha。归档：保留代码与历史 NAV，关闭默认入口，留 flag 供对照复算。 |
| S-10 | 切换 S1–S8（星舰/港湾 T−1 切换） | X（已证伪）→C拒绝 | 归档 | 8切换全 REJECT：OOS2亏20–190pt、valid永远输港湾44pt、holdout靠全程空仓exp0、DSR~0、邻域±15–90pt脆（H2b §3）。S1/S3 观察（holdout+0.5%仅因空仓，OOS2代价−20/−82pt long−300/−500pt），其余证伪。全部归档，组合层用固定比例+缓冲带代替切换。 |
| S-11 | 国家队 H2e-c（可执行510300触发）/托底S1/S2 | X | 归档（0%，只留冻结防守闸） | long+27.65%跑输BH+39% 11.4pt（H2e §2c），托底去重20/60日35–51分位抛硬币、S2放量追反向（H2e §2a）。裁决0%（FINAL §6.4/H2e §4.2）。保留已冻结 `national_team_gate`（300<MA200且4-ETF 20日净增≤0暂停新仓，休眠保险）防守闸，不配进攻仓。 |
| S-12 | 对手方开关 H2f（大单on/off）/ 资金开关 H2g（份额on/off） | X | 归档 | H2f on0.44 vs off0.42 t0.12 REJECT（F §4.2）；H2g A有效B反转 t−4.27 精确反转（H2g §5）。一律 REJECT 不进系统（FINAL §0.3注/§6.5）。归档为研究开关，默认关。 |
| S-13 | H01 5连阴 / H02 BIAS6 / H03 RSI30 / H05锤头晨星 / H07 MACD / H09黄昏星 / H10回踩MA10（超卖/形态族） | X | 归档 | 同一regime断裂：OOS2 t10–56 → valid t−20~−36翻转（H2 §3），Top10 valid−34%，H07 holdout−1.38% t−6.8。H3 证伪。归档到研究折叠区。 |
| S-14 | H11 S-gap liquid（绝对50M amp<4% + 相对4格） | X观察（E3远亲） | 归档（观察，不进组合） | 相对4格 OOS2 t−13~−17全灭，仅绝对版存活但 holdout binding失败 n=127 t−4.8、PBO0.83/corr−0.995（H2 §2）。观察，不得转正（H3 §1）。归档，复活需三条件（见§2.6）。 |
| S-15 | H13 大阴/跌停 / H08 跳空追涨 / H06 上升三法 | X观察 | 归档（观察） | H13 long每笔+2.43%最强但 valid−0.40% t−3.6显著负+停牌3.0%（H3 §2，复跑3/100 valid0/200不翻转）；H08 long正但 holdout−0.69% t−4；H06 n小（n=8 holdout，待n>2000）。观察，归档。 |
| S-16 | H04 KDJ字面 / H04r低位金叉 / H12低开 / H19大单 / H20融资开关 / H15小市值+ROE / H16价值 / H17户数 | X | 归档 | H04字面 n=0退化（J恒≥0）；H12四窗全负；H19 long显著负；H20门开精准滤掉反弹（long−0.36 vs关+0.68）；H15 OOS2+76%→valid−21%；H16 +23%→−8.4%；H17 n=19 underpowered（H2 §4/H3 §1证伪）。归档。 |
| S-17 | G小市值 N30 / G ETF动量 L60 / G择时MA / G事件POS / H14行业主线 / H18大折价 / H21转债 / H22调入 | X观察/证伪 | 归档 | G五方向全REJECT（G §2）：小市值+276%→−38%、动量Bonferroni p1.0、择时三档OOS2即输20pt、事件+2.22%→−4.01%；H14 scores仅7个月 valid−33%；H18/H21/H22无表blocked（H2 §4/H3观察）。归档到研究区。 |
| S-18 | D1/D2/D3/D3b overlay（90%+REPO/波动率目标/回撤刹车/季频） | X | 归档 | 样本外全未改善Sharpe（D §4）。归档，不进实盘。 |

重点说明（双子星/星港/星舰变体/港湾母港）：
- 双子星=港湾×卫星50/50的固定组合，对照用，无独立信号/持仓（与E1/E3相关即加权平均），OPT-178已退役+0050_remove_twin迁移，归档最彻底（代码留、入口关）。
- 星港=母港M50+卫星w=0.2的三腿拼装（harbor-segments/starport行类型即它），是当前数据收集默认档，合并到新组合层后自然消灭独立名（C200/A25即它的冻结升级版）。
- 星舰各变体一律不是新策略：默认B（post-dense）为真值锚（rebaseline 18-25），容量/市值/槽位/H2-a25/纯B3/H2 100%全部是“同一信号×不同过滤/仓位”，合并为星舰B面板里的下拉预设（默认50M+4槽；8槽仅A35；100亿市值仅研究）。
- 港湾/母港归属：港湾=E1纯腿（S-3+停车）；母港M30=E1×70%+E2×30%（H2d §1.2），两者0.998同源故合计≤70%（FINAL §0.3注），保留M30为默认包装，M50降为历史选项。

### 1.2 页面盘点（desktop-ui hash-SPA，唯一路由 `/` + hash）

| Hash/组件（路径） | 当前功能 | 归属 | 去留 |
|---|---|---|---|
| `#/dashboard` DashboardPage | 首页体检持仓/情绪/晨报 | C/D | 保留（合并为新主页面底座，见§2.4） |
| `#/backtest` BacktestPage（2748行）+ StrategyCatalogPanel（五档目录）+ HarborNavOverlay + FundFlowPanel + RecentDailyCompare + ReplicaGap | 策略回测 Timeline/归因/敏感性/paper对账；五档 OOS2/train/valid/long 目录表 | E1/E2/E3/C/X混杂 | 大改（本方案核心）：主区只留星舰B+E1/E2/E3+当前档组合+随机/300对照（见§2.5线框）；其余15+策略移到“归档/研究”折叠区默认收起；目录表 StrategyCatalogPanel 重写为三发动机+三档+基线对照；HarborNavOverlay 保留（港湾 NAV+熔断区）；FundFlowPanel 保留但用 DATAFIX 新标注（北向成交额/两融不完整警告）。MobileBacktestPage 同步。 |
| `#/watchlist` WatchlistPage + StrategyModeBar + SatelliteLegBlock（S-3池N/星舰M） | 自选池/S-3+卫星池，七档 StrategyMode 默认 starship_b | E1/E3执行 | 保留（14:30执行链不动；StrategyMode 七档收敛为三发动机+三档显示，Live恒港湾声明改为Live=星舰B（Yuan决定），见§4） |
| `#/scheduler` SchedulerPage（746行） | 调度作业状态/手动触发 | D/T | 保留（加健康检查入口，见§2.1） |
| `#/decision` DecisionPage + `#/alpha` AlphaTabs/Incubator + `#/industry-flow` + `#/market` `#/index` `#/news` | 决策快照/因子孵化/资金流/行情 | X/T/D | 归档/折叠：Alpha孵化（scout因子）移到研究折叠区；其余保留但从主导航降级（不在主页面占用首屏）。 |
| `#/journal*` `#/broker` `#/settings` SettingsPanel + StrategySettingsPanel | 日志/券商/设置（“只影响展示Live恒港湾”开关） | C/T执行 | 保留，但 StrategySettingsPanel 文案改为“只影响展示，live=星舰B（Yuan 2026-10-03）”，档位选项收敛到 A/B/C+星舰B基线。不碰下单逻辑。 |
| `#/screener`（hash有定义无组件） | 占位 | — | 归档（隐藏入口，不删除路由定义）。 |

### 1.3 调度任务盘点（services/data-sync-service/scheduler，56 jobs，Asia/Shanghai）

| Job（cron） | 功能 | 归属 | 去留 |
|---|---|---|---|
| satellite_live_panel 14:30/33/37（盘中快照） | 星舰14:30面板幂等快照 | E3执行 | 保留（Yuan链核心，不动） |
| watchlist_automation 17:30 + retry 20:30 + close_sync 17:10 + close_catchup 17-23/10min | S-3打分/卫星池；close落地 race 补跑（44/152 skip主因已修） | E1/E3执行 | 保留 |
| paper_s3_intake_CN/HK 17:42、paper_trading_intake/update 17:40/45、sleeve_paper_auto 18:20、paper_backtest_mirror 18:05、backtest_paper_recon 周一07:30 | S-3双线 intake、停车镜像、引擎轨迹mirror、周对账 | E1/E2/E3流水 | 保留（统一为三发动机 paper 流水接口，见§2.2） |
| sleeve_etf_daily_sync 17:25 + etf_daily_full_sync 月1日19:00 + etf_snapshot_sync 月2日19:30（DATAFIX新增） | 5只停车ETF日更、全市场月全量、41码月快照（etf_daily.csv 55207行至09-30） | D/E2 | 保留 |
| risk_state_sync 18:50（ETF份额/两融/北向）、bar_5min_close 18:40、index_daily_full 16:30、stock_daily_basic 17:20、cn_industry 18:15、factor_signals 18:30 | 数据层日更 | D | 保留（加 health_check 门，见§2.1） |
| allocation_decide 周一17:45、rolling_oos 月首周一、weekly_review 周一07:40（含DATAFIX §4b健康节）、trading_brief 10/12/14:30、decision_snapshot/outcome | 组合/滚动OOS/周报/简报 | C/D | 保留（allocation_decide 改为三档自动选档逻辑寄生处，不新建调度） |
| 其余（stock_basic周五、hk_daily、macro、news/alpha_radar、em_probe、webhook每分等） | 基础/另类/通知 | D/T | 保留（不动，仅归档页不再调用 scouting 结果）。 |

### 1.4 回测脚本/配置盘点

| 位置 | 内容 | 归属 | 去留 |
|---|---|---|---|
| services/data-sync-service/scripts/run_backtest / run_walk_forward(_dual) / run_monte_carlo / run_risk_state_gate | 单次回测/S-3三窗铁律/蒙特卡洛/风控闸 | T/E1 | 保留（回测引擎 v1.5 backtest_engine.py 不动；新增组合 blend 只加新脚本，不改旧） |
| scripts/eval_harbor_* / eval_starship_b / eval_homeport / eval_twin_* / eval_idle_* / eval_h2_* | 港湾×B3/卫星权重、B基线、双子星停车、H2硬化验证 | C/X | 归档（冻结为历史证据，README标注“已冻结，只读”，不再加入回测页下拉） |
| scripts/compare_* / diag_twin_* / analyze_starship_b / scout_*backtest / backtest_*专项 | paper对账/双子星诊断/星舰OOS拆解/因子孵化 | T/X | 归档（scout/compare/diag 全移到 research/ 或 archive/designs，只读不回写） |
| docs/backtests/stable/（37份冻结）+ rebaseline-dense-2026-09-29 + strategy-recipes（213行唯一可重建spec）+ strategy-params（S-3冻结值）+ validation-gates-v2 | 口径锚 | D/E | 保留（recipes/params/gates 一字不动；stable 只增新组合 freeze，不改旧） |
| karios-research/research/indicator_series（BRIEF限定独立框架）+ docs/backtests 22md+18子区 | 同源镜像+外围指标 | X/T | 保留只读（明确非第二数据源，不接入主页面；新组合不依赖它） |
| data/etf/etf_daily.csv（services内55207行）+ service/etf_snapshot.py + scheduler/etf_snapshot_job.py | 月快照自动化 | D | 保留 |

## 2. 目标架构

```
数据层（D，健康检查 fail-closed）
 → 三台发动机（E1/E2/E3，统一接口：信号/持仓/paper流水/vs随机）
 → 组合层（C，按总资产自动选档 A/B/C，缓冲带+熔断，月度再平衡单）
 → 一个主页面（今天做什么） + 精简回测页（只留可用+双基线对照）
```

### 2.1 数据层（含 DATAFIX 健康检查）

- 范围：daily（23994624行至09-30健康，中秋09-25 CN 0为休市非缺数）、amp_1430（81608行 dense 5018/日）、bar_5min（58821148行）、套筒5 ETF daily全、etf_daily.csv（已回补至09-30 55207行）、cn_etf_share（8056行）、index_daily（20903行）、stock_dailybasic、cn_moneyflow、北向（hsgt断裂标注+hk_hold至09-30 HK-only 1703411行，CN日变化不可做）、两融（SSE-only不完整日聚合过滤+UI警告，节后自动补）。
- 健康检查：复用 DATAFIX `scripts/data_health_check.py`（15表行数/最大日期/相对 trade_calendar 滞后>2标红 exit2 + 两融跳变/覆盖报警）+ `weekly_review §4b` 自动带快照。组合层/主页面/调度页统一调用：滞后>2日、amp滞后、连续skip、ETF CSV超35天未快照即 fail-closed 暂停新手只做 paper（FINAL §0.4/E §5）。
- 分支状态：DATAFIX 在 `fix/data-sync-2026-10 @ e9305ec3`（未push未合main，16文件），重整 PR 必须先合它（或 rebase 它），否则健康门无数据。

### 2.2 三台发动机（统一接口，星舰B为基线）

每台发动机统一暴露（只展示+paper，不碰实盘下单）：
`信号（T−1，规则≤2参） / 持仓（权重/槽位/单票上限） / 模拟盘流水（paper_trades 20笔口径） / 与随机安慰剂对比（分位） / valid+holdout并列（不许只引long）`

- E1 S-3选股（港湾/母港股票腿）：信号 S-3 score≥65 + Live冻结 gates（D2关回测0/paper45、D3 uptrend1.25/fan0.75、止损−5%、trailing−8%、RS0.5/diverging1.0/冷却2/熔断−25%仅CN，HK独立线 regime/RS0.6/trailing−12%/T+2）；持仓 10腿×10%槽位（M30袖7腿），单票上限组合2.5–5%（档位见H2j §2）；停车 trail8/hyst0.02冻结；T收→T+1开；成本32.28bp+转移5bp/边。证据 H2d §1.1/§3.2。
- E2 ETF逆波（B腿/B3，月调最扛跌）：信号 60d逆波月首调仓（42/60/78 ±0.9pt取中60，月频最优+12pt D §3.4）；持仓 B腿3-ETF或B3 5-ETF，B3 OIL权重上限10%（月调>20%砍半），OIL持仓日<5000万或参与度>5%当日降停车50%余款REPO；成本5bp/边。证据 H2d §3.3/D §3.2。
- E3 星舰B短线（live/基线，默认post-dense）：信号 S-gap>3%+daily amp前1/3+C1跳过（14:30/open−1>3%）+body=3+日成交≥50M过滤+4槽（A35用8槽）；T+1开买第3日收卖；14:30 print才买，无print/涨停锁当日不买 fail-closed（params 307-318），满body=3第3日14:30 print卖，跌停按 exit_skip_limit_down 冻结顺延；成本32.28bp+sqrt k150bp（200万档，long−49pt已扣）；闲置 cashShare=w(T−1)×100% {511260+518880+513100}逆波60d月频。证据 recipes 52-64/H2b §2.2/rebaseline 18-25。主页面同时显示 vs星舰B（100%口径基线）与 vs随机；E3卡片旁常驻风险条：holdout −19%/valid 50分位/PBO代理0.94/复活待H2k（照实提示，不降级）。
- 统一 paper：三台各维护 `paper_trades` 同 recipe 落账 + `verify` 对账链（E §3 P0），周报 §0.7 第5行对齐笔 diff（目标20笔看均值）。

### 2.3 组合层（按账户总资产自动选档，固定比例不切换）

沿用 H2j 三档（同一保守成本：卫星32.28bp+sqrt150bp，转移/B腿5bp/边，切换15bp/次，月再平衡）：

| 档 | 资金（总资产） | 配比（固定，不随行情改） | 单票上限 | 预期/MDD（blend+MC，详见H2j §2/§7） |
|---|---|---|---|---|
| A进攻默认A25（MDD25%） | 100–150万（当前默认，配置100–120万） | 40%港湾+20%M30+20%B3+20%容量星舰（50M 4槽；激进A35 30/15/5/50 8槽仅备选） | 2.5%组合（A35 6.25% 8槽稀释） | long+163.5%/−12.1%（真MDD约−17~−19% proxy低估已注）；valid+27.6%/holdout−8.6% |
| B过渡（去星舰） | 150–200万 | 50%港湾+30%M30+20%现金（B3/货基在现金内月调，星舰0%直到C） | 槽位10%，组合单票≤5%（港湾+M30合计≤70%） | long+136.9%/−17.1%；valid+35%/holdout−8.3% |
| C保守（FINAL 200万系统原样） | 200万+ | 60%M30 120万+20%港湾40万+10%B腿/货基20万+10%容量星舰20万（50M 4槽×5万；前置未满星舰0%回填） | 2.5%（5万/200万），B3 OIL≤10% | long+158.3%/−14.4%；holdout保守−6.7%/平衡−8.3% |

- 自动选档：月末收盘看总资产，A→B需≥155万、B→A需≤145万（±5万缓冲），B→C需≥210万、C→B需≤180万（30万缓冲），带内保持；只在每月首个交易日执行（与B腿同频5分钟，5bp/边）；切换成本15bp已扣（2年均1.0次/p95 3次）。熔断：前向60天−15%降档（H2j比FINAL −10%更松，A→B→C→全货基）、单笔−8%暂停新手、holdout扩到−25%全停、连续3笔同日全亏或单周−5%降半仓余款REPO、200万OIL条（见§3）。判据全T−1。国家队/H2f/H2g/切换开关一律不进档。
- 月度再平衡单： ETFs whole股，S-3 10%槽位无1手问题；B3 OIL>20%砍半至10%；OIL<5000万当日降50%。

### 2.4 一个主页面（今天要做什么）

在 `#/dashboard` 上叠新 `HomeToday` 卡（只加不删，旧体检卡保留）：
首屏四块，一屏看完（≤5分钟）：
1. 今天要做什么（14:30名单）：E3卫星名单（有print才买，无print/涨停锁/C1跳过 fail-closed）+ E1 S-3 T收→T+1开提示（不追14:30）+ E2本月是否调仓（非调仓日显示“无动作”）。漏一次即跟踪误差提示（152次44 skip先例）。
2. 本月再平衡单（仅每月首个交易日出现）：当前档A/B/C+总资产+目标权重+补/赎股数（whole股）+预估5bp成本；非调仓日显示上次执行+下次日期。
3. 周五5行表（FINAL §0.7复用）：NAV+两腿贡献+超额 / fills胜率+缺print率+覆盖率 / 滑点vs32.28bp+15bp敏感 / 数据版本（daily/amp/ETF CSV最大日期行数+qfq记录） / paper-vs-backtest diff。并列valid/holdout，不许只引long。
4. 各发动机健康度与复活状态：E1/E2/E3三灯（绿/黄/红）+ vs星舰B超额 + vs随机分位 + 暂停线（60天−15%/单笔−8%/holdout−25%/数据链）+ 星舰复活进度（paper n/20 + holdout收复至−10%内 + 过滤版valid转正，待H2k细化，当前显示“未满0%观察”但基线地位不变）+ 国家队闸休眠提示。

### 2.5 新回测页线框（大幅简化，反映真实筹码）

- 顶部控制条（默认真实资金可配置，当前110万）：资金输入（默认110万）+ 档位自动显示（A/B/C）+ 成本口径固定显示（卫星32.28bp+sqrt150bp / B腿5bp / 切换15bp，不可改）+ 窗口切换（long/valid/holdout并列，默认并列，不许单选long）。
- 主区（只留可用，4卡+2对照）：①星舰B基线卡（live，100%口径，附风险条holdout−19%+H2k待定）②E1卡 ③E2卡 ④当前档组合卡（按顶部资金自动切A/B/C，显示40/20/20/20等权重+单票上限+OIL降仓状态）。每卡统一四数：long/valid/holdout（扣费后）+ MDD/SR + vs星舰B超额 + vs随机分位（E1 500次/H2d §3.2，E3 15000次/F §1）。图表只留三张：NAV叠沪深300（含valid/holdout shading）、分年条（2022/23/24/25/valid/holdout，去最好20天敏感性可展开）、MDD/回撤带。删除（折叠）：所有H01–H20/G五方向/切换S1–S8/国家队/H2f/H2g单卡、单窗t值大表、邻域16扰动大表、DSR/PBO全格（只留结论一行+链接到stable）。
- 归档/研究折叠区（默认收起）：标题“归档/研究（15证伪+9观察，不进组合）”，内列H3六透镜总表一行一条+跳转stable冻结报告；切换/容量115变体只留一句话（“过滤越严long越低，切换靠空仓”）+ H2b链接。移动端同构。
- 保留指标：total/MDD/SR/Calmar/CAGR、valid/holdout并列、placebo分位、+15bp压力（星舰−40pt/港湾−5pt）、容量参与度（P(>5%)、max part）、成本实现滑点。删掉或折叠：OOS2单窗排名、单事件每笔t大表、技术指标90+因子库入口、Alpha孵化Tab入口（移到研究区）。

### 2.6 预注册升级规则（候选何时能取代星舰B成为新live/基线）

在找到更好方案前星舰B即live；任何候选（含E1/E2加权、新挖掘）取代必须同时满足（holdout n≥50后重裁，门控G3c）：①paper 20笔同recipe均值>0且跑赢星舰B同期paper均值；②holdout收复（星舰过滤版valid转正>0且holdout收至−10%内，H2b §2.7/G §4）；③邻域稳定（C1±33% ±2pt内、body/r_wide/gap不翻转，B §3）；④真placebo≥95%（同F 15000次口径）+ Bonferroni（N≈126 t≈3.5）后显著或至少不显著负；⑤容量（单票P(>5%)=0，200万max part<2%）。H11/H13/H08三条件集齐另行追加profit-leads（H3 §4）；切换S1/S3需holdout n≥50仍空仓避险且OOS2代价<10pt（H2b §4）。未满足前一律保持星舰B live，候选最高只给0–20%观察仓（A25 20%上限，超20%即自杀 H2j §4）。

## 3. 冻结参数清单（来自FINAL/H2j，不许改）与停用/复活位置

冻结清单（改任一即新策略，需重走门控v2+留档，平时不动）：
- E2：B腿60d逆波月频（42/60/78中间60，D §3.4月频最优+12pt）、M30 w_harbor=0.7（M20–M50单调无悬崖取中，H2d §3.3）、B3 OIL上限10%（>20%砍半）。
- E3：S-gap>3%、daily amp前1/3、C1 3%（±33% ±1.6pt真平台，B §3）、body=3（body4陷阱冻结不动）、日成交≥50M、4槽（A35 8槽稀释）、T+1开第3日收、32.28bp+sqrt k150bp、14:30 print/C1/涨停锁fail-closed、跌停 exit_skip_limit_down冻结口径。
- E1：S-3 score65/max_hold60/D2关（回测0/paper45）/D3 uptrend1.25 fan0.75/止损−5%（Strong ATR×2）/止盈100%不开/exitscore0不开/trailing−8%（Strong ATR，HK −12%/T+2）/gates full/RS0.5/diverging1.0/冷却2/熔断−25%仅CN/滑点0（成本模型已含）/mp10/position10%/剔除300/neutral_block开/entry auto（RS0.7+dip3%）/国家队闸开（strategy-params §1；停车 trail8/hyst0.02 pick-strong-track §3）。
- 组合：月再平衡5bp/边、切换15bp/次、缓冲145/155与180/210、熔断60天−15%/单笔−8%/holdout−25%/连续3笔全亏降半仓（H2j §3，60天−15%比FINAL −10%更松已注）。
- 成本：卫星32.28bp/笔（+15bp压力星舰−40pt/港湾−5.46pt/M30−3.82pt）、冲击sqrt k150bp（long−49pt 390→341）、REPO摩擦1.6%/年。

停用/复活规则位置（写死，不随行情改）：
- 组合熔断与降档：本方案§2.3（源H2j §3 + FINAL §0.6），执行位置 `allocation_decide`（周一17:45）+ 主页面健康灯 + 周报§0.7。
- 星舰观察仓（档位内权重）复活：FINAL §0.6.5/G §4/H2b §2.7（paper20笔同recipe+holdout收复至−10%内+过滤版valid转正，否则0%），最终细化由并行H2k给出，本方案主页面预留“H2k复活进度”槽位，H2k未到前按此三条件执行。
- 压舱失效：FINAL §4（forward 60天−10%或两腿同亏相关>0.4持续3月，或港湾/母港valid转负，或OIL连续两月第一大且跑输B3超5pt，即转全货基并复盘）。
- 观察仓（H11/H13/H08/H06/切换S1/S3）复活：FINAL §4（paper20笔均值<−1%/笔或holdout扩到−25%或再出单笔−8%即清零12个月不再启用；H11/H13/H08需holdout n≥50收复+邻域稳定+真placebo 95%上才谈paper）。
- 数据链暂停：FINAL §0.4/§0.5/E §5（daily滞后>2日/amp滞后/连续skip/ETF CSV超35天即停），检查命令 `PYTHONPATH=src .venv/bin/python scripts/data_health_check.py`（DATAFIX §4）。

## 4. 迁移步骤（4个小PR，每步可回退、带测试，先加后归档，不碰实盘下单）

原则：只加不删→再归档；每步独立分支+测试+回退（revert commit或feature-flag关）；全程不碰 `broker`/下单/券商连接（BrokerPage dynamic不动，strategy_today只展示）。

- PR1 新组合层库（只加，2–3天）：新增 `service/combo_tiers.py`（纯函数：输入总资产→输出档A/B/C+目标权重+再平衡单，复用 harbor/homeport/parking_replay，无DB写）+ `tests/test_combo_tiers.py`（H2j三档权重/缓冲145/155/180/210/熔断60天−15%单测，复用 h2d_combo.py 期望值±1pt）。回退：删文件+flag关。验收：pytest通过，不调用下单。
- PR2 主页面 Today卡（只加，3–4天）：在 DashboardPage 叠 `HomeToday`（四块见§2.4，读现有 strategy_today/catalog/watchlist-automation/health_check接口，不新建调度；allocation_decide只读调用PR1纯函数）。前端 vitest + typecheck（DATAFIX §5本机无node需在CI跑）。回退：flag隐藏Today卡。验收：默认110万显示A25，vs星舰B与vs随机双列，valid/holdout并列。
- PR3 精简回测页（加flag+折叠，3–5天）：BacktestPage 主区改四卡+双对照（§2.5线框），旧15+策略卡移到“归档/研究”折叠区默认收起（不删组件，加 `showArchived` flag默认false）；StrategyCatalogPanel重写为三发动机+三档+基线（旧五档表归档为历史快照）；FundFlowPanel合入DATAFIX标注（北向成交额/两融警告）；MobileBacktestPage同构。测试：backtest/catalog/harbor现有单测+新增归档折叠单测。回退：flag开回旧版。验收：默认只见可用，归档区可展开溯源stable。
- PR4 归档旧入口+文档冻结（删减，2天）：隐藏 `#/screener`、Alpha孵化Tab降级、StrategySettings七档收敛为A/B/C+星舰B基线（文案live=星舰B）、旧 eval/compare/diag/scout脚本README标“已冻结只读”+回测页下拉移除（文件移到 archive/ 或只改索引，不删历史行）；合 DATAFIX分支（e9305ec3）+ 跑全量后端176+34单测（DATAFIX §2）+ CI补前端vitest。回退：revert索引提交。验收：调度56 jobs不变，health_check绿，paper流水不断。
- 工作量估计：共10–14人天（含测试与CI补跑，不含H2k）。顺序PR1→PR2→PR3→PR4，不并行合main；每步 koris-desktop分支+未push前本地验（呼应DATAFIX未push纪律）。

## 5. 风险与需Yuan拍板的问题（最多5个，每个给推荐项）

1. live名义：FINAL写Live=港湾，本方案按你决定切live=星舰B（展示/文案/基线全切）。推荐：批（切），但组合内星舰权重仍按A/B/C+前置0%执行（paper 4/20未满，当前A档20%上限），名义与仓位分离，风险条常驻。备选：名义切但权重0%直到H2k。
2. 默认档：当前100–120万用A25（40/20/20/20，MDD25%）还是更稳B0（50/30/20现金，星舰0%）？推荐：A25（无条件0.02%/条件1.2%破产率双通过，412天中位到200万，holdout−8.6%只比保守深1.9pt，H2j §4/§7）。备选：怕波动先B0（valid+35%最强，但2年62%条件胜含单窗动量倾斜已声明）。
3. 星舰仓上限：A档20%（A25）还是50%（A35 8槽）？推荐：20%封顶（超20%即自杀：A35 valid/holdout双输+条件2年腰斩17.9% vs 43.6%，H2j §4）。备选：8槽稀释后30%试运行（需H2k点头）。
4. 熔断松紧：60天−15%（H2j进攻容忍）还是−10%（FINAL保守）？推荐：−15%（覆盖进攻噪声，180–210缓冲已大，误切少），但单笔−8%/单周−5%降半仓不动。备选：−10%（更早停，踏空多）。
5. H2k前复活口径：沿用paper20+holdout收至−10%+过滤valid转正（FINAL/G），还是等H2k细化后再定观察仓？推荐：沿用旧三条件先跑（H2k槽位已在主页面预留，H2k一到即替换）。备选：H2k前星舰组合权重一律0%（只看基线不持仓）。

## 附录：证据版本与纪律

- 证据版本：karios-desktop 2026-10-03现状（daily 23994624行至10-02/CN至09-30、amp_1430 81608至09-30、bar_5min 58821148至09-30、cn_hk_hold 1703411至09-30、cn_margin_total 3668、etf_daily.csv 55207至0930）；卫星以rebaseline-dense-2026-09-29为准（OOS2/train/valid/long +221.9/+71.4/+5.9/+729.9 MDD−5.5 SR4.15 holdout−19.8），港湾/母港以H2d h2d_nav.json为准；archive/designs只读不回写；源仓未改一字；种子42/123。
- 赚钱线索（profit-leads规则执行）：本轮为重整规划，未跑任何新策略回测、未发现新的“扣费后每笔为正/明显优于随机”线索。故未向 `~/Projects/wealth-ideas/profit-leads.md` 追加（注明来源本次RESTRUCTURE，特此声明无新增）。若未来H2k给出复活新证据或候选满足§2.6五条件，另行追加。
- 约束遵守声明：本文件只写方案，未改代码未改参未下单未下载未push；永不用Crimson代码数据；未读.png/.jpg；未杀他人进程。
