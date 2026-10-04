# Karios 回填四腿测试报告（只读研究，2026-10-04）

## 5行结论
1. 四个方向数据都补上了：红利低波个股53只（tx日线+tushare股息率+分红公告/除权日）、港股30只H+10对小AH双腿对齐、LOF10只价+净值、转债449只估值+319只强赎快照；东财push系全挂改用新浪/腾讯/集思录/tushare/baostock，缺的只有港股市值表和转债历史公告日（已声明用快照代理）。
2. 没有可用的新腿：9个新测（红利3、港股3、LOF2、转债强赎过滤1）在 long/OOS2/valid/holdout 下没有一个三窗同时超过M30（M30连续=+152.6/+48.5/+12.4/-7.3），组合18个（M30+腿80/20、70/30、60/40）收益回撤双超的只有LOF系6个但全靠偷看嫌疑+延迟崩，不推荐。
3. 最接近的是红利股息率DIV_YIELD10（纯股息率Top10月持）：long+198.8%超M30约46pt、日相关0.18/月0.10双过线、走廊7/10、成本×2和+1天全活，但OOS2+32.7%输16pt、valid+0.5%输12pt，不算找到，只给观察。
4. LOF折价表面最亮但不可用：LOF_DISCOUNT3 long+587%/valid+19.4%/holdout+2%三窗全超、走廊9/10、MC p5+15%，但OOS2+22%输26pt、延迟+1天long塌到+132%、valid转-17%，诚实ann审计+270%→+98%腰斩，和NICHE的lag1偷看陷阱同构，不给观察。
5. 建议组合不动：基线仍=M30，港股三腿（long+0.7~+44%、valid-11~-27%、MDD37~46%）和转债强赎过滤（long+64.7%、valid-9.7%）全灭；红利低波不单列（已有DIV_LV1观察，股票版未超）。图见backfill_legs.png（log净值，只生成未打开）。

## 口径（先看，避免误读）
- 尺子同COMPARE硬标准：long 2021-08-02~2026-08-07（1215天）、OOS2 2024-08-01~2025-08-01（242天）、valid 2026-03-02~2026-08-07（109天）、holdout 2026-08-10~2026-09-30（36天），120万起（NAV归一，收益%与起点无关；200万参与度另算，全<0.8%可用），月频次月首日收盘执行（信号用上月末收盘PIT，T→T+1），LOF日频逐日T+1，转债周频周首执行。
- M30对照用scratch/compare/compare_nav.npz连续切片：long+152.62/OOS2+48.53/valid+12.40/holdout-7.30（long同账本fresh+152.6；OOS2/valid/holdout与fresh+42.4/+35.0/-8.8差6~22pt为携带vs空仓路径差，结论两尺子一致，每个REJECT在两尺子下都至少挂一窗）。
- 成本：A股16bp/边（32bp往返，含佣印），港股20bp/边（含印花税13bp卖出+港股通费+0.92FX恒定代理±3%），LOF/ETF 5bp/边，转债20bp（40bp对照）；A股T+1（月持≥1月满足），涨停锁10%（主板，high==close且≥9.4%跳过，实测5年影响<0.3pt，THIRD_LEG同）；港股无涨跌停，LOF 10%同ETF，转债不滤（30%极少）。
- 账本已读：STRATEGY_LEDGER全文+THIRD_LEG+NICHE_QDII+CAPACITY_MAP；DIV_BH/DIV_LV1为ETF版（510050+512800买持/5防守ETF低波），本轮股票版为 genuinely new；plain转债双低周频已REJECT只测强赎过滤新变体；以上verdict沿用未重跑。
- 未碰Crimson、未开可见浏览器、未读png/jpg（图只生成未打开）、只杀自己进程、无git push/merge；并行作业文件未碰（qdii_premium/capacity/niche只读复用NAV逻辑，未动其文件）。

## Step1 数据交代（说准，不编）
- venv：~/Projects/wealth-ideas/.venv-data（akshare1.19.1+tushare1.4.29+baostock0.9.4+pyarrow25.0.1+matplotlib3.11.2，tuna镜像，无全局安装）。
- tushare token：~/.tushare/tk.csv存在（56位），daily_basic/cb_daily/fund_nav/dividend可用；hk_daily限频1次/小时（探针FAIL，批量不用）；cb_call无权限（探针无权限，已记录）。
- 拉到：div 53只tx日线1393行/只+tushare daily_basic dv_ttm1393行/只+dividend 26~100行/只+baostock日线+分红（20.4MB，372文件）；hk 30只H sina日线+20对qq历史+10对小AH的A腿tx+HSI（9.7MB，64+10文件）；lof 10只sina价+10只tushare NAV1397行/只（2.5MB）；cb本地356k+382k行复用+jsl快照30行+redeem319行（含强赎触发比130/剩余规模/天计数15/15|30/状态）+tushare 20券日线（2.5MB）。各SOURCES.md见data-backfill/<topic>/。
- 拉不到（明说）：东财push2/datacenter系（fhps_detail_em/value_em/zh_a_hist/hk_hist/hk_spot_em/lof_spot+hist/cov_info/value_analysis）SSL全挂；港股市值PIT表无；转债历史公告日无（用快照代理全程剔除，偏保守）；AH 220对只拉前30H+10小AH双腿（非全量）；全市场5000只未拉（53只采样，幸存者偏乐观已声明）。
- PIT：分红用announce信号ex执行（ths实施公告+cninfo除权+tushare ann/ex+baostock预案/除权双日）；股息率用上月末dv_ttm；LOF溢价=收盘/已公告NAV-1（ann<=信号日才可用，nav_date不用，诚实lag见审计）；转债过滤4条全只用信号日前（已公告快照全程剔除+近30天转股价值>=130天数>=15推断+剩余<3亿剔除+最后交易前5天）。

## 9候选设计（全PIT， genuinely new）
1. DIV_HY_LOWVOL10：53只中上月末dv_ttm+60d低波复合排名Top10等权月持（红利低波股票版）。
2. DIV_HY_LOWVOL15：同上Top15（邻域）。
3. DIV_YIELD10：同上纯股息率Top10（邻域/对照）。
4. HK_LOWVOL10：30只H中60d低波Top10等权月持。
5. HK_SMALL30_EW：30只H等权月持（中小盘代理）。
6. AH_DISCOUNT5：10对小AH中A/H溢价（A/(H*0.92)-1）最高5只H等权月持。
7. LOF_DISCOUNT3：10只LOF中折价（收盘/已公告NAV-1）最低3只等权日持T+1。
8. LOF_DISCOUNT2：同上Top2（邻域）。
9. CB_DLOW_NOCALL：449只中双低（price+prem）底20%（≤20只）+强赎4过滤周持20bp。

## 主表（扣费后，连续尺子；差=M30连续基线之差pt）
| 候选 | long/ann/MDD/Sharpe | OOS2/valid/holdout | vs M30 long/valid/holdout差 | 日/月/最差月相关 | 随机分位long | verdict |
|---|---|---|---|---|---|---|
| DIV_HY_LOWVOL10 | +140.7%/20.0%/-18.6%/1.18 | +33.3%/-0.1%/+0.4% | -11.9/-12.5/+7.7 | 0.149/0.112/0.315 | 100% | REJECT（long+valid挂两条，valid零） |
| DIV_HY_LOWVOL15 | +112.2%/16.9%/-16.6%/1.02 | +26.7%/+4.0%/+2.1% | -40.5/-8.4/+9.4 | 0.159/0.084/0.268 | 100% | REJECT（long大输40pt） |
| DIV_YIELD10 | +198.8%/25.5%/-17.7%/1.22 | +32.7%/+0.5%/-1.2% | +46.1/-11.9/+6.1 | 0.181/0.100/0.319 | 100% | REJECT（最接近：long超46pt双相关过线，但OOS2输16pt+valid输12pt，不给PASS只观察） |
| HK_LOWVOL10 | +8.8%/1.8%/-41.5%/0.19 | +48.2%/-11.4%/-0.1% | -143.8/-23.8/+7.3 | 0.286/0.420/ -0.02 | 80.4% | REJECT（long输144pt，月相关超线） |
| HK_SMALL30_EW | +0.7%/0.2%/-46.2%/0.14 | +61.7%/-12.4%/-2.2% | -151.9/-24.8/+5.1 | 0.330/0.466/-0.01 | 100% | REJECT（long≈0，月相关0.47超线） |
| AH_DISCOUNT5 | +44.4%/7.9%/-37.5%/0.39 | +80.9%/-26.6%/-3.5% | -108.2/-39.0/+3.8 | 0.252/0.364/0.03 | 95.4% | REJECT（long输108pt，valid-27%崩） |
| LOF_DISCOUNT3 | +587.0%/49.1%/-13.7%/2.20 | +22.4%/+19.4%/+2.0% | +434.4/+7.0/+9.3 | 0.247/0.234/0.09 | 100% | REJECT（表面三窗超但OOS2输26pt+延迟崩+诚实腰斩，不给观察，见下） |
| LOF_DISCOUNT2 | +861.8%/59.9%/-16.0%/2.34 | +39.9%/+15.4%/+3.9% | +709.1/+3.0/+11.2 | 0.243/0.230/-0.05 | 100% | REJECT（同上，延迟long+862→+83崩778pt，valid+15→-29崩） |
| CB_DLOW_NOCALL | +64.7%/10.9%/-13.7%/0.83 | +32.7%/-9.7%/+0.0% | -87.9/-22.1/+7.3 | 0.292/0.294/0.07 | 100% | REJECT（long输88pt，valid-10%挂，过滤未救回plain） |

注：vs账本fresh（M30=+152.6/+42.4/+35.0/-8.8）结论不变：DIV_YIELD long超46pt但OOS2输10pt仍挂；LOF三窗超但OOS2输20pt仍挂；其余同向。

## 严格细账
- 邻域：DIV TOP8/10/15 long+140/+199/+112（量级差87pt，Top10最亮但valid全挂）；HK LOWVOL10/EW/DISCOUNT long+8/+0.7/+44（整片输100pt+）；LOF TOP2/TOP3 long+862/+587（差275pt，短持越集中越高，脆）；CB单变体（plain已REJECT，过滤+5.5pt仍输88pt）。
- 成本×2：DIV全活（-4pt内）；HK全活（-3pt内）；LOF DISCOUNT3 +587→+379活、DISCOUNT2 +862→+526活（活但-208~-336pt）；CB +64.7→+50.5活。
- 延迟+1天：DIV全活（DIV10 long+140→+152反升，YIELD+199→+191稳）；HK LOWVOL+8→+24反升（无edge）、EW+0.7→+3.9、AH+44→+50（小票延迟噪音）；LOF崩（DISCOUNT3 long+587→+132崩455pt、valid+19→-17崩36pt；DISCOUNT2 long+862→+83崩779pt、valid+15→-29崩44pt）→ LOF不过延迟线。
- 走廊6M赢M30块数：DIV10 6/10、DIV15 6/10、YIELD 7/10（最稳）、HK 2/1/4、LOF 9/8（赢块多为M30负块+LOF单年2022+65%/2025+60%）、CB 4/10。
- MC1年p5/中位：DIV -6~-9/+17~+25、YIELD -7/+25、HK -29~-36/-2~+4（最脆）、LOF +15~+17/+52~+64（最抗但延迟崩已证伪）、CB -8/+11。
- 最差7月（M30均-6.12%/月）：DIV +2.1~+2.6少亏转正、HK -1.9~-3.5、LOF -0.5~-1.0少亏、CB +0.44唯一转正但量小。
- 分年：DIV_YIELD 2021+14/2022+23/2023+32/2024+52单年强、2025+6/2026-2熄火（2024依赖）；LOF每年+22~+91无单年依赖但延迟崩说明是日内微结构非年alpha；HK 2024+25~+72单年依赖（2024-07~12 +40~+81占全部）；CB 2021+19/2024+19/2025+22分散但2026-9。
- 诚实审计（LOF）：TRAP nav<sd +587% vs HONEST ann<=sd +270% vs HONEST ann<=sd-1 +98%（腰斩），valid +19→+18→-2，holdout +2→+6.5→-7.3；即多滞1天就从三窗超变三窗输，与NICHE lag1(+864%/+4700%)→lag3(-11%/+74%)同构，溢价只能当成本闸不能当信号。
- T+1/涨停：A股月持满足T+1，涨停锁影响<0.3pt（THIRD_LEG 5年4天）；HK T+0按T+1保守；LOF T+1满足；转债T+0按T+1保守。

## 组合（M30+腿月再平衡5bp/边；必须三窗收益和回撤全超M30才推荐）
| 组合 | long收益/MDD | valid收益/MDD | holdout收益/MDD | 过线 |
|---|---|---|---|---|
| M30连续基线 | +152.6%/-16.8% | +12.4%/-15.4% | -7.3%/-10.0% | — |
| DIV系80/20-70/30-60/40（3腿×3档=9个） | long全输M30 5~40pt（YIELD系+183~+205%仅long过，valid/holdout挂） | valid全输2~12pt | holdout偶赢+1~+3pt但收益回撤不同时超 | 0/9过线，不推荐 |
| HK/AH系9个 | long全输80~150pt | valid全输20~40pt | holdout偶赢但long大输 | 0/9过线，不推荐 |
| LOF系6个 | long+229~+384%全超、valid+14~+17%全超、holdout-2~-5%全超收益，回撤long10~13%<16.8%、valid11~13%<15.4%、holdout7~8%<10%全改善 | 表面6/6过线 | 但OOS2 5/6输（DISCOUNT3系全输5pt）、延迟崩+诚实腰斩已证伪，不推荐 |
| CB系3个 | long+100~+130%输20~50pt | valid+5~+8%输4~7pt | holdout-5~-6%输 | 0/3过线，不推荐 |

→ 表面6个LOF组合过线但全靠偷看嫌疑+延迟崩，剔除后18个有效组合0个满足，组合不动。

## 容量（120万/200万，参与度=仓位/日成交额）
- DIV 53只：120万中位0.010~0.015%/p95 0.06~0.08%/max0.29~0.44%，200万中位0.016~0.025%/max0.49~0.74%，全<2%可用（银行大票0.01%，512800系0.2% max仍可用）。
- HK H：sina amount常0无成交额，容量用A腿代理（小AH的A 120万中位<0.1%/max<1.5%可用）；港股通20bp+FX已扣，200万需按5%线降半仓（H小票淡日）。
- LOF：价amount元中位10万~1亿，120万Top3集中单腿40万在161226（日均千万）约4%、淡日>10%需拆2天或降Top5；200万淡日159518（697行小池）>20%不可集中，需限Top5+拆单。
- CB：双低分散20只，单券120万约6万/券，剩余>3亿滤后中位参与<0.5%可用；200万需<5%线（小券拆2天）。

## 裁决与记录
- 9候选全REJECT（DIV：YIELD最接近但OOS2+valid挂，不给PASS，YIELD给观察（低相关+WF7/10+延迟稳，但valid薄弱，需full市场+50笔重裁）；HK：亏+相关挂；LOF：延迟崩+OOS2挂+诚实腰斩，不给观察；CB过滤：long-88pt未救回）。
- profit-leads：本次无新增（DIV_YIELD随机100%但跨窗不一致+valid挂；LOF随机100%+走廊9/10但延迟崩+OOS2挂+诚实腰斩，不满足跨窗一致+延迟线；特此声明）。
- 基线仍=M30，Live按Yuan定，观察仓0%（DIV_YIELD观察亦0%，需另起full市场验证才有名分）。

*证据版本：karios-desktop现状（未改一字，未push未合main），跑数只在wealth-ideas scratch/backfill（run_backfill_legs.py+audit_lof.py+make_chart.py，种子42，DB只读，ETF harbor未动，图只生成未打开，只杀自己进程）；图见backfill_legs.png（7线log净值，未打开）。*

KARIOS BACKFILL DONE
