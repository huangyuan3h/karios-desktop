> **CONFIDENTIAL / DO-NOT-PUBLISH**
> 内部研究文档，位于 `karios-desktop/docs`。禁止进视频脚本、小柚公开稿、B站/知乎/头条文案。
> 基线 M30 仅作为 NAV 对照（`compare_nav.npz`），本文不复述其构造规则。

# WEAK3 HARDTEST（2026-10-05，vs M30 COMPARE）

- 日期：2026-10-05（Asia/Shanghai），种子 42，预注册 `~/Projects/wealth-ideas/scratch/weak3_hardtest/prereg.json`（main+邻域，跑前冻结，无扫参）。
- 尺子：COMPARE 四窗 long 2021-08-02~2026-08-07 / OOS2 2024-08-01~2025-08-01 / valid 2026-03-02~2026-08-07 / holdout 2026-08-10~2026-09-30，120万起，股票往返 0.30% 基准 + 2× 压力，涨跌停锁不可成交跳过，停牌无交易，信号 T 收盘 PIT，执行 T+1 开盘或更晚。
- M30（`compare_nav.npz` 连续切片，`cal`/`m30`）：long **+152.62%** / OOS2 **+48.53%** / valid **+12.40%** / holdout **−7.30%**（ann 21.19/50.98/31.03/−42.04，MDD 16.83/9.80/15.43/9.96，Sharpe 1.08/1.83/1.10/−3.20）。账本 fresh 口径 long +152.6 / OOS2 +42.4 / valid +35.0 / holdout −8.8（long 同，OOS2/valid/holdout 路径差 +6.1/−22.6/−1.5pt，两尺子结论一致， verdict 用连续切片苹果对苹果，fresh 差值亦列）。
- FOUND 硬标准（Yuan 2026-10-03）：同窗同成本下 **long∧valid∧holdout 三窗全超 M30** 才算找到，否则 FAIL（强分散可 WATCH，但 verdict 行写 FAIL）。
- 统计复用 `run_backfill_legs.py` 的 `stats_on_window`/`corr_to_m30`/`mc_block`/`wf_blocks`（拷贝进本作业脚本，未改原文件）。

## 0. 总裁决（先看这里）

| # | 候选（main） | long vs M30 | valid vs M30 | holdout vs M30 | verdict vs FOUND |
|---|---|---|---|---|---|
| 1 | 烂板回封 MAIN T+1开→T+1收（10槽等权） | +49.22% vs +152.62% **−103.40pt** | +15.34% vs +12.40% **+2.94pt** | −3.63% vs −7.30% **+3.67pt** | **FAIL**（long 挂） |
| 2 | DIV_YIELD10 Top10 月持（53样本 PIT，开盘执行） | +139.11% vs +152.62% **−13.51pt** | +0.32% vs +12.40% **−12.08pt** | −1.24% vs −7.30% **+6.06pt** | **FAIL**（long+valid 挂） |
| 3 | 长调整窄箱 60d→hold20（10槽，T+1开） | −59.17% vs +152.62% **−211.79pt** | −25.41% vs +12.40% **−37.81pt** | −4.53% vs −7.30% **+2.77pt** | **FAIL**（long+valid 大挂） |

**一句：三条全 FAIL，无新腿。烂板诚实口径从 +1572%（乐观收盘买）掉到 +49%（开盘买），成本×2 直接转负；红利全市场 PIT 不可用（DB 无 dv_ttm/分红表），53样本重跑仍 valid 挂；窄箱买卖组合直接亏 −59%，旧 +6.94% 前瞻作废。**

## 1. 候选一：烂板回封隔夜 `limitup_reseal`

- 预注册：MAIN（PASS-eligible）T+1开买→T+1收卖，跳过 T+1 开盘锁涨停，10槽等权（1/10 NAV，超10按 ts_code 截断，闲置 0），0.30% rt；N1 T+1开→T+2开；REF（reference only，NOT eligible）T收→T+1开 + 30% 成交 haircut（seed42 随机30%）。
- 数据/宇宙/幸存者：`_cache/daily_a.parquet`（2023-12+ 3.5M行）+ `limitup_5min_2024.parquet`（19974 in_reseal）+ `stock_basic`/`trade_calendar`。A-share 正则，主板+创业+科创，剔 ST（快照214 + 60d 5.3% proxy PIT），上市≥250bar，前20d均成交额≥70000千元，`sealed=(close/pre−1≥limit−0.002)&(close≥high)`、`one_word=(low≥high)`（qfq 口径，连续性修正不改比率），reseal 定义同 spec（n_bars≥45、首触≤0935、开板后回封、尾收≥day_high−0.005）。信号 10521（原研究 16882，差为 ST/流动性/上市过滤 + qfq sealed 统一口径，已声明）。样本限 2024-01-02+（5min 全覆盖仅此后，2010 signal_start 名义，实际 2024+，COMPARE long 前段 NAV 平坦）。
- 交易：REF 10495笔，MAIN 10212笔，N1 10103笔；T+1 锁涨停跳过，停牌跳过，一字日（high==low）跳过，无未来。

| 变体 | long/ann/MDD/Sharpe | OOS2 | valid | holdout | vs M30 long/valid/holdout | vs fresh |
|---|---|---|---|---|---|---|
| MAIN T+1开→T+1收 | +49.22%/8.66%/−29.22%/0.50 | +11.38%/11.87%/−14.85%/0.51 | +15.34%/39.09%/−9.81%/1.28 | −3.63%/−23.37%/−8.78%/−1.31 | −103.40/+2.94/+3.67 | −103.38/−19.66/+5.17 |
| N1 T+1开→T+2开 | −76.34%/−25.84%/−77.23%/−1.01 | −57.82% | −27.54% | −11.62% | −228.96/−39.94/−4.32 | — |
| REF T收→T+1开（乐观，不可成交） | +1572.24%/79.36%/−10.94%/3.84 | +104.95% | +125.57% | +1.52% | +1419.62/+113.17/+8.82 | — |
| REF 30% haircut | +827.14%/58.71%/−11.44%/3.56 | +50.88% | +117.19% | +5.52% | +674.52/+104.79/+12.82 | — |

- 相关（vs M30）：日 0.081 / 月 0.126 / 最差10%月 0.418（低相关，分散）。
- 随机同结构 200次（同天同k≤10，液态池 amount≥200万千元，10槽，0.30%）：真 long +49.22%，随机中位 **+1958.2%**，分位 **0.0%**（垫底；注：reseal 天为牛市簇（2024-09-30等），同天随机液态 intraday 亦被市场 beta 拉爆， median 巨大，真仍垫底 = 无 intraday alpha，方向可信）。
- 成本×2：long +49.22→**−54.74%**（−103.96pt，直接转负，换手极高），valid +15.34→−16.84，holdout −3.63→−8.44。
- 延迟+1（T+2开→T+2收）：long +27.33%（−21.89pt），valid +15.34→**−18.44%**（翻负，执行敏感）。
- 分年：2021 0.0 / 2022 0.0 / 2023 0.0 / 2024 −7.05 / 2025 +11.71 / 2026 +40.23（样本仅 2024+，2026 YTD 强但短）。
- n/窗：long 9770 / OOS2 3097 / valid 3298 / holdout 442（全>30，无 flag）。
- 容量/参与度（金额=amount千元×1000，1/10仓）：120万 med 0.0093%/p95 0.0377%/max 0.1473%，200万 max 0.2455%（<2%，可用）。
- MC 块20天2000路1年：p5 −17.75%/中位 +7.37%/P(亏>10%)13.2%/P(亏>20%)3.5%。走廊 5/10。
- **verdict：FAIL**（long −103pt，不过 FOUND；valid/holdout 虽超但 long 大挂）。
- 下一步：**DROP 作腿**（诚实开盘口径 long 大输 + 成本×2转负 + 延迟 valid 翻负 + 随机垫底；REF→MAIN gap 1523pt 即收盘假设虚胖实锤）。若仍不死心，唯一 FURTHER TEST：T+1开盘集合竞价可成交量（bar_5min 0930 未覆盖，需逐笔，50笔 paper）+ 拥挤冲击，但期望为负。

## 2. 候选二：DIV_YIELD10（股息率 Top10 月持）

- 预注册：MAIN Top10（dv_ttm TTM，月末 PIT，次月首交易日开盘执行，等权1/10，0.30% rt）；N1 Top15；N2 +1天延迟。
- 数据搜索：grep `data-sync-service` + `karios-research`：DB `daily`（OHLC，无 yield）、`stock_dailybasic`（仅 total_mv/circ_mv/turnover）、无 `dv_ttm`、无 dividend/ann/ex 表。**全市场 PIT 股息率真不可用**（大拉取禁止），已声明。最宽诚实宇宙 = `data-backfill/div_lowvol` 53只（ak_tx 日行情 open/close/amount + tushare dailybasic dv_ttm + dividend ann/ex，SOURCES.md）。幸存者：53现存，无退市，3晚上市（600905/600941/601728）达标后入池，方向偏乐观。
- 信号：月末 `yield_at`（trade_date≤信号日最新 dv_ttm），上市≥250bar，20d均 amount≥5000万（tx 元，53全大盘，基本全过），ST 无（样本无 ST），月首首交易日开盘买（缺 open 用 close fallback，记录），持有至次月首日开盘（gap+intraday 两段 + 换手×0.30%）。
- 交易：MAIN 560 stock-months（61 rebs），N1 同。

| 变体 | long/ann/MDD/Sharpe | OOS2 | valid | holdout | vs M30 cont. | vs fresh |
|---|---|---|---|---|---|---|
| MAIN YIELD10 | +139.11%/19.82%/−17.81%/1.10 | +33.00%/34.57%/−7.79%/1.52 | +0.32%/0.74%/−13.21%/0.13 | −1.24%/−8.57%/−3.74%/−0.86 | −13.51/−12.08/+6.06 | −13.49/−34.68/+7.56 |
| N1 YIELD15 | +126.61%/18.49%/−17.84%/1.06 | +27.67% | −0.19% | +0.91% | −26.01/−12.59/+8.21 | — |

- 相关：日 0.171 / 月 0.178（低相关，<0.3，分散，沿用 BACKFILL 0.18/0.10 方向）。
- 随机同结构 200次（同月随机10，月频 close）：真 +139.11%，随机中位 +29.05%，分位 **100%**（事件强，但组合仍输 M30）。
- 成本×2：long +139.11→+131.56（−7.55pt，活），valid +0.32→+0.10，holdout −1.24→−1.30。
- 延迟+1（T+2开）：long +133.57（−5.54pt，活），valid +1.11（反升），holdout −0.73。
- 分年：2021 0.0 / 2022 +14.10 / 2023 +32.55 / 2024 +52.25 / 2025 +6.04 / 2026 −2.65（2024 占大头，单年依赖）。
- n/窗：long 550 / OOS2 130 / valid 60 / holdout **10（<30 flag，只能描述）**。
- 容量：120万 med 0.0129%/p95 0.0521%/max 0.4414%，200万 max 0.74%（可用）。
- MC：p5 −8.06%/中位 +19.21%/P10 3.5%。走廊 6/10。
- **verdict：FAIL**（long −13.5pt + valid −12.08pt，不过 FOUND；holdout 虽超）。
- 下一步：**WATCH 0%**（低相关+随机100%+延迟稳，但 valid 薄弱 + 53样本非全市场 + holdout n=10）。FURTHER TEST（ exact ）：全市场 PIT（tushare dividend ann/ex 全量 + daily_basic dv_ttm 日表，需 polite 拉取 + PIT 审计，≥50笔 OOS）重裁；不过 valid 则永久观察 0%。

## 3. 候选三：长调整窄箱突破（60d→hold20/60d）

- 预注册：MAIN 60d箱 hold20；N1 60d hold60；N2 120d hold20。箱体 `box_high=max(high,n)/box_low=min(low,n)` shift(1)（无未来），`is_narrow=(box_high−box_low)/close ≤ 当日横截面20%分位`（PIT，每日可用带宽分位，n≥50），突破 `close>box_high`。宇宙全 A 2021-02+（6.74M行，5428码），ST 剔除，上市≥250bar，20d均 amount≥70000千元，60d ST proxy。入口 T+1开（锁涨停/一字/停牌跳过），持有 20d（E~E+19收）/60d，10槽等权（NAV×0.1/开盘股数，现金0，买/卖各半成本 0.15%），停牌展期≤5d。
- 信号：60d 16470，120d 15774； admitted（10槽 cap）：MAIN 530 / N1 177 / N2 531（拥挤截断，闲置多）。

| 变体 | long/ann/MDD/Sharpe | OOS2 | valid | holdout | vs M30 |
|---|---|---|---|---|---|
| MAIN 60d hold20 | −59.17%/−16.96%/−64.11%/−0.58 | +1.47%/1.53%/−21.95%/0.21 | −25.41%/−49.23%/−33.86%/−1.86 | −4.53%/−28.35%/−12.20%/−0.79 | −211.79/−37.81/+2.77 |
| N1 60d hold60 | −48.96%/−13.02%/−56.84%/−0.43 | +9.89% | −18.91% | +0.21% | −201.58/−31.31/+7.51 |
| N2 120d hold20 | −77.89%/−26.87%/−81.08%/−0.92 | −11.50% | −29.26% | −7.61% | −230.51/−41.66/−0.31 |

- 相关：日 0.319 / 月 0.319（>0.3，不分散，小盘 beta）。
- 随机同结构 200次（同天同k随机液态，hold20，10槽）：真 −59.17%，随机中位 −2.56%，分位 **0.0%**（垫底，不如瞎买）。
- 成本×2：long −59.17→−82.70（−23.53pt，更差），valid −25.41→−35.97。
- 延迟+1：long −49.91（+9.26pt仍大负），valid −30.69。
- 分年：2021 0.0 / 2022 −14.47 / 2023 −22.93 / 2024 −27.71 / 2025 +5.47 / 2026 −24.41（仅2025微正）。
- n/窗：long 521 / OOS2 117 / valid 49 / holdout **9（<30 flag）**。
- 容量：120万 med 0.0218%/p95 0.0865%/max 0.1841%（可用）。
- MC：p5 −50.46%/中位 −17.09%/P10 60.4%。走廊 2/10。
- **verdict：FAIL**（long −211pt，valid −37pt，大挂；旧 +6.94% 前瞻→买卖 −59%证伪，同布林开口覆辙）。
- 下一步：**DROP 作腿**（组合层大亏 + 随机垫底 + 走廊2/10）。不单列提示层（中位厚尾，win~52% 未达杠杆线，旧研究已记 WATCH 无需重复）。

## 4. 复现

- 预注册：`~/Projects/wealth-ideas/scratch/weak3_hardtest/prereg.json`（seed42，四窗，成本，slots，信号定义，跑前冻结）。
- 运行（单进程，`nice -n 10`，无多进程池，向量化 pandas/numpy，仅加载 2021-02+/2023-12+ 必要列）：
  `nice -n 10 ~/Projects/wealth-ideas/.venv-data/bin/python ~/Projects/wealth-ideas/scratch/weak3_hardtest/run_weak3.py`
- 输出：`weak3.json`（本报告所有数字来源），本文件，vf 摘要，ledger 追加。
- yt_daily 未碰（PID 4623 存活，未渲染视频，未杀进程，未写 video-factory 输出）。

*数据：`compare_nav.npz`（cal/m30）、`_cache/daily_a.parquet`（14.8M行）、`limitup_5min_2024.parquet`、`stock_basic`/`trade_calendar`、`data-backfill/div_lowvol`（53样本，SOURCES.md）。DB `daily` 24M行（1998~2026-10-05）、`stock_dailybasic` 仅 mv/turnover（无 yield，已 grep 确认全市场 PIT 不可用）。*
