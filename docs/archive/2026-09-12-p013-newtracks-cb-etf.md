# P0-13 新赛道 A/B：ETF 深挖 + CB 线  · 归档于 2026-09-12

## 当时的目标（todo 链接）
- `docs/todo.md` → **P0-13 新赛道实验队列**（A=现有数据即可跑 / B=先补数据），用户拍板"继续做实验/出结果/落档"。
- 本批 = A1（ETF 买持基准）+ A2（打板 v0）+ A3（ETF 深挖三刀）+ B1（CB 条款/估值线）。

## 实际做了什么
- **A1 ETF 买持基准**（`bench_etf_buyhold`）：双子星 vs 单只/组合 ETF 买持。
- **A2 打板 v0**（`diag_limitup_chase`）：非 ST/BJ 封板，i+1 开盘买、i+2 收盘卖。
- **A3 ETF 深挖**（本轮建 **ETF 面板** tushare `fund_daily`+`fund_adj`，35 只=6 宽基+29 板块，46,885 行 2021+，只读 `data/etf/`）：
  1. `diag_s3_alpha_vs_etf` —— S-3 逐笔 vs 同窗同板块/宽基 ETF（alpha 归因）。
  2. `diag_s3_etf_sync_stop` —— 板块/宽基 ETF 破位当个股退出触发（覆盖/鞭打诊断）。
  3. `diag_etf_sector_momentum` —— 板块 ETF mom20 Top3 远期 edge + 周轮动 NAV。
- **B1 CB 线**：`import_cb_minute`（1-min 聚 CB 日线 2021+，356,656 CB-day）+ akshare `bond_zh_cov_value_analysis`（PIT 转股溢价，449 只/382,643 行）→ `diag_cb_factors`、`compare_cb_mom20`、`compare_cb_doublelow`。

## 验证 / 数据
- **A1**：双子星四窗全碾压所有 ETF 买持（OOS2 +82.4 vs 最强恒科 +54.8；past_year +141.1 vs 创业板 +55.7），相关仅 0~0.5 → **标尺成立**，不改 Live。
- **A2**：追板三窗 mixed 近零（ALL +0.14/+0.07/−0.20%），开盘溢价 0.8~3.9% 全付掉；**方向关闭**，真打板需封单/level-2（parked）。
- **A3①**：S-3 逐笔均值超额 long board **+1.22%**（3/3 窗）/ sector **+1.55%**（3/3 窗，cov 78%）→ **不是纯板块 beta，被动 ETF 替代会丢右尾**；中位 −2.65%/−1.76%、胜率 35–41% → **右尾/option 型 alpha**。**不采纳"ETF 代替股票"**；记后续"ETF 打底 + S-3 右尾增强"。
- **A3②**：sector ETF 破位退出 long 停火笔 meanΔ +3.55%、鞭打 9.8%，**但 OOS2 −1.50%（WF 1/3）+ valid 零停火**（与个股止损共线）；放宽 MA20 OOS2 meanΔ **−4.76%** = **截右尾/鞭打税**。**REJECT**。
- **A3③**：Top3 mom20 远期 H20 edge 微弱正（long +0.15%、WF 3/3），**但轮动 long −44.1% vs EW +4.0%、DD 69%、0/3 窗**，分年正负交替 → 不可交易。**REJECT**。
- **B1 CB**：低价/低波 REJECT；**mom20 长窗 −24.2 vs EW +51.8（6/6 年负）**；**双低 长窗 Δ+18.3 但 WF 1/3、40bp 翻负 −12.7**。**CB 日线横截面全关（线收口）**，仅剩条款事件（需 `cb_call`/`cb_price_chg` 权限）。

## 后续影响 / 留给谁
- **ETF 线收口**：替代/信号/止损三形态全否；ETF 面板与三个诊断脚本保留可复用（`data/etf/`、`sync_etf_daily.py`）。
- **CB 线收口**：日线横截面不再补因子；要新意需更高 tushare 档（强赎/下修公告）或换资产。
- **打板**：等 level-2/封单数据，**不重开**（死因汇总 [`limitup-do-not-redo`](../backtests/limitup-do-not-redo-2026-09-12.md)）。
- **新规律**：S-3 alpha 是**右尾/option 型**（均值正、中位负）——归入 `first-principles` 复利算术律/右尾的实证；ETF 类"外生趋势门"仍死在 regime（`维1/维5`）。
- 无新 OPT/TIP；不改 Live/schema。

## 关联档
- 实验：`docs/backtests/newtracks-a-2026-09-12.md` · `etf/s3-alpha-vs-etf-2026-09-12.md` · `etf/s3-etf-sync-stop-2026-09-12.md` · `etf/etf-sector-momentum-2026-09-12.md` · `cb/{cb-daily-factors,cb-mom20,cb-doublelow}-2026-09-12.md`
- 预注册：`docs/designs/{s3-alpha-vs-etf,s3-etf-sync-stop,etf-sector-momentum,cb-mom20,cb-doublelow}-prereg-2026-09-12.md`
