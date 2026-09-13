# H-ETF-A：S-3 股票腿是 alpha 还是板块 beta？（同窗 ETF 归因 · 2026-09-12 · 只读）

> **一句话**：把冻结 S-3 的**每一笔平仓**（entry→close）与**同窗同板块 ETF** 对比：**均值超额全窗为正**（board +1.22% long、3/3 窗；sector +1.55%、3/3 窗、覆盖 78%）→ 股票腿**不是**单纯板块 beta，被动买 ETF 会**丢掉右尾**；但**中位超额为负**（board −2.65%、sector −1.76%、胜率 35–41%）→ alpha 是**右尾/option 型**，典型那笔其实跑输 ETF。**K1/K2 过、K3 挂 → 不采纳"ETF 代替股票"，但记下"ETF 打底 + S-3 只做右尾增强"为后续。**
> **关键词**：S-3 alpha 归因 ETF 同板块 右尾 beta 只读

**预注册**：[`designs/s3-alpha-vs-etf-prereg-2026-09-12.md`](../../designs/s3-alpha-vs-etf-prereg-2026-09-12.md)
**脚本**：`scripts/sync_etf_daily.py`（建面板）· `scripts/diag_s3_alpha_vs_etf.py`
**数据**：`data/etf/etf_daily.csv`（35 只，2021+，tushare `fund_daily`+`fund_adj` 复权，只读、不入 `daily`）
**口径**：冻结 `S3_CONFIG`；逐笔 close(entry)→close(close) 收益；基准同 entry/close 日历 ETF 复权 close-to-close；行业= `watchlist_score_daily.industry`（latest）关键词映射。

---

## 1. 结果（超额 = 个股 − 同窗 ETF，%）

| 窗口 | n | board 均/中位/胜率 | board 股/基均 | sector 均/中位/胜率 | sector 股/基均 | b300 均/中位 |
|------|---|-------------------|--------------|--------------------|--------------|-------------|
| OOS2 | 91 | +1.88 / −1.22 / 37% | +4.4 / +2.5 | +3.57 / −1.13 / 47% | +4.6 / +1.0 | +2.36 / −1.43 |
| train | 51 | +4.09 / +0.12 / 51% | +7.5 / +3.4 | +0.09 / −2.54 / 43% | +8.3 / +8.2 | +5.75 / −2.13 |
| valid | 16 | +9.62 / −3.14 / 44% | +22.9 / +13.3 | +3.26 / −1.84 / 50% | +22.9 / +19.6 | +20.03 / +16.47 |
| **long** | 410 | **+1.22 / −2.65 / 35%** | +2.3 / +1.1 | **+1.55 / −1.76 / 41%** | +2.7 / +1.2 | +1.58 / −3.25 |

（board = 688→588000 / 主板→510500 / 创业→159915；sector coverage OOS2 80% / train 82% / valid 100% / long 78%）

## 2. 判定（预注册线）

| 线 | 结果 | |
|----|------|---|
| K1 均值 board 超额 >0 且 ≥2 窗 >0 | long +1.22，3/3 | ✅ |
| K2 均值 sector 超额 >0 且 ≥2 窗 >0（覆盖≥50%） | long +1.55，3/3，cov 78% | ✅ |
| K3 median board 超额 >0 | −2.65 | ❌ |

**读法**：**均值全窗为正、中位全窗为负** = 分布强烈右偏——S-3 的选股超额来自**少数大赢家**（趋势腿的构造：砍亏损、放赢家），典型一笔反而略输板块 ETF。这正是 `first-principles §一.4 复利算术律 / 右尾` 的指纹。

- **量级**：long 每笔均值 股 +2.3% vs board +1.1% → 选股大致把股票腿单笔收益**翻倍**（simple-sum 口径 ≈ +492pt 对 board / +482pt 对 sector）。被动 ETF 会抹掉这部分。
- **不是 beta**：若股票腿≈板块 beta，均值超额三窗应为 ~0 或负——实际三窗全正，尤其 valid +9.6/+3.3。

## 3. 结论（回答"ETF 代替股票"）

- **不采纳被动 ETF 替代**：会放弃驱动复利的右尾（中位负但均值正、右偏）。
- **保留后续**：中位为负说明选股层不是"全面 alpha"，而是"右尾期权"→ **"宽基/板块 ETF 打底 + S-3 只做右尾增强"** 是一个有机制的新形态，值得日后单独预注册（本轮不做，先记）。
- 与 [newtracks B1](../newtracks-a-2026-09-12.md) 一致：双子星/核心买持 ETF 全胜，但那里的超额含**择时**；本页把它拆到**逐笔选股**层，仍为正均值。

## 4. 边界与诚实项

- 基准 **510500**（主板）/588000（科创）作 board 代理；S-3 选票偏科创/成长，valid 用 510300 会虚高（+20 均/中位 +16），故以 board 为准。
- 未扣 ETF/股票交易成本（双边同未扣，Delta 公平 §二.6）；逐笔 close-to-close，非 open fill。
- 行业用 latest（PIT 漂移可忽略）；sector 未覆盖的 ~20% 笔（化工/机械/交运等无对口 ETF）不计入 sector 列，board 列 100% 覆盖。
- 单窗 n 小（valid n=16，sector 结论 underpowered，仅背景）。

## 5. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src python3 scripts/sync_etf_daily.py --start 20210101 --end 20260807
PYTHONPATH=src:scripts python3 scripts/diag_s3_alpha_vs_etf.py --save-report
```
