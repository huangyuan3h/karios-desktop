# STOCK 篮剥离港股对照（2026-09-08 · 诊断 · 不动 Live）

> **一句话**：用户问"剥离港股影响多大、正向负向"。`mom_compare` 时间线 CN-only（去掉 HK 合并，同一 builder）vs 现状 A+H：**港股是正贡献**——OOS2 fused +27.5pt、valid +80pt；train 是唯一反例（CN-only +11.2pt）。三窗方向不一致 → **REJECT 剥离**，Live 不动。

## 0. 口径

- 基线：路由同逻辑（CN S-3 run + HK S-3 run 合并）→ `build_mom_compare_timeline`。
- 变体：只喂 CN run 的 snaps（HK 持仓过滤，空篮日自动落到 ETF/REPO，机制内生）。
- 窗：OOS2 / train / valid（+ past_year 旁观）。 verdict 只看相对增量。

## 1. 数

| 窗 | A+H fused / base / maxDD | CN-only fused / base / maxDD | Δfused（CN−A+H） |
|----|----|----|----|
| OOS2 | +17.8 / +43.3 / 18.0 | −9.7 / +20.9 / 29.6 | **−27.5** |
| train | +40.7 / +47.3 / 8.4 | +51.9 / +63.3 / 18.9 | **+11.2** |
| valid | +139.1 / +118.4 / 11.9 | +59.1 / +32.3 / 11.9 | **−80.0** |
| past_year | +181.2 / +168.7 / 12.6 | +86.1 / +97.5 / 18.9 | −95.1 |

pick 结构（STOCK/ETF/REPO 天数；STOCK 天中 HK 主导天数）：

| 窗 | A+H | CN-only |
|----|-----|---------|
| OOS2 | 146/106/1（HK主导 116） | 103/144/6 |
| train | 31/95/1（HK主导 24） | 38/88/1 |
| valid | 16/89/6（HK主导 16/16） | 2/102/7 |
| past_year | 89/161/6（HK主导 82） | 73/173/10 |

## 2. 解读

- valid 的上攻是 HK 带的：剥离后 STOCK 只被选中 2 天（vs 16 天），钱在 ETF 里只拿到 +59.1，少赚 80pt。
- OOS2 弱市年 HK 也扛了 +27.5pt，且回撤更大（29.6 vs 18.0）——剥离连防守都变差。
- train 是唯一 HK 拖后腿的窗（−11.2pt），单窗好 = 不采信。
- 篮子层面（base）同故事：OOS2 +43.3 vs +20.9、valid +118.4 vs +32.3，train +47.3 vs +63.3。

## 3. 判定

- **REJECT 剥离**：2/3 窗港股正贡献且含选参窗 valid；弱市年防守亦更优。Live 择强逻辑不动。
- 附带回答（2026-09-08 盘后）：当前 12 笔 S3HK paper 全 HOLD（止损 −5% / trail −12% / 60 天均未触发，最久 39 sessions；最接近 HK:00002(08-21) −3.2%），时间线 9/3–9/7 exits=[] 互证——**最近两天按回测不卖港股**，且今日 paper 照常新开 HK:02343（intake 活跃）。

脚本：`services/data-sync-service/scripts/compare_stock_basket_nohk.py`（只读，无落盘）。
