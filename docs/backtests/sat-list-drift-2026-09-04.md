# 卫星习惯名单漂移：全天振幅排名 vs 14:30-proxy 排名（2026-09-04）

> **一句话**：回测用全天振幅排名（收盘才知道），Live 在 14:30 只能用 09:30→14:30 代理振幅排名。205 个 R-wide 开闸日（OOS2+train）上两份 top-4 名单平均 Jaccard 仅 **0.43**（完全相同 20/205），但 3 日前瞻收益 **+2.05% vs +1.90%（差 0.14pp/笔，噪音级）**——名单漂移大，经济显著性无。**回测排名前视不构成实质高估，Live 保持现任，不改排名**。
> **关键词**：习惯双子星 名单漂移 排名前视 Jaccard

**脚本**：`services/data-sync-service/scripts/diag_sat_list_drift.py`（只读，只打印，不落盘）
**规则**：诊断级，选参窗 OOS2+train，valid 未碰；无论结果都不调参。

---

## 1. 方法

- 与冻结习惯配方同口径：R-wide 闸（>0.5）开闸日 + S-gap + skip_t1 + C1 3% 过滤 + strict（掉锁仓不回填）+ 取 top-4。
- A（回测）：按日线全天 `(high−low)/close` 升序。
- B（Live 可知）：按 `bar_5min` 当日 `trade_time ≤ 1430` 的 `(max high−min low)/1430价` 升序；无 5 分钟覆盖的排最后。
- 前瞻：两份名单统一用 14:30 价进、第 3 日 14:30 价出，扣 0.3% 往返（纯选择效应，进出价保持一致）。

## 2. 结果（OOS2+train，205 天，14510 eligible，proxy_blind=0）

| 名单 | 笔数 | 3日均值 / hit |
|------|------|---------------|
| A 全天振幅（回测） | 774 | +2.05% / 57% |
| B 14:30-proxy（Live 可知） | 772 | +1.90% / 53% |

- 日均 Jaccard(A4,B4) **0.43**；完全相同 20/205 天；#1 顺位相同 127/205（62%）。
- fwd 差 A−B **+0.14pp/笔**。
- `proxy_blind=0`：三年尾盘 5 分钟覆盖完整，无覆盖缺口问题。

## 3. 判定

- **名单漂移存在但无超额**：换掉一半名单，收益几乎不变——振幅排名键本身区分度有限（与 H1“无前视键打不过现任、但现任也没多强”一致）。
- **Live 本来就是 B**：`twin_star_intraday.py:353` 用快照时刻 bar 的 amp 排名，即 14:30-proxy；回测用全天 amp 的前视只带来 +0.14pp/笔的纸面差，不构成对 Live 的高估。
- **不改 Live 排名，不开新变体**；TIP-014 Phase3 名单漂移项关闭（habit-clock 已答成交价维度，本篇答选择维度）。

复现：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_list_drift.py
```
