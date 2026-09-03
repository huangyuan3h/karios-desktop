# 卫星退出：body=3 vs −5% 保护 vs body 后移动止损（2026-09-03）

> **一句话**：冻结引擎仍是 **第 3 日收盘卖**。把 −5% 写进引擎、或活过 3 天再改 5%/8% 移动止损，**三窗全拒**。OOS2 上 trail 更好看，是因为赢家续扛；train/valid 填单从 132/56 掉到十几笔，占满 clip4 的 4 个槽。
> **关键词**：机会双子星 S-gap `body=3` 保护止损 trail-after-body 占槽

**脚本**：`services/data-sync-service/scripts/compare_sat_exit_trail.py`  
**原始表**：`data/backtest_reports/sat_exit_trail_2026-09-03.json`  
**口径**：window-local 空簿、卫星冻结 clip4（4×25% strict `skip_t1`）只改退出、核心冻结择强 trail8 + 10×10% S-3、`opp_50`。主判据 twin NAV vs 冻结 body=3 twin。

**实盘**：**2026-09-03 已对齐冻结引擎**。Live / paper / 通知不再按 −5% 卖；只在第 3 个交易日收盘卖。`protect_stop_pct` / `trail_after_body_pct` 仍只给回测脚本用，默认不传。

引擎改动（默认不变）：`replay_sgap_from_context` 增加可选 `protect_stop_pct` / `trail_after_body_pct`。Live / UI 不传这两个参数。

## 0. 为什么测

冻结 S-gap 回测 = body=3 收盘。当时 Live paper 另加 cost×0.95。用户希望：活过 3 天且没打 −5% 的票，不要在 body 收盘卖掉，改跟峰值 5% 或 8% 移动止损。三窗全拒后，进一步问「是不是其实不该止损」——卫星按冻结腿不应把 −5% 当卖出规则。讨论全文：[`clip4-ops-decisions-2026-09-03.md`](./clip4-ops-decisions-2026-09-03.md)。

相关旧证据（不能替代本窗）：S-gap 边是 3 日脉冲；Scout「time3 切亏损」valid 崩；S-3 D6 profit_trail REJECT。

## 1. 变体

| id | 退出 | OOS2 开闸占比 / 均持仓 | OOS2 笔数 / 均持有 |
|----|------|------------------------|---------------------|
| `body` | 第 3 日收盘（冻结） | 64% / 3.6 | **291** / 3.0d 全 body |
| `body_protect5` | body=3 + 随时 cost−5% | 65% / 3.5 | 298 / 2.93d（41 笔 protect） |
| `protect5_trail5` | −5% + body 后峰值−5%（无强制卖） | **84%** / 3.4 | **69** / 10.4d |
| `protect5_trail8` | −5% + body 后峰值−8% | **89%** / 3.3 | **54** / 12.4d |

valid 更极端：冻结 56 笔 body → trail5 17 笔 / trail8 **7 笔**，均持有 3.0d → 7.2d / 18.1d。

## 2. 机会双子星 twin NAV（总收益 / Sharpe / 最大回撤）

相对冻结 `body` 的 Δ 写在括号里。

| 窗口 | 核心 | body=3 | body+protect5 | protect+trail5 | protect+trail8 |
|------|------|--------|---------------|----------------|----------------|
| OOS2 | +17.8 | +64.7 / 2.13 / 16.1 | −3.8 | **+15.3** | +8.8 |
| train | +40.7 | +51.4 / 4.05 / 7.8 | −2.7 | −7.5 | −16.2 |
| valid | +139.1 | +157.2 / 3.71 / 11.9 | **−14.9** | −22.2 | −4.9 |
| past_year | +181.2 | +195.9 / 2.62 / 12.6 | −10.4 | −26.0 | −41.2 |
| aligned | +190.6 | +194.9 / 2.64 / 12.6 | −16.3 | −35.6 | −41.8 |

三窗（>5pt 劣化拒收）：

| 变体 | 判定 |
|------|------|
| body=3 + protect −5% | **REJECT** valid −14.9 |
| protect + trail 5% after body | **REJECT** train −7.5 / valid −22.2 |
| protect + trail 8% after body | **REJECT** train −16.2（valid −4.9 擦边） |

## 3. 读法

- **−5% 写进引擎会伤 valid**。冻结赢面是 3 日脉冲；约 11–14% 成交会被日收盘 −5% 提前砍掉，valid 6 刀 −14.9pt。这不是偶尔灾难。**2026-09-03 Live 已去掉 overlay**（Watchlist / paper / 通知），与冻结引擎一致。
- **body=3 = 入场日起第 3 个交易日、当天收盘卖**（周一买 → 周三收盘）。盘中第 3 天仓还在。
- **body 后 trail 是占槽税**。clip4 只有 4 个槽。赢家续扛 → 开闸日从 64% 升到 84–89%，新缺口买不进去。OOS2 看起来赚的是「少笔大赢」；train/valid 卫星夏普从 3.28/1.69 掉到 0.8–1.6，past_year twin −26/−41pt。
- 卫星套筒的移动止损与核心 ETF trail8 **不是同一件事**。核心 trail 切的是一只 ETF；卫星 trail 切的是 4 槽脉冲机器的周转。核心 S-3 篮的 −5%/−8% **未改**。

复现：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/compare_sat_exit_trail.py --save-report
```
