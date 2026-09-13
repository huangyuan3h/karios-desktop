# Live 执行层 vs 回测一致性验证 · 2026-09-13

> 工具：`scripts/verify_harbor_live_vs_backtest.py`（PIT 逐日重放：Live 决策链 vs 港湾回测行）
> 口径：两边同一价格源（DB `daily`）、同一日历；映射 = Live 在 T 收盘的决策 ↔ 回测行 (T+1) 的持有 pick。
> **最终结论：决策单源统一后，三窗对账 100%（473/473）。**

## 方法
1. `multi_asset_sleeve.build_multi_asset_sleeve`（Live）用 `fetch_last_bars` PIT 截断（只喂 ≤T 的 bar）逐日重放，维护 held/entryDate 状态（与 `sleeve_paper_auto` 一致）。
2. `service.harbor.build_harbor_timeline`（回测页/Timeline）同一 ETF 序列生成每日 pick。
3. 对比三窗每个交易日（含窗口首日）的 pick（GOLD/OIL/NASDAQ/BOND10/REPO），输出 mismatch 表。

## 过程与修复
| 阶段 | 匹配率 | 修复 |
|---|---|---|
| 初测 | 90.1% | — |
| 修 ① | 94.9% | Live mom60 索引 off-by-one（`closes[-60]`=第 59 根 vs 回测第 60 根）+ 历史门槛 260 vs 200 根 |
| 修 ② | 98.7% | **回测幻影交易日**：引擎日历含假期（2025-10-02/03/06/08 等）→ 无 bar 的日子强制清仓、节后追回；Timeline/Eval 过滤到真实交易日 |
| 修 ③ | 98.9% | 回测 NASDAQ 别名换仓 peak 语义（513110≈2.4 vs 513100≈1.4 价格量级不同，跨别名续 peak 会造出假 −8% 触发） |
| 修 ④ | 99.1% | **Live `_pick` 真 bug**：选别名用 A 的 mom、返回却是 `CANDIDATES` 里第一个同 key 标的（永远 513110）→ Live 永不换别名 |
| **单源统一** | **100.0%（473/473）** | `harbor.pick_parking`（mom60/MA200/别名/覆盖）+ `harbor.parking_replay`（trail 优先、TS 级换仓、幻影日过滤）成为 Timeline / Live / `eval_etf_parking_baseline` 的**唯一实现**；对账含窗口首日决策 |

## 连带修正：港湾基线（最终）
统一后重跑 `eval_etf_parking_baseline.py`（V0 引擎不变 +46.5/+34.4/+38.7/+94.5）：
| 窗口 | P1 total | Δ vs V0 | trades | CAGR / MDD / Sharpe |
|---|---|---|---|---|
| OOS2 | +62.0 | **+15.5** | 30 | 65.3 / −14.3 / 1.84 |
| train | +45.3 | **+11.0** | 23 | 116.5 / −8.4 / 2.76 |
| valid | +50.3 | **+11.6** | 21 | 156.6 / −21.8 / 2.14 |
| long | +219.9 | **+125.4** | 127 | 27.3 / −23.4 / 1.05 |

- 判定不变（K1/K2/K3 全过）；R1 +14.7/+11.0/+10.2/+121.1、R2 +11.0/+4.2/+9.3/+39.6、R3=P1。
- API `/timeline?strategy=harbor` 与评估完全一致：OOS2 61.99/46.52、train 45.34/34.38、valid 50.32/38.74。
- 数字演进：+9.4/+6.0/+9.1/+90.0（旧）→ +9.6/+13.7/+9.0/+119.6（幻影日修正）→ **+15.5/+11.0/+11.6/+125.4（单源统一）**；旧数字仅存于历史记录。

## 残余（已文档化）
- 回测 NAV 以 prev 收盘成交代理（14:30 语义），Live 实际 **T 收盘信号 → T+1 开盘成交**；隔夜差未建模 → **决策级一致**，NAV 级如需可另开 OPT（可执行时钟变体）。
- 研究脚本（`eval_twin_star_parking.py` 等）保留旧循环，仅作历史证据，不在运行路径。

## 关联
- 基线档：[`stable/etf-parking-baseline-2026-09-13.md`](stable/etf-parking-baseline-2026-09-13.md) · 预注册：[`designs/etf-parking-baseline-prereg-2026-09-13.md`](../designs/etf-parking-baseline-prereg-2026-09-13.md)
- 单源实现：[`service/harbor.py`](../../services/data-sync-service/src/data_sync_service/service/harbor.py)（`pick_parking` / `parking_replay` / `build_harbor_timeline`）· OPT-180 ✅
