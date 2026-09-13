# Live 执行层 vs 回测一致性验证（round 1）· 2026-09-13

> 工具：`scripts/verify_harbor_live_vs_backtest.py`（PIT 逐日重放：Live 决策链 vs 港湾回测行）
> 口径：两边同一价格源（DB `daily`）、同一日历；映射 = Live 在 T 收盘的决策 ↔ 回测行 (T+1) 的持有 pick。

## 方法
1. `build_multi_asset_sleeve`（Live）用 `fetch_last_bars` PIT 截断（只喂 ≤T 的 bar）逐日重放，维护 held/entryDate 状态（与 `sleeve_paper_auto` 一致）。
2. `service.harbor.build_harbor_timeline`（回测）同一 ETF 序列生成每日 pick。
3. 对比三窗每个交易日的 pick（GOLD/OIL/NASDAQ/BOND10/REPO），输出 mismatch 表。

## 结果
| 阶段 | 匹配率 | 说明 |
|---|---|---|
| 初测 | 90.1% | — |
| **修复 ①** Live mom60 索引 off-by-one + 历史门槛 | 94.9% | `_pick` 原来 `closes[-LOOKBACK]`（=第 59 根）≠ 回测 `i-LOOKBACK`（第 60 根）；且门槛 260 根 vs 回测 200 根（MA200）→ 已改齐 |
| **修复 ②** 回测幻影交易日 | **98.7%**（464/470） | 引擎日历含假期（2025-10-02/03/06/08 等）；无 bar 的日子回测强制清仓（pick→REPO），节后首日与 Live 分叉。已在 `harbor.build_harbor_timeline` + B11 eval 过滤到真实交易日 |

残余 **6/470（1.3%）**：全部在 trail/换仓边界（NASDAQ 别名切换时 Live 重置 peak、回测按 key 连续；peak 起点对齐差异）。需要"决策单源 + 可执行时钟"重构后归零（OPT-180）。

## 连带修正：B11 港湾基线数字（更强）
幻影日修复后重跑 `eval_etf_parking_baseline.py`：
| 窗口 | P1 total | Δ vs V0 | 旧（含幻影日） |
|---|---|---|---|
| OOS2 | +56.1 | **+9.6** | +9.4 |
| train | +48.0 | **+13.7** | +6.0 |
| valid | +47.7 | **+9.0** | +9.1 |
| long | +214.1 | **+119.6** | +90.0 |

- 逻辑：假期日回测被迫卖出、节后追回 → 旧数字**低估**停车场收益（train −7.7pt、long −29.6pt）。
- P1 指标（修正后）：OOS2 59.0%CAGR/−14.6MDD/1.70sr · train 124.9/−8.4/2.88 · valid 146.4/−23.1/2.06 · long 26.8/−23.6/1.04；交易 21/20/16/101。
- 判定不变：K1/K2/K3 全过（+9.6/+13.7/+9.0，合计 +32.3，long +119.6）。
- API `/timeline?strategy=harbor` valid 已同步：fused 47.72 / base 38.74 / dd 23.1（Δ +9.0）。

## 结论与后续
- **决策级一致性已验证**：修复两个实现/数据问题后 **98.7%**；残余 1.3% 均为已定位的 trail 边界语义差，非随机漂移。
- 待办：**OPT-180**（决策单源 + 可执行时钟 T+1 open + 别名 peak 对齐 → 目标 100%）；残余清零后再对外宣称"执行=回测"。
- 本档发布后，B11 相关数字以本档修正值为准（旧 +9.4/+6.0/+9.1/+90.0 仅存于历史记录）。
