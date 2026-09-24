# 星舰停车资产并入指数轮动池（H-SAT-IDLE-INDEXPOOL）· 2026-09-21 · **REJECT**

> **何时看**：想知道「000300/000688 指数轮动池能不能替代/增强星舰停车套筒」时。
> **何时不看**：Live 操作（Live = 港湾）。
> **一句话**：冻结裁决 **REJECT**（K1 valid **−9.5** / K3 long MDD **−9.0** / K4 SR −0.35；K2 ✅ +375.6）——
> 指数池在 **OOS2 加分**（+14.4 vs 套筒）却**在 train/valid/long 全线扣分**，且把停车腿 MDD 从 −28.7 拉深到 **−39.0**。
> **#2 共线（套筒 mom60+MA200 已吃指数动量）+ #4 regime** 实锤，§7「趋势市的油，弱市的税」在新应用上复现。
> **关键词**：星舰 · 停车资产 · 指数轮动池 · STAR50 · 共线 · REJECT

> 预注册：[`sat-idle-indexpool-prereg-2026-09-21.md`](../../designs/sat-idle-indexpool-prereg-2026-09-21.md)（跑前冻结）
> 脚本 `scripts/eval_sat_idle_indexpool.py`（只读）· 报告 `data/backtest_reports/sat_idle_indexpool_2026-09-21.json`
> **Live 不动**。

---

## 1. 口径

停车资产 = **并集池** `{GOLD, OIL, NASDAQ, BOND10, SSE300 000300, STAR50 000688}`，走**同一份**
`harbor.parking_replay`（mom60+MA200 argmax、因果 trail8、次日、5bps/边、覆盖率门 ≥3）；
指数用 `index_daily` 收盘（510050/588000 无 bar，声明代理）。卫星腿冻结；`w_t = cashShare(T−1)` 因果。

## 2. 结果（total% / Δ vs standalone / MDD% / Sharpe）

| 窗口 | 星舰 standalone | A2_true 套筒 | **A_ip_true 并集池** | 并集腿 standalone |
|---|---|---|---|---|
| OOS2 | +207.2 | +239.2 (+32.0) | **+253.6 (+46.4)** | +17.5 / −17.1 / 0.65 |
| train | +36.2 | +65.3 (+29.1) | +44.9 (+8.7) | +34.2 / −14.8 / 1.91 |
| valid | +6.5 | −0.3 (−6.8) | **−3.0 (−9.5)** | −6.7 / −39.0 / 0.04 |
| long | +468.3 | +913.2 (+444.9) | +843.9 (**+375.6**) | +74.1 / **−39.0** / 0.54 |
| holdout（描述） | −3.4 | −4.7 | −4.7 | −2.5 / −4.2 |

**A_ip vs A2（long）**：Δtot **−69.3** · Δmdd **−9.0** · Δsr **−0.35**。

## 3. 裁决（冻结 K1–K4）

| 判据 | 阈值 | 结果 |
|---|---|---|
| K1 三窗 Δtot vs standalone | 全 ≥ 0 | ❌ `[+46.4, +8.7, **−9.5**]` |
| K2 long Δtot vs standalone | ≥ +100pt | ✅ +375.6 |
| K3 long MDD vs A2 | ≥ 0（不更深） | ❌ **−9.0**（−38.9 vs −29.9） |
| K4 long Sharpe vs A2 | > 0 | ❌ −0.35 |
| **裁决** | | **REJECT** |

## 4. 发现

1. **指数池是 regime 赌注**：OOS2（弱市反弹）加分 +14.4，train/valid/long 全扣（−20.4/−2.7/−69.3）。
   §7 的"**趋势市的油，弱市的税**"在**卫星锚**这个新应用上逐字复现。
2. **加深回撤**：并集腿 standalone long MDD **−39.0**（套筒 −28.7）——STAR50 是唯一活物（SSE300 几乎不入选），
   但它的高波动把停车腿拖深，与"用 B3 分散换回撤"的方向相反。
3. **#2 共线实锤**：套筒的 mom60+MA200 规则已经吃掉指数动量；并入指数主要换来的是 STAR50 的高波换手，
   不是新的 alpha。
4. **holdout 指数从未入选**（A_ip == A2）——描述行，不判定。

## 5. 边界（诚实项）

- 指数用 `index_daily` 代理（510050/588000 无 bar），ETF 费率近似，诊断级。
- 覆盖率门 ≥3 意味着**指数只可能"并入"**（不能构成"纯指数池"）——这是 canonical 规则，未改。
- 首次运行有数据装载 bug（指数收盘按池键而非 ts_code 装载）已修重跑；修正后数字如上。
- **不进 Live**；星舰前置不变（paper 3/20 + 风险授权）。

## 6. 结论 / 下一步

- **"指数轮动池并入停车"方向关闭**（K1/K3/K4 挂）。
- 星舰停车资产的最优解仍是 **B3 系**（A4_true / a25）；指数池与套筒共线且高波，不具替代价值。
- 至此三条替代路径（A 港湾 B3 · B B3 菜单 · C 指数池）全部 REJECT，D 前沿已出拐点 a25。

## 7. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_indexpool.py --save-report
```

## 8. 关联

- 前作 §7：[`../index/index-trend-bigmoney-2026-09-08.md`](../index/index-trend-bigmoney-2026-09-08.md)
- 停车候选真值：[`sat-idle-b3-drawdown-2026-09-21.md`](sat-idle-b3-drawdown-2026-09-21.md)
- 前沿：[`sat-parking-frontier-2026-09-21.md`](sat-parking-frontier-2026-09-21.md)
