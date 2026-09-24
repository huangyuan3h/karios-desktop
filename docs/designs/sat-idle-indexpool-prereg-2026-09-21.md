# 预注册：星舰停车资产并入指数轮动池（H-SAT-IDLE-INDEXPOOL · 2026-09-21）

> **单假设 · 门槛跑前冻结。** 问：把 **000300/000688 指数轮动池**按**套筒同规则**并入星舰停车池，
> 能否提升停车腿（收益与/或回撤），使其优于现行 argmax 套筒？
> **关键词**：星舰 · 停车资产 · 指数轮动池 · mom60+MA200 · 预注册

## 0. 机制（冻结）

停车资产 = **并集池** `{GOLD, OIL, NASDAQ, BOND10, SSE300 000300, STAR50 000688}`，
走**同一份** `harbor.parking_replay`（mom60 + MA200 argmax、因果 trail8、次日、5bps/边、
NASDAQ alias 逻辑、覆盖率门 ≥3）；指数用 `index_daily` 收盘（510050/588000 无 bar，声明代理）。
`port_ret_t = sat_ret_t + w_t × park_ret_t − 5bps × |w_t−w_{t−1}|`，`w_t = cashShare(T−1)` 因果。

## 1. 为什么开（背景）

- §7（`index/index-trend-bigmoney-2026-09-08`）已测：指数并入轮动池 **valid +22.4 / train +9.8 / OOS2 −3.1**
  ——"**趋势市的油，弱市的税**"，指数线第三次死在 regime 依赖；且被选中的几乎全是 **STAR50**（SSE300 三窗仅 1 天）。
- 本档是**新应用**：把该池当作**星舰闲置现金的停放资产**（§7 测的是 twin-star 核心的停车，不是卫星锚）。
- 死因预判：主 **#2 共线**（套筒 mom60+MA200 已吃指数动量）；次 **#4 regime**（OOS2 熊市反弹被选中）。

## 2. 臂（冻结）

- **A_ip_true（主判定）** = 星舰 + `w_true` 停**并集池**，5bps/边。
- **A2_true** = 现行 4-ETF 套筒（对照）。
- A_ip_true_15bps（成本敏感性，价目）。

## 3. 判定（跑前冻死）

- **K1（不劣化任何窗）**：A_ip 三窗 `Δtotal` vs standalone 全部 **≥ 0**。
- **K2（收益实质）**：A_ip long `Δtotal` vs standalone **≥ +100pt**。
- **K3（回撤不劣化）**：A_ip long **MDD ≥ A2 long MDD**（不更深）。
- **K4（效率）**：A_ip long **Sharpe > A2 long Sharpe**。
- **裁决**：**PASS = K1∧K2∧K3∧K4**；否则 REJECT。A2/A_ip_15bps 为价目/对照。
- **锚点披露**：PASS 仅作候选；**不进 Live**；落地需另起预注册 + 用户拍板。

## 4. 死因预判（跑前写死）

- 主：**#2 共线**——指数动量已被套筒覆盖，并集≈套筒 + 少量 STAR50 换手 → 零/负增量。
- 次：**#4 regime**——OOS2（弱市）指数池被选中时亏钱 → K1 挂。

## 5. 产出

- 脚本：`scripts/eval_sat_idle_indexpool.py`（只读）→ `data/backtest_reports/sat_idle_indexpool_2026-09-21.json`。
- 结论落 `docs/backtests/stable/sat-idle-indexpool-2026-09-21.md` + `SUMMARY.md`；**Live 不动**。

*冻结于 2026-09-21，跑数前。*

**结果（跑后补 · 2026-09-21）**：**REJECT**。K1 ❌ `[+46.4, +8.7, −9.5]` · K2 ✅ +375.6 · K3 ❌ −9.0 · K4 ❌ −0.35。
指数池 OOS2 加分（+14.4 vs 套筒）却 train/valid/long 全扣（−20.4/−2.7/−69.3），并把停车腿 MDD 拉深到 **−39.0**；
**#2 共线 + #4 regime** 实锤。结论落
[`../backtests/stable/sat-idle-indexpool-2026-09-21.md`](../backtests/stable/sat-idle-indexpool-2026-09-21.md)。
