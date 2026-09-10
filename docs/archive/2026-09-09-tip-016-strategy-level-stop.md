# TIP-016 · 策略级止损（产品级回撤熔断）· 归档于 2026-09-09

> 从 `docs/trading-improvement-checklist.md` 原文迁移（只读快照，不回写）。
> 完整实验记录（诊断 + 预注册 + 全部裁决）：[`docs/backtests/product-post-peak-drift-2026-09-09.md`](../backtests/product-post-peak-drift-2026-09-09.md)

---

### TIP-016：产品级回撤熔断（策略级止损 · 预注册 → 全部裁决完成 2026-09-09）

**状态**：[x] 全部裁决完成 2026-09-09——**B（HK 熔断 -25）PASS → 进冻结 + live 同码**；W1/W2/E/A 拒收
**完成日期**：2026-09-09
**备注 / PR**：引擎旋钮 `regime_streak_min` / `drought_ramp_*` / `throttle_windows`+`throttle_scale` 默认关（冻结行为守卫）；
`scripts/run_product_throttle.py`（A 外循环）；`paper_s3._circuit_blocked(market)`（B live 镜像）；
HK 基线重固化 tag `s3-hk-baseline-20260909` sha `ba03aa91aa68`；后端全量绿 + DB 基线 OK

#### 来源

2026-09-09 用户观察双子星 Timeline——06-09 峰值（~117%）后漂到 ~95%（-17%/3.5 个月），
"一直亏损却找不到描述的止损点"。诊断：仓位级止损齐 · CN 线熔断开（仅 05-18~05-28 共 8 天）·
HK 线熔断关（已由 B 补齐）· 产品层无任何策略级止损。

#### 裁决总表（预注册 → 执行，三窗 + long 投票 >5pt 劣化即拒）

| ID | 变体 | 结果 | 关键数 |
|---|---|---|---|
| B | HK 熔断 -25 | ✅ **PASS → 进冻结** | OOS2 +11.6 / train +1.8 / valid -1.2；HK past_year 线 +59.6→+64.0 |
| W1 | regime 连击 N=2/3（CN/HK） | ❌ 4/4 拒 | CN valid -45.1（胜率 9%）；HK valid -34.2/-76.4（胜率 4-9%）——只买连涨顶部，牛市全错过 |
| W2 | 旱后限仓 K=20/M=3 | ❌ 2/2 拒 | CN 三窗全劣；HK OOS2 -10.4 |
| E | ETF 绝对强度 floor 0.0 | ❌ no-op 拒 | 9 cells 全同 ±0.0；5 年长窗仅 1 天全弱（-0.1%）——零证据不进 Live |
| A | 产品级 NAV 水位节流 θ∈{7,10,12}×{half,pause} | ❌ 6/6 拒 | long：-13.9 ~ **-120.6pt**（-12:half 长窗一开 **22 个月** 2022-12~2024-10）；past_year 参考 -23.4 ~ -70.4（-12:pause 2026-03-24~04-29 锁死 4 月主升浪 -63.1pt） |

#### 两条结构性结论（§10）

1. **自感器 vs 水位计**：已实现盈亏熔断（快解除：30 天窗滚出即松手，CN 5 月实例 8 天）两次 PASS；
   MTM 水位节流（慢解除：要等价格收复半个回撤）6/6 拒——**防守机制的生死在解除动力学，不在触发**。
2. **引擎联合水位看不见产品漂移**：valid 窗引擎联合最大回撤仅 4.4%（05-25）——用户看到的 -17%
   是 twin 产品层现象（pick 重放 + 卫星 + 缩放），预注册信号在动机窗口本身不触发。

#### 关联

- 诊断 + 全部实验数：`docs/backtests/product-post-peak-drift-2026-09-09.md`（§6 指标层 / §7 预注册 / §8 W1·W2·E·B 裁决 / §9 枷锁分析 / §10 A 裁决）
- SUMMARY 拒收总表 W1/W2/E/A 四行
- 引擎旋钮保留默认关：`backtest_engine.py`（机制存档，冻结口径不变）
