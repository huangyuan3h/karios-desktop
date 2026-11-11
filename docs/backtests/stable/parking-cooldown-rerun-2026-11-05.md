# 停车场再入场冷却 · 2026-11-05 重跑（鞭打指标）· REJECT（维持 k=0）

> 前值：[`parking-cooldown-2026-09-13.md`](parking-cooldown-2026-09-13.md)（**REJECT**）· 预注册（门槛未改）：[`parking-cooldown-prereg-2026-09-13.md`](../../designs/parking-cooldown-prereg-2026-09-13.md)
> 脚本 `scripts/eval_parking_cooldown.py` · 报告 `data/backtest_reports/parking_cooldown_rerun_2026-11-05.json`
> **判定：k∈{3,5} 仍全挂 K1/K2/K4/K5 → 港湾 P1（k=0）继续维持；冷却不进 Live、不改产品路径默认值。**
> **Live 与回测的冲突（2026-11-05 OIL 513350 trail 卖出后同日/次日 ROTATE 又想买回）= 运维/交互层问题（trail 成交后压制同一 key 的当日再买提示），不是经济规则问题。**

## 0. 结论先行（Yuan 要的 0 vs 3 vs 5）

**选 k=0（现状）。** 冷却**确实**把鞭打买回砍掉了（valid 5/5/5 → 0/0/5，全窗 18 → 18→0），但代价远大于收益：

- valid 总收益 **−13.4pt**（k3）/ **−9.5pt**（k5）；
- valid MDD 只改善 **+1.2pt**（k3）/ **变差 −2.8pt**（k5），门槛是 ≥3.0pt；Sharpe **−0.40 / −0.20**（门槛 +0.10）；
- 换手反而增加（valid 21→27/25 笔），与"少动"直觉相反。

机制很清楚：k=3 把同一 key 的再入场**从 T+1 推到 T+4**，valid 的 5 次鞭打**一次没少**（`whip_rebuy_within_5` 仍是 5，与 k0 完全相同），只是把 V 型反抽的头 3 天让给了 GOLD/BOND10。**这正是 09-13 主死因 #1「截右尾」的第二次确认**，而不是新证据。

## 1. 这次改了什么（相对 09-13）

| 项 | 09-13 | 本次 2026-11-05 |
|---|---|---|
| ETF 面板 | `data/etf/etf_daily.csv` 至 2026-09-11 | 同文件，至 **2026-11-02**（5 条序列 / 1411 个交易日） |
| 网格 | `{0,1,2,3,5}` | 存档 JSON `{0,3,5}`；连续性对照另跑 `{0,1,2,3,5}`（不落盘） |
| 新增指标 | — | `trail_exits`、`whip_rebuy_within_{1,3,5}`、`days_by_key` / `time_in_oil` / `time_in_repo` |
| 机制/口径/门槛 | — | **一字未改**：同一 `parking_replay(cooldown_days=k)`、TRAIL_PCT 8、T-close 触发 / T+1-open 口径、0.05%/边、无 floor、无 STOCK gate、次优候选→否则 REPO；K1–K5 冻结不变 |

- **鞭打买回定义**：某日因 trail 出场（当日必为 REPO），**同一 key** 在随后第 1/3/5 个交易日内又被持有（买入或轮动回来）。按 key 计（纳指 513110/513100 同暴露）。trail 出场 key = 前一日收盘持有的 key。
- **k=0 vs 09-13 的差异不是机制变化**：qfq 前复权在新分红后整体重算 + 面板多 2 个月，k0 从 +62.0/+45.3/+50.3/+219.9 变为 **+50.6/+51.7/+50.2/+192.9**（valid 几乎不动 +50.3→+50.2，OOS2/long 因除权重锚下移）。脚本里的 `B11_P1` 常量已标注为"09-13 显示锚"，只用于提示漂移，不参与判定。

## 2. 收益表（total / Δ vs k0，单位 pt）

| 窗口 | k0 | k3 | k5 |
|---|---|---|---|
| OOS2 | +50.6 | +52.3 (**+1.7**) | +52.0 (**+1.4**) |
| train | +51.7 | +51.2 (−0.5) | +49.9 (**−1.8**) |
| valid | +50.2 | **+36.8 (−13.4)** | **+40.7 (−9.5)** |
| long | +192.9 | +188.5 (−4.4) | +202.5 (**+9.7**) |

## 3. 风险 / 换手表

| 指标 | k0 | k3 | k5 |
|---|---|---|---|
| valid MDD | −21.8 | −20.6（**+1.2pt**，门槛 ≥3.0） | −24.6（**变差 2.8pt**） |
| valid Sharpe | 2.13 | 1.73（−0.40） | 1.93（−0.20） |
| long MDD | −22.9 | −23.4 | −23.7 |
| long Sharpe | 0.97 | 0.96 | 1.02 |
| valid 交易数（有 side 的天数） | 21 | 27（**+6**） | 25（**+4**） |
| valid 冷却生效天数 | 0 | 18 | 25 |
| valid trail 出场次数 | 6 | 6 | 5 |

## 4. 鞭打指标（本实验存在的理由）

`trail / w1 / w3 / w5` = trail 出场次数 / 同 key 在 1 / 3 / 5 个交易日内回买的次数。

| 窗口 | k0 | k3 | k5 |
|---|---|---|---|
| OOS2 | 3 / 2 / 2 / 2 | 3 / **0 / 0** / 2 | 3 / **0 / 0 / 0** |
| train | 1 / 0 / 1 / 1 | 1 / **0 / 0** / 1 | 1 / **0 / 0 / 0** |
| valid | **6 / 5 / 5 / 5** | 6 / **0 / 0** / **5** | 5 / **0 / 0 / 0** |
| long | 12 / 9 / 10 / 10 | 12 / **0 / 0** / **10** | 10 / **0 / 0 / 0** |
| 合计 | 22 / **16 / 18 / 18** | 22 / **0 / 0 / 18** | 19 / **0 / 0 / 0** |

拆解（valid，k0 → k3）：

| 指标 | k0 | k3 | k5 |
|---|---|---|---|
| OIL 持仓天数占比 | 46.8% | 35.8% | 33.9% |
| REPO（现金）占比 | 5.5% | 5.5% | 4.6% |
| GOLD 占比 | 0% | 8.4%（9 天） | 9.3%（10 天） |
| BOND10 占比 | 0% | 3.7%（4 天） | 5.6%（6 天） |

**三条硬结论：**

1. **k0 的鞭打是真的**：valid 6 次 trail 里 5 次在次日就买回同一只（83%）；long 12 次里 9 次（75%）。Yuan 2026-11-05 遇到的 OIL 同 key 回买在历史上是**常态**，不是偶发 bug。
2. **但冷却没有消除鞭打，只把它推迟**：k3 的 `w1/w3 = 0` 是机制定义使然（冷却 3 天后才可买），而 `w5 = 18`全长窗 **与 k0 的 18 完全相同**。也就是说**每一次被挡的鞭打都在第 4 天回来了**，中间 3 天让给了次优腿。
3. **鞭打买回本身是赚钱的**：把 valid 的 5 次 T+1 回买延后 3 天，代价 −13.4pt；这 5 次回买的净贡献 ≈ 一个正的 V 反抽，砍掉它 = 直接砍右尾（#1）。

## 5. 判定明细（冻结 K1–K5，未放宽）

| k | K1 三窗每窗 ≥−0.5 | K2 三窗合计 >0 | K3 long ≥0 | K4 valid MDD ≥+3.0pt 或 Sharpe ≥+0.10（且 long MDD 不差 >1.0pt） | K5 非零邻居也过 K1/K2 | 结果 |
|---|---|---|---|---|---|---|
| k3 | ✗（valid −13.4） | ✗（−12.2） | ✗（−4.4） | ✗（MDD +1.2 / SR −0.40） | ✗ | **REJECT** |
| k5 | ✗（valid −9.5、train −1.8） | ✗（−9.9） | ✓（+9.7） | ✗（MDD −2.8 / SR −0.20） | ✗ | **REJECT** |

连续性对照（`{0,1,2,3,5}`，不落盘）：k1 valid −7.8 / long −20.4、k2 valid −6.7 / long +4.6，四档同样全挂，与 09-13 的排序一致；k5 的 long +9.7 仍是**单窗亮点**（OOS2/train/valid 合计 −9.9），按 #4 过拟合拒收。

与 09-13 对照（同门槛、同机制，数据多 2 个月）：

| k | 09-13 valid Δ | 本次 valid Δ | 09-13 long Δ | 本次 long Δ | 结论 |
|---|---|---|---|---|---|
| k1 | −7.9 | −7.8 | −21.7 | −20.4 | 不变（REJECT） |
| k2 | −6.8 | −6.7 | −0.5 | +4.6 | 不变（REJECT） |
| k3 | −13.4 | −13.4 | −10.1 | −4.4 | 不变（REJECT） |
| k5 | −9.5 | −9.5 | +5.0 | +9.7 | 不变（REJECT） |

**valid 数字逐档几乎一模一样** → 这不是样本期碰巧，是机制性结果。

## 6. 关键观察

1. **主死因仍是 #1 截右尾（预注册已写死，二次命中）**：冷却挡住的是急跌后的 V 反抽，而 ETF 停车场的收益来源之一就是这类反抽。
2. **风险目标依然达不到**：valid MDD 最好 +1.2pt（门槛 3.0）；k5 反而变差 2.8pt。**冷却不是"变现金"，而是"换腿"**——REPO 占比几乎没动（5.5%），资金去了 GOLD/BOND10，所以回撤峰值那一段没被避开（#4 regime）。
3. **换手反增**：停次优 + 回补 = valid 21→27 笔；冷却制造新的 churn。
4. **`whip_rebuy_within_5` 打平是最锋利的反证**：如果"少动=少亏"，k3 至少该让 w5 下降；事实是一次没少。
5. **k5 的 long +9.7 不可用**：三窗合计负、valid MDD 更差、K5 无邻居 → 单窗假象（与 09-13 k5 +5.0 同型）。

## 7. 处置 / 建议（研究层，不动产品）

- **维持 k=0**：`cooldown_days` 默认值在 `multi_asset_sleeve` / 展示 / Live 路径继续为 **0**；本次未改任何产品代码、未改 TRAIL_PCT、未动 M30 70/30、未动 B3。
- **Live 冲突按运维/交互处理**（这才是 Yuan 2026-11-05 真正的不适来源）：
  1. trail 成交（`SELL_TO_REPO`）后，**当日/次日对同一 key 的 ROTATE 建议在 UI/Bark 上降级或标注**「刚止损，回买=鞭打，历史上 83% 发生且事后正收益，但请人工确认」，不改引擎输出；
  2. 若要做到"机械压制"，必须按**新预注册**（真正的同一 key 再入场闸 + 归一化到 idle 份额）另开实验，不能把本 REJECT 的 k 值塞进 Live（纪律 §4）。
- 方向**不重开**：09-13 与本次两轮、valid 逐数复现，已足够定性；除非能解释"V 反抽可让渡"为何成立，那是另一个预注册。
- 前视门（L）：本次是纯 replay 改动 + 指标加法，`pick_parking` 仍只看 `prev` 收盘，无新增前视。

## 8. 复现

```bash
cd ~/Projects/karios-desktop/services/data-sync-service
export PATH="$HOME/.nvm/versions/node/v24.21.0/bin:$HOME/Library/pnpm:$PATH"
unset EASTMONEY_PROXY || true
# 存档网格（写新 JSON，不覆盖 09-13）
PYTHONPATH=src:scripts python3 scripts/eval_parking_cooldown.py \
  --grid 0,3,5 --save-report --report-name parking_cooldown_rerun_2026-11-05.json
# 连续性全网格（不落盘）
PYTHONPATH=src:scripts python3 scripts/eval_parking_cooldown.py --grid 0,1,2,3,5
# 单元测试（whip 指标 + 冷却状态机）
PYTHONPATH=src:scripts python3 -m pytest tests/test_eval_parking_cooldown_whip.py tests/test_harbor_parking.py --no-cov -q
```

## 9. 关联

- 前值 REJECT：[`parking-cooldown-2026-09-13.md`](parking-cooldown-2026-09-13.md) · 预注册：[`parking-cooldown-prereg-2026-09-13.md`](../../designs/parking-cooldown-prereg-2026-09-13.md)
- 基线：[`etf-parking-baseline-2026-09-13.md`](etf-parking-baseline-2026-09-13.md)（B11 §3.3）· 配方：[`strategy-recipes.md`](../../modules/strategy-recipes.md)
- 实现：`service/harbor.py`（`pick_parking(exclude_keys=...)` + `parking_replay(cooldown_days=...)`，默认零变化，本次**未改**）· 测试 `tests/test_harbor_parking.py`、`tests/test_eval_parking_cooldown_whip.py`
- 运行记录：`.opencode-runs/harbor_trail_reentry_cooldown_2026-11-05_report.md`
- 提交：`8c740cf4`（本报告 + JSON + 脚本指标 + 单测；未 push；紧随其后一个仅回填本行 hash 的 docs 提交）
