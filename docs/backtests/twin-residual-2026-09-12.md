# 双子星残余优化三方向：缺口门槛 / CHURN / 核心菜单（2026-09-12 · 全 REJECT/no-op）

> **一句话**：卫星信号/参数空间已冻结，逐一排查后只剩三个「未测」方向——① 卫星缺口门槛 gap>3%（硬编码、从未扫过）；② CHURN 入场过滤（first-principles 维6 唯一候选，用当前 habit 口径重验）；③ 核心多资产菜单扩展（择强 argmax 只吃 4 个 ETF）。三窗验证：① 3% 仍是平台顶（2% 在 OOS2/train 好但 valid+回撤劣，4/5% 直接拒）；② churn5 仅 PASS 非 PASS+（OOS2 −0.6），维持候选不上 Live；③ 核心菜单加股票型 ETF **强 REJECT**（OOS2 −19~−24pt、回撤 +11~+16pt）——证明现有 GOLD/OIL/NASDAQ/BOND 是**分散器而非随便的池子**。**Live 全部不动。**
> **关键词**：双子星 残余优化 gap门槛 CHURN 核心菜单 局域最优

**脚本**：`scripts/compare_twin_residual.py`（D1/D2）· `scripts/compare_core_menu.py`（D3）
**原始表**：`data/backtest_reports/twin_residual_2026-09-12.json` · `core_menu_expand_2026-09-12.json`
**口径**：习惯冻结（C1 3% · `same_1430` · `amp_1430` 排序 · clip4 4×12.5% · body=3 第 3 日 14:30 卖）+ 核心择强 trail8 + `opp_50`。三窗 OOS2/train/valid。判据 = twin NAV tot/sr/dd vs base。

---

## 0. 为什么要找这三个

`docs/todo.md` 明确「不扫新卫星参」「验证完之前不调参」；`SUMMARY` 48+ 拒收 + `first-principles` 结构吸收矩阵显示信号维度基本全吸收。逐项排查后，真正**从未测过**的只剩：

| # | 方向 | 为什么算「没测过」 |
|---|------|-------------------|
| D1 | 卫星缺口门槛 gap>3% | `_day_features` 里硬编码；C1（open→14:30）不是它，从未扫 |
| D2 | CHURN（T-1 放量） | 维6 唯一存活候选，旧测 `train +2.4/valid +1.5/OOS2 −1.0`；未在当前 habit 口径复验 |
| D3 | 核心 ETF 菜单扩展 | 择强只吃 GOLD/OIL/NASDAQ/BOND；更多候选资产 = 更大 argmax 机会集，从未测 |

引擎加研究门控 `min_gap_pct`（默认 `MIN_GAP_PCT=0.03`，行为不变；`build_sgap_timeline` 透传）。D2 用现成 `max_t1_turnover_mult`。

## 1. D1 缺口门槛（Δ twin total vs base，pt）

| 变体 | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| gap>2% | **+8.3** | +1.9 | −3.9 | worse_sharpe+worse_dd |
| gap>3%（base） | 0 | 0 | 0 | 现任 |
| gap>4% | −6.1 | −11.1 | −0.8 | **REJECT** |
| gap>5% | −33.8 | −9.5 | +12.3 | **REJECT** |

读法：`gap>2%` 放宽候选池，OOS2/train 收益升、OOS2 卫星回撤 11.4→5.2（分母更多样），但 valid 总收益 −3.9、且 sr/dd 两窗回退——**不是三窗一致的平台移动**。收紧（4/5%）明显砍掉真 alpha（窄池 → 弱市崩）。**3% 维持。**

## 2. D2 CHURN 入场过滤（skip T-1 成交额 >Nx 均值；Δ twin total vs base，pt）

| 变体 | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| churn 3x | −5.0 | +1.4 | +8.3 | worse_sharpe |
| churn 4x | −1.1 | +1.9 | +6.8 | worse_sharpe |
| churn 5x | −0.6 | +0.0 | +4.6 | **PASS（非 PASS+）** |

读法：与 2026-09-04 旧档**同形**——train/valid 正、OOS2 平/负。最松的 5x 是唯一「PASS」（无 sr/dd 劣化），但 OOS2 总收益仍 −0.6、非三窗全正，按 house 规则**不够进 Live（要 PASS+）**。机制诚实：CHURN 省的是「卫星不碾压核心」时的槽，OOS2 卫星碾压核心，省槽=省错地方。**维持候选，不动 Live。**

## 3. D3 核心多资产菜单扩展（Δ core NAV tot/sr/dd vs base）

| 变体 | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| +CSI300+CSI500 | **−21.1** / sr−0.71 / dd+10.9 | +1.6 | 0.0 | REJECT |
| +HSTECH | −19.2 / −0.61 / +13.8 | −6.2 | 0.0 | REJECT |
| +CSI500+ChiNext+HSTECH | −23.7 / −0.69 / +16.3 | **+26.0** | **−35.0** | REJECT |

读法：股票型指数 ETF 在多头里 mom60 很高，会**从集中的 STOCK 篮或分散资产手里抢走 pick**，把核心推向股票 beta——OOS2 弱市年直接 −19~−24pt、回撤 +11~+16pt。`m_all` 的 train +26.0 是「单窗好看」陷阱（valid −35）。**结论：核心 4 ETF 菜单（金/油/纳/债）是刻意挑的分散器，加股票型资产是负优化。** 真要有增量，需要的是非股票类新资产（有色/农产品等 CN 无充足数据），不在本轮。

## 3b. 追加：闲置资金吃套筒/REPO 对双子星基本无效（2026-09-12）

先澄清两个层次：
- **金/油/纳/债已经是双子星核心的候选资产**（`MULTI_TS`，择强 argmax 里选）——base core OOS2 的 pick 分布 = GOLD 60 天 / NASDAQ 22 / OIL 14 / BOND10 10。所以「原油之类的」**在回测里**。
- **闲置套筒（T6 / R5CS）是另一层**，服务的是**纯 A 股 S-3 线**（平均闲置 57%，2026-06/07 大段空仓），在 CN/HK dual 线 `run_walk_forward_dual.py` 验过（引擎 NAV 重跑 R5CS vs R5C **+3.3/+8.4/+13.5**），live `allocation.py` 三元池 + `sleeve_paper_auto.py` 已落地。

但**双子星没有大闲置**：核心本身就是多资产轮动；卫星 active 日平均已填 **3.3~3.8/4 槽**，真闲置只占组合 **~1.2~2.9%**（活跃日）。实测（`compare_twin_idle.py`，post-hoc 重混冻结习惯腿）：

| 变体 | OOS2 | train | valid | 判定 |
|------|------|-------|-------|------|
| opp50（冻结） | +82.4 | +58.3 | +84.1 | 现任 |
| 闲置→核心 | −9.3 | −0.7 | +2.4 | **REJECT**（弱市年输，与 PS-G50-X x-series 同形） |
| 闲置→REPO（0.7%/yr） | +0.0 | +0.0 | +0.0 | **no-op**（量级 <0.1pt，看不见） |

**结论：卫星闲置这一刀对双子星没有空间**（套筒对小体量闲置≈噪音）。T6/R5CS 的价值只在纯 S-3 线，且那边已建好。

## 4. 总判定

| 方向 | 结果 | Live |
|------|------|------|
| D1 gap 门槛 | 3% 平台顶，2%/4%/5% 均不达三窗一致 | 不动 |
| D2 CHURN | 5x 仅 PASS（OOS2 −0.6），非 PASS+ | 不动（候选） |
| D3 核心菜单 | 加股票型 ETF 强 REJECT（dd 恶化主因） | 不动 |
| D4 闲置资金 | 卫星闲置仅 ~1.2–2.9%，idle→core REJECT、idle→REPO ±0.0 | 不动 |

**四方向全数不采纳 → 双子星信号/结构/资金效率层确认局域最优。** 剩余「空间」不在选参，而在**验证**（paper 20 笔 C4、live↔回测对账）与**换赛道的新策略孵化**（P0-12）。

## 5. 复现

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/compare_twin_residual.py --save-report
PYTHONPATH=src:scripts python3 scripts/compare_core_menu.py --save-report
```
