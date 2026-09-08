# 卫星分数分段：高分是否更好（2026-09-08 · 诊断关闭）

> **一句话**：用户假设"卫星仓高分（60档）比 0 分表现好"。历史验证**不可行**——06-18 之前的分数是今日配方重算的合成数（审计 D3，全合成）+ 幸存者宇宙（D4），梯度不可解释；引擎真分数时代（06-18 后）valid 尾 + holdout 共 ~80 笔，四档一分全 underpowered。回放起跑后按 as-of 门杀掉，**未出数**。唯一干净路径是前瞻攒（`paper_trades.score_at_entry`，20 fills 按 C4 口径结算）。任何结果出来前不把 score 写进卫星门，Live 不动。
> **关键词**：卫星 score分段 诊断关闭 D3合成分数 D4幸存者 前瞻paper

## 0. 由来（n=4 的眼缘）

Watchlist 卫星 4 持仓：60 分 +1.5% 居中，0 分三只 +2.8/−0.9/+0.4——高分既非最好也非最差，无信号。

`0.00` 是真零值，不是缺失：前端 `fmtScore(null)` 显示 `—`（`apps/desktop-ui/src/lib/watchlist-table-cells.ts:128`），0 说明 TrendOK 综合各 setup 分项全挂（另：`failed_score_cap=79`，0 不是 cap 出来的；`service/trendok_params.py:26`）。含义 = "无 setup"，用户实际想比的是"有 setup 痕迹（60）vs 无 setup（0）"。

## 1. 预注册冻结（跑前写死，未走到判定）

- H1（一句话机制）：S-3 高分票（强 RS + 主线 + 趋势结构）缺口后 3 日延续更强，低分缺口多为一日游。
- 最可能死因自查：#2 共线（score 即 S-3 入场门维度，卫星已拿缺口 + 低波 + R-wide）/ #6（80+ thin 幸存者）/ 跨域（K-b 与 threshold 证据是 60 天持有域，卫星是 3 天脉冲）。
- 口径：习惯 `c1_x1430` 卫星腿逐笔（clip4 strict `skip_t1`、`same_1430` 14:30 买、C1 3%、body=3、第 3 日 14:30 卖）；OOS2 + train，valid 不碰。
- Score as-of：entry **前一交易日**收盘后分数（买入 14:30 前严格已知）。
- 档位（K-b 旧切点，不偷看）：`<70 / 70-80 / 80+ / unscored`——`<70` 即真分数 60 档，`unscored` 即库中无分，正对用户要比的两格。
- 度量：逐笔净 %（`pnl_pct − 0.3`），pooled 均值/hit/n + 按 entry 周聚类均值 ±SE。
- Kill 线：K1——任一窗 spread（80+ − <70）聚类 ≤0 或零在线内即关；K2——任一档 n<100 只描述不判定。PASS 也要过 valid + paper 才谈候选，永不直进 Live。

## 2. 为什么杀掉（方法论 kill，非数字 REJECT）

- D3（`audit-2026-08-22.md:29` Major，已认）：`trade_date < 2026-06-18` 的分数是今日 `DEFAULT_TRENDOK_PARAMS` 重算，非当时实录，OOS2/train 全合成——梯度 = 公式和自己比。
- D4（同上 Critical）：回填宇宙 = registry 未来热门 + daily 存活全集，退市票永不计入——长窗/老窗天然幸存者偏倚。
- 回放（`compare_sat_exit_hhmm` 同 harness，OOS2 + train）起跑后按 as-of 门中止：出数即误导，不如不出（故无数字表）。
- 真分数时代 06-18 之后：valid 尾（~7 周）+ holdout（~4 周）共 ~80 笔，四档一分 → K2 underpowered。**历史上测不了，不是没测。**

## 3. 避损含义（用户目标对照）

- 若按合成分数的假梯度加"低分不买"门：砍掉的可能是真赢家（截右尾死因 #1 的门控版），省的是幻影成本，亏的是真收益——这页关闭的就是这笔潜在损失。
- 复杂度税：卫星已由桶 + 槽 + R-wide + C1 四层吸收（结构吸收律），再加一维大概率零增量（死因 #2），多一处腐烂面。
- 唯一能加收益的证据门：前瞻 spread 转正（聚类下界 >0）+ valid 复核 + paper 20 fills。门外一律不动。

## 4. 定位

- Live / paper / 通知零改动；`body=3`、C1、R-wide 维持。
- 前瞻口径（已就绪，无需新管线）：`paper_trades.score_at_entry`（twin 纸账当前 0 行，从零起记）+ 平仓 `pnl_pct`；满 20 fills 跑本页 §1 同口径（0 vs 60-70 spread 聚类），60 sessions 内不中看。
- 本页即 verdict：诊断关闭。重开条件只有一种——前瞻 spread 转正，历史重跑永不开（D3/D4 不可逆）。
