# 回测总结（指向 **择强单轨** · 2026-08-29）

> **何时看**：任何人想「再回测 / 再优化 / 改实盘规则」之前——先走仓库根 `AGENTS.md` → Strategy / parameter changes（主流程）。
> 本页是拒收总表 + 失败模式（内容层）。
> **一句话**：**实盘默认 = 机会双子星 v3.1 clip4**（择强核心 + strict S-gap 卫星 4×12.5%、第 3 日收盘卖、无 −5%）。单轨是核心腿 / Settings 对照。  
> S-3 / 套筒 / 信号池实验是子组件与拒收档案，**不再作为并列终局结论**。已 REJECT 的变体不要再当实盘方案提出。

---

## 0. 现行终局：机会双子星（核心 = 择强单轨）

| 项 | 内容 |
|----|------|
| 定义 | 择强核心（股票篮 + 金/油/纳/债 + REPO）+ strict S-gap 卫星 4×12.5%；无仓 100% 核心，开闸 50/50 |
| 真值文档 | [`state-bucket-algo-2026-08-31.md`](core/state-bucket-algo-2026-08-31.md) · 核心腿 [`modules/pick-strong-track.md`](../modules/pick-strong-track.md) |
| 过去一年（定案 `mom_compare`+trail8） | **+190.6% / DD12.6%**（2025-08-28~2026-08-28；定义见 state-bucket §3.0）；无 trail 对照 +93.6/DD28.3 |
| 机会双子星 clip4（**实盘默认**） | 同窗 **+194.9 / sr2.64 / DD12.6**（Δ单轨 +4.3pt；定义见 state-bucket §3.0）；旧 15×5% 该窗 −0.2pt。滚到 2026-09-02 clip4 +204.0 vs 单轨 +197.6 |
| 三窗绝对 NAV（trail8） | OOS2 **+17.8** / train **+40.7** / valid **+139.1**（dd 18.0/8.4/11.9；定义见 trail8 报告） |
| 报告 | `pick_strong_trail8_20260829.json` · `past_year_twin_vs_core_2026-09-02.json` · `opportunity_twin_star_v3_clip4_frozen.json`（旧实验档内的 190.7 系当时实跑基线，数据漂移 0.1pt；现行定数 +190.6） |
| 参数定案 | LB60·MA200·hold1·100% mom（[加固实验](core/pick-strong-hardening-2026-08-29.md) **维持 A0**；hold5/短 LB/risk-adj/Top2 拒收） |
| 优化范围 | 择强打分已扫一轮；**S-3 冻结 10×10%**；卫星 **body=3 收盘、无 −5%**；下一刀优先工程/对齐，不扫新卫星参 |

对照（非定案）：STOCK 优先 +110.8%；UI Timeline 旧口径 +123.9%；纯 CN S-3 引擎 +58.3%。

---

## 1. 历史回测阶段（组件沉淀 · 已结束信号探索）

自 2026-08-09 起的全部回测实验（success + failure 全记录）：

| 轮次 | 数量 | 结果 | 对择强单轨的意义 |
|------|------|------|------------------|
| 防守向攻击 A/B/C/D | 23 项 | 20 拒收 / 3 中性 / 0 采纳 | 股票腿过滤器已够严 |
| 信号池 P1-P26 | 15 项 | **15 全部拒收** | 不再加技术形态信号 |
| 早期实验 | ~10 项 | 全部拒收 | — |
| **固化进 STOCK 腿** | 环境感知 / 仓位 / 数据修复 | 见 strategy-params | 提供择强用的股票篮 |
| **固化进多资产腿** | mom60+MA200 / MIN_HOLD5 | multi_asset_sleeve | 择强 ETF 侧规则 |
| 核心 S-3 篮 10→5/4/3 | 4 变体 | **4 全部拒收**（OOS2 −20~−39） | 操作负担不能靠砍核心篮；见 [core-stock-clip](core/core-stock-clip-2026-09-03.md) |
| 卫星 body 后续 trail / −5% 入引擎 | 3 变体 | **3 全部拒收** | Live 已去掉 −5% overlay，只 body=3 收盘；见 [sat-exit-trail](sat/sat-exit-trail-2026-09-03.md) |
| 卫星当日收盘 / 真 14:30 成交 | 2 变体 | **拒收当改写 9:30**；14:30 vs 核心 train/valid 亏 | 见 [sat-fill-same-close](sat/sat-fill-same-close-2026-09-03.md) |
| 卫星 14:30 入场过滤 C1/C2 | 4 变体 | C1 3% 相对无过滤 PASS+（tot/sr/dd）；vs 核心 valid tot −3.3，**不进 Live** | 见 [sat-entry-c1](sat/sat-entry-c1-2026-09-03.md) |
| 卫星习惯 3 天 vs 4 天 / 下午买点 | 7 变体 | 计数仍 3 天；body=4 占槽；13:30–15:00 无更佳分钟 | 见 [sat-habit-clock](sat/sat-habit-clock-2026-09-03.md) |
| 卫星持有 1/2 天 vs 3 天 | 2 变体（OOS2+train，valid 未碰） | **REJECT/total**：body=2 OOS2 −17.8/train −2.4，夏普回撤全差；body=1 退化为 body=2（引擎先退后买，同日卖不可表达）；路径表 d1 均值互证截右尾 | 见 [sat-body1](sat/sat-body1-2026-09-07.md) |
| 卫星分数分段（高分 vs 0 分） | 0 变体（诊断关闭，未出数） | **方法论 kill**：06-18 前分数全合成（D3）+ 幸存者宇宙（D4），梯度不可解释；真分数时代 ~80 笔全 underpowered；唯一路径是前瞻 paper（`score_at_entry`，20 fills 结算），出结果前不进门 | 见 [sat-score-segment](sat/sat-score-segment-2026-09-08.md) |
| 卫星 C1 + 第 3 日 10:00/14:30 卖 | 3 变体 | C1·14:30 卖三窗 tot/sr/dd 过核心；**Live 已切 habit（2026-09-03 全量跟进）** | 见 [sat-exit-hhmm](sat/sat-exit-hhmm-2026-09-03.md) |
| 卫星第 3 日条件单 D3（高点−2% 否则 14:30） | 2 变体+自检 | **REJECT/total**：OOS2 −6.4/train −2.8/valid −5.8，夏普全差，触发率 ~63%；回吐≠反转，网格不补 | 见 [sat-exit-d3trail](sat/sat-exit-d3trail-2026-09-04.md) |
| 卫星名单漂移（全天振幅 vs 14:30-proxy 排名） | 1 诊断（OOS2+train，valid 未碰） | **无超额，不改 Live**：top-4 Jaccard 均值 0.43（相同 20/205 天），3 日前瞻 +2.05% vs +1.90%（差 0.14pp/笔）；Live 本来就是快照-proxy 排名 | 见 [sat-list-drift](sat/sat-list-drift-2026-09-04.md) |
| 卫星习惯口径冻结成绩单 OPT-141 | 1 口径×三窗 | **PASS+/beats_core**：OOS2 +76.3/+2.22/−1.9 · train +14.5/+1.53/−2.7 · valid +2.7/+0.21/0（09-03 逐数复现）；入场 100% 真 14:30 bar，出场 ~5% 收盘回退（已记血统） | 见 [sat-live-caliber](sat/sat-live-caliber-2026-09-04.md) |
| 卫星习惯排名 H1（无前视键） | 2 变体 | **2 全部拒收**：gap升序 OOS2 −96pt；\|14:30/今开−1\|升序 valid +14.4 但 OOS2 −21.5（过拟合陷阱，拒） | 见 [sat-rank-hhmm](sat/sat-rank-hhmm-2026-09-04.md) |
| 卫星习惯 C1 网格 H2（2/3/4/5%） | 3 变体 | **C1=3% 维持**：2% 打平（train −4.0/sr−0.27，不换）；4% 走弱；5% train −5.8 拒收 | 见 [sat-c1-grid](sat/sat-c1-grid-2026-09-04.md) |
| 卫星习惯 bucket_q H3（1/2 vs 1/3） | 1 变体 | **1/3 维持**：1/2 选参窗 tot/sr 全弱（train −2.3/sr−0.41），valid 无差 | 见 [sat-bucketq](sat/sat-bucketq-2026-09-04.md) |
| 卫星习惯 R-wide 闸 H4（0.4/0.5/0.6） | 2 变体 | **0.5 维持**：0.4 valid −17.9；0.6 valid +13.4 但 OOS2 −15.7/train −8.1（过拟合陷阱，拒） | 见 [sat-rwide](sat/sat-rwide-2026-09-04.md) |
| 卫星习惯 C3 下跌过滤 S2（风险排除） | 2 变体 | **不进 Live**：诊断两窗同向最差（<−3% 档 OOS2 −4.27%），但组合层面冗余（跳 564/fills−1，twin −0.3pt）——桶+槽位已吸收 | 见 [sat-c3-fade](sat/sat-c3-fade-2026-09-04.md) |
| 卫星习惯 holdout 审计 S1（只读） | 19 sessions/32 fills | twin−core Δ **−5.1** ≈ 样本内第 7 百分位（p5 −5.53），分布内坏月份；**不调参**；方差才是真风险 | 见 [sat-holdout](sat/sat-holdout-2026-09-04.md) |
| 卫星习惯 CHURN 过滤 S4（风险排除） | 1 变体（六维诊断筛一） | **不进 Live，记候选**：train +2.4/valid +1.5，但 OOS2 −1.0/sr−0.03（PASS/worse）；余下五维（板块/市值/年限/大盘高开/breadth）死在诊断 | 见 [sat-churn](sat/sat-churn-2026-09-04.md) |
| 卫星习惯耗尽否决 E-veto（形态做减法） | 1 诊断（OOS2+train，valid 未碰） | **REJECT/方向证伪**：耗尽候选两窗反倒更好（OOS2 +0.91pp/train +1.34pp），覆盖仅 2.4%；20 天顶否决 3 天脉冲 horizon 错配，关闭方向不补网格 | 见 [sat-exhaust-veto-2026-09-05](sat/sat-exhaust-veto-2026-09-05.md) |
| 卫星核心门控 C-gate（听大哥的） | 1 诊断（OOS2+train，valid 未碰） | **REJECT/双窗打架**：STOCK 桶 −3.27↔+0.14 翻面，无一桶双窗一致最差；REPO 触发 <1% 与 R-wide 共线；核心×卫星交互 regime-不稳定，关闭方向 | 见 [sat-core-gate-2026-09-05](sat/sat-core-gate-2026-09-05.md) |
| S-3 入场耗尽否决（核心动刀） | 1 诊断（OOS2+train，valid 未碰） | **REJECT/零覆盖**：冻结 S-3 实现交易 0/93、0/51 命中（S-3 钓趋势早期，ret60 中位 ~5%，与末期派发顶不交集）；否决=基线，关闭方向 | 见 [s3-exhaust-veto-2026-09-05](core/s3-exhaust-veto-2026-09-05.md) |
| 对冲双子星 v0.2（全新算法 · 做空耗尽顶） | 三窗+2023+产品窗+严格有券敏感性 | **PASS+（样本内折扣版）**：vs 习惯 Δ+6540/+1221/+1333，sr全升dd全降，2023熊 +1783；敏感性 Δ+1023/+666/+508；绝对值禁入仓位决策，待 paper-shorts ≥20 笔前瞻 | 见 [hedge-twin-2026-09-05](hedge/hedge-twin-2026-09-05.md) |
| 对冲双子星 v0.2 修正（成交语义） | 同上（限价成交重算） | **REJECT/实现证伪**：正确语义下 strict 65.3%/+0.92%（门 85–93% 未过），最差 −236%，23% 亏超 5%；冻结 89% 表系 naive-touch 幻影；附带 `factor_signals` 港股误标（OPT-146）+ strategy-params §7 联动暂停 | 同上 §1–§2 |
| 大盘风格 vs 卫星 G1（理解层） | 趋势×波动分组 | up 三窗全赚，choppy 次之，down 被 R-wide 拦（19 天开 1 天）；波动率非稳定亏钱因子；**无新规则** | 见 [sat-regime](sat/sat-regime-2026-09-04.md) |
| CPA-CN v1a（外部策略独立验证 · A-only · 不动 Live） | 三窗+产品窗+长窗（10×10%·次日开盘·含成本涨跌停） | **REJECT**：OOS2 +5.8/train +13.1/valid **−13.5**（ΔS-3 −42/−21/−52pt），产品窗 −11.4，long −53.3/DD−66/最差−30%；胜率26–35%、持有~6天、~90% wedge_drop 鞭打、信号~1000/天无选择（v1 混入 HK 版作废见档 §8） | 见 [cpa-cn-v1-2026-09-08](cpa/cpa-cn-v1-2026-09-08.md) |
| SRV 指数验证（质疑指数本身 · 诊断 · 不动 Live） | Q1 指数前瞻分组 + Q2 习惯 fills 按 entry 日 SRV 分组 + Q3 阈值滑动/维度分解 | **REJECT 当开关**：Q1 两半同向但 ~0.5pt/3d 且后半无 Stable 样本；Q2 Extreme valid +1.04%/holdout −0.39% 打架，Stable 只活在 2025-12-18~2026-02-02 单块（与时段共线）；Q3 triple 57%/leader 70% 天数满分、阈值滑不动"天天 Extreme"、60 天校准窗无 Stable 却定了 Stable 线；降级纯记录（twin snapshot 已带 srv*，20 fills 后 C4 归因） | 见 [srv-validation-2026-09-08](srv/srv-validation-2026-09-08.md) |
| STOCK 篮剥离港股对照（诊断 · 不动 Live） | 同 builder，CN-only snaps vs A+H 合并 × 三窗+past_year | **REJECT 剥离**：港股正贡献——OOS2 fused Δ−27.5pt、valid Δ−80pt（valid 上攻是 HK 带的，剥离后 STOCK 只选中 2 天 vs 16 天）；唯一反例 train +11.2pt（单窗不采信）；OOS2 回撤 18.0→29.6 亦变差 | 见 [stock-basket-nohk-2026-09-08](core/stock-basket-nohk-2026-09-08.md) |
| 双子星现实版（core-HK现实 + 卫星习惯 opp blend · 策略零改动） | 同 builder，core 腿现实HK合并 vs 冻结合并 × 三窗+past_year+long | **期望重置**：OOS2 +84.0 / train +53.3 / valid +85.8 / past_year +137.8（平安90bps最终版；60bps中间版 +82.2/+51.5/+85.8/+118.7 见档）；valid −56pt 主因 pick 层（STOCK天16→29挤掉NASDAQ 38→26），不是 fill 层 | 见 [hk-settle-t2-2026-09-08](hk/hk-settle-t2-2026-09-08.md) §9/§11 |
| 指数趋势延续性（大资金主导 proposal 证伪 · 预注册 · 不动 Live） | 000300（SSE50代理）+000688（科创✓）MA20/60趋势态诊断 + 次日开盘回放 × 三窗，vs 双子星现实版+买持 | **REJECT/方向证伪+六格全败**：bull态远期四切全弱于无条件（edge −0.8~−8.8pp，科创fwd60 +5.3 vs +14.1——涨幅在V反转里，MA全踏空）；趋势腿OOS2 −6.6/−8.2、train +7.1/+30.9、valid −10.0/+7.7，6格全败twin且**全输买持**；valid窗300买持仅+0.3（"年年30%"无base-rate支撑）；弱年OOS2 twin +84 > 科创买持+39 > 300买持+17.8 | 见 [index-trend-bigmoney-2026-09-08](index/index-trend-bigmoney-2026-09-08.md) |

**48+ 次失败的共同模式**（仍有效，勿重开）：
1. 绝对量技术形态 → 无增量
2. 防守收紧 → 截断右尾
3. 与 RS 共线 / 闸门重合 → 零增量
4. 单窗好看 = 过拟合
5. 砍核心 S-3 篮宽度（10→5/4/3）→ OOS2 弱市年崩
6. 卫星 −5% 当常规退出、或 body 后续 trail → 截断 3 日脉冲 / 占满 4 槽
7. 把 Live 14:30 / 收盘成交写进冻结 T 开盘 → 日历错位；valid 上卫星边几乎消失
8. 无过滤 14:30 习惯 vs 核心：train/valid 总收益亏，train 夏普也略差；C1 3% 修好夏普/回撤，valid 总收益仍 −3.3，不进 Live
9. 14:30 改拿第 4 日 = 占槽税（aligned −16pt）；下午换分钟翻不了 valid
10. 第 3 日改 10:00 卖不如 14:30 卖；C1+第 3 日 14:30 卖才三窗过核心（习惯 Live 配方，冻结 T 开盘对照保留）
11. 习惯排名用无前视键（最小缺口 / 盘中越平静越优先）打不过全天振幅：gap 升序 OOS2 −96pt 永不重开；|runup| 升序 valid +14.4 但 OOS2 −21.5（拒）；valid 好看 + 选参窗崩 = 过拟合陷阱
12. 第 3 日盘中条件单（高点−2% 否则 14:30）三窗全拒且 valid 转负：触发率 63%，回吐≠反转，系统性卖在反弹前；回撤端也没赚到。机制证伪，不补网格
13. 习惯 R-wide 闸 0.5 单峰最优：0.4 valid −17.9；0.6 valid +13.4 但 OOS2 −15.7（拒）；C1=3% 平顶、桶 1/3 不敏感——习惯配方邻域无更优点，可复制

> 调参查找（用户说法 → 对口实验）已移至仓库根 `AGENTS.md` → Strategy / parameter changes（主源，本页不再复述）。

## 2. A 股 S-3（STOCK 腿 · 非终局产品）

> S-3 alpha = RS 转强 + 主线行业 + 环境感知 + 纪律空仓 —— **作为择强单轨的股票候选引擎保留**。

现行 NAV 基线见 [`strategy-params.md` §3](../modules/strategy-params.md)（OOS2/train/valid + 旧口径封存）。
旧 117% / 333% 已封存。

## 3. HK 线（STOCK 腿的一部分）

NAV 重固化：OOS2 **+31.3%** / train **+1.9%** / valid **+60.7%** —— train 弱，**不作独立高置信叙事**；并入择强股票篮即可。

> T+2 交收现实化（2026-09-08，已固化）：`settle_lock_sessions=2` + 滑点 +0.2 + 平安最高佣金（round-trip 90bps）后 OOS2 +2.1 / train −3.0 / valid +64.1；past_year 现实 +57.3/83笔（基线+57.7/101笔，回撤27.8→15.3）；双子星现实 +84.0/+53.3/+85.8/+137.8。规则已联网核验（T+2 ✓、印花税0.1% ✓、微费~1.27bps/边未建模≈0.2pt）；R1 pyramid关已拒（valid −21.7pt）；R2/R3 归因关闭。策略不换、paper settled账本已落地（OPT-148 ✅）。见 [hk-settle-t2-2026-09-08](hk/hk-settle-t2-2026-09-08.md)，现实数字固化于 strategy-params §1b。

## 4. 下一步（只服务择强单轨）

| 方向 | 状态 |
|------|------|
| Timeline / live 与 `mom_compare` 定案对齐 | **[done] 2026-08-29** API+导出+Watchlist 文案/live pick |
| 择强 LB/MA/hold/cost/risk-adj/Top2 网格 | **[done] 2026-08-29** 维持 A0 |
| C4 paper 对照 | 进行中 |
| 新 S-3 信号 / 扫参 | **冻结** |

## 5. 文档地图

- **策略真值** → `modules/pick-strong-track.md`
- **基础规律与不变量（新想法先自查）** → [`first-principles-2026-09-05.md`](./first-principles-2026-09-05.md)
- 参数（股票腿）→ `modules/strategy-params.md`
- 实验全记录 → `experiments-*.md`
- 审计 → `audit-verdict-2026-08-29.md`
- 旧「融合单轨」设计 → `designs/fused-single-track-optimization.md`（已加择强单轨指针）
