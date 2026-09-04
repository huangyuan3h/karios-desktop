# 回测总结（指向 **择强单轨** · 2026-08-29）

> **何时看**：任何人想「再回测 / 再优化 / 改实盘规则」之前——先读本页 + [`clip4-ops-decisions-2026-09-03.md`](clip4-ops-decisions-2026-09-03.md)（2026-09-03 讨论）+ [`modules/pick-strong-track.md`](../modules/pick-strong-track.md)（核心腿）+ [`state-bucket-algo-2026-08-31.md`](./state-bucket-algo-2026-08-31.md)（实盘默认机会双子星）。
> **一句话**：**实盘默认 = 机会双子星 v3.1 clip4**（择强核心 + strict S-gap 卫星 4×12.5%、第 3 日收盘卖、无 −5%）。单轨是核心腿 / Settings 对照。  
> S-3 / 套筒 / 信号池实验是子组件与拒收档案，**不再作为并列终局结论**。已 REJECT 的变体不要再当实盘方案提出。

---

## 0. 现行终局：机会双子星（核心 = 择强单轨）

| 项 | 内容 |
|----|------|
| 定义 | 择强核心（股票篮 + 金/油/纳/债 + REPO）+ strict S-gap 卫星 4×12.5%；无仓 100% 核心，开闸 50/50 |
| 真值文档 | [`state-bucket-algo-2026-08-31.md`](./state-bucket-algo-2026-08-31.md) · 核心腿 [`modules/pick-strong-track.md`](../modules/pick-strong-track.md) |
| 过去一年（定案 `mom_compare`+trail8） | **+190.7% / DD12.6%**（2025-08-28~2026-08-28）；无 trail 对照 +93.6/DD28.3 |
| 机会双子星 clip4（**实盘默认**） | 同窗 **+194.9 / sr2.64 / DD12.6**（Δ单轨 +4.3pt）；旧 15×5% 该窗 −0.2pt。滚到 2026-09-02 clip4 +204.0 vs 单轨 +197.6 |
| 三窗绝对 NAV（trail8） | OOS2 **+17.8** / train **+40.7** / valid **+139.1**（dd 18.0/8.4/11.9） |
| 报告 | `pick_strong_trail8_20260829.json` · `past_year_twin_vs_core_2026-09-02.json` · `opportunity_twin_star_v3_clip4_frozen.json` |
| 参数定案 | LB60·MA200·hold1·100% mom（[加固实验](pick-strong-hardening-2026-08-29.md) **维持 A0**；hold5/短 LB/risk-adj/Top2 拒收） |
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
| 核心 S-3 篮 10→5/4/3 | 4 变体 | **4 全部拒收**（OOS2 −20~−39） | 操作负担不能靠砍核心篮；见 [core-stock-clip](core-stock-clip-2026-09-03.md) |
| 卫星 body 后续 trail / −5% 入引擎 | 3 变体 | **3 全部拒收** | Live 已去掉 −5% overlay，只 body=3 收盘；见 [sat-exit-trail](sat-exit-trail-2026-09-03.md) |
| 卫星当日收盘 / 真 14:30 成交 | 2 变体 | **拒收当改写 9:30**；14:30 vs 核心 train/valid 亏 | 见 [sat-fill-same-close](sat-fill-same-close-2026-09-03.md) |
| 卫星 14:30 入场过滤 C1/C2 | 4 变体 | C1 3% 相对无过滤 PASS+（tot/sr/dd）；vs 核心 valid tot −3.3，**不进 Live** | 见 [sat-entry-c1](sat-entry-c1-2026-09-03.md) |
| 卫星习惯 3 天 vs 4 天 / 下午买点 | 7 变体 | 计数仍 3 天；body=4 占槽；13:30–15:00 无更佳分钟 | 见 [sat-habit-clock](sat-habit-clock-2026-09-03.md) |
| 卫星 C1 + 第 3 日 10:00/14:30 卖 | 3 变体 | C1·14:30 卖三窗 tot/sr/dd 过核心；**Live 已切 habit（2026-09-03 全量跟进）** | 见 [sat-exit-hhmm](sat-exit-hhmm-2026-09-03.md) |
| 卫星习惯排名 H1（无前视键） | 2 变体 | **2 全部拒收**：gap升序 OOS2 −96pt；\|14:30/今开−1\|升序 valid +14.4 但 OOS2 −21.5（过拟合陷阱，拒） | 见 [sat-rank-hhmm](sat-rank-hhmm-2026-09-04.md) |
| 卫星习惯 C1 网格 H2（2/3/4/5%） | 3 变体 | **C1=3% 维持**：2% 打平（train −4.0/sr−0.27，不换）；4% 走弱；5% train −5.8 拒收 | 见 [sat-c1-grid](sat-c1-grid-2026-09-04.md) |
| 卫星习惯 bucket_q H3（1/2 vs 1/3） | 1 变体 | **1/3 维持**：1/2 选参窗 tot/sr 全弱（train −2.3/sr−0.41），valid 无差 | 见 [sat-bucketq](sat-bucketq-2026-09-04.md) |
| 卫星习惯 R-wide 闸 H4（0.4/0.5/0.6） | 2 变体 | **0.5 维持**：0.4 valid −17.9；0.6 valid +13.4 但 OOS2 −15.7/train −8.1（过拟合陷阱，拒） | 见 [sat-rwide](sat-rwide-2026-09-04.md) |
| 卫星习惯 C3 下跌过滤 S2（风险排除） | 2 变体 | **不进 Live**：诊断两窗同向最差（<−3% 档 OOS2 −4.27%），但组合层面冗余（跳 564/fills−1，twin −0.3pt）——桶+槽位已吸收 | 见 [sat-c3-fade](sat-c3-fade-2026-09-04.md) |
| 卫星习惯 holdout 审计 S1（只读） | 19 sessions/32 fills | twin−core Δ **−5.1** ≈ 样本内第 7 百分位（p5 −5.53），分布内坏月份；**不调参**；方差才是真风险 | 见 [sat-holdout](sat-holdout-2026-09-04.md) |
| 卫星习惯 CHURN 过滤 S4（风险排除） | 1 变体（六维诊断筛一） | **不进 Live，记候选**：train +2.4/valid +1.5，但 OOS2 −1.0/sr−0.03（PASS/worse）；余下五维（板块/市值/年限/大盘高开/breadth）死在诊断 | 见 [sat-churn](sat-churn-2026-09-04.md) |
| 大盘风格 vs 卫星 G1（理解层） | 趋势×波动分组 | up 三窗全赚，choppy 次之，down 被 R-wide 拦（19 天开 1 天）；波动率非稳定亏钱因子；**无新规则** | 见 [sat-regime](sat-regime-2026-09-04.md) |

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
12. 习惯 R-wide 闸 0.5 单峰最优：0.4 valid −17.9；0.6 valid +13.4 但 OOS2 −15.7（拒）；C1=3% 平顶、桶 1/3 不敏感——习惯配方邻域无更优点，可复制

调参查找：用户说篮子太多 → [core-stock-clip](core-stock-clip-2026-09-03.md)；说止损/拿长一点/第几天卖 → [sat-exit-trail](sat-exit-trail-2026-09-03.md) + [讨论记录](clip4-ops-decisions-2026-09-03.md) + [第 3 日卖点](sat-exit-hhmm-2026-09-03.md)；说对齐 14:30 习惯回测 → [sat-fill-same-close](sat-fill-same-close-2026-09-03.md) + [C1 过滤](sat-entry-c1-2026-09-03.md) + [3 天/买点](sat-habit-clock-2026-09-03.md)。Agent 规则：仓库根 `AGENTS.md` → **Strategy / parameter changes**。

## 2. A 股 S-3（STOCK 腿 · 非终局产品）

> S-3 alpha = RS 转强 + 主线行业 + 环境感知 + 纪律空仓 —— **作为择强单轨的股票候选引擎保留**。

现行 NAV 基线：OOS2 **+47.3%** / train **+34.1%** / valid **+38.7%** n⚠️（`s3-baseline-20260828-nav`）。  
旧 117% / 333% 已封存。

## 3. HK 线（STOCK 腿的一部分）

NAV 重固化：OOS2 **+31.3%** / train **+1.9%** / valid **+60.7%** —— train 弱，**不作独立高置信叙事**；并入择强股票篮即可。

## 4. 下一步（只服务择强单轨）

| 方向 | 状态 |
|------|------|
| Timeline / live 与 `mom_compare` 定案对齐 | **[done] 2026-08-29** API+导出+Watchlist 文案/live pick |
| 择强 LB/MA/hold/cost/risk-adj/Top2 网格 | **[done] 2026-08-29** 维持 A0 |
| C4 paper 对照 | 进行中 |
| 新 S-3 信号 / 扫参 | **冻结** |

## 5. 文档地图

- **策略真值** → `modules/pick-strong-track.md`
- 参数（股票腿）→ `modules/strategy-params.md`
- 实验全记录 → `experiments-*.md`
- 审计 → `audit-verdict-2026-08-29.md`
- 旧「融合单轨」设计 → `designs/fused-single-track-optimization.md`（已加择强单轨指针）
