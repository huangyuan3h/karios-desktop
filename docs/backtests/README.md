# Karios 回测实验记录（Backtest Experiments）

> **何时看**：任何新回测实验前、复盘策略演进时。**用户要改策略 / 仓位 / 退出时，Agent 先读 [`SUMMARY.md`](./SUMMARY.md) 和本目录实验，再开口。**
> **现行产品基线**：**港湾（Harbor）= S-3 核心 + 闲置现金 ETF 停车场**（tag `harbor-p1-20260913`，Live 2026-09-13）—— [`stable/etf-parking-baseline-2026-09-13.md`](stable/etf-parking-baseline-2026-09-13.md)；真值 [`pick-strong-track.md`](../modules/pick-strong-track.md)。
> **产品候选**：**母港** = 港湾 × B3 风险预算 50/50（[B15](stable/harbor-riskbudget-2026-09-13.md)，PASS，未进 Live）。
> **历史/已作废**：机会双子星 v3.1 clip4 + 择强单轨（OPT-177 前视、B12 口径 bug）—— 见 [三策略审计 2026-09-14](audit-three-strategy-lookahead-2026-09-14.md)。
> 本目录记录通往该基线的实验（含拒收）；**新结论必须写清对港湾核心 / 母港的增量**（历史实验对双子星的增量保留）。
>
> **改策略流程**：走仓库根 `AGENTS.md` → Strategy / parameter changes（主源，含调参查找）。
> 本目录只管实验导航 + 验证纪律。
> **SUMMARY 标题注**：「指向择强单轨」是历史名；现行 = 港湾，母港为候选，双子星/择强为历史。
>
> **编码速查**：`P*` → `experiments-planned.md`（信号池 P1-P26）· `D*` → `experiments-d-pool.md`（D1-D8）
> · `A*/B*/C*` → `experiments-defensive.md`（防守 23 项）· `C1` → `sat-entry-c1` · `H1/H2/H3/H4` → rank/c1-grid/bucketq/rwide
> · `S1/S2/S4` → holdout/c3-fade/churn · `G1` → `sat-regime` · `D3` → `sat-exit-d3trail` · `rw*` → `sat-rwide`。
>
> **目录结构**（2026-09-06 按策略分文件夹 · 根只留索引与真值）：
> `core/` 择强核心+套筒（9）· `sat/` 卫星习惯 09-02~09-05（22）· `s3/` S-3 早期档案防守/信号池（11）
> · `factors/` 因子与形态（10）· `hedge/` 对冲楼（1）· `vendor/` 外购数据对拍（2）· `index/` 指数趋势证伪（1）。

---

## 本目录是什么

| 文档 | 内容 | 状态 |
|------|------|------|
| [`SUMMARY.md`](./SUMMARY.md) | **总览（含 B11–B17 + 三策略审计修正）** | ✅ **必读入口** |
| [`../modules/pick-strong-track.md`](../modules/pick-strong-track.md) | **港湾（Harbor）策略真值（S-3 核心 + 停车场）** | ✅ **产品真值** |
| [`audit-plan-2026-08-29.md`](./audit-plan-2026-08-29.md) | 组合可信度审计计划 | ✅ |
| [`audit-verdict-2026-08-29.md`](./audit-verdict-2026-08-29.md) | 审计结论（P0 已修） | ✅ |
| [`pick-strong-hardening-2026-08-29.md`](core/pick-strong-hardening-2026-08-29.md) | 择强参数加固网格 · **维持 A0** | ✅ |
| [`audit-2026-08-22.md`](./audit-2026-08-22.md) | 代码层审计（数据/执行/统计） | ✅ |
| [`experiments-tip014.md`](s3/experiments-tip014.md) | TIP-014 环境感知（STOCK 腿） | ✅ |
| [`experiments-d-pool.md`](s3/experiments-d-pool.md) | 探索池 D1-D8 | ✅ |
| [`experiments-defensive.md`](s3/experiments-defensive.md) | 防守向攻击 23 项 | ✅ |
| [`experiments-legacy.md`](s3/experiments-legacy.md) | 历史实验速查 | ✅ |
| [`experiments-planned.md`](s3/experiments-planned.md) | 信号池 P1-P26（全拒收） | ✅ |
| [`s3-gate-pickstrong-optimization-2026-09-01.md`](core/s3-gate-pickstrong-optimization-2026-09-01.md) | S-3 gate 在择强内的松闸优化（10变体三窗拒收归档） | ✅ 拒收 |
| [`sat-clip-concentration-2026-09-02.md`](sat/sat-clip-concentration-2026-09-02.md) | 卫星单票 5%→10%/12.5%/16.5% NAV（**4 只×12.5% 冻结**） | ✅ 冻结 |
| [`core-stock-clip-2026-09-03.md`](core/core-stock-clip-2026-09-03.md) | 核心 S-3 篮 10 只→5/4/3（加大单票） | ❌ 拒收（OOS2） |
| [`sat-exit-trail-2026-09-03.md`](sat/sat-exit-trail-2026-09-03.md) | 卫星 body=3 vs −5% vs body 后 trail 5/8% | ❌ 拒收 |
| [`sat-hold-path-day2-2026-09-03.md`](sat/sat-hold-path-day2-2026-09-03.md) | 卫星第 1/2/3 日收盘路径（第 2 天亏了回不回） | ✅ 观察；不改 Live |
| [`sat-fill-same-close-2026-09-03.md`](sat/sat-fill-same-close-2026-09-03.md) | 卫星成交：T 开盘 vs 收盘 vs **真 14:30** | ❌ 14:30 相对核心 train/valid 亏；不改 Live |
| [`sat-entry-c1-2026-09-03.md`](sat/sat-entry-c1-2026-09-03.md) | 14:30 入场过滤 C1（冲太高不买）/ C2（近涨停） | ❌ 不进 Live；C1 3% 修好夏普/回撤，valid tot 仍 −3.3 |
| [`sat-habit-clock-2026-09-03.md`](sat/sat-habit-clock-2026-09-03.md) | 习惯 3 天 vs 4 天 · 13:30–15:00 买点 | ❌ 计数仍 3 天；换分钟无更佳；不改 Live |
| [`sat-exit-hhmm-2026-09-03.md`](sat/sat-exit-hhmm-2026-09-03.md) | C1 + 第 3 日 10:00 / 14:30 / 收盘卖 | ✅ C1·14:30 卖三窗过核心；**Live 已切 habit（2026-09-03）** |
| [`sat-clock-unify-1430-2026-09-11.md`](sat/sat-clock-unify-1430-2026-09-11.md) | 时钟统一：14:30 买+卖 · `amp_1430` 零前视排序 | ✅ Live/回测/审计/paper 全统一（2026-09-11） |
| [`clip4-ops-decisions-2026-09-03.md`](./clip4-ops-decisions-2026-09-03.md) | 10 只篮 / 止损 / 第 3 日收盘：讨论 + Live 对齐 | ✅ 记录（第 3 日 14:30 卖）。15:00 见钟统一档 |
| [`../designs/sat-entry-filter-phase1-2026-09-03.md`](../designs/sat-entry-filter-phase1-2026-09-03.md) | 卫星入场过滤一阶段（尾盘买点 / 第 2 天 / 14:30 不买） | C1 已三窗，未进 Live |
| [`state-bucket-algo-2026-08-31.md`](core/state-bucket-algo-2026-08-31.md) | 状态分桶/机会双子星 v3.1 clip4（可执行最优） | ✅ |
| [`README.md`](./README.md) | 本索引 | — |

### 09-04 → 09-08 增补（习惯 Live 打磨 + 规律提取 · 明细只看 SUMMARY §1）

| 文档 | 内容 | 状态 |
|------|------|------|
| [`sat-rank-hhmm-2026-09-04.md`](sat/sat-rank-hhmm-2026-09-04.md) | 习惯排名 H1：无前视键（gap/‖runup‖升序） | ❌ 全拒（gap OOS2 −96 永不重开） |
| [`sat-c1-grid-2026-09-04.md`](sat/sat-c1-grid-2026-09-04.md) | 习惯 C1 网格 H2（2/3/4/5%） | ✅ C1=3% 维持（平顶） |
| [`sat-bucketq-2026-09-04.md`](sat/sat-bucketq-2026-09-04.md) | 习惯桶 H3（1/2 vs 1/3） | ✅ 1/3 维持 |
| [`sat-rwide-2026-09-04.md`](sat/sat-rwide-2026-09-04.md) | 习惯 R-wide 闸 H4（0.4/0.5/0.6） | ✅ 0.5 维持（单峰） |
| [`sat-c3-fade-2026-09-04.md`](sat/sat-c3-fade-2026-09-04.md) | 习惯 C3 下跌过滤 S2 | ❌ 组合冗余，不进 Live |
| [`sat-holdout-2026-09-04.md`](sat/sat-holdout-2026-09-04.md) | 习惯 holdout 审计 S1（只读） | ✅ 不调参；方差是真风险 |
| [`sat-churn-2026-09-04.md`](sat/sat-churn-2026-09-04.md) | 习惯 CHURN 过滤 S4 | ❌ 不进 Live，记候选 |
| [`sat-list-drift-2026-09-04.md`](sat/sat-list-drift-2026-09-04.md) | 卫星名单漂移诊断（振幅 vs 14:30-proxy） | ✅ 无超额，不改 Live |
| [`sat-regime-2026-09-04.md`](sat/sat-regime-2026-09-04.md) | 大盘风格 vs 卫星 G1（理解层） | ✅ 无新规则 |
| [`sat-exit-d3trail-2026-09-04.md`](sat/sat-exit-d3trail-2026-09-04.md) | 第 3 日条件单 D3（高点−2%） | ❌ 机制证伪，不补网格 |
| [`sat-live-caliber-2026-09-04.md`](sat/sat-live-caliber-2026-09-04.md) | 习惯口径冻结成绩单 OPT-141 | ✅ PASS+/beats_core |
| [`sat-exhaust-veto-2026-09-05.md`](sat/sat-exhaust-veto-2026-09-05.md) | 卫星耗尽否决 E-veto | ❌ 方向证伪（horizon 错配） |
| [`sat-core-gate-2026-09-05.md`](sat/sat-core-gate-2026-09-05.md) | 卫星核心门控 C-gate | ❌ 双窗打架 |
| [`s3-exhaust-veto-2026-09-05.md`](core/s3-exhaust-veto-2026-09-05.md) | S-3 入场耗尽否决 | ❌ 零覆盖 |
| [`hedge-twin-2026-09-05.md`](hedge/hedge-twin-2026-09-05.md) | 对冲双子星 v0.2（做空耗尽顶） | ❌ 实现证伪（幻影成交） |
| [`sat-bear-replay-2026-09-05.md`](sat/sat-bear-replay-2026-09-05.md) | 习惯配方熊市回放 2021–2023 | ✅ PASS，不调 Live |
| [`sat-weight-6040-2026-09-05.md`](sat/sat-weight-6040-2026-09-05.md) | 卫星权重 60/40（预注册见 designs） | ❌ 薄增益，不进 Live |
| [`sleeve-exit-hard20-2026-09-04.md`](core/sleeve-exit-hard20-2026-09-04.md) | 套筒 20d −10% 硬切 | ❌ 单窗亮，不进 Live |
| [`scoop-exhaustion-oos-check-2026-09-04.md`](factors/scoop-exhaustion-oos-check-2026-09-04.md) | 形态独立验证首跑 | ✅ 目录核查，不进 S-3 |
| [`vendor-minute-compare-2026-09-05.md`](vendor/vendor-minute-compare-2026-09-05.md) · [`vendor-adj-compare-2026-09-05.md`](vendor/vendor-adj-compare-2026-09-05.md) | 外购分钟/复权对拍 | ✅ 有条件过 / 只报不修 |
| [`first-principles-2026-09-05.md`](./first-principles-2026-09-05.md) | 基础规律与不变量（新想法先自查） | ✅ §一–§六 |
| [`validation-gates-v2-2026-09-16.md`](./validation-gates-v2-2026-09-16.md) | **验证门控 v2（唯一裁决标准 · 多窗+长窗+多角度 + 前视 L 门）** | ✅ **新预注册必读** |
| [`stable/five-strategy-v2-scorecard-2026-09-16.md`](stable/five-strategy-v2-scorecard-2026-09-16.md) | 五策略 v2 记分卡（当前数据横向比较 · 只读） | ✅ 09-16 |
| [`leg-fingerprints-2026-09-06.md`](./leg-fingerprints-2026-09-06.md) | 各腿指纹表（胜率×单笔×周转×右尾×方差） | ✅ 右尾/月度待补 |
| [`sat-body1-2026-09-07.md`](sat/sat-body1-2026-09-07.md) | 持有 1/2 天 vs 3 天（OOS2+train，valid 未碰） | ❌ 全拒；body=1 退化为 body=2 |
| [`sat-score-segment-2026-09-08.md`](sat/sat-score-segment-2026-09-08.md) | 分数分段诊断关闭（D3/D4 as-of 门，前瞻 paper 接棒） | ⛔ 未出数；0=真零值无setup |
| [`index-trend-bigmoney-2026-09-08.md`](index/index-trend-bigmoney-2026-09-08.md) | 指数趋势证伪（000300+000688 MA20/60，预注册）+ §6 静态掺 + §7 轮动池 | ❌ 四关全拒；§7 valid+22.4 但 OOS2−3.1（维1首个生命体征，复活条件见档） |

### 2026-09-11 增补（数据统一 + P0-12 孵化 C1/W1）

| 文档 | 内容 | 状态 |
|------|------|------|
| [`data-consistency-2026-09-11.md`](./data-consistency-2026-09-11.md) | CN `daily` 复权统一重建 + 日线/财报/两融回填（OPT-157） | ✅ 完成 |
| [`factors/fin-l1-tenbagger-2026-09-11.md`](factors/fin-l1-tenbagger-2026-09-11.md) | 长持找几倍股 L1+L2 pilot | ❌ REJECT |
| [`factors/style-regime-2026-09-11.md`](factors/style-regime-2026-09-11.md) | 风格×市值×市况 C1 诊断 | ✅ 描述性砖 |
| [`a1-voltarget-beta-2026-09-11.md`](a1-voltarget-beta-2026-09-11.md) | 定海 DH：前视 autopsy + **DH-2 真实指数 beta**（正年化） | ⚠️ DH-1 WITHDRAWN / DH-2 KEEP |
| [`dh1-s3-hybrid-2026-09-11.md`](dh1-s3-hybrid-2026-09-11.md) | DH × S-3 结合（叠刹车 + 配比/闲置资金 + 方向指引收紧止损）Phase 0 | ❌ REJECT |
| [`factors/alt-alpha-screen-2026-09-11.md`](factors/alt-alpha-screen-2026-09-11.md) | 另类数据 alpha 速筛（股东户数/陆股通/大单资金流） | ❌ REJECT（无增量） |
| [`factors/intraday-microstructure-2026-09-11.md`](factors/intraday-microstructure-2026-09-11.md) | 5 分钟日内微结构探针（尾盘/隔夜反转） | ❌ REJECT（成本） |
| [`factors/fund-investment-accruals-2026-09-11.md`](factors/fund-investment-accruals-2026-09-11.md) | 长史基本面 投资/应计 composite + 行业中性 + 长持 sleeve 原型 | ⚠️ INCUBATE |
| [`factors/alpha101-l0-screen-2026-09-12.md`](factors/alpha101-l0-screen-2026-09-12.md) | Alpha101 全集 L0 + §8 S0/S1 正交化 + §9 S2 可交易性 | ❌ REJECT（终结；S1 统计独立但 S2 三窗成本不过） |
| [`factors/fund-sleeve-standalone-2026-09-12.md`](factors/fund-sleeve-standalone-2026-09-12.md) | X3 投资/应计 sleeve 独立化（月频 + 波动率层 + 容量/成本/OOS） | ⏸️ PARK（超额 +2.2%/年真实但 Sharpe 0.33/DD−58%，无可用风险层） |
| [`factors/garp-sleeve-2026-09-12.md`](factors/garp-sleeve-2026-09-12.md) | GARP（质量+便宜）sleeve pilot（长史 17 年 × 三臂） | ❌ CLOSE（GARP≈value，质量无增量） |
| [`factors/gtja191-l0-screen-2026-09-12.md`](factors/gtja191-l0-screen-2026-09-12.md) | GTJA 191 全集 L0（168 条 × 三窗；无新轴） | ❌ REJECT（同价量相关族，成本不可交易） |
| [`factors/hk-alpha101-gtja191-l0-2026-09-12.md`](factors/hk-alpha101-gtja191-l0-2026-09-12.md) | HK（H 股）Alpha101+GTJA191 对照（269 条 × 三窗） | ❌ REJECT（同族复现但更差；非 A 股特有） |
| [`factors/README.md`](factors/README.md) | **因子档案总台账**（101/191 + 全部历史因子一眼判定） | ✅ 索引 |

### 2026-09-13 → 09-14 增补（港湾上线 + 三策略审计）

| 文档 | 内容 | 状态 |
|------|------|------|
| [`stable/etf-parking-baseline-2026-09-13.md`](stable/etf-parking-baseline-2026-09-13.md) | B11 新基线「港湾」= S-3 + 闲置现金停车场（P1 PASS） | ✅ **产品基线** |
| [`stable/twin-star-parking-refit-2026-09-13.md`](stable/twin-star-parking-refit-2026-09-13.md) | B12 习惯双子星重拟合（口径 bug，已作废见审计） | ❌ REJECT |
| [`stable/etf-benchmark-parking-2026-09-13.md`](stable/etf-benchmark-parking-2026-09-13.md) | B13 ETF 买持基准 × 多方法拟合（最佳拟合=×风险预算 50/50） | ✅ 标尺 |
| [`stable/parking-cooldown-2026-09-13.md`](stable/parking-cooldown-2026-09-13.md) | B14 停车场 trail 后再入场冷却 | ❌ REJECT |
| [`stable/harbor-riskbudget-2026-09-13.md`](stable/harbor-riskbudget-2026-09-13.md) | B15「母港」= 港湾 × B3 风险预算 50/50 | 🟡 **PASS 产品候选** |
| [`stable/b3-cap-2026-09-13.md`](stable/b3-cap-2026-09-13.md) | B16 B3 单资产权重上限（H-B3-CAP） | ❌ REJECT |
| [`stable/homeport-regime-2026-09-13.md`](stable/homeport-regime-2026-09-13.md) | B17 母港市况开关（H-MIX-DYN） | ❌ REJECT |
| [`audit-three-strategy-lookahead-2026-09-14.md`](audit-three-strategy-lookahead-2026-09-14.md) | **三策略前视审计 + qfq/raw 基期 + 双市场日历修复（OPT-182/183）**：8 处修复、clean 三方最终对比 | ✅ 结论级 |
| [`stable/sgap-habit-satellite-standalone-2026-09-14.md`](stable/sgap-habit-satellite-standalone-2026-09-14.md) | **习惯 S-gap 卫星腿 standalone 记录（clean 口径）**：三窗+long、年度、校验 | ✅ 研究记录 |
| [`sat/sat-valid-shortfall-diagnosis-2026-09-14.md`](sat/sat-valid-shortfall-diagnosis-2026-09-14.md) | **卫星 valid 短板诊断（H-SAT-DIAG）**：有仓日同日核心对照 + 闸门反证 → regime 依赖（非口径 bug） | ✅ 结论级 |
| [`stable/harbor-sat-weight-2026-09-14.md`](stable/harbor-sat-weight-2026-09-14.md) | **港湾×卫星 曝露曲线（H-SAT-W，预注册）**：7 档权重全不满足 K1–K3 → REJECT | ❌ REJECT |
| [`stable/harbor-b3-sat-2026-09-14.md`](stable/harbor-b3-sat-2026-09-14.md) | **三腿「母港×卫星」（H-B3-SAT，预注册）**：换基座后 w∈[0.15,1/3] 全过 K1–K3（chosen 1/3 踩线，稳健 0.15–0.25） | ✅ PASS（产品候选增量） |

### 早期与专题（结论已定 · 有事才翻）

| 文档 | 内容 |
|------|------|
| [`score-threshold-2026-08-22.md`](s3/score-threshold-2026-08-22.md) | score 65 维持（70/75 持平，80+ 拒收） |
| [`style-experiments-2026-08-22.md`](s3/style-experiments-2026-08-22.md) | 趋势/均值/小盘三风格（三窗 S-3 最佳） |
| [`tip014-dip-retry-2026-08-22.md`](s3/tip014-dip-retry-2026-08-22.md) | dip/momentum 重试明细 |
| [`sleeve-exit-study.md`](core/sleeve-exit-study.md) | 套筒退出研究（MA200 增量） |
| [`factor-ic-2026-08-22.md`](factors/factor-ic-2026-08-22.md) · [`factor-ic-phaseB-2026-08-22.md`](factors/factor-ic-phaseB-2026-08-22.md) | TIP-013 因子清单（空，无新增） |
| [`bollinger-trend-study.md`](factors/bollinger-trend-study.md) · [`macd-trend-study.md`](factors/macd-trend-study.md) · [`kdj-trend-study.md`](factors/kdj-trend-study.md) · [`uptrend-pullback-study.md`](factors/uptrend-pullback-study.md) · [`support-resistance-box-study.md`](factors/support-resistance-box-study.md) · [`long-consolidation-breakout-study.md`](factors/long-consolidation-breakout-study.md) · [`indicator-supertrend-fibonacci-priceaction-notes.md`](factors/indicator-supertrend-fibonacci-priceaction-notes.md) | 形态三噪音 + 指标分流（已归档，不重开） |
| [`performance-log.md`](s3/performance-log.md) | 性能日志 |
| [`small-agile-plan-2026-08.md`](s3/small-agile-plan-2026-08.md) | 小步快跑计划（历史） |

---

## 验证纪律（门控 v2 · 2026-09-16 用户拍板；旧三窗铁律升级）

> 唯一裁决标准 → **[`validation-gates-v2-2026-09-16.md`](./validation-gates-v2-2026-09-16.md)**
>（多窗 + 长窗 + 多角度：收益主门 G1 / 风险兑换 G2 / 一致性 G3 / 前视 L 门；裁决分
> PASS / 条件PASS / REJECT / VOID）。本节只留窗口切分与纪律，**判定数字以 v2 文档为准**。
> 只管未来：已冻结实验的 K1–K5 与 PASS/REJECT 维持不变，不重判。

1. **三窗切分（固定）**：
    - `OOS2` = 2024-08-01 ~ 2025-08-01（弱市年 · 资金流 fail-open → 实为 regime 窗）
    - `train` = 2025-08-01 ~ 2026-02-01
    - `valid` = 2026-03-01 ~ 2026-08-07（当前实盘对照窗 · n=55；**复用计数已到 4**，
      v2 G3e：新候选上限条件PASS，直到 holdout n≥50 或窗口重切）
2. **hold-out（2026-08-22 起）**：`2026-08-08 ~ 2027-02-08` 只读不调参，`n≥100` 前不改参；
   v2 下 holdout 承担**条件PASS 转正**（n≥50、`Δ ≥ −2` 且无破地板）与 n≥100 后 binding。
   旧"三窗 `>5pt劣化` 判定不含 hold-out"作废，以 v2 为准。
3. **判定标准（v2 摘要；执行以 v2 文档为准）**：**收益最重要但不是唯一**——
   G1a 三窗等权合计 `≥ 0` 且单窗地板 `−5`（维持）且 `long ≥ −5`；
   余量 `< 2pt` → 条件PASS（踩线制度化）；`−3 ≤ G1a < 0` 可用风险按价兑换
   （1pt 收益 ⇔ long MDD +2pt 或 Sharpe +0.08，上限条件PASS）；`G1a < −3` 或破地板 → REJECT；
   M50 级大额买保险走保险通道（预注册明示 + 用户拍板），不走自动门。
   单一窗好看 = 过拟合拒收（维持）。
4. **长窗**（2021-08-01 ~ 2026-08-07）：v2 起**进裁决**（G1c：`long Δ ≥ 0` 干净，`[−5,0)` 条件；
   MDD/Sharpe 主战场 + 2021–23 熊市覆盖）。`2021-08~2024-07` 无 sentiment/flow/scores 全
   `fail-open`，与 valid 非同分布，仍拆 `long_price_only` vs `long_full(2024-08~)` 披露。
5. **验收工具**:
    ```bash
    cd services/data-sync-service
    PYTHONPATH=src python3 scripts/run_walk_forward.py            # 三窗 vs 固化基线
    PYTHONPATH=src python3 scripts/run_walk_forward.py --param k=v # 试参数
    PYTHONPATH=src python3 scripts/run_walk_forward.py --windows OOS2,train,valid,holdout,long  # 含 hold-out
    PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline  # 需新文件名 + git tag
    ```
6. **纪律**：回测数字不作发布依据；**paper 实绩为准**（C4 对照）；`valid n=55 win81.8% Sharpe11` 仅发现、不可外宣，可信锚点为 `OOS2 n237 / train n123`；`B-T1 TrendOK` 当前 `--param trendok_*` 为 no-op（见审计 §3.3，需修 `recompute_scores_with_params` 注入）。
7. **审计**：改引擎/加参/引用收益前必读 [`audit-2026-08-22.md`](./audit-2026-08-22.md)（数据前视/幸存者 · 执行 200%杠杆/无流动性/calendar 天数 · 统计 129 组合多重检验）。

---

## 基线档案（data/backtest_reports/）

| 文件 | 内容 |
|------|------|
| `pick_strong_track_past_year.json` | **择强单轨**过去一年（定案 mom_compare） |
| `past_year_twin_vs_core_2026-09-02.json` | 过去一年三方：单轨 vs 双子星 v3 15×5% vs clip4 |
| `core_stock_clip_2026-09-03.json` | 核心 S-3 篮集中度三窗（拒收） |
| `sat_exit_trail_2026-09-03.json` | 卫星退出 body/protect/trail 三窗（拒收） |
| `walk_forward_baseline.json` | S-3 股票腿 CN 基线（NAV） |
| `walk_forward_latest.json` | 最近一次三窗结果 |
| `walk_forward_hk_baseline.json` | HK 并行线基线 |
| `walk_forward_dual_latest.json` | CN+HK 双线 |
| `monte_carlo_cn.json` / `monte_carlo_hk.json` | E2 panic 蒙特卡洛 3000 次验证 |
| `tip014_*.json` | TIP-014 各实验原始数据（d1/d3/d6/env_style/industry_profile/long_conf/neutral_diag） |
| `hk_trail_scan_*.json` | HK trailing 扫描 |
| `rolling_oos_latest.json` | 滚动 OOS 监控（scheduler 自动更新） |
| `paper_vs_backtest_latest.json` | C4 paper-vs-backtest 对照 |
| `index_light_backtest_latest.json` / `trend_exit_latest.json` | 红绿灯 / 趋势退出历史 |

## 当前基线数字（只收录指针 · 定义见真值源）

| 口径 | 去哪看 |
|------|--------|
| 终局成绩单（单轨 past_year / clip4 / Δ / 滚动窗） | [`core/state-bucket-algo-2026-08-31.md`](./core/state-bucket-algo-2026-08-31.md) §3.0（产品窗对照表 + 报告链） |
| S-3 CN 基线（OOS2/train/valid + DD/夏普/笔数） | [`../modules/strategy-params.md`](../modules/strategy-params.md) §3（`s3-baseline-20260828-nav`） |
| 套筒增量（idle 套利三窗 delta） | 同上 §1（sleeve 行） |
| R5CS 联合（dual 三窗） | [`../modules/strategy-params.md`](../modules/strategy-params.md) §4 版本历史（R5CS 行） |
| HK 线（train 极弱 · 不作独立叙事） | `walk_forward_hk_baseline.json`（报告）；结论见 `SUMMARY.md` §3 |

> 参数真值 → [`modules/strategy-params.md`](../modules/strategy-params.md)；审计 → [`audit-verdict-2026-08-29.md`](./audit-verdict-2026-08-29.md)。旧 117%/HK270%/算术43.1% **全部封存**。
