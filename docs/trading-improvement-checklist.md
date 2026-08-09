# Karios 交易逻辑改进 Checklist

> 基于 2026-07-22 业务评估：TradingView 第一层选股、盘后 Automation、Alpha Radar 与主线开火漏斗。  
> **范围**：交易 / 业务规则与策略校准（非纯工程性能）。代码改动仅服务于下列业务目标。  
> **不做**：用东方财富条件选股替换 TV 第一层（评估结论：无必要；东财继续承担行业资金 / 主线）。

---

## 如何使用

1. 按 **优先级（P0 → P3）** 顺序执行；同优先级内按 ID 顺序。
2. 每项完成后将 `[ ]` 改为 `[x]`，填写 **完成日期** 和 **备注 / PR**。
3. 「预期收益」是相对卫星仓纪律系统的定性评估（提高有效开火密度 / 降低池噪音），不是回测保证收益。
4. 若实施中方案有变，**就地更新本文件**，不要另起文档。
5. 工程实现时优先只改「文件范围」列出的模块；策略文案同步更新 `docs/modules/`。

### Agent / 人工任务模板

```text
请实现 docs/trading-improvement-checklist.md 中的 TIP-XXX。
要求：
- 只改该条目列出的文件 / 配置范围（含对应业务文档）
- 完成后将该条目标为 [x] 并填写完成日期
- 补充可验证的转化率或单元测试（若适用）
- 不要扩大 scope 到其他 TIP
```

---

## 背景摘要（为何改）

当前漏斗：

```text
TV Screener（候选宇宙）
  → 52W 回撤 ∈ [-15%, -5%]
  → TrendOK
  → Watchlist 监控池
  → 东财主线 / Gate / Anti-Spike / 仓位硬闸
  → BUY/ADD

旁路：Alpha Radar（S + catalystScore>85）→ 几乎直进池 → WATCH_SILENT / 主线开火
```

| 判断 | 结论 |
|------|------|
| TV 当第一层 | **合理**（可配置宇宙，不必换东财） |
| 当前 TV pills（Institutional / Falcon 类动量）+ 回踩进池 | **半合理**（两层 thesis 常打架） |
| Automation + Alpha 架构 | **合理**（池卫生 + 双通道补货） |
| 收益瓶颈 | 有效开火机会偏少、池内噪音偏多，不是「没用东财选股」 |

---

## 优先级总览

| ID | 标题 | 优先级 | 预期收益 | 预估工时 | 状态 |
|----|------|--------|----------|----------|------|
| TIP-001 | 校准 TV 第一层：双宇宙 / 对齐回踩 thesis | P0 | ★★★★★ | 0.5–1 天（配置）+ 1–2 周观察 | [x] |
| TIP-002 | 漏斗转化率仪表：TV→回撤→TrendOK→开火 | P0 | ★★★★★（度量闭环） | 1–2 天 | [x] |
| TIP-003 | Falcon / 空窗降级宇宙 | P0 | ★★★★☆ | 1–2 天 | [x] |
| TIP-004 | Alpha 进池加轻量闸（主线 / 结构 / 流动性） | P1 | ★★★★☆ | 1–2 天 | [x] |
| TIP-005 | Alpha 清池对称化（取消整源豁免） | P1 | ★★★★☆ | 1 天 | [x] |
| TIP-006 | Screener 策略版本合同（命名 / pills / 文档） | P1 | ★★★☆☆ | 1 天 | [x] |
| TIP-007 | 主线内有条件放开动量通道（B_momentum） | P2 | ★★★★☆（进攻） | 2–3 天 | [x] |
| TIP-008 | Automation 落地指标与复盘字段 | P2 | ★★★☆☆ | 1–2 天 | [x] |
| TIP-009 | Alpha 映射质量抽检与错映射惩罚 | P2 | ★★★☆☆ | 1–2 天 | [x] 2026-08-04 |
| TIP-010 | 备用宽宇宙实验（东财形态仅作对照，不替换） | P3 | ★★☆☆☆ | 1–2 天 + 对照周 | [ ] |
| TIP-011 | 开火来源归因（TV / Alpha / 手动） | P3 | ★★★☆☆ | 1–2 天 | [x] 2026-08-04 |
| TIP-012 | 研报 → Alpha 通道（评级/目标价进池） | P1 | ★★★★☆（新供给） | 2–3 天 | [x] 2026-08-05 |
| TIP-013 | 信号 IC 验证 + 自研因子探索（Alpha 191 借鉴，不照搬） | P2 | ★★★★☆（度量闭环） | Phase A 1–2 天 / Phase B 2–3 天 | [ ] |
| V6.2-01 | 弱市/DEFEND 14:30 尾盘时间锁 | P0 | ★★★★☆ | 0.5 天 | [x] |
| V6.2-02 | DEFEND 防守双轨袖子（暂缓 Beta） | P0 | ★★★★★ | 1 天 | [x] |
| V6.2-03 | Zero-Pos 持仓归零自动清字段 | P0 | ★★★★☆ | 0.5 天 | [x] |
| V6.3-01 | 超大单日资金突破闸门速杀豁免（WEAK_ATTACK） | P0 | ★★★★★ | 1 天 | [x] |
| V6.3-02 | Alpha S TrendOK recovering 加速器 | P0 | ★★★★☆ | 1 天 | [x] |
| V6.4-01 | ETF 资金流降级为系统级「资金确认因子」（Dashboard 移除大表） | P1 | ★★★☆☆（确认/过滤层） | 0.5–1 天 | [x] |
| V7.0-01 | 跨资产相关性热力网（Correlation Cap） | P1（暂缓） | ★★★★☆（防共振回撤） | 2–3 天 | [ ] 暂缓 |
| V7.0-02 | ATR 风险平价开仓尺寸（Risk-Parity Sizing） | **P0（选做）** | ★★★★★（风险金额恒定） | 1–2 天 | [x] 2026-08-05 |
| V7.0-03 | 三阶利润护城河（Profit Escalator） | 排除 | ★★☆☆☆ | — | [x] 排除 |

**预期收益图例**：★ 越多 = 越可能提高「有效开火密度」或减少「空转噪音」（在卫星仓纪律内）。

---

## V6.2 — 交易中枢（2026-07-24）

### V6.2-01：14:30 尾盘时间锁

**状态**：[x]  
**完成日期**：2026-07-24  
**文件**：`execution-action.ts`（`checkExecutionTimeLock` → `evaluateNewEntryGates` / `deriveActionCard`）

### V6.2-02：防守双轨袖子

**状态**：[x]  
**完成日期**：2026-07-24  
**备注**：Beta&lt;0.8 本期未接（1B）；HardStop=`max(EMA10, px×0.965)`（2A）。Attention Fire 对 `DEFENSIVE_SLEEVE_ALLOW` 豁免 `allowNewEntries=false`。

### V6.2-03：Zero-Pos Auto-Purge

**状态**：[x]  
**完成日期**：2026-07-24  
**文件**：`watchlist-storage.applyZeroPositionCleanup` ← `setItemPositionPct` clearing

---

## V6.3 — 极端资金流豁免 + TrendOK 修复加速（2026-07-27）

### V6.3-01：Intraday Overflow Override → WEAK_ATTACK

**状态**：[x]  
**完成日期**：2026-07-27  
**文件**：`execution_gate.py`、`dashboard.py`、`execution-action.ts`、`packages/shared` schemas  
**规则**：单板块 1D 净流入 >500 亿 **且** upCount >4000 **且** 上海时间 ≥14:30 时，将 `DEFEND`/`HOLD_ONLY` 升级为 `WEAK_ATTACK`（`allowNewEntries=true`，Suggest% 硬顶 5%）。不覆盖 `BREADTH_PANIC` / `RISK_*`。

### V6.3-02：Alpha S TrendOK recovering

**状态**：[x]  
**完成日期**：2026-07-27  
**文件**：`trendok.py`（`apply_alpha_s_trend_recovering`）、`execution-action.ts`、TrendOK Zod  
**规则**：Max Grade=S + 今日量 ≥2.5×10 日均量 + 大阳线 → `trendStatus=recovering`、`trendOk=true`、score floor 60；解除 `WATCH_SILENT`（Why=`TREND_RECOVERING`）。不自动 BUY（准买区）。

## V6.4 — ETF 资金流「资金确认因子」（2026-08-02）

### V6.4-01：ETF 资金流降级为系统级确认/过滤因子

**状态**：[x]  
**完成日期**：2026-08-02  
**文件**：`etf_fund_flow.py`（`aggregate_etf_flow_signal`）、`execution_gate.py`、`dashboard.py`、`macro_snapshot.py`、`MarketSentimentCard.tsx`、`DashboardPage.tsx`、`dashboard-format.ts`  
**规则**：ETF 资金流定位为**二级确认/过滤因子，不独立触发买卖**。

- 聚合：`ETF_WATCHLIST` 按 `category` 归并 —— broad（510300/510050/510500）→ 国家队方向（`National Team Buy/Outflow`），sector（512480/515880/159819）→ 板块方向（`Sector Momentum/Inst Outflow`）；得出 `verdict = confirm / neutral / contradict`。
- 执行闸（`compute_execution_gate`）：数据完整（`incomplete=false`，即无 shareLag 且 intradaySafe）时，`confirm` 仅追加原因 `ETF_FLOW_CONFIRM`；`contradict` 追加 `ETF_FLOW_CONTRADICT` **且把普通 ATTACK 降为 HOLD_ONLY**。永不升级；不降 `WEAK_ATTACK`（V6.3 溢出豁免）与硬 `DEFEND`；数据不完整时完全忽略。
- UI：DashboardPage **移除** ETF 大表格卡片；`MarketSentimentCard` 新增一行「资金确认 (ETF)」徽标（confirm 绿 / contradict 红 / 中性灰）。完整 ETF 明细表仅保留在 IndexPage 次级面板与 AI copy 输出中。

---

## V7.0 — 组合级风控 + 风险平价 + 利润阶梯（2026-08-05 评估采纳）

> 来源：外部系统 V7.0 改进提案；经对照现有代码（`execution-action.ts` / `execution_gate.py`）评审后**部分采纳 + 修正**。
> 评审结论（2026-08-05）：**仅选做 V7.0-02（ATR 风险平价）**，理由：原理无争议、每笔交易直接受益、改动最小（仅 `suggestFireSizePct`）、可立即验证。V7.0-01 暂缓（工程复杂、属预防性闸门，作为第二步）；V7.0-03 排除（Tier3 与现有 Chandelier 重复且更保守，Tier1 +3% 阈值与波动率逻辑自相矛盾，Tier2 分批止盈与趋势跟随哲学冲突）。

### V7.0-01：跨资产相关性热力网（Cross-Asset Correlation Cap）—— 暂缓（第二步）

**状态**：[ ] 暂缓  
**完成日期**：  
**优先级**：P1（暂缓）  
**备注 / PR**：

#### 问题

现有限制只按**单票 15% / 东财行业 30% / Sleeve 上限**切割，存在跨资产相关性盲区：
恒生科技 ETF + 腾讯 + 通信 ETF(CPO) 分属港 ETF / 港股 / A 股 ETF 三种资产，但底层 Beta 高度同向（纳指/英伟达大跌时共振回撤）。东财行业映射无法表达「跨市场同主题」暴露。

#### 目标（修正后方案）

**混合法：语义因子映射为主 + 经验相关性为辅**，不直接裸用 20 日相关系数：

1. **语义因子层（主）**：ETF → 跟踪指数（如恒生科技指数、科创 50）；个股 → 东财行业 + 跨市场主题映射（`theme_industry_map.json` 已有类似基建）。定义因子簇（如 `Tech_Beta` = 恒生科技 + 半导体 + CPO + 中概互联）。
2. **经验相关性层（辅/确认）**：20 日收益率相关性矩阵，**先对齐交易日历**（港股与 A 股节假日不同，未对齐的样本直接拉低相关性造成假阴性）；最少对齐样本数不达标则 fail-open 回退到语义层。
3. **硬约束**：语义因子簇或经验聚类（r > 0.75）的持仓占比累计 **> 30%** → 对该簇内新 BUY/ADD 下发 `CORRELATION_CAP_BLOCK`（只拦新开仓，不强制平仓）；Suggest% 同步扣减。
4. **展示**：战斗面板新增 Correlation Matrix 小面板（簇占比 + 顶部相关对）。

**明确不做**：用 20 日纯统计相关性作为唯一依据（样本少、跨市场日历错位、崩盘期相关性才最高但历史窗口反而低估）；不引入强制卖出。

#### 文件范围

| 层 | 文件 |
|----|------|
| Shared | `packages/shared/src/schemas/`（correlation 相关字段） |
| BE | 新 `service/correlation.py`（日历对齐 + 相关性 + 语义簇）；`execution_gate.py`（簇暴露计算 + BLOCK） |
| FE | `execution-action.ts`（roomCorrelation 入 min 链）、战斗面板矩阵卡片 |
| 测试 | `tests/test_correlation.py`（日历对齐 / 簇聚合 / 边界 30%） |

#### 预期收益

- **高**：堵住「三类资产同一条 Beta」的隐形集中度；降低隔夜美股单边暴跌的共振回撤。
- **风险**：阈值 30% 在小持仓组合下可能频繁触发；建议先以「告警（WARN）+ 挡 ADD」两档灰度。

#### 验证

- [ ] 单测：跨市场日历对齐；簇聚合正确；>30% 拦 BUY/ADD；<30% 放行
- [ ] 用当前组合（含恒生科技 ETF 18.4% 案例）回放确认触发与数字合理

---

### V7.0-02：ATR 风险平价开仓尺寸（Risk-Parity Sizing）—— 本轮唯一选做项

**状态**：[x]  
**完成日期**：2026-08-05  
**优先级**：P0  
**备注 / PR**：见 [`docs/archive/2026-08-05-v7-02-risk-parity-sizing.md`](../archive/2026-08-05-v7-02-risk-parity-sizing.md)

#### 问题

`DEFAULT_FIRE_CLIP_PCT=5` 对所有标的等额切子弹；唐山港日波幅 1.2% 与寒武纪 5%–8% 同仓 5%，高波动票对账户的风险贡献是低波动票的 3–4 倍。

#### 目标（修正后方案）

**按「实际止损距离」定价风险，ATR 仅作兜底代理**：

```
sizePct = min(DEFAULT_FIRE_CLIP_PCT,
              单笔风险预算(默认0.5%) / 止损距离% ,
              roomSingle, roomSector, roomSleeve, roomCorrelation)
止损距离% = (entry − 实际HardStop) / entry          # 有 TrendOK stopLossPrice 时优先
            否则 ≈ 2 × ATR% (14日)                   # 兜底
```

- 低波动（ATR%≈2%，止损 4% 内）：尺寸可到 5% 上限；
- 高波动（ATR%≈6%，止损 8%–12%）：尺寸自动缩至 2.5%–3.5%。
- **下限保护**：尺寸 < 2.5% 时标注 `SIZE_TOO_SMALL`（建议跳过或仅观察），避免碎片化小仓；上限仍受 5% clip / 15% 单票 / 30% 行业 / 各 Sleeve cap 约束（嵌套进现有 `suggestFireSizePct` min 链）。
- **修正说明**：提案公式 `0.5%/(ATR%×2)` 与示例数字（ATR 6% → 2.5–3.5%）自洽性不佳（0.5/12≈4.2%），且以 2×ATR 代替实际止损距离会失真（系统 HardStop 多为结构位，非 2×ATR）；按「实际止损距离优先」重写后两者自然一致。

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `execution-action.ts`（`suggestFireSizePct` 增 stopDistancePct/atr14/referencePrice 参数 + riskCap 入 min 链 + `sizeStopDistancePct` 输出）、`WatchlistRow.tsx`（Suggest% title 显示止损距离）、`execution-markdown.ts`（Suggest% note 更新） |
| Shared | `packages/shared/src/schemas/executionGate.ts`（`sizeStopDistancePct` 字段） |
| 测试 | `execution-action.test.ts`（低/高波动尺寸、下限保护、与 room 链交互、ATR fallback、ADD/BUY 场景） |
| BE | **无需改动**——`trendok.py` 已输出 `stopLossParts.atr14`（绝对值）与 `stopLossPrice`（结构位硬止损），FE 侧数据完备 |

#### 预期收益

- **高**：单笔触发止损对账户的伤害金额趋于恒定（0.5% 预算）；高波动票不再用等额仓位放大回撤压力。
- **风险**：需监控总仓位分母口径（Sleeve 占比 vs 总权益）；低波动票 5% 上限不变意味着其单票风险贡献仍可超预算，属可接受折中。

#### 验证

- [x] 单测覆盖低/高 ATR 场景 + 与单票/行业/Sleeve room 的 min 交互（+14 条，前端全量 467 passed / shared 57 passed / tsc clean）
- [ ] 实盘观察 2 周：开火尺寸分布是否符合「低波大仓、高波小仓」

---

### V7.0-03：三阶利润护城河（Profit Escalator）—— 已排除（2026-08-05）

**状态**：[x] 排除  
**完成日期**：2026-08-05  
**优先级**：—  
**备注 / PR**：排除理由见 V7.0 章节开头评审结论；保留本节仅作决策记录。

#### 排除论证

1. **Tier3 是重复建设**：现有 `deriveTriggerAndTrail` 已实现浮盈 ≥10% 启动 Chandelier（`CHANDELIER_ARM_PNL_PCT=10`，`trailStop = peak − 2×ATR`，exitStop = max(hardStop, trailStop)）。提案的 15% 启动档比现状**更保守**，加了等于倒退。
2. **Tier1（+3% 锁保本）与波动率逻辑自相矛盾**：ATR 6% 的高波票单日噪声即 ±5%，+3% 抬保本 = 必然被噪声扫出局，直接把 V7.0-02 想扛的波动又还回去了。
3. **Tier2（1/3 分批止盈）与「让利润奔跑」哲学冲突**：Chandelier 的本质是趋势跟随（让赢家奔跑、让利润曲线自己说话），固定位置分批止盈在数学上压低期望收益（截断右尾），仅在心理层面有价值——不值得为此改执行逻辑。
4. **增量价值低**：现有 Chandelier + 结构位 HardStop 已覆盖 90% 的锁利需求；阶梯抬升属于锦上添花，与 ATR 平价相比 ROI 低。

#### 现状盘点（供未来参考，若仍想做）

- 现有 HardStop 来自 TrendOK `stopLossPrice`（结构位/EMA），无阶梯抬升机制——若未来要拾起，只做 T1/T2（阈值按 ATR 缩放），Tier3 沿用现有 Chandelier。

#### 现状盘点（评审关键）

- 现有 `deriveTriggerAndTrail` 已实现 **浮盈 ≥10% 启动 Chandelier**（`CHANDELIER_ARM_PNL_PCT=10`，`trailStop = peak − 2×ATR`，exitStop = max(hardStop, trailStop)）——提案的 Tier3（15% 启动）比现状**更保守**，无需新增。
- 现有 HardStop 来自 TrendOK `stopLossPrice`（结构位/EMA），无阶梯抬升机制——Tier1/2 确有增量价值。

#### 目标（修正后方案）

| Tier | 触发 | 动作 | 修正点 |
|------|------|------|--------|
| T1 Breakeven Lock | 浮盈 ≥ **max(3%, 1.0×ATR%)** | HardStop = max(HardStop, cost×1.005)；标记 `BREAKEVEN_LOCKED` | **修正**：纯 +3% 对 ATR 6% 的票一天噪声就扫停，阈值按 ATR 缩放 |
| T2 Profit Lock | 浮盈 ≥ +8% | HardStop = max(HardStop, cost×1.04)；生成 1/3 仓「挂牌分批止盈」**建议**（paper，不强制） | **修正**：分批止盈与「让利润奔跑」哲学冲突，落地为建议而非强制单 |
| T3 Runner | 浮盈 ≥ +10%（沿用现有） | 现有 Chandelier `peak − 2×ATR` 已覆盖 | 不再新增 15% 档；沿用现状 |

- 兼容性：现有 `exitStop = max(hardStop, trailStop)` 天然支持 T1/T2 抬升后与 Chandelier 取高者。
- 执行面：T1/T2 抬升在 `deriveTriggerAndTrail` 内实现（输入浮盈、ATR%、hardStop、cost），输出 `escalatorTier: 0|1|2`。

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `execution-action.ts`（`deriveTriggerAndTrail` 增阶梯逻辑 + `escalatorTier` 字段）、`execution-markdown.ts`（面板列） |
| Shared | `packages/shared/src/schemas/`（escalator 字段） |
| 测试 | `execution-action.test.ts`（三档触发 / ATR 缩放 / max 链） |

#### 预期收益

- **中**：浮盈转实盈节奏更明确；减少「+10% 利润吐回归零」。
- **风险**：T1 抬到保本后回踩即离场，会砍掉部分「回踩后继续主升」的赢家——这正是 Chandelier 与 Breakeven 的固有矛盾；阈值按 ATR 缩放后矛盾显著缓解。

#### 验证

- [ ] 单测：三档触发边界 + ATR 缩放 + 与现有 Chandelier 取 max 兼容
- [ ] 实盘 2 周观察：T1 触发后被扫出局的比例（应远低于纯 +3% 阈值）

### TIP-012：研报 → Alpha 通道（评级/目标价进池）

**状态**：[x]  
**完成日期**：2026-08-05  
**优先级**：P1  
**备注 / PR**：见 [`docs/archive/2026-08-05-tip-012-research-alpha-channel.md`](../archive/2026-08-05-tip-012-research-alpha-channel.md)

#### 问题

Alpha 供给只来自新闻 catalyst（Alpha Radar RSS）——「研报是另一个量级的信息」但没进系统。研报评级/目标价/EPS 比新闻结构化得多，且发布密集（东财每日 40-60 份个股研报）。

#### 目标

**研报 → α 旁路**（复用 Alpha Radar 全部下游）：

```text
东财研报中心 API（reportapi.eastmoney.com，免费）
  → research_reports 表（info_code 去重；alembic 0019）
  → 确定性评分：score = (评级×80 + 目标价空间×20) × 时效衰减(14天半衰期)
     买入=80 / 增持=60 / 目标价20%空间=+8；多份确认 +5/份(cap +10)
  → build_research_catalyst_payload：按 symbol 聚合，形状与 catalyst payload 一致
  → compute_alpha_additions（score_min=70，复用 TIP-004 闸门：防守板块/Top10/缺行业）
  → 与 catalyst 候选合并进 automation alphaAdd；每轮 cap 10 个
  → 前端 registry source='research'（Watchlist 可溯源）
```

**关键设计**：
- 行业标签**直接用东财研报 API 自带行业**（与 EM 缓存同源）——解决"新标的 EM 缓存无行 → missing_industry 误拒"（实测 31/49 被拒，修复后 30 候选）
- 执行溯源仍走 ALPHA/MANUAL（TIP-011 三枚举不变）；registry source 加 `research` 区分通道
- 每轮 cap `RESEARCH_MAX_CANDIDATES=10`（注意力预算；payload 已按分排序）
- 调度：`research_report_sync` job 每 2h 抓最近 3 天增量（去重幂等）
- 评分阈值 70 < Alpha 的 85：1 份新鲜"买入"=80 即可进池（研报信号弱于 S 级新闻 catalyst，故用低门槛+行业闸约束）

#### 文件范围

| 层 | 文件 |
|----|------|
| BE 新 | `service/research.py`（sync/评分/聚合）、`db/research.py`（表+upsert/list/stats）、`scheduler/research_report_job.py`、`api/research_routes.py`（/api/research/reports、stats、sync） |
| BE 改 | `watchlist_automation.py`（score_min 参数 + research 候选合并 + cap）、`api/sync_routes.py`（SYNC_JOB_TYPES）、`db/schema_baseline.py`、`scheduler/__init__.py`、`main.py` |
| 迁移 | `alembic/versions/0019_research_reports.py` |
| Shared | `schemas/watchlist.ts`（source 枚举 +research）、`schemas/scheduler.ts`（job catalog） |
| FE | `watchlist-automation.ts`（channel→registry source + summary 显示研报α计数） |
| 测试 | `tests/test_research.py`（16 单测）+ `test_watchlist_automation.py`（+3） |

#### 预期收益

- **高**：新增确定性 Alpha 供给（每日 40-60 份研报 → 结构化候选）；与新闻 catalyst 通道互补（研报滞后但结构化，catalyst 及时但噪声大）。
- **风险**：研报发布时价格已部分反应（跟风风险）——靠 TrendOK/score 池内二次筛选兜底；进池 ≠ 买单。

#### 验证

- [x] 端到端：真实 API 抓取 63 条 → 入库 58 → 聚合 49 标的 → 闸门后 30 候选 → cap 10 进 run
- [x] 单测 16+3；全量后端 1313 / shared 57 / 前端 467 全绿
- [ ] 观察 2 周：研报进池票的 TrendOK/开火转化率 vs 新闻 catalyst（registry source='research' 可归因）

---

## P0 — 最高优先级（先做）

### TIP-001：校准 TV 第一层 — 双宇宙 / 对齐回踩 thesis

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：config — `Karios Pullback` URL `…/m22BmHkT/` ~34 hits；去掉动量 pills，对齐回踩

#### 问题

截图级条件（Institutional Trend、Falcon Launch）偏 **当日强势 / 启动**（涨幅、Rel vol、3M 表现），与进池硬规则 **52W 回撤 −15%～−5%** 经常正交：第一层热闹，过回撤后几乎为空；还能和 Anti-Spike / 日内 >6% 禁买叠加重杀。

#### 目标

拆成两套明确宇宙（可仍用 TV，不必换东财）：

| 宇宙 | 服务模式 | TV 条件方向 | 进池后滤 |
|------|----------|-------------|----------|
| **Pullback** | `A_pullback` | 少强调当日涨幅/Rel vol；偏均线多头、盈利、市值、中期趋势，允许价格离 52W 高有距离 | 维持回撤窗 + TrendOK |
| **Momentum**（可选启用） | `B_momentum` | 可保留 Falcon / Institutional 类 | **放宽或取消** 52W 回撤窗；仍要 TrendOK（或近高规则） |

默认至少先把现用主 screener 校准成 **Pullback 宇宙**；Momentum 宇宙可第二阶段再开。

#### 细节 checklist

- [x] 在 TradingView 上重配 / 新建 Pullback screener URL，写入 Settings（enabled）— `Karios Pullback` / `m22BmHkT`
- [x] 记录 Filter Pills 原则到 `docs/modules/screener.md` Strategy contracts（TIP-006）
- [ ] 连续 ≥5 个交易日统计：命中数、过回撤数、过 TrendOK 数（可用 TIP-002 产出）
- [ ] 若保留 Momentum screener：在导入逻辑中按 screenerId / screenTitle 分支后滤（见文件范围）
- [x] **明确不做**：用东财「均线多头排列」替换 TV 主宇宙

#### 文件范围（若需代码分支）

| 层 | 文件 |
|----|------|
| 配置 | Settings 中的 TV screener URL（运行时 DB） |
| Import | `apps/desktop-ui/src/lib/watchlist-screener-import.ts` |
| 标题模式 | `apps/desktop-ui/src/lib/screenerExport.ts`（`SCREENER_TITLE_PATTERNS`） |
| 文档 | `docs/modules/screener.md`、本文件 |

#### 预期收益

- **高**：减少「TV 命中但永不过回撤」的空转；提高进池转化率。
- **对收益率**：间接——提高池内「可回踩接」比例，从而提高 ATTACK+主线日的有效候选数。
- **风险**：Pullback 宇宙在单边狂奔市可能偏少；需 TIP-003 降级兜底。

#### 验证

- [ ] 抽样对比改前改后：同一周 `过回撤 / TV命中` 显著上升（目标方向：≥20% 相对提升，视市况）
- [ ] Institutional 类动量票不再是唯一进池来源（或已迁到 Momentum 分支）

---

### TIP-002：漏斗转化率仪表（度量闭环）

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：local — minimal：Import Debug + ack `meta.funnel`；无独立表 / 无 N 日图表

#### 问题

没有统一指标时，「健壮」「提高收益」只能拍脑袋；Automation 每天跑，但不知道哪一层在漏人。

#### 目标

每个交易日（或每次 Import / Automation）落一条漏斗快照：

| 指标 | 含义 | 本轮 |
|------|------|------|
| `tvHit` | enabled screener 最新快照去重标的数 | ✅ |
| `passPullback` | 过 52W 回撤窗 | ✅ |
| `passTrendOk` | 过 TrendOK | ✅ |
| `addedNew` | 新写入 Watchlist | ✅ |
| `alphaRejected` / alpha add | Alpha 拒绝与追加 | ✅（004 已有 + summary） |
| `fireable` | 可开火候选 | 未做 |
| N 日表格 / 新表 | 周复盘 UI | 未做（可后续） |

UI 最小形态：Import Debug 一行 Funnel；automation summary / ack 写入 Postgres `meta.funnel`。

#### 细节 checklist

- [x] 定义 funnel 字段与写入点（import debug + automation ack）
- [ ] 前端展示最近 N 日转化率（表格）— 未做
- [x] 文档写明口径（`docs/modules/watchlist.md`）

#### 文件范围

| 层 | 文件 |
|----|------|
| FE import | `watchlist-screener-import.ts`、`WatchlistImportDebug` |
| FE automation | `watchlist-automation.ts` |
| BE | `ack_run` 合并 `meta.funnel`；API `WatchlistAckRequest.funnel` |
| 文档 | `docs/modules/watchlist.md`、本文件 |

#### 预期收益

- **极高（元收益）**：后续 TIP 是否有效可证伪；避免无效改 pills / 乱降门槛。

#### 验证

- [x] 手动 Import 与 Run automation summary 可见 funnel
- [x] ack 后 latest run `meta.funnel` 可查（需 DB）
- [x] 单元测试：`formatScreenerFunnel` / summary

---

### TIP-003：Falcon / 空窗降级宇宙

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：local — Industry 5D Top5 → TrendOK（skip pullback）；`source=screener_fallback`

#### 问题

Falcon Launch 类条件可全日 **0 票** → Automation 第一层断粮，当天只能靠存量 + Alpha。

#### 目标

业务规则：当 **主 Pullback screener `tvHit==0` 或 `passPullback==0`** 时，启用降级宇宙：

- 5D 净流入 Top5（剔除防守板块）→ 东财行业名 LIKE 成分，合计 ≤80
- 只过 **TrendOK**（跳过 52W 回撤）
- `source=screener_fallback`；funnel 记 `fallbackUsed/Hit/TrendOk/Added`

#### 细节 checklist

- [x] 定义空窗：`tv_hit==0` 或 `pass_pullback==0`
- [x] 降级来源写入 funnel / ack `meta.funnel`
- [x] 降级候选只过 TrendOK（不过回撤）
- [x] `source=screener_fallback` 与主宇宙区分

#### 文件范围

| 层 | 文件 |
|----|------|
| Shared | `packages/shared/src/schemas/watchlist.ts` |
| BE | `watchlist_automation.list_fallback_universe_symbols`；`GET …/fallback-universe` |
| FE | `watchlist-screener-import.ts` |
| 文档 | `docs/modules/watchlist.md`、`screener.md`、本文件 |

#### 验证

- [x] 单测：防守行业剔除、符号封顶、funnel fallback 段
- [ ] 手工：disable Pullback → Import 见 `fallbackUsed`；有 pullback 命中日不触发

---

## P1 — 高优先级

### TIP-004：Alpha 进池加轻量闸

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：local — minimal gates: defense sector + 5D Top10；`meta.alphaRejected`

#### 问题

Screener 进池要过回撤+TrendOK；Alpha（`catalystScore>85` 且含 S）几乎直进 → 池内堆「故事对、结构差」票，注意力稀释，开火率低。

#### 目标

在 `compute_alpha_additions`（或前端 apply 前）增加 **至少 1 条** 轻量闸（可组合）：

| 闸 | 建议默认 | 本轮 |
|----|----------|------|
| 流动性 | 近 5/10 日均额或均量下限（Tushare） | 未做（后续） |
| 结构 | TrendOK=true **或** Score≥阈值（如 50）二选一放宽 | 未做（后续） |
| 板块 | 东财行业 ∈ 5D Top10（比开火主线 Top3 略宽） | ✅ |
| 硬排除 | 防守板块名单（与 BUY 闸一致） | ✅ |
| 缺行业 | 无东财行业名 → 拒绝 | ✅ |
| Top10 空 | 资金流 Top10 不可用 → 跳过 Top10 闸（fail-open） | ✅ |

进池仍不等于买单；闸的目的是 **少进垃圾监控**。

#### 细节 checklist

- [x] 选定默认闸组合并写进 `docs/modules/watchlist.md`（防守 + Top10）
- [x] `meta.alphaRejected` 记录拒绝原因分布
- [ ] 保留「纯 S + 极高分」紧急通道的开关（默认关或极严）— 未做
- [ ] 流动性 / TrendOK 闸 — 未做，需要时另开 TIP

#### 文件范围

| 层 | 文件 |
|----|------|
| BE | `.../service/watchlist_automation.py`（`compute_alpha_additions`） |
| 测试 | `tests/test_watchlist_automation.py` |
| 文档 | `docs/modules/watchlist.md`、本文件 |

#### 预期收益

- **高**：提高 Alpha 进池后 N 日成为可开火候选的比例；降低无效行。
- **风险**：漏掉「尚未有结构的早期主题」→ 用 WATCH_SILENT 白名单或人工加池补救。

#### 验证

- [ ] 回放最近 2 周 alphaAdd：加闸后数量下降，但 `主线∩TrendOK` 占比上升（人工）
- [x] 单元测试覆盖各拒绝分支

---

### TIP-005：Alpha 清池对称化（取消整源豁免）

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：local — `should_remove_symbol` 仅 Max Grade=S → `alpha_s_exempt`

#### 问题

`source=alpha_radar` **整源豁免** 三日 Score GC；故事褪色后仍占坑。`WATCH_SILENT`（Max Grade=S）已提供「禁 PURGE」保护，整源豁免过宽。

#### 目标

| 规则 | 新行为 |
|------|--------|
| Max Grade=S（窗口内有效） | 继续豁免自动化 GC；执行层维持 WATCH_SILENT |
| 其他 alpha_radar | 与 screener/manual 空仓票相同：连续 3 日 Score&lt;30 且行业不在 5D Top5 → 可移除 |
| 持仓 | 仍不因 GC 删除 |

#### 细节 checklist

- [x] 改 `should_remove_symbol`：去掉无条件 `alpha_radar_exempt`，改为查近期 Max Grade / catalyst
- [x] 文档与 downstream prompt 对齐（避免 AI 仍假设整源豁免）
- [x] 迁移说明：存量 alpha 噪音票会在随后数个交易日被清

#### 文件范围

| 层 | 文件 |
|----|------|
| BE | `.../service/watchlist_automation.py` |
| 测试 | `tests/test_watchlist_automation.py` |
| 文档 | `docs/modules/watchlist.md`、`README.md`、本文件 |

#### 预期收益

- **高**：缩小静默死票占比；注意力回到可交易标的。
- **风险**：S 级判定窗口与 GC 时序需测清，避免误删仍在发酵的主题。

#### 验证

- [x] 单测：S 豁免 / 非 S 可删 / 持仓不删
- [ ] 人工跑一次 automation dry-run 或 force，检查 remove 名单合理

---

### TIP-006：Screener 策略版本合同

**状态**：[x]  
**完成日期**：2026-07-22  
**备注 / PR**：local — `screener.md` Strategy contracts；`SCREENER_TITLE_PATTERNS` + Pullback；seed Legacy 命名

#### 问题

文档未定义 Falcon / Institutional / Black Horse 因子哲学；seed 名（Swing Falcon）与标题模式（`falcon launch`）漂移会导致触媒路径 silent 变窄。

#### 目标

每个 enabled screener 有一份「策略合同」：

- 显示名、TV URL、`screenTitle` 期望子串
- 宇宙类型：`pullback` | `momentum` | `system` | `legacy`
- Filter Pills 原则（Pullback）
- 进池后滤：是否应用 52W 回撤窗
- 是否参与 Alpha 触媒「今日 screener TrendOK」集合

#### 细节 checklist

- [x] 更新 `docs/modules/screener.md` 正式章节
- [x] 对齐 `SCREENER_TITLE_PATTERNS` 与真实 `screenTitle`（含 Karios Pullback）
- [ ] Settings UI 可选：展示宇宙类型 — **未做**（本 TIP 明确跳过）

#### 验证

- [x] 标题匹配单测覆盖 `Karios Pullback`
- [x] 新人只读 screener.md 能说出每套宇宙意图

---

## P2 — 进攻与归因

### TIP-007：主线内有条件放开动量通道

**状态**：[x]  
**完成日期**：2026-07-23  
**备注 / PR**：local — FE/BE dual gate; Why=`MOMENTUM_SURGE_ALLOW`; cap 9%; Score≥85; B_momentum only (no Momentum universe import)

#### 问题

Anti-Spike、日内 >6% 禁 BUY 抑制追高，也系统性错过 A 股主线强势延续段。与 TV 动量宇宙、B_momentum 未打通。

#### 目标

**仅在**同时满足时放宽：

1. Gate=`ATTACK`
2. 东财行业 ∈ 主线（5D Top3 ∪ Momentum Breakout）
3. TrendOK=true 且 Score≥85
4. `buyMode=B_momentum`

则日内上限 6%→9%；Why=`MOMENTUM_SURGE_ALLOW`。

**禁止**：全局取消见光死；弱市 / 非主线不得放开。本轮不做 Momentum screener 进池分支。

#### 细节 checklist

- [x] 与 B_momentum 联动（无 Momentum 宇宙则先用 buyMode）
- [x] Execution Why=`MOMENTUM_SURGE_ALLOW`
- [ ] 回测或至少 2 周纸面对照：放开子集的次日表现 vs 对照组（观察中）

#### 验证

- [x] 单测：非主线仍拦截；主线+ATTACK+高分+B_momentum 才放行；>9% 仍拦
- [x] Decision Journal 能区分普通 BUY 与动量放宽 BUY（Why 码）

---

### TIP-008：Automation 落地指标与复盘字段

**状态**：[x]  
**完成日期**：2026-07-23  
**备注 / PR**：local — summary sync+top5；BE industrySync/screenerSync `ok`

#### 问题

Scheduler / Watchlist 摘要只有 `−N screener +X alpha +Y`，缺少拒绝原因、空窗、降级、行业 Top5 列表等，复盘成本高。

#### 目标

扩展 automation run `meta` + UI 摘要：

- `top5dIndustries`（已有则展示）
- `screenerSync` / `industrySync` ok
- `alphaRejected` 分布（衔接 TIP-004）
- `fallbackUsed`（衔接 TIP-003）
- `funnel` 引用 TIP-002

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `SchedulerPage.tsx`、`formatAutomationSummary` |
| BE | `watchlist_automation.py` |
| 文档 | `docs/modules/watchlist.md` |

#### 预期收益

- **中**：加快迭代 TIP-001/003/004；间接抬收益。

#### 验证

- [x] 一次 manual automation 后 UI 能看到空窗/拒绝摘要（funnel/fb + alphaReject + sync + top5）

---

### TIP-009：Alpha 映射质量抽检与错映射惩罚

**状态**：[x]  
**完成日期**：2026-08-04  
**备注 / PR**：[`archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md`](../../archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md)

#### 问题

LLM / fallback 映射错龙头 → 盯错票；自动化会把错票送进池。**用户没时间做人工抽检**——重做方案：从已有数据全自动检测。

#### 目标

5 类自动 penalty 信号（数据驱动 / 无人工反馈）：
1. 行业不匹配（vs `data/seed/theme_industry_map.json`）— 0.6
2. 历史胜率低（paper_trades 30D < 30%）— 0.5
3. 名称歧义匹配 — 0.4
4. 板块资金流背离 — 0.3
5. 个股资金流背离 — 0.2

penalty 应用到 `compute_alpha_additions` 的 catalystScore 上；最终自动 QA 警告 + 主题胜率通过 Dashboard "Copy" markdown 暴露给外部 AI agent 决策。

#### 文件范围

| 层 | 文件 |
|----|------|
| Script | `scripts/build_theme_industry_map.py`（数据驱动种子）|
| Service（new）| `service/alpha_radar_qa.py`（5 信号综合 + 自动 QA stats）|
| Service | `service/alpha_radar_catalyst.py`（输出加 autoQaPenalty 字段）|
| Service | `service/alpha_radar_symbol_resolve.py`（_lookup_by_name 歧义检测）|
| Service | `service/watchlist_automation.py`（compute_alpha_additions 应用 penalty）|
| API | `api/alpha_radar_routes.py`（新增 `GET /api/alpha-radar/auto-qa-stats`）|
| UI | `apps/desktop-ui/src/lib/alpha-radar-catalyst.ts`（buildAutoQaMarkdown + formatCatalystStockSummaryLine QA 标）|
| UI | `apps/desktop-ui/src/lib/dashboard-export.ts`（Copy markdown 末尾加 2 section）|
| Docs | `docs/modules/alpha-incubator.md` §auto-qa-rules |
| Tests | `tests/test_alpha_radar_qa.py`（12 单测）+ `alpha-radar-catalyst.test.ts`（+5 单测）|
| Seed | `services/data-sync-service/data/seed/theme_industry_map.json`（季度跑脚本更新）|

#### 预期收益

- **中**：减少错票进 watchlist → 减少错跟踪 + 误触发研究。
- **对 AI agent**：Copy markdown 多 2 section 后，外部 AI 决策时自带错映射警告 + 主题胜率上下文。
- **对用户**：日常 0 增量操作（仍是 Sync + Copy）。

#### 验证

- [x] 5 信号综合单测 + 歧义检测（12 backend）
- [x] Copy markdown 末尾 2 section 渲染（5 frontend）
- [x] catalyst items 携带 autoQaPenalty + adjustedCatalystScore 字段
- [x] build_theme_industry_map.py 数据驱动 90 天样本覆盖 11 个主题
- [x] full pytest 1274 passed + frontend 440 passed（pre-existing 跳过除外）

---

### TIP-013：信号 IC 验证 + 自研因子探索（Alpha 191 借鉴，不照搬）

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 背景

用户在评估「Alpha 191 公式化因子」后，确认**不做全量落地**（需全市场日线底仓 + 每日截面计算 + 多空调仓，工程量大且 2015 年发表因子已被机构消化），但借鉴其核心方法论 —— **IC / Rank IC / 分层胜率** —— 先度量现有信号，再自研小因子集。

#### 目标（两阶段，Phase A 先做）

**Phase A — 现有信号 IC 度量**（1–2 天）

对现有信号（Alpha catalystScore / TrendOK Score / 研报评分 / TV 信号）计算：

- Rank IC（Spearman）：信号值 vs 未来 N 日收益（N=1/3/5/10），输出均值、ICIR、胜率
- 分层胜率：按信号分桶（如 S/A/B），各桶的平仓胜率与平均收益
- 衰减曲线：信号 → 未来 1/3/5/10 日预测力的衰减

数据源：`paper_trades`（平仓结果）、`execution_decision_changes`（信号快照）、watchlist 历史。**不新增全市场数据同步**。

**Phase B — 自研因子库**（2–3 天，用 Phase A 管道验证）

从**已有数据 + 经济学解释**出发的小因子集起步（不做 191 条）：

| 因子 | 数据源（已有） |
|------|----------------|
| 动量（N 日收益 / RS） | watchlist bars + index_dailybasic |
| 波动率 / 回撤深度 | watchlist bars |
| 换手 / 流动性 | tushare daily_basic（如已同步） |
| 个股资金流 | 东财个股资金流 |
| 行业资金流 + 主线归属 | 东财行业资金流（已有） |
| ETF 资金确认（V6.4 思路） | etf_fund_flow（已有） |

同一 IC 管道验证 → 有效的（ICIR ≥ 0.5 且分层单调）接入闸门校准或新加为辅助信号。

#### 明确不做（scope 边界）

- ❌ Alpha 191 全量公式落地（数据底仓 + 调仓执行，个人 watchlist 体系不匹配）
- ❌ 因子实盘仓位分配（IC 只做度量与校准，不自动下单）
- ❌ 为因子同步全市场日线（Phase B 仅用已有全市场数据：stock_basic / 资金流 / 指数）

#### 统计纪律

- Watchlist 规模（二三十只）样本的 IC 仅作**趋势参考**，不做显著性结论；Phase B 用全市场截面数据时 IC 才有统计意义
- 所有 IC 输出必须带样本数 N，N 不足（如 <100）时标注「样本不足，仅供参考」

#### 文件范围

| 层 | 文件 |
|----|------|
| BE | `service/factor_validation.py`（新：IC / Rank IC / ICIR / 分层 / 衰减） |
| BE | `api/backtest_routes.py`（复用）或 `api/factor_routes.py`（新） |
| DB | 可选 `factor_scores` 表（alembic revision + CREATE_SQL 同步，遵循 AGENTS.md 迁移纪律） |
| FE | 回测页 / AlphaIncubator 加「信号 IC」面板 |
| 测试 | `tests/test_factor_validation.py` |
| 文档 | 结论写回本条目（哪些信号/因子有效、是否调闸门） |

#### 验证

- [ ] Phase A：≥3 个现有信号跑通 30/90 天 Rank IC + ICIR + 分层胜率 + 衰减表（带样本数）
- [ ] Phase B：≥3 个自研因子入库并跑通同一管道
- [ ] 结论落回本条目：有效因子清单 + 建议的闸门阈值调整
- [ ] full pytest + frontend test 全绿（沿用 27 张表零变化纪律）

---

## P3 — 对照与长期

### TIP-010：备用宽宇宙实验（东财形态仅对照，不替换）

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

需实证「东财多头排列 vs TV Pullback」进同一后滤后重叠度；避免团队再争论换源。

#### 目标

选 ≥5 个交易日：

1. 导出东财「均线多头排列」（或等价）标的列表  
2. 对同一列表跑 **回撤（可选）+ TrendOK**（与系统同口径）  
3. 与当日 TV 漏斗结果算 Jaccard / 交集人数  

**结论写入本条目**：预期「交集有限、东财更宽」——用于关闭「换东财」议题，而不是上线替换。

#### 预期收益

- **低直接收益**；**高决策收益**（停止错误方向投入）。

#### 验证

- [ ] 表格落在本文件或 `docs/modules/screener.md` 附录

---

### TIP-011：开火来源归因

**状态**：[x]  
**完成日期**：2026-08-04  
**备注 / PR**：见 [`docs/archive/2026-08-04-tip-011-execution-source.md`](../archive/2026-08-04-tip-011-execution-source.md)

#### 问题

不知道实际 BUY/ADD 主要来自 TV、Alpha 还是手动 → 无法把精力投在真正贡献收益的入口。

#### 目标

Watchlist item 保留 `source`；Decision Journal / Copy 表增加来源列或周报统计：`fires_by_source`。

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `execution-action.ts`、`execution-markdown.ts`、Journal 相关 |
| 存储 | watchlist registry `source` 已有则复用 |
| 文档 | `docs/modules/README.md` |

#### 预期收益

- **中**：指导下一步把 TIP 预算砸在高贡献入口。

#### 验证

- [ ] 连续 2 周能回答「本周开火来源占比」

---

## Review notes (2026-07-22 stability pass)

After TIP-004/005/002 implementation, a defect review fixed:

| Issue | Fix |
|-------|-----|
| EM industry vs SW L1 Top10 exact match false-rejects | Top10 only when label is SW L1; granular EM fail-open |
| S GC exemption used catalyst score Top200 | `load_catalyst_window` builds S set from full aggregate |
| Funnel `Scanned` ≠ `TV` | `scanned` aligned to `tvHit`; UI shows Funnel first |
| `电力` blocked `电力设备` (BE) | defense special-case |
| ack funnel merge untested | `merge_funnel_into_meta` + unit test |

**Stability pass 2 (TIP-006 + residual P1/P2):**

| Issue | Fix |
|-------|-----|
| FE BUY gate still blocked `电力设备` | `isDefenseSector` mirrors BE special-case |
| Empty-DB seed `falcon`/`blackhorse` `enabled=true` | seed defaults `enabled=false` |
| Bare title pattern `pullback` over-match | patterns = `karios pullback` + legacy momentum titles only |
| Fallback-universe fetch failure aborted Import | try/catch → empty fallback, primary path continues |

**Still open (non-blocking):** FE/BE `DEFENSE_SECTOR_KEYWORDS` list duplication (logic now aligned); no EM↔SW map for stricter Top10; N-day funnel chart; **existing DBs** still need Settings: enable Karios Pullback, disable legacy (seed only affects empty DB).

**TIP-003 (2026-07-22):** empty window → Industry 5D Top5 non-defense LIKE universe (cap 80) → TrendOK only → `screener_fallback`.

| 项 | 原因 |
|----|------|
| 用东方财富条件选股替换 TV 第一层 | 职责不同；东财已服务主线；替换不会 magically 提高收益 |
| 全局取消日内 >6% / Anti-Spike | 与卫星仓纪律冲突；仅允许 TIP-007 条件放开 |
| 为抬进池数而大幅降低 TrendOK / Score / 催化门槛 | 通常增加噪音，降低开火质量 |
| 无限加 screener / RSS 源 | 池更大 ≠ 收益更高；先完成 TIP-002 度量 |

---

## 建议执行顺序（实操）

```text
Week 1:  TIP-001（先配 Pullback TV）+ TIP-002（埋点）
Week 2:  TIP-003（空窗）+ TIP-006（合同文档）
Week 3:  TIP-004 + TIP-005（Alpha 进/出对称）
Week 4+: TIP-008 展示 → TIP-007 动量放开（有数据再开）→ TIP-009/011 → TIP-010 对照实验
远期：TIP-013 Phase A（信号 IC 度量）→ Phase B（自研因子库，用同一管道验证）
```

每完成一项：勾选总览表 + 条目内 checklist，并在「备注 / PR」写下观察数据（转化率变化一句即可）。

---

## 验收总标准（计划是否「做完」）

全部 P0/P1 勾选且满足：

- [ ] TV 主宇宙与回踩进池 thesis 一致（或 Momentum 分支明确分离）
- [ ] 任意交易日可回答漏斗四层数字
- [ ] 主 screener 0 票日有降级或明确告警策略
- [ ] Alpha 进池有轻量闸；非 S 可被三日 GC
- [ ] 业务文档（screener / watchlist / alpha）与线上行为一致

P2/P3 为增强项，不阻塞「基础改进完成」声明。
