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
| TIP-001 | 校准 TV 第一层：双宇宙 / 对齐回踩 thesis | P0 | ★★★★★ | 0.5–1 天（配置）+ 1–2 周观察 | [ ] |
| TIP-002 | 漏斗转化率仪表：TV→回撤→TrendOK→开火 | P0 | ★★★★★（度量闭环） | 1–2 天 | [ ] |
| TIP-003 | Falcon / 空窗降级宇宙 | P0 | ★★★★☆ | 1–2 天 | [ ] |
| TIP-004 | Alpha 进池加轻量闸（主线 / 结构 / 流动性） | P1 | ★★★★☆ | 1–2 天 | [x] |
| TIP-005 | Alpha 清池对称化（取消整源豁免） | P1 | ★★★★☆ | 1 天 | [x] |
| TIP-006 | Screener 策略版本合同（命名 / pills / 文档） | P1 | ★★★☆☆ | 1 天 | [ ] |
| TIP-007 | 主线内有条件放开动量通道（B_momentum） | P2 | ★★★★☆（进攻） | 2–3 天 | [ ] |
| TIP-008 | Automation 落地指标与复盘字段 | P2 | ★★★☆☆ | 1–2 天 | [ ] |
| TIP-009 | Alpha 映射质量抽检与错映射惩罚 | P2 | ★★★☆☆ | 1–2 天 | [ ] |
| TIP-010 | 备用宽宇宙实验（东财形态仅作对照，不替换） | P3 | ★★☆☆☆ | 1–2 天 + 对照周 | [ ] |
| TIP-011 | 开火来源归因（TV / Alpha / 手动） | P3 | ★★★☆☆ | 1–2 天 | [ ] |

**预期收益图例**：★ 越多 = 越可能提高「有效开火密度」或减少「空转噪音」（在卫星仓纪律内）。

---

## P0 — 最高优先级（先做）

### TIP-001：校准 TV 第一层 — 双宇宙 / 对齐回踩 thesis

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

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

- [ ] 在 TradingView 上重配 / 新建 Pullback screener URL，写入 Settings（enabled）
- [ ] 记录 Filter Pills 全文到本条目「验收记录」或 `docs/modules/screener.md`
- [ ] 连续 ≥5 个交易日统计：命中数、过回撤数、过 TrendOK 数（可用 TIP-002 产出）
- [ ] 若保留 Momentum screener：在导入逻辑中按 screenerId / screenTitle 分支后滤（见文件范围）
- [ ] **明确不做**：用东财「均线多头排列」替换 TV 主宇宙

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

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

没有统一指标时，「健壮」「提高收益」只能拍脑袋；Automation 每天跑，但不知道哪一层在漏人。

#### 目标

每个交易日（或每次 Import / Automation）落一条漏斗快照：

| 指标 | 含义 |
|------|------|
| `tv_hit` | enabled screener 最新快照去重标的数 |
| `pass_pullback` | 过 52W 回撤窗 |
| `pass_trendok` | 过 TrendOK |
| `added_new` | 新写入 Watchlist |
| `alpha_candidates` / `alpha_added` | Alpha 候选与实际追加 |
| `fireable`（可选，次日/当日） | Gate=ATTACK 且主线等条件满足的空仓 BUY 候选数 |

UI 最小形态：Watchlist Import Debug 扩展，或 Dashboard / Scheduler 一行摘要；持久化优先 Postgres（便于周复盘）。

#### 细节 checklist

- [ ] 定义 schema（日级或 run 级）与写入点（import + automation ack）
- [ ] 前端展示最近 N 日转化率（表格即可）
- [ ] 文档写明各字段口径（TV 价 vs Tushare TrendOK 可能不一致）

#### 文件范围

| 层 | 文件 |
|----|------|
| FE import | `apps/desktop-ui/src/lib/watchlist-screener-import.ts`、`WatchlistImportDebug` |
| FE automation | `apps/desktop-ui/src/lib/watchlist-automation.ts` |
| BE | `services/data-sync-service/.../watchlist_automation.py`、可选新表 + Alembic |
| 文档 | `docs/modules/watchlist.md`、本文件 |

#### 预期收益

- **极高（元收益）**：后续 TIP 是否有效可证伪；避免无效改 pills / 乱降门槛。
- **对收益率**：不直接产生 alpha，但阻止损害收益的改动。

#### 验证

- [ ] 手动 Import 与 Run automation 各产生一条可查询记录
- [ ] 连续 5 日可画出漏斗，无需翻日志

---

### TIP-003：Falcon / 空窗降级宇宙

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

Falcon Launch 类条件可全日 **0 票** → Automation 第一层断粮，当天只能靠存量 + Alpha。

#### 目标

业务规则：当 **主 Pullback/Momentum screener 最新快照 `row_count=0`（或过回撤后为 0）** 时，启用 **降级宇宙**（仍进同一后滤：回撤可选 + TrendOK）：

候选实现（三选一，优先 A）：

- **A.** 第二个已配置的「宽」TV screener（enabled=false 平时，空窗时才用）
- **B.** 对已在 Industry Flow Top5 的成分做本地 TrendOK 扫描（有上限，如 80 只）
- **C.** 仅告警 + 人工，不自动降级（最低方案，收益最低）

推荐落地 **A 或 B**；明确记录触发条件与当日 meta。

#### 细节 checklist

- [ ] 定义空窗：`tv_hit==0` 或 `pass_pullback==0`
- [ ] 降级来源写入 automation `meta` / 转化率快照（衔接 TIP-002）
- [ ] 降级候选仍必须过 TrendOK；是否过回撤窗写死在规则里
- [ ] 避免与主宇宙重复计数污染转化率（标注 `source=fallback`）

#### 文件范围

| 层 | 文件 |
|----|------|
| BE automation | `.../service/watchlist_automation.py`、`dashboard._sync_screeners_step` |
| FE import | `watchlist-screener-import.ts`（若降级在前端） |
| 文档 | `docs/modules/watchlist.md`、本文件 |

#### 预期收益

- **高**：减少「趋势日断粮」；提高自动化日可监控覆盖。
- **风险**：宽宇宙灌水 → 必须保留 TrendOK；观察 `added_new` 是否暴涨。

#### 验证

- [ ] 人为 disable 主 screener 或 mock 0 行时，降级路径触发且 meta 可见
- [ ] 非空窗日不触发降级

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

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

文档未定义 Falcon / Institutional / Black Horse 因子哲学；seed 名（Swing Falcon）与标题模式（`falcon launch`）漂移会导致触媒路径 silent 变窄。

#### 目标

每个 enabled screener 有一份「策略合同」：

- 显示名、TV URL、`screenTitle` 期望子串
- 宇宙类型：`pullback` | `momentum` | `other`
- Filter Pills 快照（随 capture 已有则可引用）
- 进池后滤：是否应用 52W 回撤窗
- 是否参与 Alpha 触媒「今日 screener TrendOK」集合

#### 细节 checklist

- [ ] 更新 `docs/modules/screener.md` 正式章节（禁止只留代码注释）
- [ ] 对齐 `SCREENER_TITLE_PATTERNS` 与真实 `screenTitle`
- [ ] Settings UI 可选：展示宇宙类型（若改动面可控）

#### 文件范围

| 层 | 文件 |
|----|------|
| 文档 | `docs/modules/screener.md` |
| FE | `screenerExport.ts`、`alpha-radar-catalyst.ts` |
| Seed | `.../service/tv.py`（`ensure_seeded` 命名） |

#### 预期收益

- **中**：减少配置漂移导致的「以为在跑某策略、实际没跑」。
- **对收益率**：防静默失效，属于保险型收益。

#### 验证

- [ ] 标题匹配单测覆盖当前线上 screenTitle
- [ ] 新人只读 screener.md 能说出每套宇宙意图

---

## P2 — 进攻与归因

### TIP-007：主线内有条件放开动量通道

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

Anti-Spike、日内 >6% 禁 BUY 抑制追高，也系统性错过 A 股主线强势延续段。与 TV 动量宇宙、B_momentum 未打通。

#### 目标

**仅在**同时满足时放宽（示例，落地时写死常数）：

1. Gate=`ATTACK`
2. 东财行业 ∈ 主线（5D Top3 ∪ Momentum Breakout）
3. TrendOK=true 且 Score≥阈值（如 85）
4. 来自 Momentum 宇宙或 `buyMode=B_momentum`

则：

- 日内涨幅上限由 6% 提到可配置值（如 9%），或
- `INTRADAY_SURGE_BLOCK` 对该子集不起作用

**禁止**：全局取消见光死；弱市 / 非主线不得放开。

#### 细节 checklist

- [ ] 与 TIP-001 Momentum 宇宙联动；无 Momentum 宇宙则本 TIP 可推迟
- [ ] Execution Why 增加明确码（如 `MOMENTUM_SURGE_ALLOW`）便于 Journal
- [ ] 回测或至少 2 周纸面对照：放开子集的次日表现 vs 对照组

#### 文件范围

| 层 | 文件 |
|----|------|
| FE | `apps/desktop-ui/src/lib/execution-action.ts`、相关测试 |
| BE | TrendOK / buyAction 若双保险 |
| 文档 | `docs/modules/README.md`、`watchlist.md`、`downstream-ai-prompt.md` |

#### 预期收益

- **中高（进攻）**：在纪律内捕捉主线动量段，直接作用于已开火机会的完整度。
- **风险**：回撤加大；必须卫星仓仓位盖仍生效。

#### 验证

- [ ] 单测：非主线仍拦截；主线+ATTACK+高分才放行
- [ ] Decision Journal 能区分普通 BUY 与动量放宽 BUY

---

### TIP-008：Automation 落地指标与复盘字段

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

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

- [ ] 一次 manual automation 后 UI 能看到空窗/拒绝摘要

---

### TIP-009：Alpha 映射质量抽检与错映射惩罚

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

LLM / fallback 映射错龙头 → 盯错票；自动化会把错票送进池。

#### 目标

- 周频抽检：随机 N 条 S 级趋势，人工或半自动核对 `cnSymbols`
- 可选：低 confidence 不参与 `compute_alpha_additions`
- 可选：用户「映射错误」标记 → 降低该 document/symbol 权重

#### 文件范围

| 层 | 文件 |
|----|------|
| BE | `alpha_radar_symbol_resolve.py`、`alpha_radar_catalyst.py` |
| UI | Alpha Incubator 页 |
| 文档 | `docs/modules/alpha-incubator.md` |

#### 预期收益

- **中**：减少错误监控与错误 WATCH_SILENT。
- **对收益率**：避免在错票上浪费注意力与误触发研究。

#### 验证

- [ ] confidence 阈值单测
- [ ] 抽检表模板进文档（哪怕先用 Markdown 手工表）

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

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

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

## 明确不做（防 scope 膨胀）

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
