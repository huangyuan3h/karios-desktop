# Karios · 路线图（todo）

> 产品级路线图，**按领域分章节 + 优先级标记**。完成时把条目标 `[done]`，详情迁到 [`docs/archive/`](./archive/)。
>
> **对应关系**：
> - 本文件管「**做什么、为什么**」（产品/战略层）。
> - 怎么做（架构/工程债）→ [`docs/optimization-checklist.md`](./optimization-checklist.md) 的 `OPT-xxx`。
> - 怎么投（交易规则）→ [`docs/trading-improvement-checklist.md`](./trading-improvement-checklist.md) 的 `TIP-xxx` / `V6.x`。

---

## 0. 我的优先级（用户口径，不可漂移）

| 序 | 维度 | 为什么 | 当前 todo 域 |
|----|------|--------|--------------|
| 1 | **收益** | 立命之本 | §2 交易策略、§8 回测 |
| 2 | **API 开放 / AI 打通** | 让外部 AI 助手能调我的数据 | §3 API 开放 |
| 3 | **工程架构 / 部署** | 长期可持续（含云，但 DB 大概率不上云） | §4 工程与部署 |
| 4 | **浏览器 / 数据源优化** | 上云的硬约束 | §5 数据源 |

> 任何新需求先对一下表：放错了域就回正。

---

## 当前方向（2026-08-09 起 · 系统进入「验证 + 维护」期）

> §19 策略参数已封闭（S-3 双年验证通过），信号/组合层探索完毕。接下来的主线不是加机制，
> 而是**让数据说话**——用真实交易验证回测，同时维持系统健康。

1. **数据验证闭环**（最高优先）：① 卖出用行内「卖出」记录 → user_trades 期望值看板积累样本；
   ② paper S-3 实绩（cron 17:42 已挂）≥20 笔平仓 → C4 对照开跑；③ 每周复盘喂决策 Agent
2. **回拉历史资金流**（收益域最终缺口）：ClashX 节点恢复后低频回拉 → S-3 三窗完整复核 +
   「只做主线严格模式」定案（恢复路径见 §19）
3. **系统健康维护**：季度参数复核（strategy-params §3 触发信号）+ 稳定性审计项（§17）随 healthcheck 滚动
4. **等待项**（用户拍板后激活）：B2 双模式、付费 API 矩阵、美股/加拿大、Mac mini 部署、L4（券商）

> 反漂移：每 30 天回顾 §0 优先级表与本节；「验证」期间不做新机制实验（§19 封闭清单）。

### 2026-08-09 重大修复：S-3 候选池 universe 恢复（收益最高项）

**发现**：score universe 从 2026-06-18 起（paper intake 上线同日）从 TV 大池（749 票/天）缩到
watchlist registry（~15-50 票）——**S-3 实盘/paper 候选池 = 回测的 1/10**（score≥65 每天 17 只 vs
回测口径 50+），仓位利用率 ~15%。根因：run_watchlist_automation 的 score 只算 registry+fallback，
TV 快照不再灌入；且启用池只有 Pullback v3（46 票窄化趋势池）。

**修复（用户拍板「双池并存」）**：
1. 新建 **`tmpl-s3-universe-cn`「Karios S-3 Universe (CN)」** api screener（exchange SSE/SZSE +
   市值≥20亿 + PE>0 + 年涨>0 → **683 票**，≈回测 748），已 capture 入库 + AM/PM cron 自动更新
2. `service/tv.py _capture_via_api` 放开 range (0,3000)（默认 100 会截断大池——隐藏 bug）
3. `db/tv.py` 新增 `list_enabled_api_screener_symbols()`（启用 api screener 最新快照 → symbol 列表）
4. `run_watchlist_automation` symbols = registry ∪ 启用 api screener 全集（模块级 import 可 mock）

**验证**：8-07 score 表 42 → **211 票**（score≥65 17 → **56 只**）；paper S-3 候选（8-07）=0 是
regime Weak 正确空仓（近 5 日全 Weak）；trendok 计算 683 票仅 1.2s。**paper 从下周一 17:42 起
用修复后大池**（注意：回测 valid 窗 6-18 后同样是小池——回测未虚高，paper 口径反而更宽）。
验收：3503 passed 全绿 + ruff 干净 + 2 新测试。

### 2026-08-10 副作用检查：大池不淹前端 + B 股清理（小修）

**检查结论（显示链路无回归）**：
- Watchlist 表 = localStorage 自选 + registry（41 票）——683 大池不显示 ✓
- score 列排序 / S-3 候选徽标（buildS3Candidates）/ Execution Gate 面板 = 全部只作用于
  自选列表内 ✓；trend/rs-ranks API 按自选 symbols 请求 ✓
- 大池影响面 = score 表 + paper_s3（预期目标），前端零淹没 ✓

**发现并修复（B 股清理）**：S-3 Universe 683 票混入 **10 只 B 股**（900xxx SH-B / 200xxx SZ-B，
TV screener 按 SSE/SZSE 市场引入）——B 股无 trendOK 分（score=None，进不了候选）但每天白算 +
8-10 已有 3 行 score=None 落库。修复：`_is_cn_b_share()` 在 automation 合并 universe 时过滤
（registry 已确认无 B 股）。新增测试 test_is_cn_b_share（7 断言）。43 passed 相关文件。

**遗留记录**：4 个日期敏感测试（test_adj_factor/test_daily_service/test_etf_daily 的
"uptodate 跳过 fetch"断言用 last=8-8 硬编码，8-10 起失效）——pre-existing，修法 = 用相对日期
生成 last（下次维护窗口处理）。

### 2026-08-10 paper_s3_intake 干跑预演：闸门链全灭疑虑关闭 ✓

**方法**：干跑 build_s3_candidates / run_intake_s3 于三个代表性日（全 DB 数据源，零东财依赖；
插入后清理，paper_trades 恢复 3 行）：

| 验证日 | regime/闸门 | 结果 | 判定 |
|---|---|---|---|
| 8-10（今天） | Weak | 0 候选 | ✅ 正确空仓 |
| 6-03（749 票） | Diverging 但 panic active + extreme_caution | 0 候选 | ✅ 双硬闸正确拦截 |
| 5-08（749 票） | Strong + hot + 无 panic | **3 候选**（600487/600522/603083，通信，score79，RS 90%+）→ 3 笔插入 | ✅ 完整插入路径（价格=当日收盘、sleeve=5%、why 文本） |

**结论**：
- **闸门链无全灭 bug**——0 候选的每个案例都有明确硬闸（Weak/panic/sentiment），候选>0 路径
  在 5-08 完整走通 ✓（关闭 todo §19「6-22 候选 0 需确认」遗留项）
- **今天 17:42 首跑安全**：regime Weak → 0 候选无插入（正确）；首个 S-3 paper 单要等
  regime 变非 Weak（近 5 日全 Weak，预计本周内）
- 幂等（duplicate skip）+ 不碰东财 ✓

### 2026-08-10 决策 Agent 主线程修复：空响应根因 + 预取上下文 + provider fallback

**症状**：决策页提问 → "empty assistant response"（前端 DecisionPage.tsx:255 对空流抛错）。

**根因链（三层）**：
1. `ai@5.0.116` 的 streamText **工具循环在工具步骤后不再续写**（finishReason='tool-calls'、
   textStream 空）——Gemini 和 deepseek 都复现（probe 验证：工具 execute 执行成功，但无第 2 次
   模型调用）→ 生产 /decision（3 个 tools）返回 HTTP 200 + 空 body
2. 前端把空 body 判为失败（"empty assistant response"）
3. （次要）进程读 apps/ai-service/.env（旧）为主、根 .env 补齐——GEMINI_API_KEY 在根 .env，
   已由 index.ts 的 rootEnv 加载兜底（无配置问题）

**修复（decision.ts POST / 重写）**：
- **工具调用 → 预取上下文**：请求前预取 `/v1/agent/portfolio-health`（0.24s 实时）注入 system
  prompt（S3_RULES_KNOWLEDGE + 实时体检），模型走纯文本生成路径（已验证可行）
- **provider fallback（用户要求"google 不行就 fallback"）**：primary=Gemini（thinking high）失败
  或空流 → `getResolvedModel()`（openai/ollama）重试；fallback 生效时前缀 `[fallback: <provider>]`
- archive 工具（retrieve/search）随 tools 一起移除（提示用户去历史快照面板），SDK 修复后恢复

**验证**：typecheck ✓ · ai-service 144 tests ✓ · 临时实例实测：Gemini 正常回答（非空）✓ ·
  坏 key 模拟 → `[fallback: openai]` + deepseek 回答 ✓

**待办**：重启正式 ai-service（PID 31671 @4310）后生效；建议升级 `ai@5.0.116` → 5.0.x 最新
（工具循环 bug 可能已修，修后可恢复 tools 方案）。

### 2026-08-10 sync "Copy aborted: missing realtime quote" 修复

**症状**：统一作战表/复制 sync 报 `Copy aborted: missing realtime quote (today): CN:603259, CN:688235, CN:301293, CN:002064`。

**根因**：`GET /quote`（tushare realtime_quote）瞬态失败被后端静默吞掉——`except: return []`
→ 返回 `{ok: true, items: []}`；前端 `.catch(() => null)` 也吞错 → quotes 映射全空 →
watchlist 里 4 只 CN 票全部缺"今日报价" → `shouldRequireRealtimeQuote`（交易时段 + CN）拦截 abort。
（事后复测 4 只均有 11:30 报价——纯瞬态；无日志可回溯，因为两层都静默。）

**修复（两层）**：
1. 前端：`fetchQuoteChunkWithRetry`（watchlist-market.ts）——/quote 失败重试 1 次（400ms 间隔）；
   watchlist-market.fetchWatchlistQuotes + dashboard-export 内联分支统一使用
2. 后端：realtime_quote.py `_tushare_quotes` 失败时 `logger.warning`（带 exc_info）——
   静默黑洞改为可观测；下次再发生可从日志确认

**验收**：前端 typecheck ✓ · 756 tests ✓ · 后端 ruff ✓ + 28 realtime_quote tests ✓。
**生效**：Next dev 热更新即生效；后端日志改动需重启 uvicorn（下次维护一并）。

### 2026-08-10 HK 并行策略线：回测闭环完成（A 股不变）

> 用户拍板：**港股单独回测，A 股结论与港股并行**（两条独立策略线）。

**数据层（HK 基础设施已齐）**：
- universe = 本地成交量 top 500（近 60 交易日 vol 排序——零外部依赖的恒生综合代理，与
  registry HK 票并集）；恒综官方成分 API（东财/新浪/csindex）此网络不可达
- **HK score 回填**：`scripts/hk_backfill_watchlist_scores.py`（复用 live `_trendok_one`，
  bars 预取、无网络）——22.9 万行 / 500 票 / 470 交易日 / 11 分钟；HK score 无行业分量
  （行业/资金流 HK 无映射），score 顺周期偏高（入场票集中在 75-100，门槛无区分度——已确认）
- **HK regime**：`get_hk_regime()`（HSI+HSTECH 红绿灯，macro_daily 历史 ✓ 可 as-of）

**修复的 look-ahead bug（重要）**：`fetch_last_closes`（db/macro_daily.py）原无 as_of 过滤——
HK 指数信号在 as-of 模式读到"最新 80 天"（每个历史日都是今天的价格）→ HK regime 全 Strong。
已加 `as_of_date` 参数；修复后 HK 历史 regime 真实多样（2024-08 Weak 熊市 ✓ 2024-10 Strong ✓）。

**HK 回测定案**（三窗全正、夏普全 2.6+）：
| 参数 | HK 定案 | A 股 S-3（不变） |
|---|---|---|
| 闸门 | **regime 档**（score+RS+HSI regime，无行业闸——HK 无行业资金流） | full（含行业 mainline） |
| score | 65（HK 无区分度，保留） | 65 |
| trailing | **-12%**（-8 在港股波动下被高频打掉） | -8% |
| stop | -5% | -5% |
| RS | **前 40%（rs_min 0.6）** | 前 50% |
| 金字塔 | 开（2.5/0.5/1） | 开 |
| 其他 | hold60/target100/mp20/10%仓位/swap 关 | 同 |
- **三窗**：OOS2 +43.4%/DD12.7/夏普2.63 · train +23.0%/DD9.9/2.89 · valid +26.2%/DD5.7/3.64
- A 股回归：OOS2/train 与固化基线**完全一致**（124.3/4.21 · 151.1/5.92）；valid 78 笔差异
  = 8-09 数据回填演进（stash 验证非代码回归）——**基线待重固化（run_walk_forward --save-baseline）**
- ✅ **A 股基线已重固化（2026-08-10）**：train +151.1/9.7/5.92/211笔 · valid +77.2/5.0/8.26/64笔
  **与旧基线完全一致**；OOS2 318 笔 +97.8%/DD28.8/3.59（旧 +124.3/18.7/4.21）——引擎 CN 路径
  8-09~8-10 零改动（git diff 仅 HK 支持 7 行）+ train/valid 一致 → **OOS2 变化=历史数据演进**
  （tushare 数据修正），非回归；新基线已固化

**待办（下阶段）**：
1. 固化 HK 真值表 → strategy-params.md §（HK 独立段）+ run_walk_forward HK 配置
2. HK paper 路径（automation HK 池每日 score + HK paper intake，source 区分）+ watchlist HK 视图
3. A 股基线重固化（valid 数据演进后）

### 2026-08-10 HK 并行线：固化 + paper 路径 + 双市场体检卡片（全部完成）

**固化**：
- strategy-params.md：§1b HK 真值表（参数理解表）+ §4 版本历史 + §5 HK 数据质量备忘
- `run_walk_forward.py --market HK`（HK_S3_CONFIG + 独立基线 walk_forward_hk_baseline.json）
- HK 三窗（10% 口径）：OOS2 +86.9/DD25.4/2.63 · train +45.9/19.9/2.89 · valid +52.3/8.3/5.62；
  **过去一年 +83.8%/DD19.9/2.68**（5% paper 口径 ≈ +41.9%）

**paper 路径（HK 独立记账）**：
- `build_s3_candidates(market="HK")`：HSI/HSTECH regime 闸（引擎 market-aware）、无行业闸、
  RS 市场内、panic 用 CN 口径（与 HK 回测同路径）、无 exclude_boards
- `run_intake_s3(market="HK")`：source='S3HK'（db/paper_trading SOURCES 扩展）；
  8-07 干跑 20 只全插入+清理 ✓（曾发现 source 硬编码 'S3' 的 bug——HK 票记错账，已修）
- `paper_s3_intake_job`：17:42 同时跑 CN + HK（记录 paper_s3_intake_CN/_HK）
- automation 每日给 HK 池算分：`list_hk_universe_symbols()`（vol top 500 + registry HK 并集，
  db/daily.py）——HK paper 候选每天有新鲜 score

**双市场体检卡片（A 股/港股买什么·卖什么·持有）**：
- 后端 `/v1/agent/portfolio-health?markets=CN,HK`：顶层 CN（兼容决策 Agent）+ `hkHealth` 块
  （HK regime/candidates/holdings，trail -12 规则）；持仓按市场拆分
- 前端 PortfolioHealthCard：CN | HK 双栏面板（regime 徽章/候选 chips/持有行 EXIT-HOLD）
- 测试：前端 5（双栏渲染）+ 后端 portfolio/paper_s3/watchlist_automation 68 ✓

**验收**：后端 3500 passed（4 failed=已知日期敏感 pre-existing）+ ruff ✓ + 前端 83 passed + typecheck ✓


## 1. 状态看板（导航 · 详情在 §10 沉淀表 / 各章节）

| 领域 | 状态 |
|------|------|
| §2 定位/形态 | ✅ Web 唯一形态（OPT-060）+ 可分享 URL（hash-router） |
| §3 收益/交易 | ✅ S-3 策略定案 + 卫星仓复核 + user_trades 闭环；**验证期**（C4 等样本） |
| §4 API 开放 | ✅ /v1/* 整圈（OPT-045~051）+ cookbook；[ ] webhook（§14 #3） |
| §5 工程/部署 | ✅ Docker 一键 + 备份迁移 + 稳定性审计 5 修（2026-08-09）；Tunnel 端到端待验证 |
| §6 数据源 | ✅ TV Scanner API 唯一池子；[ ] 付费 API 矩阵（§12 #9） |
| §7 新闻/研报 | ✅ News Substrate 2.0 三轨 + TIP-012 研报通道 |
| §8 回测 | ✅ 引擎 v1.5 + S-3 + C1 工具；[ ] C4 paper 对照（等数据）；[ ] BacktestPage 重写 |
| §9 多市场 | 🟡 美股/加拿大远期（§12 #14/15） |
| §10 沉淀 | 27+ archive（每完成一项补一行） |
| §13 Longevity | ✅ 换电脑跑/恢复数据；🟡 云相关暂缓（等 Mac mini） |
| §15 用户反馈 | ✅ 3 条全部落实（watchlist.md 使用笔记落档） |
| §16 L3→L4 | ✅ L3 五里程碑全完成；L4-P1 券商研究未拍板（红线：P0 未清不碰） |
| §17/18 工程加固 | ✅ Gate 全清 + 覆盖率 91.8% + R1-R7（详情 → [`archive/2026-08-09-todo-slim-eng-hardening.md`](./archive/2026-08-09-todo-slim-eng-hardening.md)） |
| §19 策略优化 | ✅ 信号/组合层封闭（S-3 双年验证）；⏸ 回拉（等节点）；验证期 |


## 2. 产品定位与形态

- ✅ **Tauri 降级 + 形态迁移（2026-08-04 · OPT-060）**：Web = 唯一交付形态；Tauri 保留源码，
  ≤0.5 天可复活 → [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)
- ✅ **可分享/可订阅 URL（2026-08-09 · hash-router）**：15 页面 + 深链接 `#/stock/<sym>` + journal 子模式
  （`lib/hash-router.ts`，6 测试；决策 Agent 输出/周报 markdown 链接直达）
- [ ] **[P1] 基础 AI 能力保留**：内置 Chat Panel + 摘要生成（不依赖外部 AI 时本地也能用）
- [ ] **[P4] 完整产品定位文档**：「卫星仓纪律化操作工具」一页式宣言


## 3. 收益 / 交易策略（最高优先级 · 优先级 1）

> 架构/工程债 → `OPT-xxx`；交易规则 → `TIP-xxx` / `V6.x`；本节只放产品/战略层收益决策。
> 交易策略真值现在集中在 §19 + `modules/strategy-params.md`，本节约束"做哪些"。

- [ ] **[P0] 数据源质量审计（续）**：OPT-050 已拍板续 Tushare 不引 Wind/Choice/iFinD/聚宽；
      剩余：付费 API 矩阵对比（§12 #9）与 TV Capture 决策（§6）
- ✅ **重启回测系统**（§8 已重做：OPT-063 引擎 v0 → OPT-070 v1.5 闸门 → §19 S-3 定案 + C1 工具）
- ✅ **漏斗转化率闭环**（2026-08-02 · TIP-002）→ [`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)
- ✅ **卫星仓上限复核（2026-08-09）**：单票 15%/clip 5% 维持；**S-3 候选豁免 30% 板块/簇 cap**；
      旧口径统一 20 票 → 见 §19
- ✅ **TIP-011 开火来源归因**（2026-08-04）→ [`archive/2026-08-04-tip-011-execution-source.md`](./archive/2026-08-04-tip-011-execution-source.md)
- ✅ **TIP-009 Alpha 映射自动 QA**（2026-08-04）→ [`archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md`](./archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md)
- ✅ **V7.0-02 风险平价开仓**（2026-08-05）→ [`archive/2026-08-05-v7-02-risk-parity-sizing.md`](./archive/2026-08-05-v7-02-risk-parity-sizing.md)
- ✅ **TIP-012 研报→α 通道**（2026-08-05）→ [`archive/2026-08-05-tip-012-research-alpha-channel.md`](./archive/2026-08-05-tip-012-research-alpha-channel.md)
- ✅ **TIP-013/014 Copy 新鲜度 + 强制刷新**（2026-08-06）→ [`archive/2026-08-06-tip-013-014-copy-freshness.md`](./archive/2026-08-06-tip-013-014-copy-freshness.md)
- ✅ **TIP-015 决策 Agent 闭环 M1**（2026-08-06）→ [`designs/tip-015-decision-agent-loop.md`](./designs/tip-015-decision-agent-loop.md)
- ✅ **user_trades 闭环打通（2026-08-09）**：SELL 记录曾被后端 400 硬卡（缺成本/入场日）→ 已修
  （校验放松 + 可选成本补填 + 无条件记录）；**使用提示：卖出用行内「卖出」按钮**（期望值看板
  TradeStatsPanel 自动累计；「纪律+真实数据验证」路线的数据管道）


## 4. API 开放与外部 AI 打通（优先级 2）

- ✅ **OpenAI 兼容 /v1/* 整圈（OPT-045/046/047）**：8 endpoint（market/watchlist/journal/explain/version/
  schema/errors/changelog）+ 人类可读文档 → [`archive/2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md)
- ✅ **API Key 配额 + OpenAPI（OPT-051）**：多 Key + 三窗口滑动配额 + /v1/quota + Swagger/ReDoc
  → [`archive/2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md)
- ❌ **MCP server**：cancelled 2026-08-04（自写 agent 已 100% 覆盖；后续要启用按原描述单独起 OPT）
- [ ] **[P1] 决策/告警 webhook**（AI agent 订阅 Karios 事件）：设计稿阶段（§14 #3）
- **范围边界（不做）**：Telegram Bot/推送/日报/自动下单/监控巡检 → 归外部 AI 助手（/v1/* 拉数据）；
  Karios Chat Panel 仅"看+问"局部交互 → [`integrations/ai-agent-cookbook.md`](./integrations/ai-agent-cookbook.md)


## 5. 工程架构与部署（优先级 3）

- ✅ **DB 走向决策（OPT-053）**：本地 PG 权威 + 备份 3 副本 + 半年期复审 → [`designs/db-direction-2026-08.md`](./designs/db-direction-2026-08.md)
- ✅ **Docker 一键起 + UPS（OPT-056）** + **DB 备份/迁移（OPT-061）** → §12 已完成表
- ✅ **Tauri 降级（OPT-060）** + **legacy 清理（OPT-059）** → §12 已完成表
- ✅ **Alembic 纪律 + DB 测试清理纪律**：AGENTS.md（schema 改必须迁移；requires_postgres 测试必须自清）
- [ ] **[P1] Tunnel 端到端验证**：脚本骨架就绪（OPT-048），需用户装 cloudflared（brew install cloudflared）
- [ ] **[P1] 内网穿透方案**：Tailscale / Cloudflare Tunnel / FRP 三选一（等 Tunnel 验证后定）


## 6. 数据源 / 浏览器替代（优先级 4）

### 数据打通作战计划（2026-08-10 立 · 用户拍板「先把数据打通，让回测更精准可预测」）

> 盘点基线（2026-08-10 实测 DB + 接口）：详见 `docs/archive/2026-08-10-data-gap-audit.md`

**现状全貌**
- A 股：daily 5760 只（2023-01 起 · 08-10 当日）· adj_factor 97% 填充 · 指数/行业资金流/情绪/ETF 均 08-10 ✅
- 港股：daily 2803 只（1998 起 · 08-10 当日 · 腾讯链生效）· score 496 只停在 08-07 ⚠️
- 滞后 1 天（08-07）：主线评分 / stock_dailybasic / USDCNH(macro) / HSI+HSTECH ⚠️
- CN score 覆盖异常：每日仅 30~210 只，08-10 只算出 3 只 B 股（A 股算分 job 疑似未跑）⚠️

**P0 — 直接影响回测可信度**
- [x] **P0-1 港股复权统一**（2026-08-10 ✅ 完成）：腾讯 qfq + tushare 不复权混写 `daily` 同表实锤
  （01398 差 48% / 00388 差 11%）→ `scripts/hk_adj_consistency_check.py`（校验）
  + `scripts/hk_reseed_qfq.py`（全量重灌 2804 只 2022-06 起 qfq，317.7 万行 0 失败）
  → HK 基线重固化（OOS2 +268.0/DD29.7/2.21 · train +26.9/18.9/1.91 · valid +60.6/8.3/6.32）
  → 归档 `docs/archive/2026-08-10-data-gap-audit.md`；⚠️ 每日增量须保持腾讯 qfq 口径
- [ ] **P0-2 港股 universe 升级**（2026-08-10 实测关闭）：恒指官网 SPA 接口需深度逆向（动态
  chunk 未公开）、维基被墙、akshare 无成分接口、东财被墙（用户偏好不碰）→ **维持 vol top 500
  代理**；偏差 ~5-10% 在 S-3 执行层进一步缩小（只买高 score/RS 票）；幸存者偏差成分列表也
  无法解决——**收益递减，不再深挖**

**P1 — 数据新鲜度 / 稳定性**
- [x] **P1-3 滞后表统一补跑 + staleness 监控**（2026-08-10 ✅）：根因=uvicorn 进程 `lru_cache`
  缓存空 tushare key（`load_dotenv` 无 override）→ hk_basic_sync/macro_daily "假成功"——
  修复：`config.py` load_dotenv(override=True) + 重启 uvicorn；**HSI/HSTECH 加腾讯 fallback**
  （tushare index_global 100 次/天限频——`_fetch_hk_index_via_tencent`，已补到 08-10）；
  主线评分/情绪补到 08-10；health `/datasources` 加 hk_daily/hk_macro/hk_score/mainline 监控
  （带市场过滤 whereSql）；stock_dailybasic 08-07 仅回测 total_mv 用、不在回测窗内=低优先
- [x] **P1-4 A 股算分 job 修复**（2026-08-10 ✅ 已修复）：根因两处——① uvicorn 15:59 重启
  错失 17:30 cron（一次性，misfire 不补跑，已手动补算 08-10）；② **`compute_trendok_for_symbols`
  硬编码 200 只上限** → `record_score_snapshots` 改 200/块分 chunk（CN 204→700 · HK 200→497 已验证）；
  待观察：后续 3 个交易日 cron 自动跑覆盖 ≥700 只

**P2 — 增强**
- [x] **P2-5 A 股长历史（>2023）**：腾讯 fqkline 翻页免费拉（替代 tushare 2000 积分）→ 回测可扩窗
  （2026-08-10 已实测 A 股腾讯接口可用；正式实施待扩窗需求）
- [ ] **P2-6 HK amount NULL 回补**：腾讯源重拉近 2 周（07-29~08-07 洞）
- [ ] **P2-7 退市股历史**：无免费解，记录幸存者偏差即可

**HK 回测数据支撑复核（2026-08-10 · qfq 修正数据重扫）** ✅ 定案参数确认有支撑：
- **trail-12 确认**：qfq 数据上 train/valid 双窗最优（+26.9/+60.6）；trail-8 回撤爆（DD41%）、
  trail-15 OOS2 崩（+111）——「港股波动大、-8 高频打掉」结论在修正数据上不变
- **RS0.6 确认**：OOS2 是分水岭（0.5→+97.6 崩 vs 0.6→+268）——不是过拟合，是 OOS2 窗强支持；
  0.7 三窗均略低
- **已知方法学局限（不深入，记录）**：HK 定案是「三窗全正」拍板——valid 被看过后才定，
  valid 非严格一次性确认（与 A 股同流程，用户拍板口径）；universe 代理偏差 ~5-10% 未消除
- 结论：**不重扫参数网格**（反模式 §19 封闭清单）；只复核已定案值 ✓

**稳定性总纲**：HK 已有多源链（腾讯→新浪→yfinance→tushare）；**A 股仍是 tushare 单源** →
腾讯链复制到 A 股日线 + 实时报价（2026-08-10 已实测 A 股腾讯接口可用）→ 双市场同构多源 fallback
+ staleness 监控 = 数据源稳定闭环。**tushare 2000 积分不买**（以上缺口它均不解决）。

### 跨市场资金协调（2026-08-10 立 · 用户需求：双市场都强时的资金分配判断）

> 需求：A 股/港股独立 regime 闸门已满足「都不好=双空仓 / 一个好=单市场」；**双好时需强弱判断 + 资金分配**。
> 现状：CN=Weak 空仓 + HK=Strong 进攻（今天已在正确工作）；但 paper 双线各自 mp20×5%=各自 100%，
> **无共享资金池**（真实资金视角=双线叠加暴露）；regime 只有三档离散（Strong/Diverging/Weak），**无强度量化**。

- [ ] **T1 联合回测引擎**：共享资金池跨市场回测（现 `run_walk_forward.py` 单市场独立跑）
- [ ] **T2 regime 强度分**：绿灯数/指数动量/广度 → Strong 内强弱子分（CN+HK 同构口径）
- [ ] **T3 资金分配规则候选 + 回测验证**：
  R1 等权 50/50（最简）· R2 强度比加权（T2 产出）· R3 跨市场相对动量（CSI300 vs HSI 20 日）· R4 主次固定（60/40）
- [ ] **T4 paper 资金池化**：双线各自 100% → 共享池按规则分配（拍板后做）
- [ ] **T5 paper_s3 HK 每日首跑观察**：17:42 双市场 intake（_CN/_HK）连跑 3 交易日验证
  （cron 联动、HK 日线同步先行、候选非空时正确插入）
- [x] **T6 HK 实时报价链港股验证**（2026-08-10 ✅）：新浪 hq.sinajs.cn 主链对 HK 标的实测通过
  （00700/02899/01787 当天 16:04 价）——HK 盘中决策/止损刷新链路 OK

### 数据源 / 浏览器替代（原条目）

- ✅ **TV Scanner API = 唯一池子**（2026-08-01 · OPT-057）；ego-lite/Chrome CDP 仅 fallback
  → [`archive/2026-08-01-opt-057-tv-capture-three-track.md`](./archive/2026-08-01-opt-057-tv-capture-three-track.md)
- [ ] **[P1] TV Capture 数据源决策**（待拍板）：A股 3 screener 用 Tushare / TV API / 1:1 复刻验证
- [ ] **[P1] 付费 API 矩阵**（§12 #9）：Tushare/聚宽/iFinD/Wind 对比 → `archive/YYYY-MM-datasource-matrix.md`
- [ ] **[P2] 自建爬虫兜底**：仅上述都不可行时启动（最低优先级）
- [ ] **[P2] 资讯 RSS 源扩张**：≤20 个源（噪音 vs 收益边际递减）


## 7. 新闻 / 研报

- ✅ **研报源评估**（TIP-012 实测 2026-08-05）：东财研报中心 API 免费可用（单日 40-60 份个股研报、
  评级/目标价/EPS/行业全结构化）；巨潮/慧博/Wind 无需再评估
- ✅ **研报→α 通道（TIP-012）**：确定性评分 + TIP-004 闸门 + registry source='research'
  → [`archive/2026-08-05-tip-012-research-alpha-channel.md`](./archive/2026-08-05-tip-012-research-alpha-channel.md)
- [ ] **[P2] 是否独立子项目**：`karios-research`（避免污染主仓卫星仓逻辑）
- ✅ **News Substrate 2.0 三轨全完成（2026-08-02 · 老婆反馈 #2）**：
  Track 1 RSS 分级（Tier A/B/C/D + 投资级替换 13 源）· Track 2 LLM enrichment（tickers/sectors/
  event_type/importance + watchlist-aware 评分）· Track 3 Morning Brief（08:30/12:30 top 7 推送）
  ——细节见 [`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md) 旁记录与 `modules/news.md`
- ⚠️ 观察：`news_enrich_job` 依赖 ai-service 在线（离线即 49/0 全失败）；失败原因已入库可诊断（2026-08-09）


## 8. 回测（已重启 · S-3 定案）

- ✅ **前置条件**：与 live 同口径（共享规则代码 OPT-070）+ 历史 bars 齐全 + paper 先行——全部达成
- ✅ **回测引擎 v1.5**（OPT-063→070）→ [`archive/2026-08-07-opt-063-backtest-engine.md`](./archive/2026-08-07-opt-063-backtest-engine.md)
- ✅ **Paper v0.1/v0.2 + S-3 模式**（paper_s3 同码闸门，cron 17:42）→ §16 L3-P1 / §19.1 G4
- [ ] **[P1] paper 实绩对照（C4）**：≥20 笔平仓后，回测结论 vs paper 真实表现逐条核对
- [ ] **[P2] BacktestPage 重写**：等 paper 数据有数字后做（现有页面为参数敏感度工具）
- ⚠️ 纪律：回测数字不作发布依据；paper 实绩为准


## 9. 多市场 / 远景（最低优先级）

> 加拿大生活规划会推高美股优先级，但当下数据不足决策。

- **[P3] 美股 symbol 闸门**：参考 HK 闸门（`OPT-041`）的 8 处 `symbol→ts_code` 改造范式
- **[P3] 时区 / 交易日适配**：美股 vs CN vs HK 时区不同，scheduler (`scheduler/*.job.py`) 需要按市场分别触发
- **[P3] 美股数据源**：yfinance 已被 rate-limit 实测（`OPT-043` 注）；评估 Polygon / Alpaca / IEX Cloud / Tiingo
- **[P4] 加拿大税务/账户模型**：完全 P4，先保持基础 symbol 闸门就够

---

## 相关执行清单（不在本文件更新范围内）

| 清单 | 命名 | 用途 | 状态 |
|------|------|------|------|
| 架构优化 | `OPT-001` ~ `OPT-044+` | 工程实现债 / 性能 / 兼容 | 滚动维护 |
| 交易改进 | `TIP-001` ~ `TIP-011` | 业务规则校准 | 大部分完成 |
| 交易中枢 | `V6.2-*` / `V6.3-*` | Execution Gate 子规则 | 完成 |

> **规则**：新任务先在 `todo.md` 起条；明确属于工程债 → 转 OPT-xxx；属于交易规则 → 转 TIP-xxx。todo 本体不写实现细节。

---

## 数据源 / 系统现状速览（备忘）

- **CN 行情**：Tushare（200/年）为主基线，akshare 多源兜底；港股走 tushare.hk_basic + akshare(stock_hk_daily) + yfinance(^HSI 指数)
- **ETF**：fund_basic 已同步（2102+），fund_daily 已启用，`OPT-042` 完成
- **新闻**：RSS + AI 摘要（`OPT-038` 并行化）
- **指数**：index_daily + index_dailybasic（`OPT-033` 批量读 + `OPT-034` 去重）
- **回测**：BacktestPage 隐藏，效果差；paper-trading 缺位
- **执行**：Execution Gate + Action Card + Decision Journal 三件套已上线（V6.x）

> 本节只用来"对账"，变更频繁请改 `optimization-checklist.md` / `trading-improvement-checklist.md`，本文件保持指向。

---

## 维护规则

1. **新增条目** 用 `[Px] {一句话动词 + 名词}`，必要时给 2-3 行补充。
2. **完成判定**：标 `[done] YYYY-MM-DD`，同时把摘要迁到 `archive/YYYY-MM-{slug}.md`，todo 上保留一行完成链接。
3. **优先级漂移**：如果某条用户口径变了，**先动 §0，再动该节**，避免局部拧。
4. **每 30 天回顾一次 §1 状态看板**，把长期 P0 但未动的项目显式降级或归档。
5. **防回潮（2026-08-09）**：实验/修复的**详情**只写在一个真值处——回测实验 → `strategy-params.md`/`backtest-strategy.md`；工程加固 → `optimization-checklist.md`/`archive/`；todo 只保留状态一行 + 外链。完成即压缩。

---

## 10. 已沉淀到 archive/

> "沉淀"指"重大判断 + 不希望被忘记"的事件，不是所有实现细节。一个事件通常一条独立归档（见 `archive/README.md`）。

| 日期 | 事件 | 归档位置 |
|------|------|----------|
| 2026-08-09 | **todo 精简整理**：§17（Gate+覆盖率波 1-13）与 §18（R1-R7）详情原样迁移；§19 实验细节并入 strategy-params/backtest-strategy 后压缩；1179→578 行 | [`archive/2026-08-09-todo-slim-eng-hardening.md`](./archive/2026-08-09-todo-slim-eng-hardening.md) |
| 2026-08-08 | **L4-Gate 全清（H1~H10 + K1/K4）**：4 个 live bug 根因修复（intake key 错位 / camelCase×2 / journal 校验）、测试隔离纪律化（233 假账户+141 假 session 清理、db_rows_baseline 27 表验收）、fail-open 清单（修 2 激进项）、时区/数值健壮性、API 契约对照（删前端 okBook 死字段）、调度幂等（ingest heartbeat 测试锁定）、安全扫描（本地 CSRF Origin 守卫 11 测试） | [`archive/2026-08-08-l4-gate-audit.md`](./archive/2026-08-08-l4-gate-audit.md)（后端 1435 passed + 前端 515 passed + tsc 干净；L4 准入 Gate 6/7 项达标，剩归档动作已完成——§17 全部勾选） |
| 2026-08-07 | **L3-P5 / OPT-067**：组合相关性防火墙（V7.0-01 转正）——9 个语义因子簇（ETF 前缀 + 东财行业 + HK 科技清单）+ 20 日经验相关性（日历对齐 fail-open）；簇 >30% 拦簇内新开仓（CORRELATION_CAP_BLOCK）+ Suggest% roomCorrelation min 链；回测页「组合相关性防火墙」面板；实测 tech_hk 34.2%（腾讯+恒生科技 ETF）超限实拦，00700×513180 r=0.926 | [`archive/2026-08-07-opt-067-correlation-firewall.md`](./archive/2026-08-07-opt-067-correlation-firewall.md)（1388 后端 + 500 前端全绿；**L3 五里程碑全部完成**） |
| 2026-08-07 | **OPT-066**：journal 上游 symbol 防御层——`is_valid_watchlist_symbol`（CN/HK/ETF 格式校验）+ diff/ingest 双层过滤（坏卡 `rejectedCards` 可观测）+ 前端提交前过滤；坏 symbol 永远进不了决策日志 | [`archive/2026-08-07-opt-066-journal-symbol-defense.md`](./archive/2026-08-07-opt-066-journal-symbol-defense.md)（1379 后端 + 495 前端全绿） |
| 2026-08-07 | **L3-P4 / OPT-065**：周度决策质量复盘——决策量 / paper 净口径 / 卖出归因 / 漏斗健康度 → 中文 markdown 报告；决策 Agent「分析」tab 新增周报卡（复制喂 AI agent）；首次实测：本周 38 条信号 97% 来自 ALPHA（自动提示供给单一化） | [`archive/2026-08-07-opt-065-weekly-review.md`](./archive/2026-08-07-opt-065-weekly-review.md)（1376 后端 + 494 前端全绿） |
| 2026-08-07 | **L3-P3 / OPT-064**：卖出归因（前向收益分桶 by close_reason + 组合暴露）+ 回测页（SidebarNav「回测」）；**期间修复 2 个 live bug**：(1) intake 读 journal 的 key 错位 → paper 自上线从未有真实数据；(2) service 层 snake_case 读 db camelCase → run_update 永不更新（修复后首笔真实闭环 CN:600000 pool_exit）；测试基建加 teardown 防 DB 污染 | [`archive/2026-08-07-opt-064-exit-attribution-backtest-page.md`](./archive/2026-08-07-opt-064-exit-attribution-backtest-page.md)（1370 后端 + 494 前端全绿；已知问题：journal 上游 hash symbol 待修） |
| 2026-08-07 | **L3-P2 / OPT-063**：回测引擎 v0——信号回放（watchlist_score_daily 历史实际分）+ `_pick_close_reason` 同码复用（as-of score 注入防前视）；36 组敏感度网格（score×hold×stop）+ CLI/API；实测近 7 周全组合净期望为负（敏感度价值；不作发布依据） | [`archive/2026-08-07-opt-063-backtest-engine.md`](./archive/2026-08-07-opt-063-backtest-engine.md)（1365 后端全绿；v0.2：TV 池回撤窗口 / 月度滚动 / BacktestPage） |
| 2026-08-07 | **L3-P1 / OPT-062**：Paper v0.2——HK 接入 + 分市场成本模型（CN 30bps / HK 60bps 往返）；pnl_pct 重定义为净口径，stop/target 按净值触发；`/v1/paper-trades` 加 market 过滤 + stats byMarket；决策 Agent 页分市场展示；db 层切 dict_row 退役位置索引 hack；Alembic 0022（legacy 回填 CN/0） | [`archive/2026-08-07-opt-062-paper-v02.md`](./archive/2026-08-07-opt-062-paper-v02.md)（1352 后端 + 494 前端全绿；汇率/ETF 记入 L3-P3） |
| 2026-08-03 | **OPT-059 / §12 #19**：隐藏页 / legacy 清理——SimTradePage + `/simtrade` API、BacktestPage + `/backtest/*`、`testback/` 框架整体退役删除；Alembic `0017_drop_backtest_tables` 删表（2+132 行旧数据）；baseline/测试/文档同步 | [`archive/2026-08-03-opt-059-legacy-cleanup.md`](./archive/2026-08-03-opt-059-legacy-cleanup.md)（1247 后端 + 429 前端测试绿；唯一失败为既有 trendok flaky，stash 验证与本次无关）|
| 2026-08-04 | **OPT-061 / §12 #18**：DB 本地备份 + 跨机迁移包——`db_backup.sh`（pg_dump -Fc + iCloud mirror + 25h last-age 跳过）+ `db_restore.sh`（docker cp + pg_restore --jobs=4 + alembic + manifest cross-check）+ `karios_migrate_export.sh`（tarball bundle）+ `install-db-backup-launchd.sh`（plist 03:00 + RunAtLoad + Wake + DATABASE_URL env）；设计稿 `designs/db-backup-and-migrate-2026-08.md` 解决"电脑休眠 → 唤醒后 launchd 不补跑错过的 job"问题（3 trigger 叠加 + last-age 检查兜底）| [`archive/2026-08-04-opt-061-db-backup-migrate.md`](./archive/2026-08-04-opt-061-db-backup-migrate.md)（端到端 2 次演练：round-trip drop+restore 21s + 新 Mac 模拟全新容器 restore 44 表 + 00700.HK 2026-08-04 487.6 数据完整）|
| 2026-08-04 | **OPT-060 / §12 #11**：形态迁移 · Tauri 降级——根 + apps/desktop-ui 的 tauri scripts/deps/concurrently 全删；`src-tauri/` Rust 源码 + `scripts/build-sidecars-macos.sh` 按 §2 P0 "保留 build 配置" 不动；顶层 docs（README / AGENTS / docs/README / docker-one-click / next.config / Dockerfile）同步；6 新单测全绿 | [`archive/2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md)（决策真值：Web = 唯一交付形态；Tauri 复活需 ≤ 0.5 天接入）|
| 2026-08-01 | doc 大扫除：3 个旧模块文档迁移至 `archive/modules-legacy/`（与 V6.x 规则脱节） | `archive/modules-legacy/README.md` |
| 2026-08-01 | OPT-045 Phase A：4 个稳定发现性 endpoint + API Key 鉴权 + 17 单测全绿 | 见 `optimization-checklist.md` OPT-045 |
| 2026-08-01 | OPT-046：3 个只读业务 endpoint（/v1/market/snapshot + /v1/watchlist/items + /v1/decision-journal/query）+ 18 单测全绿 | 见 `optimization-checklist.md` OPT-046 |
| 2026-08-01 | OPT-047：/v1/explain/{symbol} + docs/api/ 6 份人类可读 + scripts/bump-api-version.sh + 14 单测 | 见 `optimization-checklist.md` OPT-047 |
| 2026-08-01 | **OPT-045 整圈归档**（OPT-045/046/047 合并视角）：/v1/* 端到端 8 个 endpoint 落地 + 6 份人类可读文档 + 49 v1/* 单测 | [`archive/2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md) |
| 2026-08-01 | OPT-048 脚本骨架：Tunnel 一行起 + 生产模式 + setup 文档 + 12 单测 | 见 `optimization-checklist.md` OPT-048 |
| 2026-08-01 | OPT-049：paper_trades 表 + 2 cron + 2 /v1 endpoint + 19 单测；Alembic 0011 | [`archive/2026-08-01-opt-049-paper-trading.md`](./archive/2026-08-01-opt-049-paper-trading.md) |
| 2026-08-01 | OPT-050：数据源审计（5 候选对比 + 决策 = 续 Tushare 不引 Wind）+ healthcheck 脚本 | [`archive/2026-08-01-opt-050-data-source-audit.md`](./archive/2026-08-01-opt-050-data-source-audit.md) |
| 2026-08-01 | OPT-051 / §12 #5：API Key 多 Key + 三窗口滑动配额 + /v1/quota + Swagger/Redoc + docs/api/openapi.md | [`archive/2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md) |
| 2026-08-01 | OPT-052 / §12 #6：Alpha Radar 扩展 HK 标的识别（hk_mapping prompt + resolve_hk_mapping + trend_json.hkSymbols + aggregate 合并 + watchlist HK 跳过 EM industry 闸门）| [`archive/2026-08-01-opt-052-alpha-radar-hk.md`](./archive/2026-08-01-opt-052-alpha-radar-hk.md) |
| 2026-08-01 | OPT-053 / §12 #10：DB 走向决策（5 选项对比 + 备份 3 副本 + 6 触发条件 + 半年期复审）| [`archive/2026-08-01-opt-053-db-direction.md`](./archive/2026-08-01-opt-053-db-direction.md)（决策真值在 `designs/db-direction-2026-08.md`）|
| 2026-08-01 | §12 #8 ego-lite spike：Chrome capture 替代方案调研（TV Scanner API 发现 + spike 验证）| [`designs/ego-lite-spike-2026-08.md`](./designs/ego-lite-spike-2026-08.md) |
| 2026-07-27 | V6.3 极端资金流豁免 `INTRADAY_OVERFLOW_OVERRIDE` + Alpha S TrendOK recovering | 见 `trading-improvement-checklist.md` V6.3 节 |
| 2026-07-24 | V6.2 14:30 尾盘时间锁 + 防守双轨袖子 + Zero-Pos 归零清场 | 见 `trading-improvement-checklist.md` V6.2 节 |
| 2026-07-22 | 漏斗转化率 / Pullback 主宇宙校准 / Alpha 进池闸 / Alpha GC 对称化 | 见 `trading-improvement-checklist.md` TIP-001~006 |
| 2026-07-29 | HK + ETF 闸门全打通（OPT-041~044） | 见 `optimization-checklist.md` |
| 2026-08-01 | OPT-056 / §12 #7：Docker 一键起 + UPS 自动恢复（3 Dockerfile + 4 compose service + 6 脚本 + setup doc + 57 tests）| [`archive/2026-08-01-opt-056-docker-one-click.md`](./archive/2026-08-01-opt-056-docker-one-click.md)（脚本骨架完整，端到端实跑需用户跑 `scripts/docker-up.sh --migrate`）|
| 2026-08-02 | **OPT-058 / §12 #20+#21**：漏斗 N 日表格（TIP-002 收尾：`GET /watchlist/automation/runs` + FunnelHistoryTable）+ Paper-trading v0.1 关闭条件（target_hit / score_floor / pool_exit，fail-open 纪律）| [`archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md)（后端 50 相关测试 + 前端 5 新测试全绿）|
| 2026-08-04 | **TIP-009 / §3 P2**：Alpha 映射质量抽检 + 错映射惩罚（数据驱动 · 用户零操作版本）——5 信号自动 QA（行业不匹配 / 历史胜率低 / 名称歧义 / 板块资金流背离 / 个股资金流背离）；theme→industry 映射从历史 alpha_radar_trends 自动聚类（90d 数据 → 11 主题 / 季度跑脚本更新）；penalty 应用到 `compute_alpha_additions` 的 catalystScore；Dashboard Copy markdown 末尾新 2 section（⚠ Mapping warnings + Theme historical win-rate）喂外部 AI agent 决策；新增 `GET /api/alpha-radar/auto-qa-stats` | [`archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md`](./archive/2026-08-04-tip-009-alpha-mapping-auto-qa.md)（1274 后端 + 440 前端全绿；用户日常仍是 Sync + Copy，0 增量操作）|
| 2026-08-04 | **TIP-011 / §2 P2**：开火来源归因（TV/Alpha/手动）——`source` 贯穿 write-path：前端 `deriveActionCard` 按 TV screener 快照 + Alpha catalyst 集合写 `source`（closed enum TV/ALPHA/MANUAL）→ `diff_snapshots` 透传到 `execution_decision_changes.source` → paper_trades intake 镜像到 `paper_trades.source`；新增 `GET /v1/execution/source-stats`（按来源出 BUY 信号量 + 平仓胜率 + 持仓数）；Copy markdown 新 section「Execution · Source attribution (30d)」；alembic 0018 + 全量 1295 后端 + 456 前端测试全绿 | [`archive/2026-08-04-tip-011-execution-source.md`](./archive/2026-08-04-tip-011-execution-source.md) |
| 2026-08-01 | OPT-057 / §12 #8.5：TV Capture 三轨架构（Scanner API + ego-lite + Chrome fallback）+ 新建 screener 模板化 UI + 5 模板 live API 验证通过 + capture 流程端到端走通 | [`archive/2026-08-01-opt-057-tv-capture-three-track.md`](./archive/2026-08-01-opt-057-tv-capture-three-track.md)（47 新单测 + 1055 全绿；Scanner API filter 必须用数组格式 `[{left,op,right}]`；HK `exchange=HKEX`，US `exchange∈[NASDAQ,NYSE,AMEX]`；**最终决策**：TV Scanner API 池子基本够用，ego-lite/Chrome CDP 仅作 fallback）|

---

## 11. 注意力预算（自用）

> 散点信息太多时容易自乱。这节规定每天 / 每周的"读哪里 / 改哪里"。

| 周期 | 必读 | 可选 |
|------|------|------|
| 每天开工前 | 本 todo §1 状态看板 + **§12 当前 # 编号** | — |
| 每天开工前 | `modules/watchlist.md` Execution Gate 节（确认 live 闸与纸面一致） | `modules/screener.md` 若今天改了 screener |
| 每周一次（周末） | §0 优先级表 → 是不是要漂移 | §10 沉淀表 → 是不是有重大事件该归档 |
| **每周一** | **§12 这周要打的 # 编号** → 在 freelancer-arch / cloud-deployment / data-source-audit 找上下文 | — |
| 改动 schema / 新依赖前 | `AGENTS.md` + `optimization-checklist.md`（OPT-xxx 进行中列表） | — |
| 修改交易规则前 | `trading-improvement-checklist.md` 最新一条 → 沿革 | — |
| 想做 idea 但排不上 P0 | 起一份草稿到 `designs/`（不要污染 todo） | — |

**反模式**：

- ❌ 把 §1 看板改满 ✓ 之后没有任何 archive 落地 —— todo 不能"假装完成"。
- ❌ 没有拍板就长期留在 todo P0；要么降级要么归档。
- ❌ 新建散点 markdown 文档（"会议纪要" / "杂记"）—— docs/ 只允许本 todo + 真值模块 + 设计草稿 + 归档。

**每日 / 每周加 1 条**：
- 跑 `bash services/data-sync-service/scripts/data-source-healthcheck.sh` → 失败立即处理（不囤）

---

## 12. 实施清单（按 ROI 排序 · 凑时间一个个实现）

> §12 只按 ROI 重排给跨领域工作流；各域 P0/P1 在 §2-§9。已完成的 # 只留一行外链。
> 设计稿：[`cloud-deployment-options.md`](./designs/cloud-deployment-options.md) · [`freelancer-architecture.md`](./designs/freelancer-architecture.md)

### 剩余（未完成）

| # | 动作 | 域 | 预估 | 依赖 | 预期收益 |
|---|------|----|------|------|----------|
| 9 | **付费 API 矩阵评估** | §6 数据源 | 1-2 天 | — | 影响未来上云选型 |
| 12 | **BacktestPage 重写**（基于 paper 数据） | §8 回测 | 3-5 天 | paper 有 N 日数据 | 参数敏感度工具，不作发布依据 |
| 14 | **美股 symbol 闸门** | §7 多市场 | 3-5 天 | 加拿大规划启动 | 远期触发 |
| 15 | **加拿大税务/账户模型** | §7 多市场 | 远期 | — | 远景 |
| 2b | **Tunnel 端到端验证** | §4 工程 | 0.5 天 | 用户装 cloudflared | 远程访问前提 |

### 已完成（一行归档 · 全部 ✅）

| # | 动作 | 归档 |
|---|------|------|
| 1 | OpenAI 兼容 /v1/* + 可发现性（OPT-045/046/047） | [`2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md) |
| 3 | paper-trading v0（OPT-049） | [`2026-08-01-opt-049-paper-trading.md`](./archive/2026-08-01-opt-049-paper-trading.md) |
| 4 | 数据源质量审计（OPT-050 · 续 Tushare） | [`2026-08-01-opt-050-data-source-audit.md`](./archive/2026-08-01-opt-050-data-source-audit.md) |
| 5 | API Key 配额 + OpenAPI 文档（OPT-051） | [`2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md) |
| 6 | Alpha Radar 扩展 HK（OPT-052） | [`2026-08-01-opt-052-alpha-radar-hk.md`](./archive/2026-08-01-opt-052-alpha-radar-hk.md) |
| 7 | Docker 一键起 + UPS（OPT-056） | [`2026-08-01-opt-056-docker-one-click.md`](./archive/2026-08-01-opt-056-docker-one-click.md) |
| 8/8.5 | ego-lite 调研 + TV 三轨决策（OPT-057） | [`2026-08-01-opt-057-tv-capture-three-track.md`](./archive/2026-08-01-opt-057-tv-capture-three-track.md) |
| 10 | DB 走向决策（OPT-053） | [`designs/db-direction-2026-08.md`](./designs/db-direction-2026-08.md) |
| 11 | 形态迁移 Tauri 降级（OPT-060） | [`2026-08-04-opt-060-tauri-deprecation.md`](./archive/2026-08-04-opt-060-tauri-deprecation.md) |
| 13 | MCP server | ❌ cancelled 2026-08-04（自写 agent 已够） |
| 16/17 | hover tooltip + Dashboard 精简（§15 反馈） | 2026-08-01（§15） |
| 18 | DB 本地备份 + 跨机迁移（OPT-061） | [`2026-08-04-opt-061-db-backup-migrate.md`](./archive/2026-08-04-opt-061-db-backup-migrate.md) |
| 19 | 隐藏页/legacy 清理（OPT-059） | [`2026-08-03-opt-059-legacy-cleanup.md`](./archive/2026-08-03-opt-059-legacy-cleanup.md) |
| 20/21 | 漏斗 N 日表格 + Paper v0.1 关闭条件（OPT-058） | [`2026-08-02-opt-058-funnel-history-paper-v0.1.md`](./archive/2026-08-02-opt-058-funnel-history-paper-v0.1.md) |


## 13. Longevity · 系统长期生命力（用户 2026-08-01 真痛点）

> 真值：[`designs/karios-longevity-2026-08.md`](./designs/karios-longevity-2026-08.md) · Mac mini 方案：[`designs/mac-mini-deployment.md`](./designs/mac-mini-deployment.md)

| 痛点 | 状态 |
|------|------|
| 换电脑也能跑 | ✅ Docker 一键起（OPT-056）→ ~2 小时 |
| 换电脑也能恢复数据 | ✅ DB 备份+跨机迁移（OPT-061）→ 新 Mac 5 分钟；iCloud 兜底 |
| 数据独立于 Mac / 远程兜底 / 长期关机 fallback | 🟡 暂缓（Neon 副本 / Tailscale / Hetzner VM · 等用户拍板） |

**Mac mini 时代**（用户拿到设备那天）：Docker compose 自动启动 + 本地 PG 单一数据源 + LaunchAgent；
不做：compose 内 PG、双数据源、现在就迁移 → 详见 mac-mini-deployment.md


## 14. AI Agent 打通 + TV 数据源（用户 2026-08-01 优先级修正 · 已决策）

- ✅ **AI agent 集成 cookbook**（2026-08-01）→ [`integrations/ai-agent-cookbook.md`](./integrations/ai-agent-cookbook.md)
- ✅ **/v1/* 持续稳定**（OPT-045~051 整圈 + 配额 + 文档）→ §12 已完成表
- [ ] **决策/告警 webhook（AI agent 订阅 Karios 事件）**：设计稿阶段（§14 #3）
- ✅ **TV 数据源决策**（2026-08-01）：TV Scanner API = 唯一池子，ego-lite/Chrome CDP 仅 fallback
- ✅ **§13 远程部署暂缓确认**：Neon/Tailscale/VM 等云相关全部暂缓（用户："暂时云还有一段路"）


## 15. 老婆使用 watchlist 的反馈（2026-08-01 已收到 · 不污染 P0）

> **状态**：已收到具体反馈，需落实
> **来源**：老婆亲自使用 Karios 后给的具体建议
> **行动**：反馈汇总后落 `docs/modules/watchlist.md` 末尾"用户使用笔记"段；衍生需求列为 §3 / §12 的 P1 子条目

### 已收到反馈

| # | 反馈 | 影响范围 | 优先级 |
|---|------|----------|--------|
| 1 | Watchlist table header 参数看不懂，hover 上去能明白每一个参数干什么的 | WatchlistPage table columns | P1 |
| 2 | 新闻模块特别是 dashboard 这里的部分没有她财经新闻准 | Dashboard news + modules/news.md | P2 |
| 3 | Dashboard 里面有些内容重复，参数看不懂不知道干什么 | DashboardPage cards | P1 |

### 衍生需求（待落实）

- [x] Watchlist table columns 加 hover tooltip（P1 · 2026-08-01 完成 → `lib/watchlist-column-help.tsx` + `ColumnHeader`）
- [x] Dashboard 精简重复内容 + 参数说明（P1 · 2026-08-01 完成 → `lib/dashboard-card-help.tsx` + `DashboardHeader`；Last sync table → 单行；Index rule 块 → hover）
- [x] News 模块质量评估（是否需要替换/补强）（P2）→ ✅ **done 2026-08-02** → News Substrate 2.0 全三轨完成（Track 1: 13 investment-grade sources；Track 2: LLM enrichment；Track 3: Morning Brief cron + API）。详见 §7 下方。
- [x] 反馈落到 `docs/modules/watchlist.md` 末尾"用户使用笔记"段（2026-08-09 落档：3 条反馈 + 落实 + 交易记录闭环提示）
- [x] 衍生需求（P1）列入 todo §3 或 §12（2026-08-09 确认：hover tooltip=§12 #16 ✅、Dashboard 精简=§12 #17 ✅、News 质量=§7 News Substrate 2.0 三轨 ✅）

---

## 16. 升级方向：L3 → L4（2026-08-07 立 · 系统演进真值）

> **决策**：当前系统评估为 **L2.5（纪律化决策完成态，缺验证闭环）**。以 **L3（验证闭环）** 为当前目标，**L4（执行闭环）** 为长期愿景。
> **真值文档**：[`designs/l3-l4-evolution-roadmap.md`](./designs/l3-l4-evolution-roadmap.md)（分级定义 / 完成判定 / 里程碑 / 红线）。
> **本节的条**：拍板前在 §16 起条；落地时转 OPT-xxx（工程）或 TIP-xxx（规则），完成后按 §10 归档。

### 分级（简版）

| 级别 | 定义 |
|------|------|
| L2 | 纪律化决策（信号 + 闸门 + 仓位 + 日志）—— 已达成 |
| **L3（目标）** | **验证闭环**：回测 / paper / 成本滑点建模 / 归因复盘 / 参数敏感度，信号价值可度量 |
| L4（远期） | **执行闭环**：券商 API + 半自动下单 + 回执对账 + 组合级实时风控（人始终在环） |

### L3 里程碑（当前目标 · 预计 8-12 周）

| # | 里程碑 | 内容 | 依赖 | 状态 |
|---|--------|------|------|------|
| **L3-P1** | 度量基座 | paper v0.2：HK 接入 + 滑点/佣金/印花税建模 + 成交假设统一 | §8 paper v0.1 已有 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-062-paper-v02.md`](./archive/2026-08-07-opt-062-paper-v02.md)（OPT-062：CN+HK 净口径成本模型 + byMarket 统计 + decision 分析分市场；T+1 由盘后 cron 节奏天然满足；FX 汇率/涨跌停/停牌/ETF 记入 L3-P3 精化） |
| **L3-P2** | 回测引擎 | 与 live Execution Gate 同口径回测（同一份规则代码）+ ≥5y 历史 + 参数敏感度视图 | L3-P1 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-063-backtest-engine.md`](./archive/2026-08-07-opt-063-backtest-engine.md)（OPT-063：信号回放 + `_pick_close_reason` 同码复用 + 净成本；36 组网格 CLI/API；**实测 2026-06-18 起全组合净期望为负——为阈值再校准提供依据**；v0.2：TV 池回撤窗口 / 月度滚动 / BacktestPage UI） |
| **L3-P3** | 归因与敏感度 | 卖出归因分桶（卖早/卖晚/卖对）；参数敏感性报告；卫星仓上限复核（15%/30%/sleeve） | L3-P2 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-064-exit-attribution-backtest-page.md`](./archive/2026-08-07-opt-064-exit-attribution-backtest-page.md)（OPT-064：卖出归因分桶 + 组合暴露 + **回测页（用户可见位置）**；过程中修复 2 个 live bug：intake journal key 错位（paper 从未有真实数据）、service/db camelCase 错位（run_update 永不更新）；journal 上游 hash symbol **已修** OPT-066 双层防御） |
| **L3-P4** | 决策 Agent M2 | 周度复盘：喂 paper 实绩 + 归因 + 漏斗数据，输出「本周决策质量报告」 | L3-P1/P3 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-065-weekly-review.md`](./archive/2026-08-07-opt-065-weekly-review.md)（OPT-065 v0：数据驱动周报 + 决策 Agent「分析」tab 展示；M2 v1：LLM 深度解读 / 自动推送归外部 agent） |
| **L3-P5** | 组合风控 | V7.0-01 相关性热力网转正落地（Correlation Cap + 共振预警） | L3-P2 | ✅ **[done] 2026-08-07** → [`archive/2026-08-07-opt-067-correlation-firewall.md`](./archive/2026-08-07-opt-067-correlation-firewall.md)（OPT-067：9 语义簇 + 日历对齐相关性 + >30% 拦新开仓 + roomCorrelation min 链；实测 tech_hk 34.2% 超限实拦） |

### L4 里程碑（长期愿景 · 6-12 个月 +）

| # | 里程碑 | 内容 | 依赖 | 状态 |
|---|--------|------|------|------|
| **L4-P1** | 券商研究 | 券商 API 矩阵（可用性/合规/费率/沙箱），拍板试点 | — | [ ] 未拍板不动代码 |
| **L4-P2** | 半自动下单 | 人工确认 → broker API → 回执入库；幂等重试；先小额实盘 | L4-P1 | [ ] |
| **L4-P3** | 组合风控实时化 | 相关性 cap + 共振熔断 + 盘中风险预算（L3-P5 盘中化） | L3-P5 | [ ] |
| **L4-P4** | 自动对账 | 券商持仓 ↔ 本地 Watchlist 每日对账 + 异常告警 | L4-P2 | [ ] |
| **L4-P5** | 多市场执行 | US/CA 数据 + 时区调度 + 执行 | §7 P3 数据源先行 | [ ] |

### 红线（不可漂移）

1. **人始终在环**——L4 自动下单必须人工确认，不做无人值守
2. **先验证后执行**——L3 验证闭环是 L4 执行闭环的前置，不允许跳过
3. **同口径是铁律**——回测 / paper / live 共享同一份规则代码
4. **卫星仓定位不变**——核心仓在系统外；信号再强也不等于全家 all-in
5. 每个里程碑交付可勾选的「证据」（测试/报告/归档链接），todo 不假装完成

---

## 17. L4 准入 Gate：全模块排查与加固（2026-08-07 立 · ✅ 全清 2026-08-08）

> 详情已迁 → [`archive/2026-08-09-todo-slim-eng-hardening.md`](./archive/2026-08-09-todo-slim-eng-hardening.md)（K1/H1~H10 全清 + 覆盖率波 1-13：整体 65.9%→91.8%、11 核心模块全 ≥85%、
> 顺带修 5 个真 bug + 3 个产品缺陷）。
> 计划文档：[`archive/2026-08-08-l4-gate-audit.md`](./archive/2026-08-08-l4-gate-audit.md)。

- **Gate 状态**：K1/H1~H10 全部 [x] 2026-08-08（见存档）；剩余风险处置 ①②③ 全 done（B7 行业缺口回填 5543/5543、数据源健康告警上线即抓 4 真问题、阈值再校准实验=不调 live 参数）
- **观察清单**（非紧急）：`etf_daily_full` 每月 1 日回填限流（9-01 复查）；`news_enrich_job` 失败原因现入库（2026-08-09 增强）
- **稳定性审计（2026-08-09 · 业务+工程双轨 · 5 修）**：① backup_age 假 FAIL（healthcheck 36h vs 备份 25h 跳过节奏）→ 阈值 50h；② 资金流盘后 job 工作日天天失败（17:35 太早）→ **cron 18:15** + 三部分诊断；③ option_iv 161 失败 + ④ eastmoney_industry 16 失败（东财风控 IP 拉黑）→ `em_push2_http` 三链与 eastmoney_industry 加 **EASTMONEY_PROXY+COOKIE**；⑤ news_enrich 失败原因透传入库；归因非问题：hk_basic（.env 21:44 已补 key，重启验证）、top_inst（自愈）、dify nginx 循环（无关）。验收 3500 passed + 基线重存


## 18. 工程稳健性加固（2026-08-08 立 · ✅ R1-R7 全清 2026-08-08）

> 详情已迁 → [`archive/2026-08-09-todo-slim-eng-hardening.md`](./archive/2026-08-09-todo-slim-eng-hardening.md)。
> 结论：lint 门禁修复（225 ruff 清零）+ 执行链/小文件覆盖率补足 + FE 组件层起步（testing-library）
> + job 失败告警端点 + 契约测试（golden fixtures 三链确认无漂移）+ ai-service coverage gate（86%）。
> 观察：`db_rows_baseline check` 需在 UI 闲置时执行（dev server 写库会误报）。


## 19. 策略优化作战计划（2026-08-09 立 · 用户拍板「推高胜率 · 不过拟合 · 超所有指数 ≥10%」）

> 背景：一年回测（2025-08-01~2026-08-07）基准 = 科创50 +67%/年（最强），目标线 = +77%/年；
> 差距是结构性的（指数=权重满仓吃 beta，策略=纪律性空仓+仓位摊薄）。**唯一红线：任何改动必须
> 过拟合可控**（参数有业务依据 + 样本外验证）。**参数真值 → [`modules/strategy-params.md`](./modules/strategy-params.md)；
> 实验结论 → [`modules/backtest-strategy.md`](./modules/backtest-strategy.md)；基线 → `data/backtest_reports/walk_forward_baseline.json`。**

### 目标（可量化 · 验收口径）

| 指标 | 当前 | 目标 | 口径 |
|------|------|------|------|
| 年化超额 vs 最强基准 | -36%（+31% vs +67%） | **≥ +10%** | 一年窗口（walk-forward 验证窗口径） |
| 胜率（净） | 35-48% | **≥ 50%** | 净口径（扣 0.3% 往返成本） |
| 夏普（近似） | 4.0 | ≥ 2.5（保持） | 平仓收益序列 · 标注 approx |
| maxDD | 3.5-16% | ≤ 15% | 5% 仓位折算口径 |
| 样本量 | 75-117 笔/年 | **≥ 150 笔/年** | 低于 100 笔的方案不采信 |

### 手段清单（状态一览 · 细节见 strategy-params/backtest-strategy）

| 手段 | 状态 | 一句话结论 |
|------|------|-----------|
| A1 RS 过滤 | ✅ 定案 0.5 | 前 50%；0.7/0.8 过拟合（验证窗劣化） |
| A2 趋势评分 | ❌ 弃用 | 绝对量因子过拟合（训练 +12.4/验证 -15.9）→ 负面清单 |
| A3 主线强化 | ✅ valid 量化 | gates 对照：flow+mainline 在 valid +32.6pt/回撤减半/胜率+13pt；严格模式定案等回拉后补 OOS2 |
| A4 专注池 | ✅ 固化 | **排除创业板（exclude_boards=300）**：三窗 0 劣化（OOS2 +17.4pt）；科创不可砍 |
| B1 仓位自适应 | ✅ 定案 scale=1.0 | Diverging 满仓=最大单项贡献；Weak 空仓 |
| B2 双模式方案库 | 🟡 暂缓（用户） | 趋势 vs 短线并存，regime 切换 |
| B3 滑点 | ✅ 定案 0.05 | 0.1% 断崖 -31pt；分档滑点留给季度复核 |
| C1 walk-forward 工具 | ✅ 交付 | `scripts/run_walk_forward.py`（三窗 + 基线 + 劣化拒收） |
| C2 稳健性检查 | ✅ 完成 | 平台期判定框架（见 strategy-params §4 最佳值判定） |
| C3 方案档案 | ✅ 完成 | strategy-params §4 版本历史 + backtest-strategy 负面清单 |
| C4 paper 对照 | ⏳ 等数据 | ≥20 笔平仓后开（含 user_trades 真实样本） |

**执行顺序**：A1 → B1 → C1（基建）→ A2/A3/A4/B3（信号与组合封闭）→ C4（随数据滚动）。

### §19.1 贴合回测审计（2026-08-09 · 全部闭环 ✅）

> 目标：① 回测严格证明的错误操作必须提醒 ② 贴合差距逐个修 ③ 操作只按回测口径。
> E1-E5（paper 三常量拍板改 + 恐慌冷却 + 隐藏阈值复核）与 G1-G6（S-3 区块绕过 BUY 门槛、
> Diverging banner、低 RS 红标、paper_s3 同码闸门、TrailStop 展示、5% 保守仓位）全部完成——
> 细节见 [`archive/2026-08-08-l4-gate-audit.md`] 与 `modules/strategy-params.md` §1 备注。

#### C. 操作纪律（手动执行时对照）

1. **Copy 的 S-3 区块 = 当日唯一操作依据**（选股+仓位已按回测）
2. 买入前自查：regime 非 Weak（banner）✓ → score≥65 ✓ → RS 前50%（绿色徽标）✓ → 主线 ✓ → 非恐慌冷却期
3. **卖出只按**：移动止损 -8% / 固定 -5% / 60 天——**禁止**因"涨了 10%"或"评分回落"卖出（回测证明是错误）
4. 恐慌冷却期（极端谨慎日 +3 天）不买新票
5. 每笔 5%（paper S-3 口径），同时 ≤20 笔（mp20 定案）；加仓每票至多 1 次（+2.5% 触发，半仓）
   （2026-08-09 复核：原「≤10笔」为 S-3 定案前旧口径，已统一为 20）

### §19.2 回测极致化（2026-08-09 · 参数空间已封闭 · 不再做参数微调）

| 步骤 | 结果 |
|------|------|
| 1 第二样本外年（2024-08~2025-08 弱市） | ✅ **通过**：+52.9%/胜率49.1%/超额 vs 科创50 +10.3% → S-3 双年验证策略 |
| 2 A2 趋势评分 | ❌ 弃用（见手段清单） |
| 3 执行增强 | 金字塔 ✅ 固化（trigger 2.5%/0.5x/1次 · 阈值单调 +1%>+30%）；ATR ❌ 弃用 |
| 4 max_positions | 定案 **20**（mp10 收益低 40%；mp30 回撤 16.8% 劣化） |
| 5 swap 换仓 | 机制保留**默认关**（SWAP_ENABLED=False；正确基线下增量 +0~3pt 双窗不一致） |
| 6 参数灵敏度盘点 | 最佳值判定框架（平台期 / 双窗一致 / 取舍三分类）→ strategy-params §4 |
| 8 机制级实验 | 行业 cap ❌、市值分层 ❌（风格轮动因子）、回撤熔断 ❌ → **组合层探索封闭** |
| 9 2023 第三年验证 | ❌ 不可行（幸存者偏差 + 闸门数据缺失）→ 发布级 = **双年验证 + paper ≥20 笔实绩** |
| 13 决策 Agent 回测感知 | ✅ portfolio-health（S-3 退出体检）+ S3_RULES_KNOWLEDGE + 三类问题 tool 覆盖 |
| 7 季度参数复核 | ✅ 例行：每 3 个月三窗复核一轮，记 strategy-params 复核列；触发信号见该文档 §3 |

**✅ 组合重测定案（2026-08-09 晚 · 含 trailing -8 正确基线 · 10% 仓）**：
mp20 = +155.1%/DD10.1/183笔（一年）；paper 5%×20 纪律口径 ≈ +77.5%/DD~5%。
**S-3 最终参数组（固化）**：score 65 · hold 60 · target 100 · floor 0 · trailing -8 · stop -5 ·
RS 0.5 · diverging 1.0 · 冷却 3 · 滑点 0.05 · mp 20（回测 10% / paper 5%）· exclude_boards 300 · swap 关。
**基线（2026-08-09 晚重固化）**：OOS2 +124.3/DD18.7 · train +151.1/DD9.7 · valid +77.2/DD5.0（含金字塔+排除创业板）。

#### mainline/flow 历史回拉 + S-3 定案复核（⏸ 数据源受阻 · 用户拍板暂停）

- **目标**：回拉 OOS2/train 的行业资金流历史（现仅 2026-02-06 起）→ 三窗完整复核 + A3 严格主线模式定案
- **valid 窗 A3 量化已先行（2026-08-09）**：gates none/regime/full 三档对照 —— regime 闸门=最大单项
  （三窗 +26~48pt/回撤砍半）；flow+mainline=第二正贡献（valid +32.6pt）→ 结果入 strategy-params 复核列
- **风控备忘（三层排查 · 用户协助实测）**：① 指纹 cookie（qgqp_b_id+nid18 最小集）② TLS 栈
  （macOS curl 通 / Python OpenSSL 断 → subprocess curl）③ IP/频率黑名单（失败重试风暴=拉黑诱因，
  换未黑节点可突破一次）+ 验证码层（当前卡点）；`lmt=0` 只返回最近 120 条 → 历史段需分页
- **已入库**：37423 行（312 行业 × 120 天，2026-02-06~08-07）→ **valid 窗闸门数据完整**（重测 +3.2pt 改善）
- **已生效**：ClashX 规则 eastmoney→PROXY 置顶；`retry_backfill_until_done.py`（探活+低频）；
  代理层已全面支持（em_push2_http / eastmoney_industry 加 EASTMONEY_PROXY+COOKIE，2026-08-09）
- **恢复路径**：① 换未黑 ClashX 节点 → ② `PYTHONPATH=src python3 scripts/backfill_industry_flow_history.py --since 2024-07-01 --rounds 2 --workers 1 --sleep-between 15` → ③ 或抄官方前端历史接口参数（替代 lmt=0 上限）→ 回拉后：三窗复核（对比基线）+ A3 严格模式定案
- cookie 刷新指引：Chrome 打开东财页 → F12 → Application → Cookies → data.eastmoney.com → 复制到 .env `EASTMONEY_COOKIE=`

### 明确不做（过拟合温床 · 封闭清单）

- ❌ 继续扫参数网格（收益递减，边际=过拟合）
- ❌ 按"哪年好看"选年份（样本选择偏差）
- ❌ 增加无业务故事的规则（每规则必须有回测证据+业务解释）
- ❌ 参数插值细化（mp25/score70/trailing7% 类）——收益曲线在平台期平坦，1% 精度=拟合噪声

### 反模式（不可漂移）

- ❌ 用回测数字当发布依据——paper 实绩为准（§8 既有纪律）
- ❌ 为数字好看加无业务依据的参数（每加 1 参数 = 必须独立贡献 + 业务故事）
- ❌ 只看单窗口结果（必须 walk-forward 双窗达标）
- ❌ 采信 <100 笔样本的方案
- ❌ 前视调参（分数/红绿灯/资金流必须 as-of——OPT-070/071 已立纪律）
- ❌ 把"胜率"当唯一目标（超额收益 + 回撤 + 夏普综合判断）
