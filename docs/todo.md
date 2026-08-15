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

### 2026-08-12 系统评估 + 提升计划（用户要求 · 评分存档）

**评分**（基于全量事实：测试 3349+746+64+142 / 备份 5 dump / 回测 1401 笔 / paper 3 笔 /
**后端覆盖率 92.2%**（2026-08-12 全量实测，3352 passed））：
工程稳健度 **8.2**（覆盖率 92.2% 为强证据，修正原"门槛偏低"误判——实测已超）·
业务可置信度 **6.8** · 成熟度 **7.6**。

**提升计划（按 ROI）**：
1. **[P0] paper 样本积累到 ≥20 笔 → C4 定案**（业务可置信度 +1~1.5）：等 17:42 链自然积累；
   期间可用已搭框架每周跑 `scripts/paper_vs_backtest_report.py` 观察
2. **[P0] webhook 真实订阅落地**（工程 +0.3）：用户按 cookbook §9 创建接收端 + 订阅，
   让 E1~E7 告警链路真跑（当前 0 订阅）
3. **[P1] 覆盖率门槛提升 ✅ 2026-08-12**：全量实测 **92.2%**（3352 passed）→
   `cov-fail-under` 52% → **85%**（留安全余量；日常快速跑按 AGENTS.md 惯例用 --no-cov）
4. **[P1] 灾备现状确认 ✅**：妻子电脑已有备份（跨机副本存在，OPT-061 iCloud 镜像+
   迁移包）；Mac mini 投入条件 = **总资产 +20% 里程碑**（届时 C4 也已定案，双条件成立再投）
5. **[P2] 季度参数复核**：下次 ~2026-11（strategy-params §3 节奏），触发信号清单已就绪
6. **[P2] 资金流历史回拉**（ClashX 恢复后）：full 闸门历史复核 + OOS2 证据补齐

**「总资产 +20% 何时到」估算**（2026-08-12 · 满仓口径 · 策略资金=总资产 100%）：
最近 12 个月复利年化 ~232% → 1-2 个月（最强期）；长窗年化 28% → 8-10 个月（保守）；
中性估计 **3-6 个月**。Caveat：paper 3/20 未定案（按回测打 7 折）；2026 为历史最强年
（均值回归风险）；CN 常有空仓等待期；策略资金占比 <100% 时时间按占比拉长。

> 反漂移：以上不新增机制实验（§19 封闭）；全部是"验证 + 维护"期动作。

### 2026-08-12 新定案（当日完成 · 已归档 OPT-085~094）

- **CN 红灯日禁开仓**（OPT-094 定案）：红绿灯回测（OPT-093）证实 CN 红灯日入场负 EV
  （胜率 27% vs 42%）→ 反事实三窗 + walk-forward 通过（valid 收益 +10.7pt）→ 引擎
  `light_red_block` + live `S3_LIGHT_RED_BLOCK`（红灯日候选=0）+ 前端红标；**HK 红绿灯
  无区分度**（右上尾假象），HK 仓位启发式删除、不接入禁开
- **webhook 事件推送 P1+P2 全落地**（OPT-090/091）：E1~E7 事件源 + HMAC 签名投递 +
  前端订阅页（cookbook §9 有示例）；等待用户创建实际订阅
- **一键启动**（OPT-092）：`npm run dev` 统一管理三服务（uvicorn 已去 --reload；
  改 Python 代码需重启 dev）

> 反漂移：每 30 天回顾 §0 优先级表与本节；「验证」期间不做新机制实验（§19 封闭清单）。

### 2026-08-11 重大事故修复：smoke 测试误杀全部 S3HK paper 仓位（20/20 → open 恢复）

**事故**：08-11 08:22 pytest 运行中 `tests/test_postclose_smoke.py`（H2，requires_postgres，直连真实 DB）
第 3 步 `run_update(today_iso="2026-08-07")` **未 mock `get_open_paper_trades`**——对真实 DB 全量 open
扫描；其 `_mock_prices` 对**所有非 `.SZ` ts_code（全部 HK）返回 close=100** → 20 只 S3HK 全部被假
`target_hit`（18 只，pnl +100%~+10370%）/ `stop_hit`（2 只：02359=198、00669=147.9）平仓，close_date
写死 08-07（测试 UPDATE_DATE）。铁证：close_price=100 + close_date=08-07 + 同一秒 updated_at=00:22:13Z。
同 bug 08-09 已误杀 2 只 TV 仓（假价 200/300）——**隐藏了 3 个月的数据污染源**。

**修复（双层）**：① `_mock_prices` 只服务 smoke symbol 自身 ts_code（其他返回空）；② 第 3 步 patch
`get_open_paper_trades` 只返回自己的行——测试恢复"只验证自己的链路"的隔离语义；
③ 恢复 22 行 open（20 S3HK + 2 TV，清假 close 数据，今晚 17:45 update 用真实价正常管理）。
验收：smoke test 前后 paper_trades 状态零变化 ✓ · 202 相关测试全绿 ✓。

**连带**：uvicorn 去掉 `--reload` 重启（4330，nohup 落 /tmp/karios-uvicorn.log）——misfire 循环停止
（此前每 2-3 分钟 hk_basic_sync/TU_SHARE_API_KEY 失败）；HK 数据健康全绿。**今晚 17:42/17:45 是
T5 最终验证**（CN allocation-zero 跳过 + HK 正常买入 + 20 只 S3HK 保持 open）。

**2026-08-11 T5 验证结果 ✅**：17:31 watchlist_automation ✓ · 17:42 paper_s3_intake_CN+HK ✓ ·
17:45 paper_trading_update ✓——三连全绿（sync_job_record 实证），20 只 S3HK 保持 open。

**2026-08-11 watchdog 迁移：launchd → apscheduler**：`paper_chain_watchdog`（launchd
StartCalendarInterval）实测**未在 18:05 触发**（runs=1 仅加载当天手动测试那次）→ 迁移为
`scheduler/paper_chain_watchdog_job.py`（18:05 Asia/Shanghai cron，与三连 cron 同源宿主，
健康时也落 sync_job_record `|ok` 可观测）；launchd plist 已 bootout；注册 scheduler 目录 +
SYNC_JOB_TYPES（coreClose 组）；测试/前端全绿。

### 2026-08-11 稳定性自动化：uvicorn launchd 托管 + paper 链 watchdog + alpha_radar 修复

**uvicorn launchd 托管**：`~/Library/LaunchAgents/com.karios.uvicorn.plist`（KeepAlive 失败自动拉起 +
ThrottleInterval 10s + 日志 ~/.karios/logs/uvicorn.log）——根治"手动 nohup 裸跑 → 进程漂移 → cron
缺失 → paper 漂移"事故链（今日 smoke 事故的根源背景）。

**paper 链 watchdog**：`scripts/paper_chain_watchdog.py` + `com.karios.paper-chain.plist`（周一至五
18:05 北京）——自检 17:30 watchlist_automation / 17:42 paper_s3_intake(CN+HK) / 17:45 update 是否
跑过；缺哪个且当日 close_sync 成功就自动补跑（score 新鲜度是 paper≈回测的执行底座；8-05~8-07
缺口 = score 时效衰减 → 8-10 补跑买漂移票的根因）。

**alpha_radar 修复（断供 5 天）**：`alpha_radar_pipeline_job` 是 12h 间隔触发 + cooldown 默认 12h
→ 每次触发必撞 cooldown 被静默跳过（lastRunAt 停在 08-06，ingest/process 在跑但无新料）；
修复 = `DEFAULT_COOLDOWN_HOURS 12→6` + fetch job 落 sync_job_record（成功/跳过/失败都可观测）。
手动跑通：2026-08-11 抓取 1 主题（全球铜供给挤压，catalystGrade A）✓

### 2026-08-11 CN 复权统一（数据修复 · 三窗全面改善 · 基线重固化）

**问题**：A 股 daily 存 tushare 不复权——除权日跳空被趋势指标误读为崩盘。2025-08-01+ 审计：
**4625 个除权日 / 3483 只票，其中 794 个 ≥5% 假跳空（775 只 ≈ 15% 池子污染）**。HK 8-10 已修，
CN 未做（T5 前唯一剩余的数据真值缺口）。

**修复**：`scripts/cn_reseed_qfq_tx.py` 腾讯 fqkline qfq 全量重灌 5224 只 CN A 股 2023-01 起
（54000 行更新 / 0 失败 / 87 分钟；只动 OHLCV 不动 amount/adj_factor；当日价 qfq=raw → 每日
增量无需改口径）。**踩坑三连**：① 本地 adj_factor 方案不可行（tushare 因子有滞后+口径残差
~0.2%）；② macOS Python `_scproxy` 无视 env 读系统代理（ClashX 127.0.0.1:7890 节点抖动挂起）
→ 脚本内 `ProxyHandler({})` 强制直连；③ 腾讯 WAF 501 需 20s 退避重试。

**score 全量回填**（271714 行 / 133s）→ **三窗全面改善并重固化基线**：
OOS2 **+134.2**%（+26.0pt / DD 26.0→14.5）· train **+171.2**%（+9.7pt）· valid **+111.9%**
（+17.1pt / 胜率 64.5%）；**过去一年（10%×20 口径）+273.9% / DD15.3 / 夏普 6.66 / 301 笔**
（旧基线 +155.1% / 10.1 / 6.31 / 183 笔）。中途修正：`already_qfq` 只比最后 3 行导致旧除权票
被误跳过（首轮 794→764 假跳空只消 30 个）→ 改**全量序列对比**后 794→258（剩 = 除权日叠加
真实大跌，正常）。**唯一副作用**：score 表历史值全部变化（合理——旧值基于失真数据），
paper 侧 20 只 S3HK + 2 TV 不受影响（存储价格不变）。

### 2026-08-11 用户方针定案：L4 前全自动化 + 回测极致化

**总方针**：L4（实盘）之前，① 一切流程自动化 + 贴近回测；② 回测做到极致 → 保证收益；
todo = 待办事项唯一入口。

**用户实际交易节奏（系统必须服务这个节奏）**：
- **10:00 开盘分析** · **12:00 午间分析** · **14:30 操作**（大部分是买单）
- 止损用**条件单**（券商端），手动只处理突发情况
- 三个时间点 = **快速获取市场信息后继续工作**（分钟级消费，不是深度分析）
- universe 池原来用 TV 控制计算量——**必要性待验证**（683 票 1.2s → 全市场 ~10s，计算量
  可能根本不是瓶颈；TV 的真正价值是质量筛选）

**由此推出的行动（H1.5，本周~8 月底）**：
1. **三时段快照简报**：10:00 开盘简报（隔夜+情绪+候选）· 12:00 午间简报（候选新增/撤销+
   价格偏离）· 14:30 操作卡（买入卡列表 + 条件单清单）——每个都是分钟级消费格式
   → ✅ 已落地（2026-08-11）：`service/trading_brief.py` 组装现有块（portfolio-health regime/
   holdings/止损线 + paper_s3 候选 + 新闻 top5），存 morning_briefs 表（新 type
   trading-open/midday/action + markdown 列，Alembic 0028）；`trading_brief_job` 三 cron
   （工作日 10:00/12:00/14:30）+ SYNC_JOB_TYPES + SCHEDULER_JOB_CATALOG + 手动 API
   （POST /api/news/brief/generate?brief_type=trading-*）；前端 `TradingBriefCard`（watchlist
   页，三 tab + react-markdown + 复制/刷新，5 测试）；首跑实证：14:30 操作卡输出
   「Regime + S-3 候选(无,正确) + 持仓/条件单 4 只（止损/移动线/到期）」✓
   回归：3544 后端 + 770 前端全绿
2. **买入卡可执行化**：S-3 候选 → 结构化买入卡（symbol/建议价/仓位%/gate 理由）→ 14:30
   用户确认即执行——**用回测引擎今日快照驱动**（positions_by_day = "回测说今天该买什么"）
   → 已由 ① 的候选 section 覆盖前半（候选字段齐），「确认即执行」= TradeActionDialog 复用
   （H2 与决策 Agent 复盘一起做）
3. **条件单清单**：每持仓 → 止损价（-5% 固定 / -8% 移动）+ 建议条件单参数，用户券商端下发
   → ✅ 已由 ① 的 holdings section 覆盖（止损/移动/到期 + 接近止损线预警）✓
4. **universe 去 TV 依赖验证**：实测全市场 trendok 计算时间 → 若可承受，把 TV 筛选逻辑
   本地化（市值/PE/流动性用 stock_dailybasic）→ universe 全市场实时化（消 TV 快照滞后）
5. **盘中异常警报**（H2 提前）：单票 -8% 触发 / cron 失败 / 候选突变 → 推送（先 API/网页，
   webhook 后续）——"突发情况我来处理"的前提是系统先叫
   → 候选突变/接近止损线预警已由 ① 的 alert section 覆盖（盘中 12:00/14:30 两次）；剩余
   = 单票 -8% 盘中触发推送（H2）

### 2026-08-12 TV screener 全功能下线（用户拍板 · 剥离 todo）

**背景**：universe 全市场化（8-12）后 TV 无核心消费方（live 算分/回测/候选均不依赖）；
alpha_radar 独立链路不受影响。**下线范围 = 全部功能**，保留历史数据（只读不删）。

**剥离清单（2026-08-12 全部完成 ✅）**：
- [x] **1. 停 cron**：tv_screener_capture_am/pm（scheduler/SYNC_JOB_TYPES/SCHEDULER_JOB_CATALOG 移除）
- [x] **2. 后端剥离**：tv 模块 11 文件 + tv_chrome + funnel_health_job 全删（main/dashboard/
  watchlist_automation/schema_baseline 引用清理；funnel 指标整体退役——TV 是其唯一候选源；
  Pullback 过滤函数保留——基于 daily K 线独立于 TV）；alembic 0002 迁移 CREATE_SQL 内联
  （不依赖已删模块）
- [x] **3. 前端剥离**：ScreenerPage/TradingViewSettingsPanel/WatchlistImportDebug/
  watchlist-screener-import 删除 + SidebarNav/AppShell 路由 + DashboardPage screener 状态行 +
  WatchlistToolbar Import 按钮；**第二轮补剥**（8-12 晚，用户报 404 残留）：
  `lib/execution-source.ts` TV 符号源改恒空集（归因类型保留，不再调 TV API）· 删
  `queries/screener.ts`/`api/tvCapture.ts`/`screenerExport.ts` · `dashboard-export.ts` 的
  screener 段/常量/`fetchTodayScreenerSymbolsByTitle`/`screenerTrendOkSymbols` 全删 ·
  `alpha-radar-catalyst.ts` CatalystCopyContext 去 screener 字段；ChatPanel 保留 TV 快照
  渲染（历史消息 Reference 兼容，catch 兜底）
- [x] **4. 归因保留**：execution_source='TV' 保留（历史 BUY 归因）；watchlist-automation
  funnel 相关前端逻辑移除
- [x] **5. 数据保留**：tv_screener_snapshots/tv_capture_jobs/tv_screeners 等表不删（只读）
- [x] **6. 测试清理**：删除 264 个 TV/funnel 测试 + 适配 dashboard/automation/catalyst 相关；
  全量回归后端 3284 passed + 前端 728 passed + ruff/typecheck 干净
- [x] **7. 文档**：AGENTS.md TV 段删除 · screener.md 归档 modules-legacy · todo §6 更新

**验收**：后端无 TV 残留 import ✓；前端无 TV 网络调用（唯一例外 = ChatPanel 历史消息
渲染，有 catch 兜底）✓；数据表完好 ✓

### 2026-08-12 策略固化：live 与回测同码审计 + universe 全市场统一（用户核心诉求）

**审计结论 + 修复（watchlist 显示 = 回测口径）**：
1. **live 算分 universe 全市场化**（最大不一致）：live 每日算分原为 registry ∪ TV api-screener
   （~700 只），回测 8-12 已统一全市场 5226 → `watchlist_automation._score_universe_symbols`
   CN 分支改为 daily 表全市场（TV api-screener 退役；实测 universe 构建 2.7s + 候选 0.6s，
   每日算分完全可承受）；测试隔离适配（mock `_score_universe_symbols`）
2. **熔断 live 显示**：`portfolio_health` 加 `circuitBlocked` 字段（CN 线调 paper_s3
   `_circuit_blocked`）→ 前端 PortfolioHealthCard 徽章「回撤熔断·暂停开仓」+ 空候选原因
   （Weak / 熔断 / 分数未更新 三态区分）
3. **参数审计**：score65 / RS0.5(HK0.6) / 止损-5 / trail-8 / 60天 / 金字塔 / max20 / 10% / 熔断-25
   ——paper_s3（候选+live熔断）· portfolio_health（显示）· paper_trading（止损执行）三处同码 ✓
4. 修复 flaky 测试：realtime_quote HK 路由测试 `_fetch_em_hk_quote` 未 mock（依赖真实东财
   网络）→ 确定性 mock
5. **联合回测补齐**：dual 脚本无 R5C 规则 + RULES 大小写 bug（'R5c'.upper()='R5C' 永不匹配）
   → 修复后重跑：**R5C 联合四窗 = OOS2 +319.7 / train +52.0 / valid +83.0 / 长窗 +955.2
   （DD25.5，vs 纯 CN +250.8/DD40.9——弱势切 HK 长窗翻近 4 倍且回撤更低）**

验收：后端 3548 passed + 前端 770 + ruff/typecheck 干净；live 实测全市场 universe 生效
（CN=5229/HK=501）+ circuitBlocked 返回 ✓

### 2026-08-12 长窗落地 + 回撤熔断定案（收益域重大升级 · 用户拍板）

**扩窗/回填全完成**：daily 2021-01 起（5226 只/629 万行/0 失败/6.1h）+ 指数 2021 起 +
score 全窗口回填 617 万行（1272 天，`--universe full` 全市场口径）。**踩坑**：① 回填把
WAL 堆到 30G 撑爆 Docker 盘（checkpoint 因系统盘满失败）→ 清 docker cache 30.5G +
CHECKPOINT + VACUUM FULL 恢复；② 引擎 `_load_rs_ranks` 查询硬编码 `date(2024,1,1)` 下限
→ 长窗 2021-2023 RS 全空被 fail-closed 拦截（假 0 交易），已修（下限 1998）。

**长窗（2021-08~2026-08，全市场口径）暴露弱市脆弱性**：2021 +119 / 2022 **-166** /
2023 **-691**（胜率 20%）/ 2024 +606 / 2025 +1614 / 2026 +1325；合计 +225%/DD89。TV 小池
口径从未暴露（三窗不覆盖 2023 + 小池天然筛选）。指数动量/EMA20/强度分/高分票数量四个
市场状态过滤器全部验证无效（2025 年同指标下都赚钱）→ **改自适应防御：回撤熔断**。

**回撤熔断定案**：`drawdown_circuit_pct=-25`（近 30 天已实现净盈亏 ≤-25% 且 ≥3 笔 →
暂停新仓；窗口 30 天，45/60 天扫描均劣化；-20 伤 train/-30 长窗 DD 63.5）。
效果：长窗 **2022 转正 +93、2023 减亏 428pt（-691→-263）、DD 89.3→40.9、夏普 2.11→2.65、
总收益 +225→+251**；三窗 OOS2 +112.7/5.22 · train +76.7/3.31 · valid +88.2/8.80（重固化，
基线文件已存）。代价 = 牛市空仓期（2025 收益 -658pt，用户拍板「可以接受特定时间空仓」）。
**仅 CN 线**（HK 未验证）；live 同码镜像：paper_s3 `S3_CIRCUIT_PCT=-25` + `_circuit_blocked()`
（closed 行 closeDate 窗口 + pnlPct 净口径，4 新测试）。三处定案配置同步（run_walk_forward /
reconciliation / rolling_oos）。**universe 去 TV 结论**：全市场算分实测 617 万行/86min
（live 每日 ~5s）→ 计算量完全可承受，TV 池退役（回测与 paper 统一全市场）。

验收：3548 后端 passed + ruff 干净 + 基线重固化 ✓

### 2026-08-12 红绿灯定案：CN 红灯日禁开仓（用户指令 · OPT-093/094）

**背景**：红绿灯仓位启发式（红→0-10% 等）是 8 月初经验值，用户质疑后做回测验证。

**回测**（`scripts/backtest_index_lights.py`，1196 CN + 599 HK 笔按入场日灯分层）：
- **CN 红灯日显著差**：胜率 27% vs 绿/黄 41/42%，中位 -5.5% vs -2.1/-2.5% → 定义正确
- **HK 无区分度**：红/黄/绿中位 -5.0/-2.0/-5.1%，红灯均值 +18% 系右上尾暴利单假象
  → 删除 HK 仓位启发式；严禁按均值反转（过拟合教训）

**反事实 + walk-forward（CN）**：剔除红灯日入场——OOS2 胜率 48→54% 收益不降；
valid 61→79% 收益 +10%；无窗变差。引擎 `light_red_block=True` 三窗：OOS2 +1.0pt /
train 持平 / valid +10.7pt（回撤 11.8→1.5%）→ **定案**。

**落地（回测与 live 同码）**：引擎 `GATE_REASON_INDEX_RED` + live `S3_LIGHT_RED_BLOCK=True`
（CN 红灯日 `build_s3_candidates` 返回 [] → 无推荐、paper 不买入）+ 前端 A 股闸门红标
「红灯日 · 禁开新仓」；HK 不接入。**代价**：CN 红灯日空仓等待（约 24% 入场日），
用户拍板接受。

### 2026-08-11 演进方向定案（验证期）：自动验证 + 自动执行 + 终局实盘

**H1（8 月底前）**：
- [x] **滚动 OOS 自动化**（2026-08-11 ✅）：`scheduler/rolling_oos_job.py`——每月首个周一 08:15
  自动跑最近 90 天窗（CN full gates + HK regime 两套定案配置）→ 落
  `data/backtest_reports/rolling_oos_latest.json` + sync_job_record（亏损/夏普<0/零交易 →
  WARN）。**首跑即抓到真实信号**：2026-05-13~08-11 窗 HK **-8.5%/DD19.5/夏普-3.2**（55 笔）、
  CN 1 笔（Weak+恐慌空仓=正确）——近期 HK 执行环境劣化，周报决策 Agent 消费该文件
- [x] **C4 半程对照**（2026-08-11 ✅）：recon 快照加收益偏差三列（aligned_return_diff_pct /
  bt_return_median_pct / paper_return_median_pct，Alembic 0027 + CREATE_SQL 同步）——
  aligned 逐票 btReturnPct/paperReturnPct/returnDiffPct（symbol→ts_code 解析修 daily 查询）；
  persist detail 带 aligned 项；前端 BacktestReconCard 显示「偏差 ±Npt」（|diff|≥2pt ⚠）。
  **首跑实证**：8-10 HK 偏差中位 **-5.5pt**（paper 8-10 入场 vs 回测 8-05 = 8-05~07 cron 缺口
  的收益代价被量化）。另修 5 个日期敏感/隔离测试（adj_factor/daily_service/etf_daily
  uptodate → 相对日期；close_sync 默认窗 → patch get_last_success；user_trades fetch_sell
  → 只断言自己的行）
- [x] **长窗落地（2026-08-12 ✅）**：长窗（2021-08~2026-08 完整周期）已回测并重固化基线
  （+250.8%/DD40.9/夏普2.65，熔断后 +251%/DD40.9；年度 2021 +341/2022 +93/2023 -263/2024 +606/
  2025 +956/2026 +1325）→ strategy-params §1/§3 已记录，回撤熔断定案，见上「长窗落地 + 回撤
  熔断定案」节

**H2（8-12 提前完成 ✅）**：决策 Agent 自动驾驶复盘——周报 cron（周一 07:40）+「下周行动计划」LLM 自动产出（OPT-083，前端确认+重新生成）；剩余：
盘中极端警报（单票 -8%，实时报价链已有）；webhook 事件订阅（§14 #3）

**H3（验证期解除后）**：L4 实盘研究（C4 ≥20 笔平仓后）+ 实盘执行自动化（券商 API）

**明确不做**：美股/加拿大 · MCP server · 新策略机制（§19 封闭）· 付费 API 商业化

### 2026-08-11 数据问题定案：行业资金流历史 = 数据天花板（不补）

东财 121 天上限（实测 lmt 无效）· 同花顺 WAF 403（实测）· 腾讯无接口（实测）· 新浪无历史。
**重构问题**：回测 replay「当时的系统」——OOS2 时 live 本无行业闸门 → regime-only 是诚实结果；
真正缺的是「闸门贡献验证」，免费证据链已齐（A3 valid +32.6pt / 长窗对照 / C4 仲裁）。
500 元 tushare 积分唯一增量 = OOS2 反事实验证（且聚合口径有拼接断层）——**待长窗结果拍板**；
2025-12-15 前缺口不补，cron 快照继续存（未来滚动 OOS 自然有数据）。

### 2026-08-11 扩窗 2021-01 启动 + 资金流回拉证伪（回测必须真实）

**A 股扩窗（P2-5 激活 · 后台执行中）**：腾讯 qfq 链今日全通（cn_reseed_qfq_tx.py 已验证 5224 只
3.4M 行零失败）→ `--since 2021-01-01` 重灌扩窗（PID 16794，预计 ~2h）+ 新脚本
`index_hist_extend.py` 补齐 5 个 regime 指数 2021-01 起（6800 行，2023 衔接数值一致 ✓）→
明早 score 回填 2021-08-01 起 → **长窗回测（2021-08~2026-08 完整周期：2021 顶/2022 熊/2023 弱）**：
验证 S-3 跨周期稳健性（回测真实性的最大背书）+ 若稳健则收益执行信心直接增强。
已知局限（记录）：universe 幸存者偏差随窗延长放大（当下 683 票回放历史，同三窗口径）。

**资金流回拉 todo 证伪（关闭）**：实测东财 `push2his` 行业资金流 daykline **仅保留 121 个
交易日（2026-02-09 起）**——lmt=0/5000 均同（此前回拉脚本 37270 行 2026-02-06 起 = 接口上限，
非节点问题）。OOS2/train 前半的行业闸门数据**物理不可得**（tushare moneyflow_ind 需 2000 积分，
已拍板不买）→ 「三窗完整复核」不可行，改记录式结论：**OOS2=regime-only（+134.2%）· valid=full
gates（+111.9%）**——引擎在数据缺失日 fail-open 回放 live 当时能力（backtest_engine.py:710），
语义诚实；「只做主线严格模式」live 已 full gates 生效，历史窗无法回放 = 数据天花板，接受。
「ClashX 节点恢复后回拉」前提作废——恢复与否都拉不到更早。

### 2026-08-11 paper 仓位 5% → 10%（与回测同口径 · 用户拍板）

`service/paper_s3.py S3_POSITION_PCT 0.05 → 0.10`（注释含决策理由）；名义上限 5%×20=100% →
10%×20=200% 同回测；金字塔加仓 sleeve 同步（10%×0.5=5%）；**paper 实绩期望 = 回测数字本身**
（此前 ≈ 一半）。纯杠杆旋钮（夏普恒定，strategy-params §1 已论证）→ 无需三窗重验；
已持仓 sleeve 5% 入库不回溯，新开仓生效。2 个 sleeve 断言测试同步（0.025→0.05 / 0.02→0.04）。
另：发现 `pnpm dev` 曾顶掉 launchd 托管的 uvicorn（--reload 复活）→ 已重新 load 并确认 PPID=1。

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

### 2026-08-11 对账自动化 + 两个 paper 系统级 bug 修复（T5 观察暴露）

**对账自动化收尾**：weekly review 增加「回测 vs Paper 对账」段（recon 快照进周报 markdown，
决策 Agent 直接消费）；`GET /api/backtest/recon/latest` ✓；**前端 `BacktestReconCard`**
（watchlist 页体检卡片下方——最近 4 个对账快照 ✓/⚠ + 缺/多计数，3 新测试 + 760 前端全绿）✓

**T5 观察暴露两个系统性 bug（已修+测试）**：
1. **pool_exit 误杀 S-3 paper 仓**：`run_update` 的 pool_exit（registry 成员检查）对
   S3/S3HK 仓生效——S-3 HK universe（vol top 500）本就不在用户自选 registry →
   8-10 补跑的 20 只 S3HK 全被 misfire 补跑的 update cron 平掉。修复：`exclude_pool_exit`
   参数（S3/S3HK source 豁免 pool_exit，v0 手动仓行为不变）
2. **trailing peak 用了入场前历史高点**：highs 计算未按 entry_date 过滤（62 天 lookback 内
   的最高价当 peak）→ 入场当天就可能"回撤达标"被平（HK:01818 立平实证）。修复：peak 只取
   entry_date 之后（含）的 high
3. 附带确认：**uvicorn --reload 反复重启 + misfire_grace 12h = 错过的 cron 疯狂补跑**
   （08-10 22:00~23:40 每 2-3 分钟一轮 misfire 记录）——补跑本身没问题，但 scheduler 宿主
   不稳是 T5 的根因背景；**今天 17:42/17:45 是最终验证**（修完 pool_exit/trailing 后
   update 应保持 8-10 的 20 只 open）
- 8-10 那批 S3HK 已重插修复后复跑验证：20/20 open、0 closed ✓；3 新回归测试 + 39 paper
  trading 测试 ✓；全量 3517 passed（5 日期敏感 + 1 playwright 环境缺失，均 pre-existing）

### 2026-08-11 对账自动化：矫正操作成为每周常态

**闭环落地**（上轮手动对账 → 本轮自动化）：
- `service/reconciliation.py`：`reconcile_day(day, window, end_date)`（回测每日快照 vs paper
  实持 → aligned/missing/extra + 入场日偏移 + 明细）+ `run_and_persist`（幂等落库）
- `db/reconciliation.py` + Alembic 0026 `backtest_paper_recon`（recon_date+market 唯一；
  **window 是 PG 保留字，手写 SQL 需 `"window"` 引号**——sed 曾误伤参数行，已修）
- `scheduler/backtest_recon_job.py`：**周一 07:30** 自动对账上周五（valid 窗 CN+HK 重放，
  ~3 分钟）→ sync_job_record
- `GET /api/backtest/recon/latest`：决策 Agent/周报拉取最近快照
- 首次快照：8-07 HK missing=19（8-05~8-07 cron 缺失的历史断层量化入档）
- 测试：5 新 recon 测试（对齐/偏移/extra/HK 块/persist）+ 110 相关 ✓；36 scheduler jobs ✓

**下一步**：前端体检卡片展示 recon 摘要 + 周报接入（决策 Agent 消费）；今天 17:42 T5
关键验证（allocation 已落库 CN=0/HK=1——CN 应全 allocation-zero 跳过）

### 2026-08-11 回测矫正真实操作的闭环（用户核心诉求：回测必须可复制到真实世界）

**方法论（用户拍板）**：分配规则只用红绿灯（as-of 可观测）——`service/allocation.py` 同码函数
（`weights_from_regimes`/`resolve_weights`，R5c：CN 可投→100% CN、仅 HK→100% HK、双弱→0/0），
回测脚本与 live 共用同一份代码；速率/强度在双强时不可跨市场公平比（HK 绝对波动大），取消 R5b。

**三层闭环**：
1. **引擎每日持仓快照**：`BacktestRun.positions_by_day`（每交易日收盘后"应持有"清单——
   symbol/entry_date/score/position_pct；62 引擎测试含新快照测试 ✓）
2. **同码分配**：allocation.py + run_walk_forward_dual R5 分支改为调 `weights_from_regimes`
3. **对账工具**：`scripts/compare_backtest_paper.py [--date D] [--end E]`——某日回测应持有
   vs paper 实持 → 一致/缺票/多票 + 入场日对齐检查；每周一跑上周五喂决策 Agent

**首次对账实证（2026-08-10，HK）**：回测应持有 20 只 vs paper 实持 20 只但**集合仅 5 只对齐**——
根因：8-05~8-07 S-3 intake cron 未跑（sync_job_record 无记录）→ 8-05 该入 18 只缺席 →
8-10 补跑买入的 15 只是"8-10 当天达标"的漂移集合。**paper 入场时点= cron 完整性**——这是
"回测复制不到真实"的第一份量化证据，对账工具每周暴露此类漂移。另：CN 侧 3 只 ALPHA/TV
来源持仓非 S-3 口径（回测 0 正确）。

**连带发现**：① 8-10 17:42 paper_s3 cron 未跑（→ T5 观察升级为"今天 17:42 验证，再丢即去
--reload"）；② 08-10 06:34 运行中 uvicorn hk_basic 报 TU_SHARE_API_KEY 空（--reload 换进程
一次性，重启后 2782 更新成功 ✓）；③ 对账窗口需 `--end` 支持滚动（valid 只到 08-07）。

### 2026-08-11 跨市场资金协调 T1/T2/T3：强度分 + 联合净值引擎 + 动态替代 R5c 胜出

**T2 regime 强度分（CN/HK 同构 [0,100]）**：`regime_strength_score()`——三子分全指数日线
（greens 绿灯占比 0-30 · momentum 20 日动量 0-40 · structure 均线投票 0-30），as-of 安全、
HK 无广度也同构；实测 CN Weak 28.4（空仓 ✓）/ HK Strong 100（进攻 ✓）；体检卡片加 strength 徽章。
强度分不作闸门（regime 仍是闸）——只用于相对分配。

**T1 联合净值引擎**：`scripts/run_walk_forward_dual.py`——复用单市场 simulate，trades 重建
**资金复利日净值**（逐日 mark-to-market × position_pct，平仓日扣成本，union 日历 ffill），
周度权重联合 NAV。**关键修正（第二轮）**：① nav 复利单位 bug；② 逐日重复累加入场涨幅→改每日增量；
③ **净值加权在权重切换时丢失累积收益**（切换=资金池从另一市场 1 元重走）→ 改**收益率加权资金池复利**。

**T3 初测两轮**：第一轮 R1~R4 固定权重 + R2 追强证伪（净值加权口径，R1 看似最优）——被
用户直觉质疑后修正口径：**R5 动态替代（regime 档驱动，Weak 强制清零）三窗一致胜出**，
R5c（CN 可投→100% CN，仅 HK 可投→100% HK）OOS2/train/valid = +492/+316/+113 vs R1
+283/+107/+76；过去一年 +619%/DD20.4/夏普3.53 vs 纯 CN +490%/31.5/3.03（回撤砍 11pt）。
**实证**：CN Weak 156/257 天（61% 空仓率）但 HK 可投仅 33%——互补性被高估，替代价值在
错位时段 + 双强期 CN 优先。R5c 入候选（验证期纪律不上 live）；R5b 双强速率比较需
风险调整动量（绝对动量 HK 天然波动大不可比）。细节 → `docs/modules/strategy-params.md` §6。

验收：后端 8 新强度测试 + 116 相关测试 ✓ · ruff ✓ · 前端 757 passed + typecheck ✓

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
| §4 API 开放 | ✅ /v1/* 整圈（OPT-045~051）+ cookbook；✅ 应用内通知中心（OPT-082）；✅ 外部 webhook P1（OPT-090） |
| §5 工程/部署 | ✅ Docker 一键 + 备份迁移 + 稳定性审计 5 修（2026-08-09）；Tunnel 端到端待验证 |
| §6 数据源 | ✅ TV Scanner API 唯一池子；[ ] 付费 API 矩阵（§12 #9） |
| §7 新闻/研报 | ✅ News Substrate 2.0 三轨 + TIP-012 研报通道 |
| §8 回测 | ✅ 引擎 v1.5 + S-3 + C1 工具 + 长窗 2021-08 起 + 回撤熔断 + BacktestPage 结论页；[ ] C4 paper 对照（等数据） |
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
- [x] **体检区信息层：α 事件 + 行业资金流 叠加 S-3 体检（2026-08-12 完成 · P1+P2 全落地 OPT-085）**
      C4 验证铺垫已就绪：开仓 signal_snapshot 落库（alembic 0029）——≥20 笔平仓后做
      「行业资金领先/α事件标签 vs 收益」对照；正向性三纪律：①定位风险提示层 ②不进门槛 ③验证后转参数
      目标：回测底层不动（score/gate/止损=source of truth），叠加两个**正交信息层**帮助判断
      ——回测量价回答"该不该持有/买入"，事件层回答"为什么现在有波动风险/催化"，
      资金层回答"钱在不在往这个行业走"。展示只提示、不设门槛，不改任何回测参数。
      结合点（S-3 持仓体检 · A 股/港股并行 卡片内嵌）：
      1. **持仓 × α 事件**（个股级）：alpha_radar_trends 按 cn_symbols / mapping_confidence
         ≥0.7 匹配持仓 → 行内 `📰 事件：<trend>（催化A/风险 · 映射0.85 · 2天前）`；
         risk_status 风险类标 ⚠
      2. **持仓 × 行业资金流**（行业级）：持仓 → SW L1 行业（industry_taxonomy /
         hk_industry）→ 5 日主力净流入（flow5d / dailyRankings）→ `🧭 半导体 5日+12.3亿
         （第3/31）`；持续流出标 ⚠（如 ETF:513180 恒生科技 5 日流出）
      3. **候选 × 事件+资金**：候选行附加 `📰 催化（XX）· 🧭 行业流入+3.1亿` 或 `🧭 流出 ⚠`
      4. **头部信号汇总**：`3 持仓无事件冲突 · HK:2099 行业资金领先 · ETF:513180 行业流出 ⚠`
      路径：P1 = 持仓×资金流（CN 映射已就绪）+ 持仓×α事件；P2 = 候选附加行 + 头部汇总；
      数据时效标识（资金流盘后 17:35 日频 / α ingest 即用）随卡片展示；
      延伸（不阻塞）：C4 paper 实绩对照后可回测验证"行业资金领先是否提升 S-3 胜率"
- ✅ **user_trades 闭环打通（2026-08-09）**：SELL 记录曾被后端 400 硬卡（缺成本/入场日）→ 已修
  （校验放松 + 可选成本补填 + 无条件记录）；**使用提示：卖出用行内「卖出」按钮**（期望值看板
  TradeStatsPanel 自动累计；「纪律+真实数据验证」路线的数据管道）


## 4. API 开放与外部 AI 打通（优先级 2）

- ✅ **OpenAI 兼容 /v1/* 整圈（OPT-045/046/047）**：8 endpoint（market/watchlist/journal/explain/version/
  schema/errors/changelog）+ 人类可读文档 → [`archive/2026-08-01-opt-045-v1-api-surface.md`](./archive/2026-08-01-opt-045-v1-api-surface.md)
- ✅ **API Key 配额 + OpenAPI（OPT-051）**：多 Key + 三窗口滑动配额 + /v1/quota + Swagger/ReDoc
  → [`archive/2026-08-01-opt-051-api-key-quota-openapi.md`](./archive/2026-08-01-opt-051-api-key-quota-openapi.md)
- ❌ **MCP server**：cancelled 2026-08-04（自写 agent 已 100% 覆盖；后续要启用按原描述单独起 OPT）
- [x] **决策/告警 webhook**（AI agent 订阅 Karios 事件）：✅ P1+P2 全落地 2026-08-12（OPT-090/091；cookbook §9）
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
- [x] **ai@5.0.116 → 5.0.230 升级**（2026-08-11 ✅）：5.x stable 最新（不跨大版本）；
  144 测试 ✓ + typecheck ✓ + 已 touch reload 加载（tsx watch 不因 node_modules 变化重启）；
  **工具循环 bug 验证仍在**（probe：Gemini + echo tool → finish='tool-calls'、textLen=0）——
  5.0.230 未修复，todo「SDK 修复后恢复 tools」假设不成立 → **decision 维持预取上下文方案**（tools 不再恢复）

- [x] **P2-6 HK amount NULL 澄清（2026-08-11 ✅ 误判关闭）**：实测 07-20~08-07 每天 ~122 只
  amount NULL——**99.3% 是停牌/无成交票**（vol=0、close 恒值，如 00007 腾讯数据止于 2024-03），
  amount 本就无意义；真实缺口仅 0-1 只/天（如 08603 源间不一致：akshare 写 vol 无 amount、
  腾讯无当日数据）——数量级可忽略，**不回补**；8-10 起增量同步 amount 全有 ✓（仅当
  vol>0 且 amount NULL 时才需要关注，可入 staleness 监控条件）
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

- [x] **T1 联合回测引擎**（2026-08-11 ✅）：`scripts/run_walk_forward_dual.py`——单市场
  simulate 复用 + trade 级资金复利日净值重建（含成本）+ union 日历 ffill + 周度权重联合 NAV
  → 三窗 × 4 规则分配对比（口径：联合行与单市场资金视角可比）
- [x] **T2 regime 强度分**（2026-08-11 ✅）：`regime_strength_score(market, as_of_date)`——
  CN/HK 同构三子分（greens 0-30 + momentum 0-40 + structure 0-30，全指数日线、as-of 安全）；
  实测 CN 28.4/Weak · HK 100/Strong（与现况一致）；体检卡片加 strength 徽章（/v1/agent/portfolio-health）
- [x] **T3 资金分配规则初测**（2026-08-11 ✅ 修正后结论：**动态替代 R5c 胜出**）：
  初始 R1~R4 固定权重全部低估联合（**净值加权 bug**：切换时丢累积收益→改收益率加权资金池复利）；
  R5 系列（regime 档驱动）：**R5c（CN 可投就 100% CN，仅 HK 可投才切 HK）= 三窗一致胜 R1**
  （OOS2 +492 vs 283 · train +316 vs 107/夏普 4.79 · valid +113 vs 76）；过去一年 +619%/DD20.4 vs 纯 CN
  +490%/31.5 —— 用户直觉验证：A 强全 A、B 强全 B、双强走 A、下跌不投；但"互补性"被高估
  （CN Weak 期 HK 可投仅 33%）；双强速率比较（R5b）需风险调整动量（绝对动量 HK 天然 2-3× 波动）→ 细节
  strategy-params.md §6；R5c 入候选（§19 验证期纪律，不立即上 live）；T4 paper 池化按 R5c 待拍板
- [x] **T4 paper 资金池化**（2026-08-11 ✅ 用户拍板）：R5c 周度决议落库 `allocation_weights`
  （Alembic 0025 + `db/allocation.py` 幂等，周一 17:45 `allocation_decide_job` 决议，intake 兜底
  即时决议）；paper S-3 intake sleeve = 5% × 当周权重，**权重 0 市场不开新仓**
  （allocation-zero skip；已持仓退出管理照常走 update cron）；金字塔加仓 sleeve 同步缩放；
  回测与 live 共用 `service/allocation.py` 同码（run_walk_forward_dual R5 分支已改调同函数）。
  验证：本周（8-10 周）决议 CN=Weak/HK=Strong → w=0/1（当前状态正确）；21 paper_s3 测试 ✓
- [x] **T5 paper_s3 HK 每日首跑观察（2026-08-12 ✅ 关闭）**：8-10 17:42 cron 曾未跑（--reload
  进程漂移），手动补跑 + 去 --reload + launchd 托管后，**8-11 三连全绿**（17:31 automation ✓
  · 17:42 intake CN+HK ✓ · 17:45 update ✓，sync_job_record 实证），20 只 S3HK 保持 open；
  8-12 继续正常。根因已根治（uvicorn launchd 托管 + paper 链 watchdog 18:05 自检补跑）
- [x] **T6 HK 实时报价链港股验证**（2026-08-10 ✅）：新浪 hq.sinajs.cn 主链对 HK 标的实测通过
  （00700/02899/01787 当天 16:04 价）——HK 盘中决策/止损刷新链路 OK

### 数据源 / 浏览器替代（原条目）

- ✅ **TV 全功能下线（2026-08-12）**：universe 全市场化（5226 只，每日算分 ~5s）后 TV 无
  核心消费方 → 代码/UI/路由/cron 全部剥离（历史数据保留只读）；`execution_source='TV'`
  归因保留；详见上「TV screener 全功能下线」节
- [ ] **[P1] 付费 API 矩阵**（§12 #9）：Tushare/聚宽/iFinD/Wind 对比 → `archive/YYYY-MM-datasource-matrix.md`
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
- [ ] **[P1] paper 实绩对照（C4）**：≥20 笔平仓后，回测结论 vs paper 真实表现逐条核对——**框架已搭**（OPT-087：`scripts/paper_vs_backtest_report.py` 可跑；已揪出并修复 trailing 口径漂移）；现 2 笔样本，等积累
- [x] **[P1] 环境×买入风格适配实验（TIP-014，2026-08-14 完成 ✅）**：用户真实执行节奏 = 14:00-15:00 随机时刻按信号买入；目标是用回测找出「市场环境 → 买入风格」规律，用规律指导交易系统演进。**成果**：① neutral_block（弱/中性日禁开仓，valid +10.7pt，DD 12.1→2.7，胜率 60.8→78.2）；② entry_style auto（主升追强 RS0.7 / 电风扇低吸 -3%，valid +4.7pt，电风扇日 avg +12.8→+17.4）；③ 板块画像（电子/有色/机械跨窗轮动，auto 不绑定板块，主线闸门动态 Top3 独立）；④ 长窗置信度验证（2021-08~2026-08 五年零劣化，2026 +96.3pt）；⑤ HK 线实验结论=不适用（仅 2 个指数红绿灯信息不足，维持 score）；**⑥ 情绪历史回填（方案 A，2026-08-14 ✅）**：`backfill_sentiment_history.py` 零成本重算 2024-08 起情绪（tushare daily 是唯一输入）+ 补 trade_calendar 2024-01 起；**⑦ E2 panic_cooldown 3→2（2026-08-14 ✅ 固化）**：回填暴露弱市年 3 天冷却锁死 OOS2（288964 次拦截）→ 2 天三窗 OOS2 92.6(+8.2) · train 103.1(+22) · valid 持平 · 长窗 270.1 vs 235.4（+34.7pt, DD 45.1→33.0）+ 蒙特卡洛稳健（53.4% 分位, 95% 下界 ≥+99.8%）；E1（大环境条件化）实验证明不需要已删除。全部已固化进 S3_CONFIG（三处）+ live paper 同步。详见 trading-improvement-checklist TIP-014 + designs/data-gap-backfill-2026-08.md
- [ ] **[P2] 板块特点画像（用户提出，TIP-014 延伸）**：不同板块在不同环境下的收益特征分析——主升买什么板块、电风扇买什么板块、防御期买什么板块；**部分完成**：`scripts/tip014_industry_profile.py` 已产出跨窗画像（OOS2 工程机械/汽车零部件、train 有色/通信、valid 电子）；待做：画像→可执行规则（如主线行业白名单动态化）
- [x] **BacktestPage 重写（2026-08-12 ✅）**：改为「S-3 回测结论展示页」——定案基线
  （CN/HK 三窗 + 长窗 2021-08 起 + 参数徽章 + 年度明细）+ 滚动 OOS（warning 红标）+ 回测
  vs Paper 对账；原参数敏感度工具收进折叠「高级」区（默认收起）；新增后端
  `GET /api/backtest/overview`（读固化基线 JSON + 滚动 OOS + 长窗常量）
- [ ] **[P2] 回测探索方向池（2026-08-14 立 · 逐个验证价值后再决定是否做）**：
  - **D1 电风扇日细分（主线在 vs 无主线）**：**❌ 数据不足（2026-08-14）**：valid 全部 18 笔 fan 日都属于"无强主线"（top3 平均分<70），"有主线但轮动快"的子集零样本无法对比；顺带发现 dip 组仅 1 笔 +35.8%（样本太少不定论），与 TIP-014 电风扇样本不足待办合并
  - **D2 环境感知持有期（卖出侧空白）**：**✅ 已固化（2026-08-14）**：`max_hold_env_shorten=45`——uptrend 入场 45 天强制平仓；全局缩短拒收（hold45 OOS2 -13.5）→ 环境感知版 valid +11.4pt（104.4→115.8）、长窗 +11.4pt（279.8→291.2）、OOS2/train 持平；扫描 30/-32.3、45/+11.4、50/+0.3、55/持平 → 45 峰值；S3_CONFIG 三处同步 + 基线重固化 + 2 测试
  - **D3 环境感知仓位（卖出侧空白→仓位旋钮）**：**✅ 已固化（2026-08-15）**：`env_position_scale="uptrend:1.25,fan:0.75"`（v4）——按入场日环境缩放仓位（uptrend 1.25× / fan 0.75×，其余 1.0）；v1（1.2/0.8）与 v4 均通过三窗铁律，v4 最优：OOS2 +24.6 / train +19.5 / valid +26.4，夏普两升一平、Calmar OOS2 7.3→10.0 / train 8.3→13.1，长窗 270.1→333.9（+64pt）；v3（仅 fan 减仓）valid 微降拒收；机制=主升日入场质量最高（与 D2 互证）放大下注；S3_CONFIG 三处同步 + live（paper_s3 `_env_position_scale_for`）+ 基线重固化 + 前端展示 envPositionScale
  - **D4 score_confirm_days（连续 N 天分数达标才买）**：~~30 分钟出结果~~ **❌ 已排除（2026-08-14）**：confirm=1/2 一致拒收（valid -19.3pt）——auto 追强本就要求强势票，再加连续确认=双重收紧，错失 valid 主升段强票
  - **D5 行业集中度上限 max_per_industry**：~~防主线切换全仓风险~~ **❌ 已排除（2026-08-14）**：上限 8 → valid -47.1pt、12 → valid -38.6pt 一致拒收——**主线行业集中持仓就是 alpha 本身**（valid 电子 48/55 笔），限制=砍收益；与主线白名单机制配合，集中是主动选择非风险
  - **D6 利润护城河 profit_trail 复核（A6 重试 · 低优先级）**：**❌ 已排除（2026-08-15）**：在当前基线（D2/D3/E2 固化后）重试 6 变体（t10-4/t10-6/t15-6/t15-8/t20-6）全部拒收——唯一 t10-6 OOS2 +5.1 但 train -21.2 / valid -8.5（违反三窗铁律）；盈利后收紧回撤 = 截断右尾利润腿（Chandelier -8% 已是最优平衡）；与 V7.0-03 同一根因，确认维持关闭（引擎能力保留）
  - **D7 真实分钟线验证尾盘执行**：**🔄 部分落地（2026-08-14）**：东财 push2his 被 IP 风控 → 用户提示"不带 proxy"破案：**macOS urllib 经系统代理（ClashX 127.0.0.1:7890）走代理节点 IP，被风控的是代理 IP，本地宽带干净**——`em_get_json` proxies={"http":None} 直连即通（rc=102→正常返回）；`backfill_em_history` 温柔回填（1.5s/请求、每 30 停 10s、5 天/窗口、幂等）已实测补齐 02099 全部 8 月 5m（66 根/天，末根收盘与日线一致）；**腾讯当日 1 分钟线**（hkMinute/minute 接口）16:35 job 自动积累；用户定性分钟线现阶段价值低 → 历史回填暂停，等积累够再验证 last_hour_low 近似
  - **D8 港股情绪数据**（涨跌家数等）：HK 环境感知缺失拼图，需新数据源
  - 判定标准：三窗 0 劣化 + 单窗收益提升（todo §19 铁律）；单一窗好看 = 过拟合拒收
  - **实验全记录（成功+失败）→ [`backtests/`](./backtests/README.md) 文件夹**（tip014 / d-pool / defensive / legacy / planned 五份）
  - **信号池 P1-P8（2026-08-15 立 · 用户 brainstorm 技术形态候选）**：海龟突破/放量突破/200日均线/均线斜率/双均线金叉/三线多头/短线反转/长阴反转——逐个验证「先有效 → 再查与 RS 重合 → 长窗+三窗+蒙特卡洛 → 有效才固化」；计划与判定标准见 `backtests/experiments-planned.md`：
    - **P1 海龟唐奇安突破**：**❌ 已拒收（2026-08-15）**——OOS2 单窗暴增 +31pt（DD 5.7）但 train -37.5 / valid -8 / 长窗灾难（DD 45→90.8，收益 -65pt）→ **典型单窗过拟合**；突破 gate 在结构牛捕获延续、震荡段追高即套，与 A2「绝对量因子无增量」同向；引擎 `breakout_days` 保留默认 0
    - **P2 放量突破**：**❌ 已拒收（2026-08-15）**——V2.0 OOS2 +76pt（DD 6.7）但 train -97 / valid -124 / 长窗 -215pt + DD 121 → 单窗过拟合；A 股「放量=高潮/出货」，放量日追入恰是情绪顶点，valid 胜率 81.8%→43.6%；V1.5 温和版也全窗劣化；引擎 `volume_breakout_mult` 保留默认 0
    - **P4 均线斜率**：**❌ 已拒收（2026-08-15）**——三窗全劣化（valid -59~-108）+ 长窗灾难（DD 45→83-92）——干净的全面失败；**MA20 斜率与 RS 高度共线**（强势票 MA20 天然上扬），gate 只是重复过滤；A2 结论从"均线多头状态"扩展至"均线加速度"；引擎 `ma_slope_min_pct` 保留默认 0
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
| 2026-08-12 | **S-3 信息层 + C4 对照框架（OPT-085~089）**：体检区 α 事件/行业资金流叠加（P1+P2 + signal_snapshot）；防守向回测攻击 23 项零采纳（防守空间=准入质量+纪律）；C4 paper-vs-backtest 逐笔对照框架（修 trailing 口径 bug：live 盘中最高 vs 回测收盘）；体检卡闸门红标；BacktestPage C4 对照卡 + 网格交互 | [`archive/2026-08-12-opt-085-089-s3-info-layers.md`](./archive/2026-08-12-opt-085-089-s3-info-layers.md)（后端 3340 + 前端 742 全绿） |
| 2026-08-12 | **webhook 推送 + 一键启动 + 红绿灯定案（OPT-090~094）**：webhook 三层 P1+P2 全事件源（E1~E7，HMAC 签名/退避/限频）+ 前端订阅页 + cookbook §9；`npm run dev` 一键三服务（去 --reload）；红绿灯回测（CN 红灯日显著差→保留+禁开定案；HK 无区分→删除启发式仓位）；alembic 0030 | [`archive/2026-08-12-opt-090-094-webhook-lights.md`](./archive/2026-08-12-opt-090-094-webhook-lights.md)（后端 3349 + 前端 746 全绿） |
| 2026-08-12 | **全系统健壮性审查（OPT-074）**：三路扫描 60+ 缺陷全修——前端默认 30s 超时 + SSE/流式 5min 兜底 + 卸载 abort（原 AI 挂起永久锁死 UI）；Rust sidecar 启动移后台线程 + Cmd+Q 清理（原孤儿进程占端口）；ai-service fetch 层 10min 硬顶 + 流 cancel 转发 + 去 process.env 全局污染；后端 RSS 20s 超时 / close_sync 重试 / yfinance 60s 超时 / statement_timeout / 10 处静默 pass 补日志 / 7 job print→logger；修 alembic fileConfig 禁用全库 logger（污染 pytest caplog）| OPT-074 见 optimization-checklist（后端 3284 + 前端 728 + ai 142 + cargo/tsc/ruff 全绿） |
| 2026-08-12 | **TV 剥离第二轮**：execution-source TV 源改恒空集（'TV' 归因保留）· 删 queries/screener + api/tvCapture + screenerExport · dashboard-export 去 screener 段/常量 · 前端零 TV 网络调用（ChatPanel 历史渲染保留 catch 兜底）· 728 tests 全绿 | 见 todo §6 剥离清单 |
| 2026-08-11 | **smoke 测试误杀 S3HK paper 仓（重大事故修复）**：`test_postclose_smoke.py` 未隔离真实 DB——`run_update` 全量扫描 + 价格 mock 对所有 HK 返回 100 → 20 只 S3HK 假平仓（+2 只 TV 08-09 被误杀）；双层修复（mock 只服务自身 symbol + `get_open_paper_trades` 只返回自己的行）+ 恢复 22 行 open + uvicorn 去 `--reload` 重启（misfire 循环终止） | 见上「当前方向」事故段（todo 就地记录，细节同文） |
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
| 12 | **BacktestPage 重写**（基于 paper 数据） | §8 回测 | ✅ 2026-08-12（OPT-089） | — | 参数敏感度工具，不作发布依据 |
| 14 | **美股 symbol 闸门** | §7 多市场 | 3-5 天 | 加拿大规划启动 | 远期触发 |
| 15 | **加拿大税务/账户模型** | §7 多市场 | 远期 | — | 远景 |
| 2b | **Tunnel 端到端验证** | §4 工程 | ✅ 2026-08-14（OPT-116） | — | 远程访问前提 |
| 2c | **[P1] 移动端适配**（2026-08-14 用户反馈）| §4 工程 | ✅ v1 2026-08-14（OPT-117）：MobileShell 三 tab（执行/持仓/对账）· 手机优先独立视图；后续按需：买入/卖出操作按钮、tab 记忆 |

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
- [x] **全局通知中心（2026-08-12 ✅ 应用内版先行）**：任何页面铃铛+toast 提醒，点击跳 watchlist 详情（接近止损/EXIT/recon 缺票/cron 失败/OOS 预警 + 本地买入提醒）——OPT-082
- [x] **外部 webhook 推送（2026-08-12 ✅ P1+P2 全落地）**：订阅/事件/投递三层 + E1~E7 全部事件源（job 失败 / paper 链断 / 接近止损 / OOS 预警 / 对账缺票 / 候选新增 / 盘中 -8%）+ 前端订阅管理页 + cookbook §9——OPT-090 + OPT-091（设计稿已拍板）
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
| D1 强度分数止损选择器 | ❌ 关闭（2026-08-13 实验） | 分箱扫描全拒收（OOS2 全劣化）→ strength 分数无增量，离散 regime 更优 |

**执行顺序**：A1 → B1 → C1（基建）→ A2/A3/A4/B3（信号与组合封闭）→ C4（随数据滚动）。

### §19.1 贴合回测审计（2026-08-09 · 全部闭环 ✅）

> 目标：① 回测严格证明的错误操作必须提醒 ② 贴合差距逐个修 ③ 操作只按回测口径。
> E1-E5（paper 三常量拍板改 + 恐慌冷却 + 隐藏阈值复核）与 G1-G6（S-3 区块绕过 BUY 门槛、
> Diverging banner、低 RS 红标、paper_s3 同码闸门、TrailStop 展示、5% 保守仓位）全部完成——
> 细节见 [`archive/2026-08-08-l4-gate-audit.md`] 与 `modules/strategy-params.md` §1 备注。

### §19.2 市场强度连续分数 → 止损参数（D1 · 2026-08-13 提案 · 用户拍板待定）

> **想法（用户）**：动态构建市场强弱数字（0-100/0-1），用它回测验证"已知函数套进去"是否有效
> （本质像反向传播——用分数调止损参数）。

**关键事实：数字已存在**——`regime_strength_score`（market_regime.py:1034）返回 0-100
连续强度分（成分 = 绿灯占比 + 20 日动量分 + 结构投票分），CN/HK 共用标尺；体检卡头部
today 已显示（如 strength 26.4）。**离散 regime（Strong/Diverging/Weak）就是它的粗离散化**
（全绿=Strong）→ 分数是 regime 的超集，粒度更细。`as_of_date` 参数支持**历史每日回放**
→ 回测窗口 2021-2026 可全量重算，数据可行性成立。

**实验完成（2026-08-13 · 全部拒收 → D1 关闭）**：strength ≥ X 用 ATR 线（X=40/50/60/70，三窗）：

| X | OOS2 | train | valid | 判定 |
|---|------|-------|-------|------|
| 40 | +113.9 (-9.4) | +94.1 (+20.3) | +77.3 (-11.8) | ✗ |
| 50 | +111.8 (-11.5) | +92.1 (+18.3) | +77.7 (-11.4) | ✗ |
| 60 | +109.6 (-13.7) | +93.8 (+20.0) | +88.4 (-0.7) | ✗ OOS2 |
| 70 | +114.2 (-9.1) | +78.8 (+5.0) | +89.6 (+0.5) | ✗ OOS2 |

**结论**：train 全线大胜（宽松档让更多日子用 ATR）；但 **OOS2 全面劣化 -9~-14pt**——
2024-25 弱市年里"strength 分数高但未全绿"的脆弱强势日，ATR 止损依然亏。
**离散 regime（全绿=Strong）比连续分数更优**——它天然过滤了这种脆弱强势。
连续 strength 分数作为止损选择器**无增量**，维持 regime 规则（OPT-105 固化）。
引擎 `atr_stop_strength_min` 保留为实验开关（默认 0 = 关闭），不新增连续拟合代码。

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

### §19.3 Alpha 前向数据收集（2026-08-13 用户拍板 · 唯一新变数 · 为回测积累样本）

> **背景**：alpha（催化雷达）当前只做入场辅助 + 信息层，**不参与退出**（S-3 退出
> 只认价格/时间线）。用户提出"alpha 做退出"的设想——原理（催化兑现/证伪→行情结束→
> 退出）合理但**未验证**。历史回测不可行（alpha 是 2026-08 才上线的数据源，2021-2026.07
> 无信号），用户拍板：**从现在开始前向收集，收集一段时间后用于回测**——这是
> S-3 参数空间封闭后"为数不多的变数"，长期跟踪。

**已有资产（自动积累中）**：
- `alpha_radar_trends`：每日事件流（grade/confidence/riskStatus/createdAt 天然时间序列）✓
- `paper_trades.signal_snapshot`（alembic 0029）：S-3 paper **入场时点**的 α 事件快照 ✓
- `scripts/alpha_guidance_report.py`（OPT-109）：user_trades × α 事件对照统计（当前 4 笔，方向性）

**已落地的收集项（OPT-110 · 2026-08-13）**：
1. ✅ **user_trades 入场+退出快照**：`user_trades.alpha_snapshot` JSONB（alembic 0032）——
   POST /trades 每笔 BUY/ADD/SELL 自动记录 **as-of** α 状态（无前视：仅事件时间
   ∈ [trade_date-14d, trade_date] 计入；`fetch_trends_as_of` + `alpha_snapshot_for`）；
   best-effort（α 层故障不阻断交易记录）
2. ✅ **退出时点快照**：SELL leg 同样落快照——"α 恶化时退出 vs 死扛"反事实的数据源
3. ⏸ **每日持仓 × α 活跃度**（可选）：trends 表在保留期内可回推（createdAt 时间序列）；
   若未来 ops prune 影响，再加每日落库

**使用时机（6-12 个月后 · 样本 ≥20-50 笔时）**：
- 入场：有 α 背书 vs 无 α 的 PnL/胜率对照（初版已跑，OPT-109）
- 兑现：α 事件后 X 天内收益分布（催化兑现规律）
- 退出：α 转弱时退出 vs 死扛 的反事实（**验证通过才允许 alpha 进退出**）
- 验证结论：支持 → 按 S-3 流程（三窗/样本外）评估入参；不支持 → 保持现状，成本仅几张快照

**纪律**：收集只加快照不改信号；alpha 退出在验证前不进入任何退出逻辑（OPT-097 铁律不变）。

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
