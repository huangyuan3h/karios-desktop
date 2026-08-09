# §17/§18 工程加固详情存档（2026-08-09 · todo 精简迁移）

> 2026-08-09 为控制 todo 体积，将 §17（L4 Gate + 覆盖率波 1-13）与 §18（工程稳健性 R1-R7）的
> 完成详情原样迁移至此。状态均为 ✅ done。真值/后续追踪：
> - L4 准入 Gate 计划 → [`2026-08-08-l4-gate-audit.md`](./2026-08-08-l4-gate-audit.md)
> - OPT 系列工程项 → [`../optimization-checklist.md`](../optimization-checklist.md)
> - 覆盖率验收门槛 → `services/data-sync-service/scripts/coverage_gate.py`

---

## 17. L4 准入 Gate：全模块排查与加固（2026-08-07 立）

> **决策**：进入 L4（券商对接/执行闭环）之前，全模块过一遍，消灭 P0/P1 级「影响判断和逻辑」的问题。
> **背景**：2026-08-07 完成 L3-P1~P5 过程中暴露 3 个 live bug + 1 个数据污染事件，证明「测试全绿 ≠ 逻辑正确」（mock shape 与 db 不一致掩盖了数周）。必须系统性排查。
> **详细计划**：[`archive/2026-08-08-l4-gate-audit.md`](./archive/2026-08-08-l4-gate-audit.md)（9 项横切检查 + A/B/C 模块分级 + P0/P1/P2 加固项 + 退出标准）。

### Gate 状态

| 项 | 状态 |
|----|------|
| K1：`decision.py` 读 camelCase 错位（paper 进不了决策快照） | **[x] 2026-08-08**：4 处错位 + exit_attribution 1 处漏网 + 附带发现 `import json` 缺失（extract_pending_actions 从未工作） |
| H1 数据口径审计 | **[x] 2026-08-08**：全量对照表完成（详见 archive/l4-gate-audit §4）；decision.py 覆盖率 43% → 99% |
| H3 测试隔离复查（26 个 requires_postgres 文件） | **[x] 2026-08-08**：7 处污染源修复（含 flaky 根因：UTC/上海跨天窗口）；清 233 测试账户 + 141 测试 session + 48 假 changes；`scripts/db_rows_baseline.py` 验收 OK |
| K4 correlation 簇回归 | **[x] 2026-08-08**：持仓全保护；补 8 条簇规则（电子/元件/PCB/小金属/化学制药等）；correlation.py 57%→95%；遗留：stock_basic CN=0 致行业缺失（B7）+ fail-open 激进语义（入 H5 清单） |
| H2 盘后链路端到端冒烟 | **[x] 2026-08-08**：`test_postclose_smoke.py` 五步链路全绿；**抓到生产 bug**：run_intake side 变量泄漏（最后一条 action 污染所有 insert，解释 paper_trades 长期 1 行）已修 + 回归测试；基线验收零变化 |
| H4 前端决策链边界矩阵 | **[x] 2026-08-08**：13 个边界用例（满仓/负 room/ETF 豁免/correlation 绑定/ratchet）；前端 515 passed + tsc 干净 |
| H5 fail-open 语义清单 | **[x] 2026-08-08**：扫描 11 文件 75 处 except；**修 2 个激进项**（宏观死锁读取失败→fail-closed 锁激活且不缓存；registry 读取失败→不再批量删止损）；2 个高危项记录设计权衡（日内风控 bar 陈旧、breadth panic 依赖 sentiment 兜底）；其余 14 项中低危记录 |
| H6 时区/日历一致性 | **[x] 2026-08-08**：调度全 Asia/Shanghai ✓；**修 `_messages_on` UTC→上海日界**（凌晨消息漏出快照）+ `_holding_days_for` None 崩溃；跨周末/跨月测试；HK 日历差异文档化 |
| H7 数值健壮性扫描 | **[x] 2026-08-08**：评分函数 None 守卫 ×7（返回 0 不崩）+ 测试锁定；扫描全服务层 float/int/除零路径无其他崩溃 |
| H8/H9/H10 契约/调度/安全 | **[x] 2026-08-08 H8**：v1 三端点模型全匹配 + docs/api ✓；删前端 `okBook` 死字段（tsc 干净）。**[x] 2026-08-08 H9**：26 模块 ON CONFLICT + ingest heartbeat 幂等测试锁定；无幂等缺陷。**[x] 2026-08-08 H10**：gitignore/硬编码密钥 0 命中；.env.example 补 `GEMINI_API_KEY`；/v1 鉴权面确认（business/explain 挂 require_api_key、quota 挂 enforce_quota、discovery 无鉴权设计）；新增 `LocalOriginGuardMiddleware` 拒非本机 Origin 写请求（11 测试锁定）；全量 1435 passed 零变化。 |

### 剩余风险处置（2026-08-08 Gate 后 · 用户拍板 ①→②→③ 全做）

| 项 | 状态 |
|----|------|
| ① B7：stock_basic/EM 行业缺口 | **[x] 2026-08-08**：根因 3 层——① missing 正则把 ETF 当 CN 股票（15 开头）→ 增量 sync 永远 0 resolved、表卡死 1630 行；② push2 主域名死亡无 fallback；③ 空批次记 success=True 假绿。修复：market 字段过滤（CN 总数 7389→5543 修正）+ fallback 链 `push2→push2delay→emweb F10`（EM2016 二级）+ 空批次记 failure；**回填 5543/5543=100%**，16 个缺失 symbol 全恢复；baseline 脚本补 2 张表；全量 1440 passed 零变化 |
| ② 数据源健康告警 | **[x] 2026-08-08**：`services/data-sync-service/scripts/data_healthcheck.py` 6 项检查（28 job 健康/daily 新鲜/TV 快照/EM 覆盖/分数新鲜/备份年龄）+ `scripts/install-healthcheck-launchd.sh`（每日 08:30 + 登录 + FAIL 桌面通知）；**上线即抓 4 真实问题**：adj_factor 连续 4 周五失败=17:00 与 daily_sync 并发抢 tushare 配额→**错峰 18:30**；stock_daily_full 6-13 起失败=已重定向 close_sync 降噪处理；etf/news 入观察 |
| ③ 阈值再校准实验 | **[x] 2026-08-08**：18 组合聚焦网格（score 60/70/80 × hold 5/10 × stop -6/-8/-10，2026-06-18~08-07）**全组合仍负**；相对最优 score 70+stop -6（-0.76%，72 笔，win 38.9%）；hold 5/10 无差异；**结论：不调 live 参数**（-0.76% 仍负期望，调参只挖浅 1%）；收益归因在信号供给单一化（97% ALPHA）→ 立为 L4 后业务课题 |
| **观察清单**（healthcheck 暴露，非紧急） | **[w]** `etf_daily_full`：8-01（每月 1 日 19:00 回填）限流失败，9-01 复查；`news_enrich_job`：8-07 20:59 一次失败（enriched 24/49 部分成功），前面多次成功——每天 2 次跑，若复发深挖 |
| **稳定性审计（2026-08-09 · 业务+工程双轨 · 5 修）** | **[x] 全部处理**：① **backup_age 假 FAIL**：healthcheck 36h 阈值 vs 备份 25h 跳过节奏（最长合法间隔 49h）→ 阈值 36→50h；② **cn_industry_post_close_sync 工作日每天失败**（8-04~07）：17:35 太早（东财盘后行业数据 17:30-18:30 才发布）→ **cron 17:35→18:15** + error_message 加三部分诊断（原来只给 "unknown"）；③ **option_iv_daily 161 次失败**（8-06 起）：东财风控（回拉风暴 IP 拉黑）拒 push2 期权接口（502/Empty reply）→ `em_push2_http.em_get_json` 三链加 **EASTMONEY_PROXY + COOKIE**（与 fflow 同配方，**同时修复 etf_fund_flow_em/realtime_quote 同源问题**）；④ **eastmoney_industry_sync 16 次失败**（同根因 push2/emweb unreachable）→ 该模块 urllib 层加代理（_open_url helper）；⑤ **news_enrich 恶化 49/0**：ai-service 离线（环境状态）+ 失败原因从未入库 → `enrich_batch`/`run_enrichment_cycle` 透传 firstError 进 error_message（下次失败可诊断）；**归因非问题**：hk_basic 72 次失败=当时 .env 缺 key（21:44 已补全，重启服务验证）、top_inst suspicious_empty_lhb（8-08 起自愈 91 连成功）、tv_snapshots 27h（周末无盘中快照正常）、docker-nginx-1 重启循环（dify 无关项目）；**测试隔离**：em_push2_http/eastmoney_industry 测试加 _PROXY/_COOKIE 清空 fixture（host .env 有代理时旧 mock 走真网络）；验收：**3500 passed 全绿 + 基线重存（score 回填 367691 行，无服务状态下 save）** |

### 覆盖率波 1（§8 计划 · 目标 BE ≥75% + 11 核心模块 ≥85%）

| 项 | 状态 |
|----|------|
| `scripts/coverage_gate.py` | **[x] 2026-08-08**：读 coverage.json，按核心模块清单（11 个 ≥85%）+ 整体阈值（--overall）fail；豁免清单显式（tv/capture.py 退役路径）；输出 模块×覆盖率×缺口 可直接定位 |
| 11 核心模块 ≥85% | **[x] 2026-08-08**：decision 99.5 / correlation 94.9 / exit_attr 91.5 / weekly_review 90 / trendok 87.3 / research 88.6 / execution_journal 92.6 / execution_source 84.9+ / paper_trading 87.5 / backtest_engine 77.2→85 / watchlist_automation 70.5→85 全达标（补 ~40 测试） |
| **顺带修复 3 个真 bug** | **[x]** ① `get_top_5d_industry_names` 未 strip 行业名（GC 精确匹配失效）→ 加 strip；② `backtest_engine` end_of_window 平仓后未 del positions → `open_at_end` 虚高（迭代中 del 修复 + list() 快照）；③ `_with_retry` 全失败 raise 语义确认（原测试预期错误） |
| 整体覆盖率 | **[x] 2026-08-08：75.0% GATE PASSED**（基线 65.9%，三会话累计 +9.1pp / ~290 新测试，1759 全绿）——第 3 会话覆盖：trendok（alpha-S recovering/risk buy blocks/quote merge/industry flow）、alpha_radar_process（extract/save/batch/pending）、tv_chrome（profile copy/start/stop 全路径）、macro_daily（paging/合约解析/full sync 驱动）、broker（decode/ai-extract/images/条件单删除）、etf_fund_flow 剩余（universe/frame merge/classify）、macro_snapshot_on_demand（metrics/on-demand 系列/enrich） |
| 验收 | **[x] 2026-08-08**：**波 1 整体验收**：后端 1759 passed / 2 skipped 全绿 + 27 张表零变化 + `coverage_gate.py` GATE PASSED（OVERALL 75.0%，11 核心模块最低 87.9% 全 ≥85%，tv/capture.py EXEMPT） |

### 覆盖率波 2（波 1 后续 · 继续推整体覆盖率）

| 项 | 状态 |
|----|------|
| 会话 4（75.0% → 79.7%） | **[x] 2026-08-08**：1978 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 79.7%，-1023 missed）——top_inst_flow 68→95.1%（网络层/retry/tushare/provider 编排 53 测试）、query_routes 46.1→98%（resolve/quotes/全部小 endpoint 27）、option_iv 68.5→97%（sync driver/akshare fallback/paging 27）、db/index_daily 46→88%、db/news 49→97%、db/tv 38→99%、db/index_basic 14→93%、news_enrich 58→88%（_call_llm/enrich_batch/cycle 22）、db/macro_daily 58→91%、db/watchlist_automation 50→96%、mainline 70→98%（metrics 计算层 13） |
| 会话 5（79.7% → 83.2%） | **[x] 2026-08-08**：2229 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 83.2%，-1038 missed）——etf_daily 20→99、db/broker 40→97、alpha_radar_mapping 38→100、market_detail 56→96、close_sync 56→97（trade calendar 全分支/分页/断点续传）、adj_factor 24→100、db/stock_eastmoney_industry 50→97、em_push2_http 61→100、sina_http 46→97、fund_basic 51→99、service/daily 45→96、db/stoploss 34→100、db/research 43→94、db/stock_basic 66→97（fetch_market_stocks 分页+quote 聚合） |
| 会话 6（83.2% → 83.9%） | **[x] 2026-08-08**：2295 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 83.9%，-155 missed）——watchlist_routes 64→90（registry CRUD/backfill/automation 全 endpoint 21）、alpha_radar_routes 51→98（sources/trends/sync/process/remap/risk 21）、sync_routes 67→98（全部 sync 端点 + /sync/jobs 聚合含降级分支 24） |
| 会话 7（83.9% → 84.6%） | **[x] 2026-08-08**：2494 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 84.6%，-141 missed）——trendok 92→99（`_trendok_one` 全分支：exit_now/momentum exhaustion/ETF fallback/sector divergence/T1 sniper/intraday distribution/RS leader/Alpha-S recovering + compute 集成含 registry 持仓/宏观死锁 73）、market_regime 84→96（指数信号全信号分支/realtime merge/HK on-demand/breadth/liquidity/缓存 42）、market_sentiment 89→96（panic 规则/capitulation/FTD/breadth 分页+intraday 并发/risk_mode 全分支/sync 日期解析 84） |
| 会话 8（84.6% → 86.1%） | **[x] 2026-08-08**：2656 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 86.1%，-337 missed）——service/tv 81→98（screener CRUD/template/sqlite 迁移/capture 三轨 dispatch 全分支/job 队列 wait 全状态 72）、db/trade_review 43→99（全 CRUD + fetch 序列 cursor mock 19）、service/morning_brief 37→94（freshness 分档/watchlist boost 分级/分类规则/select 过滤链/brief 生成 25）、api/news_routes 37→100（17 endpoint 全绿）、service/watchlist_momentum_alerts 69→91（breakout/exit/hold 三态 + tranche 阶梯 16）、db/top_inst 62→96（daily/summary upsert + description 列映射查询 13） |
| 会话 9（86.1% → 87.5%） | **[x] 2026-08-08**：2870 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 87.5%，-307 missed）——service/etf_fund_flow 87→99（sync 全流程/skip 分支/spot 估算/tushare 历史 merge/分页回退 52）、service/macro_daily 85→99（分页抓取 380 天/HS 三源 fallback/SGX/INE 解析器/sync 恢复矩阵 36）、service/macro_snapshot_on_demand 81→100（yf/akshare/tushare 三源链全分支/df→metrics 全转换 41）、service/alpha_radar_qa 81→100（五类 penalty 信号/catalyst 批量/stats 聚合/名称歧义 18）、service/dashboard 87→98（summary 全开关矩阵/盘前 clamp/screener 状态机/三 sync 流程 39）、service/industry_fund_flow 78→98（东财数据源/日线 kline/akshare 回退/SW L1 分类过滤/交易日跳过逻辑 28） |
| 会话 10（87.5% → 89.3%） | **[x] 2026-08-08**：3034 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 89.3%，-381 missed）——service/macro_snapshot 72→100（realtime overlay 全分支/PUT IV 卡/backfill pct/警告矩阵 22）、service/alpha_radar_pipeline 76→100（cooldown/rounds 计算/ingest/process/sync 全状态机 26）、service/alpha_radar_symbol_resolve 72→100（CN/HK 双前缀解析/三 lookup 链/hybrid fallback 13）、service/eastmoney_industry 79→100（push2/push2delay/emweb 三源链/增量 sync 全分支 29）、service/hk_daily 66→100（tencent→ak→yf→tushare 四源链/darwin 禁用/full sync 恢复 22）、service/hk_basic 60→100（月度 skip/映射器/全错误路径 11）、api/broker_routes 54→100（账户 CRUD/快照/图片/import/sync/条件单 14）、api/backtest_routes 53→100（run/sensitivity/report/exit-attribution/weekly-review/correlation 13）、api/system_prompts_routes 52→100（9 endpoint 全分支 + HTTPException 透传 8） |
| 会话 11（89.3% → 90.4%） | **[x] 2026-08-08**：3125 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 90.4%，-2101 missed）——api/journal_routes 52→100（CRUD 全分支/404 矩阵）、api/execution_journal_routes 54→100（快照校验矩阵/列表/变化/journal.md 默认日期）、api/industry_flow_routes 44→100（fund-flow/mainline 4 端点成功+错误+默认值）、service/index_daily 40→100（skip/resume/无 key/全流程/空 df/未来日期跳过/失败记录）、service/index_basic 32→100（同上结构）、service/stock_basic 68→100（skip/无 key 记录/空 df/success/异常/状态查询）、db/trade_calendar 57→99（upsert 全转换/查询矩阵/summary）、service/trade_calendar 67→97（分页/空页/默认日期）、service/market_bars 71→92（ETF 分支/ts_code 后缀校验/_lookup_name 三态/force 四源 sync 链） |
| 会话 12（90.4% → 91.8%） | **[x] 2026-08-08**：3222 passed / 2 skipped 全绿 + 27 张表零变化 + GATE PASSED（OVERALL 91.8%，-1797 missed）——scheduler 全目录 26→94%（create_scheduler 注册矩阵 31 job id + trigger 类型/简单 cron job 三分支参数化 ×9/close_sync 3 分支+post/close_catchup 5 分支记录矩阵/daily_sync 4 分支/watchlist_automation 4 分支/decision 三 job/paper_trading 双 job/morning_brief 双触发器 5 分支/news_fetch-enrich 各 3 分支/research_report 3 分支/alpha_radar 三 job+env 解析）；**修复产品缺陷**：create_scheduler 原只注册 tv_screener_capture_am，PM 快照任务从未调度，补注册 JOB_ID_PM |
| 会话 13（FE：queries 层测试） | **[x] 2026-08-08**：desktop-ui 608 tests 全绿（63 文件，1 skipped），All files 69.4%→74.6%，**lib/queries 43.96%→79.92%**、lib/api 69.38%→92.51%——backtest 0→100（run/sensitivity/exit-attribution/correlation 路径+clusterExposure 4 分支）、broker 0→100（keys/fetch/options/hooks/invalidate）、execution-journal 0→100（默认上海日期/refetch 60s/120s/limit）、weekly-review 0→100（enabled/staleTime）、tvCapture 0→100（轮询/失败抛错/超时 fake timers）、decision 6.77→92（fetch 系列/create/update/rename/append/delete/markdown 渲染 6 分支）、sentiment 30→100、alphaRadar 20→92（4 fetch+4 options+hooks+3 mutation+invalidate）、news 23→85、research 24→100；**技术要点**：mock useQuery 捕获 options 直接调 hook（无 jsdom 免渲染）、tvCapture fake timers 先 attach assertion 防 unhandled rejection；note：apps/desktop-ui eslint 坏为预先存在（eslint-plugin-import 解析失败，既有文件同样失败） |
| 剩余大块 | db/industry_fund_flow（67%）、hk_*（hk_daily 66%/hk_basic 60%/hk_daily_tx 81%）、db/paper_trading（80%）、db/decision（84%）、market_bars（71%）、tv/scanner_api（86%）、db/morning_brief（23%）、db/industry_mainline_metrics（22%）、db/journal（17%）、service/etf_fund_flow（87%）、service/macro_daily（85%）、service/macro_snapshot_on_demand（81%）、service/alpha_radar_qa（81%）、service/dashboard（87%）、service/industry_fund_flow（78%）、service/macro_snapshot（72%）、service/alpha_radar_pipeline（76%）、api/broker_routes（54%）、service/alpha_radar_symbol_resolve（72%）、service/eastmoney_industry（79%）、api/backtest_routes（53%）、api/system_prompts_routes（52%） |

## 18. 工程稳健性加固（2026-08-08 立 · 波 2 后工程视角排查）

> **背景**：BE 91.8% + FE lib 74.6% 之后，从工程角度全仓排查发现的非功能性缺口。
> **原则**：与 §17 铁律一致——每个修复 = 测试 + 验收证据；一次一个修复不混 scope。

### 不稳健点清单（按风险排序）

| # | 问题 | 风险等级 | 证据 |
|---|------|---------|------|
| E1 | **全仓 lint 门禁失效**：`eslint-config-next` 解析 `eslint-plugin-import` 失败，本地与 CI 的 `pnpm lint` 都会挂；无 husky/PR 门禁，红线坏了无人拦 | P0（门禁级） | 本地 `npx eslint src/lib/api/client.ts`（既有文件）同样失败，非本次测试引入 |
| E2 | **BE 交易执行链仍低于稳健线**：execution_gate 87.7%（交易闸）、paper_trading 87.9%、db/decision 84%、db/execution_journal 80%、db/paper_trading 79.7%——核心线 85% 达标但异常/边界分支有真实缺口 | P1 | coverage.json 2026-08-08 |
| E3 | **FE UI 层基本裸奔**：无 testing-library/jsdom/E2E；components 仅 2 文件有测试、hooks 2 个；74.6% 只是 src/lib 层数字，表格/表单/SSE 页面交互无保障 | P1 | vitest coverage 配置仅 include src/lib |
| E4 | **契约漂移风险（结构性）**：OPT-009 明言 Python 响应手工对齐 shared Zod；queries 测试只验证路径字符串不验证后端返回形状 | P2 | docs/todo.md §OPT-009 / queries 测试现状 |
| E5 | **调度失败无告警消费**：sync_job_record 落表但无人消费；BackgroundScheduler 单进程内无分布式锁；healthcheck 脚本不检查 job 失败 | P2 | data_healthcheck.py 6 项检查范围 |
| E6 | **BE 小文件低覆盖**：alpha_radar_daily 0%、db/journal 17%、db/morning_brief 23%、db/industry_mainline_metrics 22%、tv/ego_lite 28% | P3 | coverage.json |
| E7 | **ai-service 无覆盖率 gate**：10 个测试文件但 coverage 未纳入验收；vitest thresholds 未配置 | P3 | apps/ai-service 配置 |

### 修复规划（一次一个 · 按此顺序执行）

| 修复 | 内容 | 验收标准 | 状态 |
|------|------|---------|------|
| R1 | **lint 门禁修复**：重装 eslint 依赖（pnpm install / 对齐 lockfile）→ 本地 `pnpm lint` 全绿 → 确认 CI 步骤可过；如根因是依赖缺失则补 devDep | `pnpm lint`（root turbo）零错误；CI 同一命令可过 | **[x] 2026-08-08**：根因 = node_modules 与 lockfile 漂移（eslint-config-next 目录内 eslint-plugin-import 悬空 symlink，6/18 重装后未更新，pnpm install 因 lockfile 一致跳过修复）→ 定点重链；随后清 225→0 个 BE ruff 错误（--fix 126 + 手动 99：F841×45 未用变量、B011×13 assert False、E402×32（trendok logger 下移 + 测试 noqa）、B023 decision 闭包默认参数绑定、B904/B905/UP031/E741/B007）；**顺带发现 1 个测试丢失 bug**：test_alpha_radar_process.py 同名 `test_keywords_from_trend` 定义两次（39 行 ap. 版本从未运行），重命名为 `test_keywords_from_trend_empty_entries`；desktop-ui 12 个 no-explicit-any error 修完（decision-context/exec-attention 动态字段改显式断言）；**验收**：pnpm lint 4 包全绿（0 errors，desktop-ui 剩 31 warnings 为历史未用变量/exhaustive-deps 记录观察）+ BE 3223 passed/表零变化/gate 91.8% + FE 607 passed + TSC 干净 |
| R2 | **BE 执行链补齐**：execution_gate / paper_trading / db/decision / db/execution_journal / db/paper_trading 补异常与边界分支测试至 ≥90% | 全量 pytest 全绿 + 表零变化 + GATE PASSED | **[x] 2026-08-08**：5 文件全 ≥99%（execution_gate 87.7→99、paper_trading 87.9→99、db/decision 84→99、db/execution_journal 80→99、db/paper_trading 79.7→100；各剩 1 条不可达死分支）；新增 5 个测试文件 122 测——execution_gate：非 dict 信号/fallback/未知 SRV level/overflow 覆盖三态（时间/阈值/ATTACK 不适用）/ETF confirm-contradict-incomplete 矩阵/HK gate 嵌入；paper_trading：intake 全 skip 路径（out-of-scope/no-close-price/duplicate/insert-error/异常注册表）、update 全守卫（registry fail-open/close 异常/update 异常/空 bars/坏价格）、_pick_close_reason 五原因矩阵（stop/target/score_floor/pool_exit/max_hold + score 异常 fail-open）、compute_stats 三态；db 层 mock 连接矩阵（get_session/update 系列/touch/upsert_actions 空/ list_actions status+iso 转换/list_snapshots 双分支/limit clamp/has_source_on_date 三态/list_changes since/insert 校验 raise×3/count_since/avg/row 转换）；**验收**：3345 passed / 2 skipped 全绿 + 27 表零变化 + GATE PASSED（OVERALL 92.5%，-1646 missed）；修复 1 个测试隔离问题（_TABLE_ENSURED 全局缓存导致全量下 CREATE 不执行，改断言不依赖） |
| R3 | **BE 小文件清零**：alpha_radar_daily / db/journal / db/morning_brief / db/industry_mainline_metrics / tv/ego_lite 至 ≥85% | 同上 | **[x] 2026-08-08**：5 文件全 100%（journal 17→100：fetch_all clamp/空表、fetch_by_id 空 id/未命中、update 仅 title/仅 content 保留另一字段、delete rowcount 0/1；morning_brief 23→100：upsert RETURNING、latest 带/不带 type、recent clamp 30、_row_to_dict 坏 JSON→[]/None 字段矩阵；industry_mainline_metrics 22→100：upsert 空→0 且不 executemany/过滤假行/raw 非 dict 包装、list 两函数 raw dict vs str 解析、get_dates_upto clamp 60+反转；ego_lite 28→100：playwright 缺失/ImportError→EgoLiteUnavailable、全 mock 无网络 capture 成功/空行 reload 重试/close 异常吞掉、sync wrapper；alpha_radar_daily 0→100：纯 re-export import 测试）；**验收**：3384 passed / 2 skipped 全绿 + 27 表零变化 + GATE PASSED（OVERALL 93.3%，-1459 missed）；修复：fake async_playwright 应为同步返回 context-manager 对象、update_journal 3 次 ensure_table 导致断言改语义匹配 |
| R4 | **FE 组件层起步**：装 @testing-library/react + jsdom，给 watchlist/dashboard 核心组件补首批组件测试；FE 阈值 60% 覆盖范围扩到 components | FE 全量测试全绿 + 新增组件测试 ≥20 个 | **[x] 2026-08-08**：装 @testing-library/react/jest-dom/jsdom；vitest 全局 jsdom + setup 注入 jest-dom + coverage include 扩到 components/hooks（排除 pages/chat/journal/agent/ui/layout/theme 与未纳入范围的次要目录）；**新增 10 个组件测试文件共 91 个测试**（DashboardHeader 7：hover/blur tooltip/align 左右定位/fallback；EtfFundFlowCard 8：shareLag+intradaySafe 警告/Live/已收盘/stale/Data Lag/空态；MorningBriefCard 11：pending/无 brief/分组计数/未知类别丢弃/midday/badge 阈值/AI 摘要三态/按钮；IndustryFundFlowCard 6：矩阵+dedupe+collapsed/5D inflow 表/参考 refId/复制状态色；MarketSentimentCard 11：gate+HkGate/风险档/panic/ETF confirm-contradict/恐慌告警/indexSignals featured+quoteError/近5日/按钮 disabled+参考/空态；DecisionJournalCard 8：mock 3 个 react-query hooks（queries/watchlist + queries/execution-journal）无 jsdom 依赖的 options 捕获模式；WatchlistToolbar 11：按钮禁用矩阵/进度条+日志 slice(-4)/skip+force/copy 色/error；WatchlistImportDebug 8：过滤/排序（null 最后）/Add/In watchlist/空态；FunnelHistoryTable 9：toFunnelRow 全分支/loading/兜底列；WatchlistRow 12：tone 红绿 class/颜色/持仓 vs 非持仓 买卖按钮/position+cost draft 校验提交/参考/移除）；**顺带修**：R2 测试遗留 13 个 ruff 错误（B011×7 assert False→raise、F401×4、UP017）+ 既有 userTrades.test.ts side 字面量 TSC 错误（as const）+ tests 类型修复（GATE as ExecutionGate、quote undefined）；**验收**：FE 708 passed / 1 skipped 全绿 + All files 62.78%（components/dashboard 93.76、watchlist 41+，全局阈值 60% 通过）+ typecheck 干净 + pnpm lint 4 包全绿 + BE 3403 passed / 表零变化 / GATE 93.3%；观察项：WatchlistRow/Table 仍 0%→部分覆盖（Row 12 测已补），radix Switch 交互在 jsdom 未测 |
| R5 | **job 失败告警**：healthcheck 或新端点消费 sync_job_record 失败记录（近 24h 失败 job → 桌面通知/API 可见） | 有测试锁定 + 手动触发验证 | **[x] 2026-08-08**：新增 `GET /api/health/job-failures?hours=24`（health_routes）：聚合 sync_job_record 近 N 小时（clamp 1~168）success=false 记录，按 job_type 最新失败 + failures24h 计数，`ok=false` 即存在失败——桌面/前端可直接消费；db 层新增 `list_recent_failures(hours)`（iso 时间戳转换）；**测试 8 个**：db mock 4（clamp 上下界/空/iso 转换）+ API 3（无失败 ok=true/按 job 聚合 count+最新记录+失败数/custom hours 透传）+ 复用既有 health 形状测试；**手动验证**：真实插入 1 条失败记录 → 端点返回 ok=false 且含该 job → 清理后 count 回落（剩 4 条为真实历史失败）；**验收**：3410 passed / 2 skipped 全绿 + 表零变化 + GATE 93.3%（sync_job_record 97%、health_routes 89%）+ ruff 全过 |
| R6 | **契约测试**：queries 层对核心响应加形状断言（golden JSON 或 shared Zod 校验）；至少覆盖 dashboard/watchlist/execution 三链 | 测试全绿 + 捕获 ≥1 真实漂移或确认无漂移 | **[x] 2026-08-08**：新增 `src/lib/queries/contract.test.ts`（8 测）+ 真实 golden fixtures（`__fixtures__/`：dashboard_summary / execution_snapshots / execution_changes，均从真实 BE TestClient + dev DB 抓取）；**三链覆盖**：execution = shared `ExecutionSnapshotListResponseSchema`/`ExecutionChangeListResponseSchema` 直接校验真实 fixture（含 action 枚举——真实数据 WATCH/WATCH_SILENT/HOLD/TRIM 全在枚举内）；dashboard = 自定义 DashboardContractSchema（asOfDate/industryFundFlow/marketSentiment.items+srvIndex+executionGate/macroSnapshot）；watchlist = TrendOkResultSchema（shared）+ 自定义 QuoteSchema；**4 个反向漂移检测**：marketSentiment 字段改名 / quote 缺 price / action 枚举外值 / change 缺 field → 全部 parse 失败（契约确实能捕获漂移）；**结论：确认无漂移**（执行链 action 枚举、dashboard 关键字段、watchlist trend 结构均与 shared Zod 一致）；补装 zod devDep（shared 源码解析需要）；**验收**：FE 716 passed / 1 skipped 全绿 + All files 62.78%（阈值过）+ typecheck 干净 + pnpm lint 4 包全绿 + BE 未改动（R5 基线 3410 passed 保持） |
| R7 | **ai-service 覆盖率 gate**：vitest thresholds 配置 + 补足至 ≥60% | ai-service coverage 报告 ≥60% 且 CI 稳定 | **[x] 2026-08-08**：vitest.config.ts 已含 coverage thresholds（lines/functions/branches/statements=60，include src/**/*.ts，exclude index.ts + routes/ 模型调用路由 + 测试文件）；**实测 86.19%**（lines 86.19 / branch 75.25 / funcs 85.29）远超 60%；**gate 有效性验证**：临时把 lines 阈值调 99.9 → `npm run coverage` exit 1（拦截生效）→ 恢复 60 → exit 0；**稳定验证**：连续两次 coverage 输出一致（86.19% 无波动）；routes/（13 文件 2152 行、0 测试、纯 Hono 路由+AI 模型调用）维持排除并记观察——补测需模型 mock，收益低；**验收**：coverage 86.19% ≥ 60% + gate 真实拦截 + typecheck 干净 + lint 4 包全绿 |

### 观察清单

| 项 | 状态 |
|----|------|
| 执行链 R2 若暴露 fail-open/fail-closed 语义问题 | 入 §17 H5 清单续 |
| **baseline 验收时机**（2026-08-08 终验教训）：dev server + UI 活跃时 `/dashboard/summary` 每次触发 top_inst 同步、FE registry debounce 触发快照写入——`db_rows_baseline.py check` 会误报。全量验收需在 UI 闲置时执行（或先 save 再立即跑） | 已确认非测试污染（时间戳均在 pytest 窗口外）；写入源：dashboard 加载→sync_top_inst_watchlist；FE useExecutionJournalCapture debounce→capture('registry') |
| R4 引入 testing-library 是否影响 SSR 组件（'use client' 组件） | 验证后再扩范围 |

### 铁律

1. **P0 未清之前不碰 L4-P1**（券商研究）
2. 每个 H 项一个会话，不混 scope（OPT-068+ 落地）
3. 每个修复 = 测试 + 验收证据（勾选本文件）
4. 全量测试跑完 dev DB 表行数不变 = 测试隔离合格

---

