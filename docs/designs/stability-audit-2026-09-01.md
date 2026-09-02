# 稳定性审计 · 数据 + 架构 + API 储备（2026-09-01）

> **状态**：调研完成，待排期。决策真值落地后迁出本目录，摘要进 `archive/`。
> **范围**：43+ 定时任务 / 8 类外部数据源 / 单实例架构。
> **样本**：08/29-09/01 15 条 `sync_job_record` 失败（14 low / 1 high），均已修复。
> **方法**：grep 实证 + 调度器/DB/健康链路代码审阅 + 三路并行探索（同步全景 / API 爬虫 / 架构拓扑）。

---

## 1. 结论先行

| 维度 | 评级 | 一句话 |
|------|------|--------|
| EOD 核心链 `17:10 close → 17:30 watchlist → 17:42 S3 → 18:15 行业/情绪` | 脆弱但已织网 | 时序松耦合 + `catchup_missed_eod_chain` + `paper_chain_watchdog 18:05` 双保险补齐 `MemoryJobStore` 重启丢任务缺陷，但单实例 + 单 `TUSHARE_TOKEN` 仍是 SPOF |
| 数据新鲜度 | 总体稳定，尾部波动多 | 样本多为外部瞬态（`200/min` 限频 / `IP ban` / `no_iv_data` / `LLM 5xx` / 单 RSS 源失败），分级正确 |
| 长尾链 `HK日线/宏观/Alpha/富化` | 最不稳 | 依赖方最多、发布时点最滞后、IP 封禁最集中 |
| 架构 | 单机可靠，集群不可扩 | `uvicorn 单worker` + `psycopg 无池` + `statement_timeout 120s` 唯一护栏，无熔断/舱壁/持久化队列 |

**是否需要更多爬取**：**否。先加固回落链与可观测性，再补 1-2 个付费兜底**。当前"主力 tushare + 多级免费回落"已够，盲目加爬会加剧东财 IP 封禁面。

---

## 2. 数据稳定性

### 2.1 近期故障归因

| 故障 | 次数 | 根因 | 影响 | 现状 |
|------|------|------|------|------|
| `etf_daily_full / adj_factor 频率超限 200/min` | 2 | `fund_daily` 全量 1000+ ETF 串行冲限 `services/data-sync-service/src/data_sync_service/service/etf_daily.py` `adj_factor.py:85` | 低，决策已切 `sleeve_etf_daily_sync 17:25 仅5只` | 已修复：错峰 `adj 18:30` + sleeve 5只化 |
| `research_report_sync eastmoney_ip_ban_latched` | 3 | 东财全站 IP 封（家宽/代理出口 502）需 `ProxyHandler({})` 直连 `service/em_push2_http.py` | 低，研报为 Alpha 加分项 | 已修复：直连优先 + `_EM_BLOCKED 15min latch` + `_PROXY_DEGRADED` |
| `option_iv_daily no_iv_data` | 2 | 非交易日/无成交期权无 IV | 低 | `skipped` 属正常 |
| `news_fetch 1 source failed` / `news_enrich failed=5/25 LLM 500/timeout` | 4 | RSS 单源 isolate 正常；LLM `300s timeout / 500` 瞬态 `service/news_enrich.py` `apps/ai-service/src/model.ts` | 低，`pending→failed→retry_after` 自愈 | 已修复 |
| `rolling_oos unknown error` / `backtest_paper_recon not in window` | 2 | 窗口固化漂移 | 低 | 已修复 |
| `paper_chain_watchdog close_sync missing` **high** | 1 | 17:10 收盘链中断导致下游三 job 缺失 `scheduler/paper_chain_watchdog_job.py` | 高，触发自愈 | 已自愈 `catchup_missed_eod_chain` + `watchdog 18:05` |

**判断**：无"数据静默腐烂"，均为可重试瞬态；高优仅 1 次且已自愈。整体收敛。

### 2.2 链路健康度

| 链 | 优先级 | 限流 | 重试 | 脆弱点 | 评级 |
|----|--------|------|------|--------|------|
| CN 日线 `close_sync` `service/close_sync.py:88 pro.daily` | tushare 单源 | 200/min 分页 `limit 5000` | `_with_retry 3×1.5s` | 当日空帧延迟 | 稳 |
| HK 日线 `hk_daily.py:81` | `Tencent ifzq → ak Sina → yfinance → tushare` | tushare `1/min` | per-ticker 断点 `last_ts_code` 续跑 | `darwin` 禁 ak (`mini_racer` 崩) | 中 |
| 行业资金流 `industry_fund_flow.py:206` | `EM dataapi bkzj → EM daykline curl → ak sector_fund_flow` | 无文档 | `_with_retry 3×0.4s` + `curl --max-time 20` | **EM IP ban 高发** | 脆 |
| 情绪 `market_sentiment.py:694` | `ak zh_a_spot_em → tushare daily pct` | 同上 | `ThreadPool 4-6` | ak 在 darwin 失效 | 中 |
| 宏观 `macro_daily.py` | `tushare index_global 100/day → Tencent hkHSI/HSTECH → yfinance` | 100/day | 380d 分页 | 100/day 必 fallback | 中 |
| ETF | `tushare fund_daily` + sleeve 5只 | 200/min | `sleep 0.35s` | 全量仍擦限 | 稳(sleeve)/脆(全量) |
| 新闻/研报/Alpha | RSS/RSSHub `127.0.0.1:1200` + Jina + Tavily | Jina 429/503 | 单源 isolate + `ThreadPool` | RSSHub 单实例 `CACHE_TYPE=memory` 重启丢缓存 | 中 |

### 2.3 调度与 EOD 依赖

- `BackgroundScheduler(timezone=UTC, coalesce=True, max_instances=1, misfire 12h)` `scheduler/__init__.py:62` — 50+ `CronTrigger(tz=Asia/Shanghai)` + `DateTrigger +3s` 启动自愈 `catchup_missed_eod_chain` (`scheduler/__init__.py:411`)。
- EOD 时序：`17:10 close_sync → 17:15 index_basic → 17:20 daily_basic → 17:25 sleeve_etfs → 17:30 watchlist → 17:42 S3 intake → 17:45 update → 18:05 watchdog → 18:15 cn_industry → 18:20 sleeve_paper → 18:45 behavior_audit`。全靠 `cron + skip-if-already` 松耦合，下游 `watchdog` 与 `startup catchup` 为第二道网。
- 重启丢任务：`misfire_grace 12h` 仅对已在 `MemoryJobStore` 的 job 有效；>`12h` 离线需 `catchup_missed_eod_chain` 跨日自愈（已加固 2026-08-10/08-25）。

---

## 3. 架构稳定性

| 域 | 现状 | 风险 |
|----|------|------|
| 调度 | 单实例内存 store，无 `SQLAlchemyJobStore` / 分布式锁 | 扩容 2 副本必重复执行；`misfire` 仅内存有效 |
| DB `db/__init__.py:8` | `psycopg.connect + statement_timeout=120s`，无连接池/`NullPool`，无 `lock_timeout`，DB 层无重试 | 前端多标签 `refetchOnWindowFocus` + 多 job 并发易 `too many clients=100` |
| 健康/告警 `api/health_routes.py` `db/webhook.py` | `GET /healthz` + `GET /api/health/datasources 12类 age vs threshold` + `sync_job_record → system_events(high/low) → webhook 5/15/60m×3 + HMAC + 30/min限频` | 仅 EOD 链有 watchdog；长尾无独立 watchdog；Bark 需自配否则 high 仅落库 |
| 前端轮询 `lib/queries/intervals.ts` `query-client.ts` | `dashboard 60s(交易时段)/watchlist 10min/macro 45s/notifications 5min` + `refetchOnWindowFocus:true` 无 jitter | 多标签 herd；`dashboard/summary FIRST_COMPLETED 0.3s` 扇出阻塞单 worker |
| 部署 `docker-compose.yml` `server_entry.py` | `postgres 16-alpine volume` + `data-sync 单worker uvicorn` + `migrate service_completed_successfully` + `HEALTHCHECK 15s curl /healthz` | 3 SPOF：PG 单实例无 PITR、data-sync 单进程、RSSHub 单实例；`NEXT_PUBLIC_*` 构建时注入需重建镜像 |

---

## 4. API 全景

| 类 | 代表接口 | 认证/成本 | 限流 | 角色 | 备用 |
|----|----------|-----------|------|------|------|
| 付费 tushare Pro `config.py:TU_SHARE_API_KEY` | `daily/adj_factor/fund_daily/index_global/stock_basic/hk_basic/trade_cal/daily_basic/index_dailybasic` | 积分制 ~200/年 单 key | 200/min / 100/day / 1/min | 主力 7 cron | 是，多级回落 |
| 免费 HTTP | `EastMoney push2/dataapi/push2his` `Sina hq.sinajs.cn` `Tencent ifzq` `akshare Sina` `yfinance` | 无 key，IP/Cookie 指纹 | 无文档，WAF 封 IP | 回落主力 | 是，共用出口 IP |
| 自爬 RSS/HTML | `RSSHub 1200` `EastMoney reportapi` `push2his kline` `minute_capture` | 无 | 无 | 情报/研报/分钟线 | 半接（`curl + Referer + COOKIE` 绕 TLS 指纹） |
| LLM 付费 `apps/ai-service:4310` | `news_enrich / alpha_radar extract-batch` `deepseek-v4-flash / MiniMax-M3` | 按 token，`LLM_TIMEOUT_S 300s` | 平峰价 `20/23/05` cron (OPT-108) | 富化 | - |

**储备选项（性价比排序）**：
1. **tushare 多 token 轮换**（最低成本）：现单 key 单点限频，补 1 备 key + `round_robin / quota_watchdog` 扛周五峰值。
2. **Tencent 行情加量**（免费但需固化）：已是 HK 首选，储备动作是把 `Sina 50/批 + TTL 30s` 缓存固化。
3. **BaoStock / Sina 付费行情**（备选）：仅在 `close_sync count_rows <3000` 触发对账，不常态爬。
4. **Wind / Choice 付费**（不建议）：成本高 25 倍，无不可替代覆盖。

---

## 5. 是否需要更多爬取

| 拟加爬取 | 收益 | 代价 | 建议 |
|----------|------|------|------|
| 全市场分钟线/Level2 | 低（策略仅日线+快照） | 高（IP ban 面 ×N） | 不做 |
| 全市场 ETF `fund_daily` 日更 | 低（决策仅 5 只 sleeve） | 高（1000+ 次/天必撞） | 保持月频 + sleeve 日更 |
| 增量行业/研报全量页 | 中 | 中 | 做熔断而非扩页 |
| HK yfinance 全量 | 低（已降为末级） | 高（IP 限流） | 不扩，维持 `Tencent→ak→tushare` |

**储备应做深度而非广度**：
1. 把现有回落链做实 + 加探针（`push2/dataapi/Sina/Tencent` 1 次/10min 黑盒探测，IP ban 即 `system_events high`）。
2. 补 1 个异构付费兜底：tushare 备 key 轮换优于新增 3 个免费爬虫。
3. 降噪而非增量：`RSSHub CACHE_TYPE=redis` / volume 再谈扩源（当前 `memory` 重启丢缓存）。

---

## 6. 落地清单（→ OPT）

| ID | 标题 | 优先级 | 工时 | 依赖 |
|----|------|--------|------|------|
| OPT-124 | Tushare 多 token 轮换 + 配额看门狗 | P0 | 0.5-1 天 | 无 |
| OPT-125 | DB 连接池 + DB 重试 + 慢查询护栏 | P1 | 1-2 天 | 无 |
| OPT-126 | 东财出口探针 + 熔断可视化 | P1 | 1 天 | 无 |
| OPT-127 | 前端轮询 jitter + ETag + 后端限流（P2 储备） | P2 | 1 天 | OPT-125 |

详见 `docs/optimization-checklist.md` OPT-124~127 节。

---

## 7. 证据索引

- 调度：`services/data-sync-service/src/data_sync_service/scheduler/__init__.py:62` `scheduler/paper_chain_watchdog_job.py` `scheduler/watchlist_automation_job.py`
- DB：`services/data-sync-service/src/data_sync_service/db/__init__.py:8` `db/sync_job_record.py` `alembic/env.py:poolclass=NullPool`
- 健康：`services/data-sync-service/src/data_sync_service/api/health_routes.py` `api/query_routes.py:39` `db/webhook.py` `service/notifications.py`
- 数据源：`service/close_sync.py:88` `service/industry_fund_flow.py:206` `service/market_sentiment.py:694` `service/em_push2_http.py` `service/sina_http.py` `service/hk_daily*.py`
- 前端：`apps/desktop-ui/src/lib/query-client.ts` `lib/queries/intervals.ts` `lib/queries/dashboard.ts` `lib/queries/systemHealth.ts`
- 部署：`docker-compose.yml` `services/data-sync-service/Dockerfile` `services/data-sync-service/server_entry.py`
- 共享：`packages/shared/src/schemas/scheduler.ts:171` `SCHEDULER_JOB_CATALOG` `SYNC_JOB_TYPES` (`api/sync_routes.py:314`)
