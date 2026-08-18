# 决策/告警 webhook 事件订阅（todo §14 #3 · P1 · 已拍板 2026-08-12）

> 状态：已拍板（§7 四项全决）。实现后迁出 docs/designs/。

## 1. 目标与消费者

**目标**：把 Karios 系统内已经产生、但只在 UI/DB 里的"事件"变成可推送的 webhook
事件流，让外部消费者（决策 Agent、用户自己的 AI 助手、后续的推送服务）在事件
发生时收到结构化通知——"突发情况我来处理"的前提是系统先叫。

**消费者优先级**（todo §0 表：收益 > API 开放）：
1. 决策 Agent / 周报链路（已有 /v1/* 拉取，webhook 是**推**，补足拉不到的"发生时"语义）
2. 用户个人 AI 助手（cookbook 场景）
3. 后续网页/手机推送（本轮不做，事件流先打通）

## 2. 事件源盘点（可行性核心：全部现成，零新数据采集）

| # | 事件 | 现在在哪产生 | 触发节奏 | 状态 |
|---|------|-------------|---------|------|
| E1 | **cron 任务失败/恢复** | `sync_job_record`（每个 job 落 success/error） | 任何 job 运行时 | ✅ 已有表，缺推送 |
| E2 | **paper 链断链**（17:30/17:42/17:45 缺跑或失败） | `paper_chain_watchdog`（18:05 自检+补跑） | 每交易日 18:05 | ✅ 已有 job，缺推送 |
| E3 | **单票盘中 -8%**（极端警报） | 无（实时价已有：`realtime_quote.py` /quote） | 盘中 10:00~15:00 | ⚠️ 需新增巡检 job（见 §6） |
| E4 | **接近止损线** | `trading_brief._alerts_section`（12:00/14:30 brief） | 工作日 12:00/14:30 | ✅ 已有计算，缺推送 |
| E5 | **S-3 候选突变**（昨日候选今天消失/新增） | `portfolio_health.s3Candidates`（每日） | 17:30 分数更新后 | ⚠️ 需 diff 逻辑（轻） |
| E6 | **滚动 OOS warning**（亏损/夏普<0/零交易） | `rolling_oos_latest.json`（每月首周一 08:15） | 月度 | ✅ 已有产物，缺推送 |
| E7 | **回测 vs Paper 对账缺票**（missing>0） | `recon`（每周一 07:30） | 每周 | ✅ 已有产物，缺推送 |

**结论：可行。** 7 类事件 5 类有现成产物，只有 E3（盘中巡检）和 E5（候选 diff）
需要新增轻量逻辑；推送层本身是全新的（订阅表 + 投递器）。

## 3. 架构

```
事件源（7 类，见 §2）
   │  emit_event(type, payload)          ← 在现有 job/服务的产出点各加一行
   ▼
events 表（事件日志，幂等去重）
   │  订阅匹配（subscription.event_types LIKE / jsonb）
   ▼
webhook_subscriptions 表（url + secret + 事件类型订阅 + enabled）
   │
   ▼
投递器（deliver_events 每 1 分钟扫 pending）
   ├─ HMAC-SHA256 签名头（X-Karios-Signature: sha256=<hex>，用订阅 secret）
   ├─ 超时 5s · 失败退避重试（5/15/60 分钟 ×3，仍失败标 dead + 落事件日志）
   ├─ 限频：单订阅 30 事件/分钟（防风暴）
   └─ 幂等：事件 id 去重（投递前查 delivered）
```

**表**（Alembic 迁移 + CREATE_SQL 同步）：
- `webhook_events`：id, event_type, payload(jsonb), created_at, dedupe_key
- `webhook_subscriptions`：id, url, secret, event_types(text[] 或逗号串), enabled, created_at
- `webhook_deliveries`：id, event_id, subscription_id, status(pending/sent/failed/dead), attempts, next_retry_at, last_error

**API**：
- `POST /api/webhook/subscriptions`（创建/管理订阅，带 secret 生成）
- `GET /api/webhook/subscriptions` · `DELETE /api/webhook/subscriptions/{id}`
- `POST /api/webhook/test`（发测试事件验证连通）

**签名校验（消费者侧）**：payload + HMAC-SHA256(secret) → 外部 AI 可验证真实性。

## 4. 融入方式（挂载点 · 与现有系统最小的缝合）

| 挂载点 | 事件 | 改动 |
|--------|------|------|
| `db/sync_job_record.record_run`（或每个 job 的收尾） | E1 | 加 emit_event 一行 |
| `paper_chain_watchdog_job` 补跑/失败分支 | E2 | 加 emit_event |
| **新增** `scheduler/intraday_alarm_job.py`（盘中 10:00-15:00 每 5 分钟） | E3 | 新 job：拉 open paper trades 实时价，跌超 -8% emit；一次性警报（同一票当日只报一次） |
| `trading_brief.generate_trading_brief`（midday/action） | E4 | 组装时已有 alert 列表，顺手 emit |
| `portfolio_health` 或 17:30 automation 后 | E5 | 候选 diff（对比昨日），轻量 |
| `rolling_oos_job` 收尾（warning 时） | E6 | 加 emit_event |
| `backtest_recon_job`（missing>0 时） | E7 | 加 emit_event |

所有挂载点都遵循同一模式：**在事件产物生成处 emit_event(type, payload)**，
payload 用现有结构化数据（job summary / alert 行 / recon 行），不新造数据。

## 5. 与决策 Agent 的配合

- 决策 Agent 目前是**拉取**（/v1/* + brief + recon）——webhook 补"发生时"推送；
  订阅后 Agent 收到 E2/E3/E6/E7 可自动触发复盘/行动
- 事件 payload 与 /v1/* 的 schema 风格一致（snake_case dict，字段注释在路由模块，
  沿用 OPT-009 漂移防护约定）
- **不做**：webhook 的回调执行（Agent 收到后自己调 /v1/*，保持 Karios 无状态被动）

## 6. 分期

- **P1（已拍板 · 本轮实现）**：订阅/事件/投递三层 + E1（job 失败）+ E3（盘中 -8% 巡检
  ——1 小时一轮，条件单兜底）
- **P2（下一轮）**：E2/E4/E6/E7 挂载（每个 ~10 行）+ 候选 diff E5（评估后）+ 前端订阅管理页
- **不做**：网页/手机推送、Telegram Bot（§4 范围边界）

## 7. 拍板（2026-08-12 全决）

1. **消费方**：两者都要——订阅表 + secret 管理 + cookbook 示例；决策 Agent 后续按需订阅，
   Karios 保持无状态被动
2. **E3 盘中巡检频率**：**1 小时一轮**（用户有券商条件单兜底盘中极端，-8% 报警只做
   兜底提醒，不做 5 分钟高频）
3. **订阅管理 UI**：先 API + cookbook 示例，前端管理页 P2 后置
4. **E5 候选 diff**：P2 评估后做（候选消失常是闸门关闭的正常表现，噪音评估优先）
