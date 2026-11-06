# Sync 东财不可用降级修复报告（2026-11-06）

> 任务：「我只修报错」。东财（East Money）实时源自 ~11-03 不可用时，
> Dashboard Sync 从 FAILED + 巨型 raw transport 红 banner，
> 改为**完成-部分降级 + 中文短提示**，其余任务照跑。

## 1. 根因（已用线上只读接口验证）

- Dashboard Sync All = 4 步：`industryFundFlow` / `marketSentiment` / `macroDaily` / `news`，
  总 `ok = all(steps.ok)`（`service/dashboard.py: dashboard_sync*`）。
- `marketSentiment` 步骤 = `sync_cn_sentiment` + `etf_fund_flow_watchlist`
  + `top_inst_watchlist`（+ 可选 `option_iv`），硬失败任一即整步 FAILED。
- `etf_fund_flow_watchlist` 是**纯东财实时源**（`push2delay/push2.eastmoney.com`，
  `service/etf_fund_flow_em.py`）。11-06 实测：
  `GET /api/health/job-failures` 显示该 job 24h 内 10 连败，错误正是截图里的
  `RemoteDisconnected / curl (52) Empty reply / urllib` 三连栈；
  `em_probe` 同期显示 `push2.eastmoney.com, push2his.eastmoney.com` failing。
  零行 → `ok=False` → 毒化整步 → 总 FAILED，raw 栈经 `message` 直达顶部红 banner。
- 已有先例：`option_iv` 的 `no_iv_data` 早已做过同样的中和（ok=True + warnings），
  本次把该模式推广到“可识别的东财上游不可用”。

## 2. 改动（共 7 个文件，均为加法/小手术）

后端（`services/data-sync-service/`）：

1. **新增** `src/data_sync_service/service/em_source_status.py`
   - `is_em_source_unavailable()`：仅当错误可识别为东财上游时 True
     （`eastmoney.com` / `eastmoney_ip_ban` / `push2delay` / `push2his` /
     `datacenter-web.eastmoney`；或 `eastmoney + transport 词`；
     或 `direct:+curl:+urllib:` 三连签名）。
     `no_iv_data / no_rows / missing_symbols / suspicious_empty_lhb / tushare 超时` 等一律 False。
   - `neutralize_em_substep()`：`ok=False` → `ok=True + degraded/unavailable + 中文 message`，
     raw 文本保留在 `rawError` 供审计；非东财失败返回 None（保持红）。
2. `src/data_sync_service/service/etf_fund_flow.py`
   - 零行 + 错误命中东财不可用 → 写 `success=False` 审计行
     （`error_message="em_source_unavailable: …"`，**last-success 不推进**）、
     **不写任何行**（无伪造零值）、返回降级 `ok=True`。真实失败路径原样不动。
3. `src/data_sync_service/service/dashboard.py`
   - `_sync_sentiment_step`：etf 腿中和（新旧两种形状都认）；
     top-inst 仅当**所有**失败分段都是东财不可用才中和
     （默认 tushare 优先，tushare 真失败仍保持红）；
     sentiment 计算本身保持严格。
   - 三个 `dashboard_sync*` 总装加顶层 `degraded + degradedMessage`（中文），`warnings` 透传；纯加字段，老前端忽略即兼容。

前端（`apps/desktop-ui/`）：

4. `src/hooks/useDashboardSync.ts`
   - `syncResultErrorMessage` 截断到 300 字（残留失败也不再出现巨型 banner）。
   - 新增 `dashboardSyncDegradedNote()`：`degradedMessage` / warnings → 中文降级提示；不碰 ok 契约。
5. `src/components/pages/DashboardPage.tsx`
   - 红 banner 下方加琥珀色降级提示；Last sync 面板加「部分降级」pill，降级步骤显示 ◐ 琥珀色；busy 面板显示步骤 warnings。
6. `src/hooks/useDashboardSync.test.ts`：降级 note（4 例）+ banner 截断（1 例）。
7. **新增** `services/data-sync-service/tests/test_em_degraded_sync.py`：18 例（见 §4）。

未碰：`em_push2_http.py`、`test_em_push2_http_extra.py`（他人未提交改动，原样保留）；
无源迁移、无代理改动（EASTMONEY_PROXY 保持空）、无数据重写、无交易改动。

## 3. 行为对照

| 场景 | 修前 | 修后 |
|---|---|---|
| 仅东财实时源不可用 | Sync FAILED + 巨型 raw 栈红 banner | Sync OK + 「部分降级」琥珀提示 + 中文短注；其余腿照跑 |
| 真实失败（sentiment 计算、tushare、news、industry） | FAILED | 仍 FAILED（未动） |
| 审计 | raw 栈只在 banner | `sync_job_record` 留 `success=False` 行 + `rawError/providerErrors` 在 meta |
| 缓存数据 | — | 沿用 DB 上次有效行，不写零值；asOf 保持旧日期 |
| 源恢复 | — | 下次 Sync 自动正常（无任何持久禁用开关），见测试 recovery 例 |

## 4. 测试/验证结果

- 后端：`pytest -k "dashboard or etf or sentiment or top_inst or em_"` → **644 passed, 3 skipped**（含新增 18 例：分类器 6、中和 3、ETF 降级/真失败/恢复 3、情绪步 5、总装混合/真失败 2）。
- 组合验证（mock fetch + 真实 `_sync_sentiment_step` + 真实 `dashboard_sync_parallel`）：
  `ok=True, degraded=True`，中文 `degradedMessage`，`upserts=[[]]`（零写入），审计行 `success=False`，JSON 可序列化（SSE 安全）。
- 前端：`vitest useDashboardSync.test.ts` → **14 passed**；`tsc --noEmit` 干净；`eslint` 干净。
- 线上只读验证（未重启、未触发同步）：`/api/health/datasources`（em_probe stale、push2/push2his failing）、`/api/health/job-failures`（etf 腿 10 连败 raw 栈与截图一致）。

## 5. 生效方式（需要 coordinator 手动 reload，**不要 kill -9**）

- 运行中进程：PID **50090**，`uvicorn data_sync_service.main:app --app-dir src --host 127.0.0.1 --port 4330`
  （启动于 ~11-05，加载的是旧代码）。
- 本次改动是纯 Python 源码变更，**必须 reload 服务才生效**；未做任何自动重启。
- 安全 reload（二选一，推荐热 reload 无缝）：
  1. 若 uvicorn 以 `--reload` 启动：无需操作，存盘即自动重载（可用 `curl --noproxy '*' localhost:4330/sync/jobs` 确认进程仍活）。
  2. 否则优雅重启：在 repo 根执行
     `cd services/data-sync-service && PYTHONPATH=src nohup python3 -m uvicorn data_sync_service.main:app --app-dir src --host 127.0.0.1 --port 4330 > .reload.log 2>&1 &`
     前提是先让旧进程自行退出（`kill 50090`，等其 drain；**不要 `-9`**），并确认新进程监听 4330 后再离场。
- 验证（东财 outage 持续期间也可做，均为读或幂等 Sync All）：
  1. Dashboard 点 Sync → 应显示 **OK + 「部分降级」** 琥珀提示，不再是 FAILED + raw 栈；
  2. `curl --noproxy '*' 'http://127.0.0.1:4330/api/health/job-failures?hours=1'` 仍可见 etf 腿审计行（`em_source_unavailable` 前缀）；
  3. 次日东财恢复后同一次 Sync 应回到纯 OK（warnings 消失）。

## 6. 局限

- `insert_record(success=False)` 仍会写一条按日去重的 `job_failed` system event（审计需要，last-success 不动）；
  peripheral streak（≥3）仍可能触发已有 webhook 逻辑——未动该逻辑，属已知噪音。
- 调度器侧 `research_report_sync` 的 `eastmoney_ip_ban_latched` 失败不在本次 Sync-All 路径内，未改（仍红，属实）。
- `etf_fund_flow_watchlist` 不在 `SYNC_JOB_TYPES` 内，`/sync/jobs` 看不到它——沿用现状，未扩 scope。
