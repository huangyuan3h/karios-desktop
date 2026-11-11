# Sync 东财不可用降级修复报告（2026-11-06）

> 提交：基线 `4c3bbbb7`（降级机制）→ 收紧 `5b7620d3`
> （传输签名+EM 归因、AND 语义、文案修正）。
> 本行由 docs-only 提交补充；代码以 `5b7620d3` 为准。

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

> 2026-11-06 复核收紧（review follow-up）：分类器改为
> **传输签名 + EM 归因双重要求**（`eastmoney_ip_ban` latch 为唯一例外），
> 多字段 **AND 语义**——任一字段出现真实失败即整体保持红；
> 文案不再断言“已沿用上次有效数据”，改为“已有有效数据予以保留、缺失仍记为缺失”。

后端（`services/data-sync-service/`）：

1. **新增** `src/data_sync_service/service/em_source_status.py`
   - `is_em_source_unavailable()`：仅当错误可识别为东财上游时 True
     （`eastmoney_ip_ban` latch 单独充分；否则需 EM 归因
     [`eastmoney`/`push2delay`/`push2his`/`datacenter-web.eastmoney`]
     **+** 传输签名[RemoteDisconnected/empty reply/(52)/abort/reset/timeout/502-504 等]，
     或带 EM 归因的三连签名 `direct:+curl:+urllib:`）。
     `no_iv_data / no_rows / missing_symbols / suspicious_empty_lhb / tushare 超时`、
     仅提及 EM URL 的 payload/schema/编程错误、别家 host 的三连签名一律 False。
     多字段 AND：`error` 为 EM 传输错但 `errors[]/providerErrors/historyError`
     任一段是真实错误 → 整体 False。
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

- 后端：`pytest -k "dashboard or etf or sentiment or top_inst or em_"` → **649 passed, 3 skipped**（含新增 23 例：分类器 7〈正向 3：截图 banner/latch/带归因三连；负向 4：既有真错误/裸 host 提及/别家三连/多字段 AND〉、中和 3、ETF 4〈降级/真失败/恢复/history 混合真失败〉、情绪步 7〈含 EM 掩盖 integrity 错误仍失败〉、总装 2）。
  tsc/eslint/vitest 均以真实退出码验证通过（无管道遮掩）。
- 组合验证（mock fetch + 真实 `_sync_sentiment_step` + 真实 `dashboard_sync_parallel`）：
  `ok=True, degraded=True`，中文 `degradedMessage`，`upserts=[[]]`（零写入），审计行 `success=False`，JSON 可序列化（SSE 安全）。
- 前端：`vitest useDashboardSync.test.ts` → **14 passed**；`tsc --noEmit` 干净；`eslint` 干净。
- 线上只读验证（未重启、未触发同步）：`/api/health/datasources`（em_probe stale、push2/push2his failing）、`/api/health/job-failures`（etf 腿 10 连败 raw 栈与截图一致）。

## 5. 生效方式（需要 coordinator 手动 reload，**不要 kill -9**）

- 运行中进程：PID **50090**（uvicorn worker），父 **50076**
 （`uv run uvicorn data_sync_service.main:app --app-dir src --host 127.0.0.1 --port 4330`，
  cwd `services/data-sync-service`，启动于 2026-11-05 13:44，跑的是旧代码）。
- 归属：50076 的父是 **50001 `pnpm run dev`**（repo 根 `pnpm dev`  supervisory 链，
  再上是 pnpm 10.28.2 的 `pnpm run dev` 49998）——**无 launchd/supervisord 接管**，
  就是开发态 pnpm dev 树。`lsof` 确认仅 `127.0.0.1:4330` 监听。
- 本次改动是纯 Python 源码变更，**必须 reload 服务才生效**；未做任何自动重启。
- 安全 reload 精确命令（cwd=`services/data-sync-service`）：
  `uv run uvicorn data_sync_service.main:app --app-dir src --host 127.0.0.1 --port 4330`
  做法：优雅退出旧链（`kill 50090` 等其 drain；**不要 `-9**），或直接重启 `pnpm dev` 树；
  确认新进程监听 4330 后再离场。
- 验证（东财 outage 持续期间也可做，均为读或幂等 Sync All）：
  1. Dashboard 点 Sync → 应显示 **OK + 「部分降级」** 琥珀提示，不再是 FAILED + raw 栈；
  2. `curl --noproxy '*' 'http://127.0.0.1:4330/api/health/job-failures?hours=1'` 仍可见 etf 腿审计行（`em_source_unavailable` 前缀）；
  3. 次日东财恢复后同一次 Sync 应回到纯 OK（warnings 消失）。

## 6. 局限

- `insert_record(success=False)` 仍会写一条按日去重的 `job_failed` system event（审计需要，last-success 不动）；
  peripheral streak（≥3）仍可能触发已有 webhook 逻辑——未动该逻辑，属已知噪音。
- 调度器侧 `research_report_sync` 的 `eastmoney_ip_ban_latched` 失败不在本次 Sync-All 路径内，未改（仍红，属实）。
- `etf_fund_flow_watchlist` 不在 `SYNC_JOB_TYPES` 内，`/sync/jobs` 看不到它——沿用现状，未扩 scope。
