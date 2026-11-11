# Integrity harden report — 2026-10-24

## Problem list（CRITICAL / WARN / INFO）

- [CRITICAL] Backup RPO ≈ 48h（已修）: `STALE_THRESHOLD_SEC=25h` + 每日 03:00 launchd ⇒ 前一天 ~23.8h 的旧 dump 当天被 skip（log: 10-16..24 隔天 skip，`85663s < 90000s`）。Header 注释 "~24h max loss" 与事实不符。已改为 calendar-day 规则（见下）。
- [CRITICAL] `data_healthcheck.py` 把环境问题刷成整单 FAIL（已修）: `scheduler_jobs` 因 `hk_basic_sync`（无 key）、`alpha_radar_*`（model 不存在）、`xq_follow_snapshot`（cookie/JSON）常年 FAIL；`tv_snapshots` 自 ~08-12 stale 也 FAIL。真 FAIL（bars/scores/close sync）被噪音淹没。已拆 CRITICAL/WARN，`--notify` 只在 CRITICAL 响。
- [WARN] Put IV 单点曾染红 Sync All（保持/加固）: Eastmoney IV 失败已在 `_sync_sentiment_step` 软降级（WIP 保留），本次补上顶层 `warnings` 透出 + 回归测试，保证 only-IV-degraded 时整体 `ok=true`。
- [WARN] `backup_age` 阈值虚高（已修）: 原 `>50h FAIL` 是 "隔天 dump" 的合理化；daily dump 保证后改为 `>26h CRITICAL / >20h WARN`。
- [INFO] 不完整残留 dump 已清理: `karios-20261024-071356.dump`（8.7MB，mtime 停止、无进程持有、相对正常 3.8GB 明显中断）+ 0 字节 `.err` 已删除；好 dump 未动。Parent 的 `--force` dump（`karios-20261024-071408.dump`）当时仍在跑，未干扰。
- [INFO] 本次操作事故（已恢复，见文末）: 误 `git stash pop` 把 2026-10-03 旧 stash 合进 worktree，已完整撤销并逐项核对，worktree 与 stash 均恢复原状。

## Files changed + behavior

1. `services/data-sync-service/scripts/db_backup.sh`
   - 删除 `STALE_THRESHOLD_SEC`，skip 条件改为 **calendar-day 规则**: 非 `--force` 时，仅当最新 dump 文件名日期 = 今天才 skip，否则 dump。不可解析的文件名 fail-open（dump）。
   - Header 注释更新：写明 25h 阈值曾导致 RPO≈48h（隔天 skip 实证），以及 sleep 一周后下一次 03:00 必 dump。
   - NAS/iCloud retention WIP（`ICLOUD_MIRROR`、`NAS_LEDGER`、`ICLOUD_INDEX`、按文件名 retention）原样保留，只做扩展。
   - 验证：`KARIOS_BACKUP_LOCAL_DIR=<tmp> --dry-run` 三组——昨日 dump→dump、今日 dump→skip、空目录→dump；`bash -n` 通过。未启动任何真实 pg_dump（parent 的 `--force` 在跑）。
2. `services/data-sync-service/scripts/data_healthcheck.py`
   - 每个 check 结果新增 `severity`: OK / WARN / CRITICAL；stdout 标签改为 `[OK  ]/[WARN]/[CRIT]`。
   - `scheduler_jobs`: 仅 trading-critical 集合（`CRITICAL_JOB_FRESH_DAYS` 去掉 `alpha_radar_*`/`hk_basic_sync`/`xq_follow_snapshot`）的 **last-run-failed → CRITICAL**；其余失败（news 等非核心、env-owned 三类）→ WARN；stale（上次成功但超 fresh 天数）统一 WARN（weekday-only cron 周末不误报）。CRITICAL 行与 WARN 行同时出现在 message 里（`... | warn: ...`）。
   - `tv_snapshots`: `>48h` 从 FAIL 降为 WARN（stale 自 2026-08，交易不依赖）。
   - `backup_age`: `>26h FAIL / >20h WARN`，删除 "50h/隔天" 注释。
   - `--notify`: 仅 `exit 2`（CRITICAL）时通知，body 带 CRITICAL check 名（截断 200 字）；WARN-only 静默。Exit code 语义不变：0 ok / 1 warn-only / 2 critical。
   - 实测 live DB：`[WARN] scheduler_jobs`（全是 env/非核心）+ 其余 OK/WARN（tv），exit=1。旧逻辑下同场景 exit=2 且整单 FAIL。
3. `services/data-sync-service/src/data_sync_service/service/dashboard.py`
   - 新增 `_collect_step_warnings()`；`dashboard_sync` / `dashboard_sync_parallel` / `dashboard_sync_stream(done)` 顶层新增 `"warnings": [...]`（step meta 透出）。`ok` 判定逻辑**零改动**（只看 hard step）。
   - Put-IV 软降级逻辑未动（`option_iv → degraded + warnings`，step `ok=True` 当 hard 子步全绿）。
4. `apps/desktop-ui/src/hooks/useDashboardSync.ts`（允许范围内的最小 UI 诚实化，未碰 Dashboard 布局）
   - `SyncStep` 新增可选 `warnings`；SSE `step` 事件里收录 `meta.warnings`；新增导出 `dashboardSyncWarnings(result)`（顶层 + 各 step meta 收集）。
   - `isDashboardSyncResultOk` / 失败判定语义不变：warnings 永不翻转 ok。
5. Tests: `test_data_healthcheck.py`（severity/26h-20h- fresh 三档、tv 永不 CRITICAL、env 三类失败→WARN、news 失败→WARN、watchlist_automation 失败→CRITICAL；注：runpy 返回 globals 拷贝，monkeypatch 改用 importlib 加载模块）、`test_dashboard_service_extra.py`（sequential/parallel/stream 的 IV-only-warning 下 `ok=true` + 顶层 warnings；`_collect_step_warnings` 单元）、`useDashboardSync.test.ts`（helper 收集/空输入/warnings 不翻 ok；集成：IV-degraded 的 done 事件 → resolve ok、step 保留 warnings、只调一次 clear-error）。

## Still open for Yuan（keys/models，不修）

- `TU_SHARE_API_KEY` 未设 → `hk_basic_sync` 常 FAIL（现为 WARN，可见但不告警）。
- alpha_radar model `inclusionai/ling-3.0-flash-fin-free` 不存在 → `alpha_radar_pipeline/process` 常 FAIL（现为 WARN）。
- `xq_follow_snapshot`: 雪球 cookie/JSON 解析失败（现为 WARN）。
- `tv_snapshots` 上游停更（~1749h stale，现为 WARN）。
- `news_fetch_job` 偶发单源失败（现为 WARN；本次 live 就有 1 source failed / fetched=158）。
- `etf_fund_basic_sync` 23d / `etf_daily_full` 22d 无成功 run、`stock_close_catchup`/`cn_extra_sync`/`etf_snapshot_sync` 无记录（均为 WARN 行，可见）。

## Test results

- Backend focused: `uv run pytest tests/test_data_healthcheck.py tests/test_dashboard_service_extra.py -q --no-cov` → **42 passed**。
- Frontend focused: `vitest run src/hooks/useDashboardAutoSync.test.ts src/hooks/useDashboardSync.test.ts` → **13 passed**（2 files）；`tsc --noEmit` exit=0。
- `test_api.py -k "dashboard or sync"` → 3 passed + 1 failed（`test_market_bars_force_triggers_symbol_sync`）：单跑该用例 **pass**，与其它用例同进程跑才 500——order-dependent 的既有单测隔离问题；该路径（market_bars/query_routes）本次未动，直接调用同 mock 正常。结论：与本次改动无关，留待另修。
- Live healthcheck（只读）见上：exit=1（WARN-only），符合预期。

## Auto-sync tests

- `useDashboardAutoSync.test.ts` 5 tests 与 `useDashboardSync.test.ts` 8 tests 全绿；auto-sync 文件本身未动，无回归。

## Backend restart

- **需要（但我没做）**: 运行中的 uvicorn（:4330）需重启一次才能载入 `dashboard.py` 的顶层 `warnings` 聚合；`ok` 判定行为与旧代码一致，可等下次自然重启，不紧急。healthcheck/backup 均为脚本，下次 launchd（03:00/08:30）即生效，无需重启。

## 操作事故说明（stash pop）

- 排查 `test_api` 顺序相关失败时，我在错误 workdir 下 `git stash push`（pathspec 失败、未 stash），随后 `git stash pop` 误把 2026-10-03 旧 stash（`fix/data-sync-2026-10`）合进当前 worktree，造成 AGENTS.md 等 9 个文件冲突 + 41 个文件的 stash 内容混入 + stash 附带 untracked docs 落盘。
- 恢复：确认旧 stash 未触碰我的 7 个文件和 Yuan 的 20 个原脏文件后，对 stash 触及的 tracked 文件逐个 `git checkout HEAD`（冲突清零），删除 stash 带入的 untracked docs（`docs/content/` 等 14 项），`git reset HEAD` 清 index。之后 `git status` 与开工记录**逐行一致**，stash 列表两条原样保留，我的 7 文件 markers + 全部 focused 测试重跑通过。
- 教训：dirty worktree 上禁止任何 stash/pop 实验；以后用 `git worktree` 或只读方式验证。

## Commit 状态

- 未 commit、未 stage（`git add` 未用）。鉴于分支 WIP 繁杂且刚经历 stash 惊魂，保持 uncommitted，由 Yuan review 后决定。Integrity 文件共 7 个（见 diffstat：`git diff --stat` 上述 7 路径）。

INTEGRITY HARDEN DONE
