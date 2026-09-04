# OPT-138 红套件清零  · 归档于 2026-09-04

## 当时的目标（todo 链接）
- `docs/todo.md` P0-5：系统评估 2026-09-04 低于 8 分项之首（工程 6）
- `docs/optimization-checklist.md` OPT-138：后端全量 pytest 绿 + 前端全绿 + tsc 零报错

## 实际做了什么
- F0 遗留 mock 补齐（7）：`test_paper_trading.py` 5 例 + `test_paper_trading_extra.py`（`_patch_all` 加 fill stub）4 例 + `test_paper_s3.py` 3 例——`run_intake` 改走 `resolve_next_open_fill` 后旧测试打到真 DB，`inserted=0`
- 过期断言更新（4）：webhook `第三资产→择强单轨`、前端 export `回测口径→股票腿pick=STOCK`、trading_brief mock 改 `portfolio_health`、behavior_audit 补 STOCK pick mock
- 语义变更锁死（2）：`intraday_alarm` 低 severity 不推 Bark（新测 low 不推）；etf_daily 主/全拆分后 MODULES 指 sleeve + run_full 单测
- 时间炸弹（1）：fund_basic 硬编码 2026-08 改动态当月
- 真 bug（2）：`paper_entry_fill._next_session_after` 用 `daily.000001.SH` 当交易日历代理——指数 bar 在 `index_daily`，代理恒空，CN intake 几乎开不出仓（全库仅 1 笔 S3），改走 `trade_calendar_utils`；dev 库 `paper_trades.signal_snapshot` 是 `json` 与代码 `JSONB` 漂移（0029 ADD COLUMN 对老库是 no-op），`close_paper_trade` 带 extra 必炸——新增 Alembic `0039_signal_snapshot_jsonb`，baseline 测试 head 同步
- 慢测试（8）：close_sync 系 + postclose_smoke 打到带指数退避重试的真 tushare 路径，orchestration 测试 stub 掉 cn_extra 五件套（48 例 9 秒）
- 删幽灵 ignore：`test_watchlist_momentum_v1_1.py` 文件已不在，`pyproject --ignore` 指向空气 + lastfailed 残留，一并清掉
- 前端 tsc：`satNameTsFromAction` 参数补 `| null`（函数体本就 `?? []` 空安全）

## 验证 / 数据
- 后端：`pytest --no-cov -q --timeout=180` → **3671 passed, 3 skipped, 0 failed**（167s；修前 25 failed 且全量 15 分钟跑不完）
- 前端：875 例 → **874 passed, 1 skipped**；`tsc --noEmit` 零输出；ruff 全过
- DB 纪律：`db_rows_baseline save → targeted pytest → check` 两次 OK；全量后 +27 行经查是线上活写（2 分钟无测试运行仍 +35），非测试污染；顺手清掉自己 kill 后台任务留下的 2 paper + 4 change + 4 snapshot 孤儿行
- 遗留：`db_rows_baseline check` 在白天跑永远红（watchlist/cron 活写）——check 只在无活写窗口可信；`execution-markdown.ts` fallback 分支还留着"S-3 回测口径"旧命名，记入 OPT-140

## 后续影响 / 留给谁
- OPT-139：`twin_star_reminder_job` 等 15 个无 try 的 job（本次修测试时确认现状未动）
- OPT-140：audit_issues 卫星误报（业务口径）+ 上条旧命名
- OPT-141：Live 习惯口径引擎化（本次确认 intake 日历 bug 修完后 paper CN 腿才能正常开仓，C4 实证才有数据）
