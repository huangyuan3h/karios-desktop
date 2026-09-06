# opt-057-058-dailybasic-twin-audit · 归档于 2026-08-31

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文只留 5 条未完成 OPT（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-058：双子星 14:30 决策链路审计（t-1 信号时序 + 核心腿 ETF 数据）

**状态**：[x] done
**完成日期**：2026-08-31
**优先级**：P0（双子星 · 今日决策卡 = 每日执行核心，须"最新、正确、逼近回测"）

#### 发现并修复

| # | 问题 | 影响 | 修复 |
|---|------|------|------|
| 1 | `_pick()`/`_stock_basket_mom_from_holdings()` 用 `closes[:-1]` 假设 daily 表最后一根是"当日盘中 bar"；**实际盘中无任何进程写 daily** → 盘中（14:30）信号回退到 **t-2**，落后回测一天 | 14:30 决策卡的 pick 可能不是 t-1 信号（mom_compare 排序/MA200 过滤临界时翻转） | `_signal_closes()` 显式剔除当日 bar（`d >= today` 跳过）→ 盘中/盘后统一取 t-1 收盘信号 |
| 2 | `etf_daily_full` cron = `0 19 1 * *`（**每月 1 次**）且最近两次运行全部 tushare `fund_daily` 限速失败 → GOLD(518880)/BOND10(511260) 停更 2026-08-21 起 7 天 | GOLD/BOND10 的 mom60/MA200 用 7 天前价格，pick 失真；GOLD 修复后 above=False 被正确过滤 | 新增 `sleeve_etf_daily_sync`（工作日 17:25，5 只核心腿 ETF `fund_daily` 增量，0.35s 间隔避限速）+ `/sync/sleeve-etfs` + catalog（order 13）+ `etf_daily_full` 保留月频 |
| 3 | trail8 盘中判定风险 | 决策卡 14:30 时 trail 不触发（daily 无当日 bar）→ 与回测"当日收盘判、次日执行"一致 ✓（验证后无需修复） | — |

#### 验证

- [x] `_signal_closes`：盘中场景（无当日 bar）信号基准 = t-1 收盘；盘后场景剔除当日 bar 后仍 = t-1 ✓（与回测 `prev_day` 语义逐位一致）
- [x] 补齐后 5 只 ETF 全部 `max(trade_date)=2026-08-31`；`_pick()` 实测 = OIL mom60 4.98%（GOLD 2.41 ✗MA 被过滤），与决策卡展示一致
- [x] 测试：`test_multi_asset_signal_freshness.py`（4 例）+ 存量 76 例全绿
- [x] shared build + UI tsc 干净

#### 反模式

- ❌ 让信号依赖"daily 表最后一根 = 当日"的隐式假设（盘中/盘后/周末语义不同 → 用 `_signal_closes` 显式表达 t-1）
- ❌ 决策路径依赖月频/超限的全市场同步（核心依赖单独小同步 + 幂等 upsert）

---

### OPT-057：stock_dailybasic 停更根因修复（双子星卫星数据依赖）

**状态**：[x] done
**完成日期**：2026-08-31
**优先级**：P0（双子星 (Twin-Star) 卫星每日依赖 `total_mv` 选低波候选）
**关联 todo**：[§19 S-3 双子星固化](../todo.md) · [`docs/backtests/state-bucket-algo-2026-08-31.md`(./backtests/state-bucket-algo-2026-08-31.md) §7.8

#### 根因

- `db/stock_dailybasic.py` 的 `sync_daily_basic_for_date` 是**孤儿函数**：全库无调用者，无调度任务写入 → 数据停在 2026-08-07（08-10 审计 P1-3 已标"滞后+无 staleness 监控"，当时判"仅回测用=低优先"，双子星上线后升级为每日实盘依赖）。

#### 修复

| 项 | 内容 |
|----|------|
| 调度 | `scheduler/daily_basic_job.py`（工作日 17:20 Asia/Shanghai，紧接 index_basic_sync 17:15）|
| 同步 | `sync_daily_basic_gap()`：表内 max 日期起逐交易日增量拉 tushare `daily_basic`（幂等 upsert + `sync_job_record`）|
| API | `POST /sync/daily-basic?end_date=`（UI 立即同步）|
| 监控 | `health_routes._SOURCES` 新增 `daily_basic` 源（label 估值市值，48h 阈值 + 周末容忍）→ 系统自检横幅可报警 |
| UI | `SCHEDULER_JOB_CATALOG` 新增 `stock_daily_basic_sync`（indexMacro 组，order 12）|
| 数据 | 补齐 08-10~08-31 缺口 **88,677 行**（16 个交易日）；卫星信号 asOf 恢复 08-28（T-1 完整交易日），无滞后 note |

#### 验证

- [x] 补齐后 `max(trade_date)=2026-08-31`，08-25~08-31 每日 ~5,545 行
- [x] `_sat_signal` 实测：asOf 08-28 · gateOpen True · breadth 0.588 · gapCount 111 · 候选 000712.SZ 等（无滞后 note）
- [x] 后端全量 pytest：165 passed（引擎/混合/提醒/路由/API 无 regression）
- [x] shared build + UI tsc 干净

#### 反模式

- ❌ 让卫星信号吞掉"信号日回退 + 滞后 note"（保留兜底，数据链路断了也要可见）
- ❌ 用 `stamp` 而非增量同步补缺口（逐日 tushare 拉取 + ON CONFLICT 幂等，可重跑）
