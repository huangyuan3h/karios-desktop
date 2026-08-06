# TIP-012 研报 → Alpha 通道 · 归档于 2026-08-05

## 当时的目标（todo 链接）

- `docs/todo.md` §7 [P2] 研报 → α 通道（评级/目标价进 Watchlist 旁路，参考 Alpha Radar TIP-004 流程）

## 实际做了什么

研报通道全链路（数据源 → 存储 → 评分 → 进池 → 溯源），复用 Alpha Radar 全部下游：

- **数据源**：东财研报中心 `reportapi.eastmoney.com/report/list`（免费、无鉴权、`qType=0` 个股研报）；实测单日 40-60 份；北交所（920xxx）过滤
- **存储**：`research_reports` 表（info_code unique 幂等去重；alembic 0019 + schema_baseline 同步）
- **评分**（确定性，无 LLM）：`score = (评级×80 + 目标价空间×20) × 0.5^(days/14)`；买入=80 / 增持=60 / 目标价20%空间=+8；per-symbol 聚合 + 确认加成 +5/份（cap +10）
- **进池**：`build_research_catalyst_payload` 产出 catalyst 形状 payload → `compute_alpha_additions(score_min=70)` 复用 TIP-004 全部闸门（防守板块/Top10/缺行业）→ 合并进 automation alphaAdd，每轮 cap 10（`RESEARCH_MAX_CANDIDATES`）
- **关键坑（实测发现）**：新标的 EM 行业缓存表无行 → 31/49 被 `missing_industry` 误拒；**改用东财研报 API 自带行业字段**（`indvInduName`，与 EM 缓存同源）后 30 候选正常
- **溯源**：执行溯源仍 ALPHA/MANUAL（TIP-011 不动）；registry source 枚举 +`research`（前端 `applyAutomationRun` 按 `channel` 标记）
- **调度**：`research_report_sync` job 每 2h 抓最近 3 天增量；`/api/research/reports|stats|sync` 3 个 endpoint；SchedulerPage job 目录 + SYNC_JOB_TYPES 同步
- **前端**：automation summary 显示「研报α +N」

## 验证 / 数据

- 端到端：抓 63 → 入库 58 → 聚合 49 标的 → 闸门后 30 候选 → cap 10 进 run（药明康德 100 / 仕佳光子 86 / 东方电缆 85）
- 拒绝分布：low_score 18（增持 60 分，预期）、defense_sector 1
- 全量测试：后端 1313 / shared 57 / 前端 467 全绿；`tsc` clean

## 后续影响 / 留给谁

- **观察 2 周**：registry source='research' 的票 TrendOK/开火转化率 vs 新闻 catalyst——验证研报通道是否有正贡献（TIP-002 度量哲学）
- 可选二期：LLM 提取研报核心逻辑（复用 news_enrich 模式）为 aiSummary；前端研报列表面板（当前只有 stats API）
- 若进池噪音大：提高 `RESEARCH_SCORE_MIN`（70→75）或收紧 cap
