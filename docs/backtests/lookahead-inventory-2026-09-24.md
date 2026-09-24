# 前视（look-ahead）清单与修复台账 · 2026-09-24

> **目标**：让整个项目不再有前视问题。分三块：**(A) 已知并已修**（文档留档 + 标注）、
> **(B) 新发现待修/待删**、**(C) 护栏**（把前视从人工审计变成机器守卫）。
> **纪律**：凡引用前视版本数字的冻结档，一律加"⚠️ 前视，已修复，数字作废"横幅，不追溯重判 K1–K5。
> **关键词**：前视 look-ahead 台账 OPT-159/177/182/183/212/218/224 修复

---

## A. 已知并已修复（文档留档 + 标注）

| ID | 位置 | 前视 | 修复 | 档 |
|----|------|------|------|----|
| OPT-159 | `strategy_a1_voltarget.build_panel` | 用本月末成交额选本月票（~+20pt/yr） | 改上月末 as-of；`test_pit_no_lookahead` 钉死 | [a1-voltarget-beta](a1-voltarget-beta-2026-09-11.md) |
| OPT-177 | `pick_strong_grid.build_nav_from_cache` / `pick_strong_track.build_mom_compare_timeline` | ETF trail8 用**当日收盘**触发却记 0 收益 | trail 改 t-1；`trail_causal=True` 默认 | [audit-trail8](audit-trail8-2026-09-12.md) |
| OPT-182 | 卫星 `same_1430`（`state_bucket_track`） | `daily`(qfq) × `bar_5min`(raw) **基期混用** | `_mark`/`_raw_ratio` 重建 raw；广度 MA 用 raw | [audit-three-strategy](audit-three-strategy-lookahead-2026-09-14.md) |
| OPT-183 | `backtest_engine._load_calendar` | `daily` CN/HK 同表 → 并集日历污染（HK-only 日加速 CN 腿） | 按 `config.market` 过滤；`state_bucket_track` 固定 CN-only | [audit §F](audit-three-strategy-lookahead-2026-09-14.md) |
| OPT-212 | `backtest_engine` 同日进出 / `blend_overlay_timeline` 吞首日 / 5min 回填覆盖 live | 建仓日当天评估出场（幽灵交易）；首日收益被吞；`DO UPDATE` 回填覆盖 | holding==0 跳过出场 + MTM at-cost；首日修；回填切 `on_conflict="nothing"` | [OPT-212](../optimization-checklist.md) |
| OPT-218 | `next_open` 回退查实时 DB / A股 T+1 / next_open 涨停基准日；ETF 尾部拼接基准断裂 | 模拟日之后分数可见；成交日按当日 close 出场；raw 接复权尾 | `skip_live_score_lookup=True`；丢非 force 当日出场；`base_day`；按锚点缩放尾部 | [OPT-218](../optimization-checklist.md) |
| OPT-224 | `_breadth_at_1430` 薄样本 | 14:30 闸只看 20–50 只 print → 幽灵开闸 | `MIN_BREADTH_COVERAGE=0.2` + 现场快照落库 + 源优先级 + 派生 15:00 mark | [OPT-224](../optimization-checklist.md) |
| — | `pick_strong_track`/`pick_strong_grid` 之外的 14:30 时钟统一 | 旧全天振幅排序键 / 收盘卖 / 收盘广度 | HABIT_RECIPE：`amp_1430` + `exit_hhmm=1430` + `gate_1430=True` | [OPT-182](../optimization-checklist.md) |

**结论**：以上均**已修复**，其"修复前数字"作废；引用它们的文档以对应审计档为准。已给下列档加横幅：
`audit-trail8-2026-09-12.md`、`audit-three-strategy-lookahead-2026-09-14.md`、`SUMMARY.md`。

---

## B. 新发现待修 / 待删（2026-09-24 代码审计）

| # | file:line | 问题 | 计划 | 影响面 |
|---|-----------|------|------|--------|
| B1 | `service/portfolio_nav_sim.py:168-190` | **同一日**用当日收盘决定 sleeve 持仓（MA200/trail8），并记**当日收益** = OPT-177 同族 | ✅ **已修**（2026-09-24，改 t-1 因果 + 幽灵探针 `test_no_lookahead_on_exit_day`） | `run_walk_forward_dual.py`、`impulse_walk_forward.py`、`multi_sleeve_walk_forward.py`、`multi_sleeve_grid.py`、`sleeve_nav_sim.py`；OPT-119 时代 sleeve 数字（修复前偏高） |
| B2 | `scripts/sleeve_exit_variants.py:56-83` | 复制 B1 同一前视 | ✅ **已删脚本**（2026-09-24）；`sleeve-exit-study.md` 加"⚠️ 前视已修复、数字作废"横幅 | 该档 |
| B3 | `state_bucket_track.py` | `rank_key=None`（全天振幅旧前视键）+ `gate_1430=False`（收盘广度）是 **`same_1430` 的默认** | ✅ **已加护栏**：不安全组合发 `logger.warning`；`allow_lookahead=True` 显式静默（冻结旧臂专用）；测试 3 条 | 任何忘记传参的 `same_1430` 调用方 |
| B4 | `scripts/compare_sat_*.py` + `diag_sat_*`、`compare_twin_realistic.py` | 研究脚本复现旧口径（`rank_key`/`gate_1430` 缺省）→ 基线腿带前视 | ✅ **已标注**：`backtests/README.md` 加时代口径警告 + 14 份 `sat/*.md` 加横幅；B3 警告运行时 surface | 各 sat 实验档 |
| B5 | `backtest_engine.py:263-267,766` | `exit_at_1430` 声明+加载 14:30 print 但**从未消费**（死旋钮） | ✅ **已删**（配置字段 + 数据加载条件）；`next_1430` 入场保留；注释说明引擎无 14:30 出场腿 | — |
| B6 | `ext_minute_csv.py:246` | 历史 vendor CSV 导入默认 `on_conflict="update"`（可覆盖 live 14:30 print） | ✅ **已修**：切 `on_conflict="nothing"`（回填永不重写） | 5min 回填 |
| B7 | `scripts/rebuild_cn_daily_qfq.py` / `cn_reseed_qfq_tx.py` / `hk_reseed_qfq.py` | qfq 全序列重写（含未来分红口径） | ✅ **已声明**：三脚本加 "LOOK-AHEAD DECLARATION"（qfq 依赖未来分红 → 冻结窗可漂移，须记数据版本 L5） | 全库 qfq |
| B8 | `state_bucket_track._universe_where` | 无显式 `.HK`/`market` 过滤，靠 `stock_dailybasic` mv 门 | ✅ **已加显式过滤** `d.ts_code NOT LIKE '%.HK'`（测试同步） | CN/HK containment |

---

## C. 护栏（防再犯）

1. **`same_1430` 契约**：`replay_sgap_from_context` 当 `fill_mode=same_1430` 时，若 `rank_key` 为 `None` 或 `gate_1430=False`，默认应 **raise**，除非显式 `allow_lookahead=True`（冻结实验专用）。把"默认不安全"变成"默认拒绝"。
2. **sleeve trail 契约**：`simulate_sleeve_nav` 与 `parking_replay` 统一 t-1 因果；加合成幽灵探针测试（信号日/当日反向大波动 → 断言零前视收益），仿 `test_engine_entry_timing.py`。
3. **L-pattern grep 进 CI/审计**：`trade_date >= end`、`shift(-`、`mp.get(day)` 出现在决策/trail、缺 `market` 过滤的 `_load_rows` 复制、`on_conflict="update"` 回填。
4. **OPT-213**：`run_walk_forward.py --verdict v2` 自动算 G1/余量/铰链 + L 门逐条 tick。

---

## D. 待办（滚动）

- [x] B1 修 `portfolio_nav_sim` + 幽灵探针（2026-09-24）
- [x] B2 删 `sleeve_exit_variants.py` + 档横幅
- [x] B3 加 `same_1430` 契约护栏（warning + opt-out + 测试）
- [x] B5 删 `exit_at_1430` 死旋钮
- [x] B6 CSV 回填切 `on_conflict="nothing"`
- [x] B8 `_universe_where` 加显式 `.HK` 过滤
- [x] B4 sat 研究脚本/档加"旧臂带前视"标注（README + 14 档）
- [x] B7 qfq 重灌文档声明（3 脚本）

## E. 验证

- `tests/test_portfolio_nav_sim.py` 9 passed（含幽灵探针）
- `tests/test_state_bucket_track.py`（UniverseWhere + LookaheadGuard）6 passed
- `tests/test_engine_*` + `test_backtest_engine*` 366 passed
- ruff clean（改动文件）
