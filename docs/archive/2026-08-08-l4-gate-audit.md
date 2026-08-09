# L4 前全模块排查与加固计划（L4-Gate Audit）

> **完成日期**：2026-08-08 · 归档于 2026-08-08
> **todo 链接**：docs/todo.md §17（L4 准入 · H1~H10 + K1/K4 全部勾选）
> **一句话结论**：L4-Gate 9 项横切检查 + P0/P1/P2 加固全清——4 个 live bug 根因修复（intake key 错位 / camelCase 错位×2 / journal 上游校验缺失）、测试基建纪律化（27 张表行数零变化验收）、fail-open 语义显式化、调度幂等 + 本地 CSRF 防线补齐；**后端 1435 passed + 前端 515 passed + tsc 干净**；剩余 flaky 1 个（test_decision 快照 roundtrip 时序，根因已修，连跑稳定）；覆盖率目标写入 §8（BE 90% / FE 40% 三波推进）。

> **何时看**：进入 L4（券商对接/执行闭环）之前的必做清单。L3 五里程碑已完成，但同日推进速度较快，且过程中暴露过 3 个 live bug（intake key 错位、service/db camelCase 错位、测试数据污染）+ 1 个数据污染事件——说明**口径与隔离问题在系统里是系统性风险，不是个例**。
> **目标**：全模块过一遍，消灭 P0/P1 级「影响判断和逻辑」的问题；对每个模块做加固；建立可复现的验收标准。
> **状态**：已完成（2026-08-08 归档）。

---

## 0. 为什么要做（背景）

2026-08-07 一天内完成了 L3-P1~P5（OPT-062~067），但过程中挖出的问题证明**「看起来正常」不等于「逻辑正确」**：

| 事件 | 影响 | 根因类别 |
|------|------|----------|
| intake 读 journal 的 key 错位 | paper 自上线**从未有真实数据**（candidates=0） | 数据口径（camelCase vs snake_case） |
| service 层读 db camelCase 错位 | run_update **永不更新/平仓**（231 条交易卡死） | 数据口径 |
| requires_postgres 测试不清理 | 230+ 假数据污染 paper/快照/日志 | 测试隔离 |
| journal 上游 hash symbol | 决策日志 75% 假行 | 上游校验缺失 |

**教训**：这些 bug 都存在数周且测试全绿——因为测试 mock 用了与 db 层不一致的 shape。排查必须**以「真实数据形状」为准**，而不是以代码注释为准。

---

## 1. 排查方法论（横切检查清单）

每个模块过一遍以下 9 项，逐项记录「检查结果 / 风险等级 / 是否修复」：

### 1.1 数据口径一致性（P0 · 已抓 3 例同类）

- [ ] **db 层 `_row_to_dict` 类函数的 key 形状**（camelCase）与**消费方读取 key**（snake_case 或 camelCase）逐一对照
- [ ] 已知现场：`service/decision.py:118-125` 读 `entry_date`/`pnl_pct`（db 返回 `entryDate`/`pnlPct`）→ **paper 数据进不了决策快照/归档**（确认是否如此）
- [ ] 方法：对每个 `list_*` / `get_*` / `fetch_*` 的返回 dict，grep 所有消费方，检查读取 key 是否匹配
- [ ] 防复发：服务层统一用「双 key 兼容 helper」（`_row_str`/`_row_number`，参考 paper_trading）或约定「服务层只读 camelCase」

### 1.2 前视偏差（P0 · 影响回测/统计可信度）

- [ ] 任何「按日期回放」的逻辑（backtest_engine 已做 as-of 注入；还有别的吗？）
- [ ] `weekly_review` / `exit_attribution` 是否有读「未来数据」的路径
- [ ] TrendOK/score 历史查询是否有 as-of 约束（`fetch_latest_score_since` 语义）

### 1.3 幂等与调度（P1）

- [ ] 每个 cron job：重复执行是否幂等？（paper intake 已 OK；其他 job 检查 ON CONFLICT / 日期覆盖语义）
- [ ] 调度时间与时区：所有 `Asia/Shanghai` cron 是否一致；周末/节假日跳过逻辑
- [ ] `close_sync` → `post_close_sync` → `watchlist_automation` → `paper intake/update` 的执行顺序与数据依赖（盘后链路）

### 1.4 时区与交易日历（P1）

- [ ] `today_iso()` vs 上海交易日；跨日/跨周末的 `_holding_days_for`（日历天 vs 交易日，是否接受）
- [ ] HK 交易日历（与 A 股节假日不同）在 paper/相关性里的处理（correlation 已 union 对齐；paper 用的是 calendar days——已记录）

### 1.5 fail-open 语义（P1 · 影响决策正确性）

- [ ] 数据缺失时的行为清单：score_floor（OK）、pool_exit（OK）、SRV/ETF flow（已定义）——**逐一确认「缺数据时是保守还是激进」**，缺数据时错误方向=把 DEFEND 判成 ATTACK 之类

### 1.6 数值健壮性（P1）

- [ ] `positionPct`/`costPrice`/`score` 的 float 解析（`float(None)` 崩溃点）；除零（entry_price <= 0 已处理，其他除法？）
- [ ] 空列表/空 dict 遍历（`max()` on empty、`[0]` index on empty）

### 1.7 测试隔离（P1 · 已修 1 文件，还有 25 个 requires_postgres 文件）

- [ ] 26 个 `requires_postgres` 测试文件逐个检查：是否插数据 + 是否有 teardown
- [ ] 已知已修：`test_execution_source_db.py`（teardown 清理）
- [ ] 高风险文件：`test_api.py`、`test_alpha_radar_upsert.py`、`test_daily_db.py`、`test_hk_daily.py`、`test_index_daily.py`、`test_macro.py`、`test_industry_fund_flow_read.py` 等——**跑前/跑后表行数对比**

### 1.8 API 契约漂移（P2）

- [ ] Pydantic 模型 ↔ `_row_to_dict` ↔ 前端 TS 类型 三处一致性（重点：v1_business、backtest、decision）
- [ ] `docs/api/` 是否与实现同步

### 1.9 安全与密钥（P2）

- [ ] `.env` 泄漏检查（gitignore、API key 不进代码）
- [ ] `/v1/*` 鉴权覆盖范围（哪些 endpoint 无鉴权暴露了家庭持仓数据）

---

## 2. 模块清单与责任区

### A 级（直接影响交易判断 · 优先排查）

| # | 模块 | 关键文件 | 主要风险 |
|---|------|----------|----------|
| A1 | 决策链（前端核心） | `apps/desktop-ui/src/lib/execution-action.ts`（suggestFireSizePct / deriveActionCard / evaluateNewEntryGates / 止损链） | 尺寸 min 链逻辑（新增 correlation 后 6 项 min 的正确性）、T+1 锁、sleeve 账本 |
| A2 | Paper 交易 | `service/paper_trading.py` + `db/paper_trading.py` | **刚修过 2 个 key bug，复查 + 双 key helper 覆盖全路径** |
| A3 | 执行闸门 | `service/execution_gate.py`（ATTACK/HOLD_ONLY/DEFEND 状态机） | 优先级顺序（硬 DEFEND > SRV > regime > ETF flow）、V6.3 溢出豁免边界 |
| A4 | TrendOK 评分 | `service/trendok.py`（2084 行） | 分数构成、stoploss ratchet、realtime 合并 |
| A5 | 进池自动化 | `service/watchlist_automation.py` + `db/watchlist_automation.py` | **list_registry 的 payload 展开（positionPct 顶层）——所有消费方 key 是否匹配**；alpha 进池闸 |
| A6 | 决策日志 | `service/execution_journal.py` + `db/execution_journal.py` | diff 语义（刚加 symbol 防御）、`list_changes` 的 camelCase（**intake 已按此修正，其他消费方？**） |
| A7 | 归因与统计 | `exit_attribution.py` / `weekly_review.py` / `backtest_engine.py` | 双 key 兼容已做；样本量语义；前视 |
| A8 | 组合风控 | `service/correlation.py` | 簇映射完整性（漏簇=漏保护）、ETF 前缀覆盖、经验相关 fail-open |

### B 级（影响数据/信号质量 · 次优排查）

| # | 模块 | 关键文件 | 主要风险 |
|---|------|----------|----------|
| B1 | Alpha Radar 全家桶 | `alpha_radar_pipeline/process/ingest/mapping/qa/risk/filter/catalyst.py` | symbol 解析（hash 防御是否覆盖）、映射惩罚、QA 数据源 |
| B2 | TV 抓取 | `tv.py` / `tv_capture_worker.py` / `tv_chrome.py` | 快照解析容错、Scanner API 数组格式、job 队列 |
| B3 | 研报通道 | `research.py` | 评分确定性、cap、去重 |
| B4 | 新闻体系 | `news.py` / `news_enrich.py` / `morning_brief.py` | enrichment 失败降级、ticker 提取 |
| B5 | 行业资金流/主线 | `industry_fund_flow*.py` / `mainline.py` / `industry_taxonomy.py` | 行业名归一化（东财 vs SW L1）、日期覆盖 |
| B6 | 市场情绪/宏观 | `market_sentiment.py` / `market_regime.py` / `sector_rotation_index.py` / `macro_snapshot.py` | breadth 降载缓存、TTL、as-of |
| B7 | 行情同步 | `daily.py` / `adj_factor.py` / `hk_daily_*.py` / `etf_daily.py` / `fund_basic.py` | 复权因子、增量幂等、yfinance 限流 fallback |
| B8 | 机构席位 | `top_inst_flow.py` / `option_iv.py` / `sector_rotation_index.py` | 数据浅（2 个月）——确认消费方 fail-open |

### C 级（工程健壮性 · 有余力做）

| # | 模块 | 关键文件 | 主要风险 |
|---|------|----------|----------|
| C1 | API 面 | `v1_business_routes.py` / `backtest_routes.py` / `decision_routes.py` | 契约漂移、错误码 |
| C2 | 调度 | `scheduler/*.job.py`（25+ 个） | 时区、幂等、失败重试 |
| C3 | 决策 Agent | `decision.py`（TIP-015）/ ai-service `/decision` | **decision.py:118 key 错位（已发现现场）**、上下文注入 |
| C4 | Broker 截图 | `broker.py` | 解析失败降级 |

---

## 3. 已知问题清单（先修 · 立即可复现）

| # | 问题 | 位置 | 等级 | 修复方案 | 状态 |
|---|------|------|------|----------|------|
| K1 | **paper 数据进不了决策快照**：`decision.py` 读 `entry_date`/`pnl_pct`（db 返回 camelCase）→ 决策归档/上下文里 paper 部分为空 | `service/decision.py:118-125` | **P0** | 双 key 兼容或改 camelCase；补测试（mock 用真实 camelCase shape） | **[x] 2026-08-08** |
| K2 | requires_postgres 测试隔离：26 个文件中多数无 teardown，跑全量测试可能再次污染 dev DB | `tests/test_*.py` | **P1** | 逐个检查插数据点 + teardown；高风险文件先跑前后行数对比 | [ ] |
| K3 | 前端测试 mock 的 journal/paper shape 是否与 db 一致（测试掩盖 bug 的根源） | `apps/desktop-ui` + 后端 tests | **P1** | 排查计划 1.1 的执行产物：统一 mock shape 约定 | [ ] |
| K4 | correlation 簇映射完整性未验证（漏簇 = 漏保护） | `service/correlation.py` | P1 | 用全量 registry + 历史 score 行业列表回归簇映射，输出「未归类持仓」清单 | **[x] 2026-08-08** |
| K5 | 盘后链路顺序依赖（close → post_close → automation → paper）无端到端冒烟测试 | scheduler | P2 | 一次「盘后全链路 dry-run」脚本 | [ ] |

---

## 4. 加固项（按优先级 · 每项含验收标准）

### P0（进入 L4 前必须完成）

**H1. 数据口径全量审计（K1 + 1.1）**
- 做法：对所有 db 层返回 dict 的消费方做 key 形状对照表（脚本辅助：grep `t.get("snake")` vs `_row_to_dict` 的 camel）
- 验收：`decision.py` paper 快照有真实数据；审计表里无「snake 读 camel」残留；新增服务层测试用 camelCase mock
- **状态：[x] 2026-08-08**。审计结果：

| db 层返回模块 | shape | 消费方核对结果 |
|---------------|-------|----------------|
| `db/paper_trading`（list/get/update） | camelCase（entryDate/pnlPct/…） | decision.py **4 处错位已修**；exit_attribution L172 `pnlPct` **漏网错位已修**；alpha_radar_qa/weekly_review ✓ |
| `db/execution_journal`（list_changes） | camelCase（newValue/oldValue/…） | decision.py **2 处错位已修**；filter_journal_changes/v1_explain 双 key ✓；v1_business 双 key ✓ |
| `db/watchlist_automation.list_registry` | symbol/source/addedAt + payload 顶层展开（camel） | 全消费方（trendok/paper/intake/backtest/v1_business/decision）一致 ✓ |
| `db/decision` | camelCase（_row_dict 映射） | service/decision ✓；list_actions 的 snapshot_date 为 snake 但无消费方读取（记录在案） |
| `db/morning_brief` / `db/trade_review` | camelCase | api 层 Pydantic 透传一致 ✓ |
| `db/tv_capture_jobs` | snake_case | service/tv.py 读 snake ✓ 一致 |
| 其余 db 模块（daily/industry_fund_flow/macro 等） | snake_case（tushare 风格内部契约） | service 读 snake ✓ 一致 |

- **附带发现并修复（K1 之外）**：
  1. `service/decision.py` **未 import `json`** 却调用 `json.dumps/loads` → NameError 被 except 吞掉 → `extract_pending_actions` 调度（decision_action_job）**从未真正工作**（processed 恒空）。已加 `import json`。
  2. 测试 mock 与 db shape 不一致（K3 根源实证）：`test_decision.py` 的 `list_changes` mock 用 snake `new_value`，恰好配合 bug 代码「假绿」——已改真实 camelCase mock。
- decision.py 覆盖率：**43% → 99%**（新增 13 个纯逻辑测试，全 camelCase mock，无 DB 写入）。

**K4. correlation 簇映射回归（2026-08-08 完成）**
- **真实数据回归结果**：当前 4 个持仓全部受保护（HK:00700→tech_hk、ETF:513180→tech_hk、CN:300628→tech_comm、CN:601899→metal）✓；无「未归类持仓」。
- **规则覆盖缺口（修复）**：27 个 registry symbol 归 other 中，8 只科创板芯片股（688981 中芯/688041 海光/688256 寒武纪/688008 澜起/688012 中微/600584 长电/603986 兆易/688072）行业标为东财一级「**电子**」→ 规则未命中 → **未来买入无簇保护**。已补规则（顺序敏感）：
  - 「电子」→ semiconductor（**必须在「消费电子」之后**，子串匹配）；「元件」「军工电子」→ semiconductor
  - 「印制电路板」→ tech_comm（PCB/CPO 链，沪电 002463）
  - 「小金属」→ metal（锡业 000960）；「化学制药」→ health
  - 修复后芯片股全部 → semiconductor ✓
- **遗留（记录，非 K4 scope）**：
  1. 16 个 watchlist symbol `industry=None`（stock_eastmoney_industry 缺行：药明康德 603259/百济 688235/天合光能 688599 等）——根因 **stock_basic 表 CN codes=0** → 东财行业增量 sync 永远无 missing codes → 表卡在 1630 行。属 B7 行情同步问题，待修。
  2. **fail-open 语义**：行业缺失 → `other` → 不参与 cap = **漏保护（激进方向）**——列入 H5 fail-open 清单。
- correlation.py 覆盖率：**57% → 95%**（+10 测试：K4 规则/em_industry 查询与失败/矩阵全路径/部分对齐/fail-open/带矩阵 evaluate）。

**H2. 盘后决策链端到端冒烟（K5 前置）**
- 做法：脚本模拟一个完整交易日：journal 有 BUY 信号 → intake 建仓 → 次日 update → 平仓 → 归因/周报可见
- 验收：全链路数据在 API 上可见；所有消费方无 key 报错
- **状态：[x] 2026-08-08**。`tests/test_postclose_smoke.py`（requires_postgres，teardown 全清）：

| 步骤 | 链路 | 验证点 |
|------|------|--------|
| 1 | `ingest_snapshot`（真实 DB 写 snapshot+changes） | changed=True、card 校验通过 |
| 2 | `run_intake`（仅价格 mock） | candidates=1、inserted=1、open 行 entryDate/market/source 正确 |
| 3 | `run_update`（价格 2x → target_hit） | closed=1、closeReason=target_hit、pnlPct/grossPnlPct/costsPct 非空 |
| 4 | `analyze_exit_attribution` | byReason.target_hit.count>=1（K1 修复的 pnlPct 读取验证） |
| 5 | `build_weekly_review` | paper.closed>=1、byReason.target_hit、avgNetPnlPct 非空 |

- **抓到生产 bug（K 级新增）**：`run_intake` 的 `action` 变量是函数级作用域——过滤循环遍历所有 changes 后残留为**最后一条 action change 的值**，插入循环 `side=action` 全用它 → 尾部是 WATCH/TRIM/EXIT 时**所有 insert 失败或记错 side**（这解释了 paper_trades 长期只有 1 行）。已修复（每个 candidate 重新读取）+ 回归测试 `test_run_intake_insert_side_not_leaked_from_last_change`。
- **数据隔离要点**：ingest 的 diff 会对比真实最新快照 → 跨日反向变化污染真实 symbol 日志（672 行已清）；冒烟用「空基线快照」做 diff 起点，只产生测试 symbol 的行。
- 验收：冒烟连跑 3 次稳定；全量 1416 passed；27 张表零变化。

**H3. 测试隔离复查（K2）**
- 做法：26 个 requires_postgres 文件跑前/跑后对比关键表行数（paper_trades / execution_* / daily / *daily 抽样）
- 验收：全量测试跑完，表行数不变；有插入的测试全部加 teardown（AGENTS.md 纪律生效）
- **状态：[x] 2026-08-08**。审计结果：

| 文件 | 问题 | 修复 |
|------|------|------|
| `test_alpha_radar_upsert.py` | 3 测试插 source+document 零清理 | 加 autouse teardown（`DELETE FROM alpha_radar_sources WHERE id LIKE 'test-src-%'`，CASCADE 清 documents/trends） |
| `test_api.py::test_dashboard_sync_endpoint_shape` | 真实触发盘后同步 → 每次跑 capture 快照+写 changes 残留 | mock `dashboard_sync_parallel`（测试本意是 endpoint shape） |
| `test_api.py::test_broker_accounts_state_shape` | POST /broker/accounts 创建账户不清理（**历史累积 233 个 Test Account**） | finally 删除 account + state |
| `test_api.py::test_alpha_radar_endpoints_shape` | init-defaults 有 `disable_sources_except` 副作用（禁用非默认源） | mock `add_default_sources` |
| `test_decision.py::test_session_crud_roundtrip` / `test_delete_message_endpoint` | 插 session 不清理（**历史累积 141 个测试 session**） | finally 删除 session（CASCADE 删 messages） |
| `test_decision.py::test_snapshot_build_and_search_roundtrip` | ① upsert 覆盖当天真实快照（status 打回 open）② **flaky 根因**：消息 `created_at=UTC now` 与 `shanghai_today()` 窗口在 UTC 跨天时错位（每天仅 16/24 概率过） | 快照 save/restore + 消息时间钉到 UTC 当日 + 显式传 UTC snapshot_date；连跑 3 次稳定 |
| `test_execution_source_db.py::test_insert_changes_persists_source_field` | 硬编码 `CN:600000`（真实浦发银行！）未注册清理集合 → **污染真实 symbol 的决策日志**（历史 14 条） | 改用 `_fresh_symbol()`（自动注册清理） |

- **历史残留清理**（修复前的累积）：233 个 Test Account + 141 个测试 session/102 消息 + 48 条 snap-tgt 假 changes 已删；真实数据保留（1 个真实账户、5 个真实 session、3 个快照）。
- **新增验收工具**：`scripts/db_rows_baseline.py`（save/check 27 张关键表行数）——全量测试跑完 `check` 必须 OK。最终验收：1404 passed 且 27 张表零变化。

### P1（L4 启动前完成）

**H4. 前端决策链单元回归（A1）**
- 做法：`execution-action.ts` 的尺寸 min 链（clip/single/sector/sleeve/risk/correlation 6 项）构造边界测试矩阵；止损链（HardStop/TrailStop）ratchet 测试
- 验收：新增 ≥10 个边界用例（0 仓位、满仓、负 room、ETF 豁免、correlation 绑定）
- **状态：[x] 2026-08-08**。`execution-action.test.ts` 追加「H4 boundary matrix」13 个用例：

| 边界 | 用例 |
|------|------|
| 满仓 | positionPct=15（零 room）→ null；positionPct=18（负 room）→ null |
| 负 room | sector sum 35% → null；sleeve 62/60 → null；roomCorrelation=-2 → null |
| 零 room | sleeve=60/60 → null；sleeve=55 与 clip 平 → note=sleeve |
| 微小 room | sector 29.95%（room<0.1）→ null |
| ETF 豁免 | positionPct=14.9 + isEtf → 仍 clip 5（single cap 豁免） |
| risk 边界 | riskCap==clip（stop 10%）→ note 保持 clip（须严格更紧才标 risk） |
| 止损 ratchet | trailStop>hardStop → exitStop=trailStop；hardStop>trailStop → exitStop=hardStop；pnl<10% 永不武装 |
| correlation 绑定 | BUY 在 cluster 27.5% → Suggest 2.5 note=correlation；ADD 在 31% → CORRELATION_CAP_BLOCK |

- 验收：前端全量 **515 passed / 1 skipped** + tsc 干净。注意：测试改动后跑全量 vitest 确认（曾因全局 sed 误伤 1 个既有用例，git checkout 还原后重新追加）。

**H5. fail-open 语义清单（1.5）**
- 做法：把「缺数据时的默认行为」整理成表（模块 × 数据源 × 缺数据行为 × 方向是否正确）
- 验收：表完成；发现「缺数据=激进」的项已改为保守或加测试锁定
- **状态：[x] 2026-08-08**。扫描 11 个核心 service 文件（75 处 except 分支），清单：

**已修复（缺数据=激进 → 保守）**：

| 项 | 位置 | 原行为（激进） | 修复（保守） | 测试 |
|----|------|----------------|--------------|------|
| 1 | `trendok._read_latest_sentiment_for_macro_lock` | 情绪表读取异常 → (None,None) → **崩盘禁买锁失效** | 失败 → extreme_caution（锁激活）+ **不缓存**（恢复后自动解锁） | `test_trendok_macro_lock` +2 |
| 2 | `trendok.compute_trendok_for_symbols` | registry 读取异常被吞 → 全部视为未持有 → **批量删除所有存量止损**（破坏性） | registry 状态 UNKNOWN → 跳过删除（fail-closed 保留保护） | `test_compute_trendok_keeps_stored_stoploss_when_registry_read_fails` |

**高危项记录（设计权衡，不修）**：

| 项 | 位置 | 行为 | 处理依据 |
|----|------|------|----------|
| 3 | `trendok` 日内风控（riskMetricsLive） | 最新 bar ≠ 今日 → 日内暴涨/跳空追高拦截失效 | bar 陈旧与休市/未开盘无法区分；误保守会全天误杀。关联 H6 |
| 4 | `execution_gate` BREADTH_PANIC | downCount 缺失→0 → panic 不触发 | sentiment 层已有 errors→caution 兜底（market_sentiment.py:1194） |

**中低危记录（18 项中的其余）**：SRV 缺失仍可 ATTACK（已文档化 SRV_UNKNOWN）；CN/HK 指数信号 <2 条用任意 2 条 fallback；行业 Top10 门槛缺失时跳过（TIP-004 已文档化）；清池缺数据不移除（池污染）；auto-QA 惩罚缺失按 0；premium/炸板率失败按 0 不产生 caution；Alpha S 催化剂加载失败按空集（保守）；enrichment/机构席位/期权 IV 全部 fail-closed 或中性。

**顶层防线整体健康**：execution_gate 状态机、sentiment errors→caution、Alpha grade 缺省 "B"、watchlist 分数门槛——全部「缺数据→阻止」。

**H6. 时区/日历一致性（1.4）**
- 做法：paper `_holding_days_for`、cron 触发、HK 交易日差异——文档化 + 测试
- 验收：跨周末持有天数测试；HK 假期不产生「假更新」
- **状态：[x] 2026-08-08**：

| 项 | 结论 |
|----|------|
| 调度时区 | 22 个 cron job 全部 `Asia/Shanghai`（scheduler 默认 UTC 被 trigger 自带时区覆盖）；6 个 interval job 时区无关；盘后链顺序正确（close 17:10 → automation 17:30 → intake 17:40 → update 17:45 → decision 18:30）；intake/update/automation/decision 全 `1-5`；close_sync 无 1-5 但内部有非交易日跳过 ✓ |
| **修复：`_messages_on` UTC 边界 → 上海日界** | 旧实现用 UTC 日期窗口 + `shanghai_today()` 用主机本地时区——上海凌晨（UTC 前一日）消息被静默漏出快照窗口。改：`SHANGHAI_TZ = +08:00` 显式常量 + `_messages_on` 用上海 00:00~23:59:59.999999 边界 |
| **修复：`_holding_days_for` None 崩溃** | `date.fromisoformat(None)` 抛 TypeError（只 catch ValueError）。改 `str()` 归一 + 双异常 catch；测试锁定跨周末=3/跨月=3/无效=0/反向=0 |
| HK 日历差异 | paper `_holding_days_for` 计**日历天**（v0 已文档化权衡）；correlation 经验相关用 union 对齐（已做）；HK 无 score_floor（fail-open 保守）。文档化，不改语义 |

**H7. 数值健壮性扫描（1.6）**
- 做法：grep `float(`、`int(`、`/ `、`[0]` 关键路径；对 None/0/空输入补 guard
- 验收：关键服务函数对 None/空输入不抛异常（fail 有日志）
- **状态：[x] 2026-08-08**：

| 修复 | 位置 |
|------|------|
| 评分函数 None 守卫 ×6 + ema20_prev | `trendok._clip01` / `_score_sub_ema` / `_score_sub_macd` / `_score_sub_breakout` / `_score_sub_rsi` / `_score_sub_volume` ——None 输入返回 (0.0, 0.0) 而非 TypeError（测试锁定 `test_score_subs_are_none_safe`） |
| `_holding_days_for` None/TypeError | 见 H6 |

- **扫描结果**：trendok 空列表指标（_atr14/_ema/_rsi/_macd/_bonus）返回 None/[]/0.0 ✓；correlation 全空输入 OK；`week_bounds` 非法输入显式 ValueError（输入校验，设计如此）；`round_trip_cost_pct('XX')` 显式 ValueError（closed enum）✓；db 层 `_float`/`_int` 已带 try ✓；`int(raw)`/`float(conf)` 均为环境变量/内部可信数据。

### P2（有余力做）

**H8. API 契约漂移表（1.8）**：三处 key 对照 + docs/api 同步
- **状态：[x] 2026-08-08**：

| 检查 | 结果 |
|------|------|
| `docs/api/` 与实现 | 存在（business/discovery/explain/errors/openapi + api-contract.md 设计文档 + CHANGELOG 0.1.0 机制）✓ |
| Pydantic ↔ 实际响应（自动对照脚本） | `/v1/market/snapshot`、`/v1/paper-trades`（18 字段）、`/v1/watchlist/items` 全匹配 ✓ |
| dict 返回的 API（decision/analysis、watchlist/registry、correlation-status） | 已有 shape 测试锁定（test_decision/test_api/test_v1_business） |
| **修复：前端 `CorrelationStatusResponse.okBook` 死字段** | 类型定义了 `okBook: boolean` 但后端从不返回且前端无人消费——已从类型删除（对齐实际契约），tsc 干净 |

**H9. 调度幂等复查（1.3）**：每个 job 的重复执行语义
- **状态：[x] 2026-08-08**：

| 类别 | 幂等机制 | 结论 |
|------|----------|------|
| 数据同步（daily/index_daily/index_basic/macro_daily/etf/industry_flow/mainline/sentiment/stock_basic/eastmoney/stoploss/trade_calendar/tv/alpha/news/research/decision/watchlist_score，26 模块） | 全部 ON CONFLICT upsert | ✓ 重复跑覆盖不重复 |
| 决策快照（ingest_snapshot） | content_hash heartbeat：同内容 → touch 不插行 | ✓ **新增测试锁定**（`test_ingest_snapshot_is_idempotent_on_same_content`：重放不产生新 change） |
| paper intake | (symbol, entry_date, side) 唯一索引 + duplicate skip | ✓ 已有测试 |
| broker state | INSERT 前存在性检查 | ✓ |
| news items | md5(link) 幂等 id + ON CONFLICT | ✓ |
| tv screeners | upsert ON CONFLICT | ✓ |
| watchlist_automation_runs | uuid 追加（run 历史日志语义） | ✓ 记录型非数据污染 |
| alpha pipeline | 批次 fail-closed（入库 0 → 删除新批次保留旧卡片） | ✓ |

- 无需要修复的幂等缺陷；补 ingest 幂等测试 1 个（H9 验收）。
**H9. 调度幂等复查（1.3）**：每个 job 的重复执行语义
**H10. 安全扫描（1.9）**：gitignore/.env/密钥 + /v1/* 鉴权面
- **状态：[x] 2026-08-08**：

| 检查面 | 结果 |
|--------|------|
| `.gitignore` / 密钥入库 | `.env` 已忽略且未追踪；硬编码密钥全库扫描 0 命中（sk-/api_key/secret/password 排除 env 读取）✓ |
| `.env.example` 漂移 | 补 `GEMINI_API_KEY=`（ai-service model.ts:256 消费，example 缺失导致新环境配不了 Gemini）✓ |
| /v1/* 鉴权面 | business（7 只读）+ explain 挂 `require_api_key`（KARIOS_API_KEYS 为空时 opt-in 关闭）；quota 挂 `enforce_quota`（含配额）；discovery 4 端点无鉴权（设计如此）✓ |
| 网络面 | 后端绑定 127.0.0.1（server_entry.py），外部不可达 ✓ |
| **修复：本地 CSRF 写面** | 内部 /api/* 写端点无鉴权 + CORS `*` → 新增 `LocalOriginGuardMiddleware`（api/security.py）：非幂等方法 + 非本机 Origin（localhost/127.0.0.1/[::1]/tauri://karios-desktop://）→ 403；无 Origin（curl/桌面客户端）放行 ✓ |
| 测试锁定 | `tests/test_security_origin_guard.py` 11 用例（恶意 403/本机放行/无 Origin 放行/GET 只读放行）✓ |

- 全量验收：**1435 passed / 2 skipped** + 27 张表零变化。

---

## 5. 执行顺序与节奏

```
第 1 天（P0）：
  H1 数据口径审计（含 K1 修复）→ H3 测试隔离复查（先跑基线对比）
  → H2 盘后链路冒烟（依赖 H1/K1 修复后的真实数据）

第 2 天（P1）：
  H4 前端决策链回归 → H5 fail-open 清单 → H6 时区/日历 → H7 数值扫描

第 3 天（P2 + 收尾）：
  H8/H9/H10 → 全量测试（后端 + 前端 + tsc）→ 更新本文件勾选 → todo §17 归档
```

**节奏规则**：
- 每个 H 项一个会话，不混 scope（遵循 AGENTS.md 的 OPT 任务纪律）
- 每个 H 项完成 = 勾选 + 测试 + 一句「验收证据」
- P0 未清之前不碰 L4-P1（券商研究）

## 6. 进入 L4 的退出标准（Gate）

- [x] K1 修复且决策快照含真实 paper 数据
- [x] 全量测试跑完 dev DB 表行数不变（H3 验收）
- [x] 盘后链路冒烟通过（H2 验收）
- [x] 前端决策链边界矩阵 ≥10 用例全绿（H4 验收）
- [x] fail-open 清单完成，无「缺数据=激进」残留（H5 验收）
- [x] 后端 + 前端全量测试 + tsc 全绿（仅剩已知 flaky）
- [ ] 本文件 §4/§5 全部勾选，归档到 `docs/archive/`（H1~H10/K1/K4 全勾选完成于 2026-08-08，仅剩归档动作待用户拍板）

## 7. 执行方式

- 本计划拆成 OPT-068+ 落地（见 `docs/optimization-checklist.md`），每个 H 一条
- 排查过程发现的每个 bug：修 + 测试 + 记录到本文件「排查日志」表
- 与 todo §16（L3→L4 升级方向）衔接：本 Gate 是 L4 的准入条件

---

## 8. 覆盖率目标（COV Gate · 2026-08-07 用户拍板）

> **用户要求**：后端整体覆盖率 **90%**，前端 **40%**。可解释（gate 失败要知道缺什么）。

### 8.1 目标表

| 面 | 当前 | 目标 | 统计范围 | Gate 方式 |
|----|------|------|----------|-----------|
| BE 整体 | **64.3%**（21711 行/未覆盖 7743） | **90%**（未覆盖 ≤2171） | `src/data_sync_service/**` | pytest `--cov-fail-under=90`（分波提升，避免一次巨改） |
| BE 核心逻辑（11 模块） | decision 43% · correlation 57% · weekly_review 55% · backtest 75% · paper 78% · automation 71% · execution_source 80%（高：gate 88% trendok 86% journal 86% research 89% exit_attr 91%） | **≥85%** | 逐文件 | 新增 `scripts/coverage_gate.py`（读 coverage.json，按清单 fail） |
| FE 整体 | lib 范围 **69.4%**（不含 components）· 全 src 估算 ~25% | **40%** | `src/**`（vitest include 扩到全 src） | vitest thresholds lines/branches/functions/statements ≥ 40 |

### 8.2 后端 90% 路线（三波，每波跑一次 gate）

**波 1 · 核心逻辑（A 级 → ≥85-90%）**

| 文件 | 当前 | 目标 | 补测要点 |
|------|------|------|----------|
| `service/decision.py` | 43% | 90% | **先修 K1（key 错位）**；快照构建/回溯/action 提取/match_executions 分支 |
| `service/correlation.py` | 57% | 90% | 簇映射全量回归（全 registry + 历史行业）、经验相关边界（<15 样本 fail-open）、30% 边界 |
| `service/weekly_review.py` | 55% | 90% | 空簿/样本不足/auto-notes 各分支、markdown 渲染 |
| `service/backtest_engine.py` | 75% | 90% | 网格、end_of_window、停牌路径、market 过滤 |
| `service/paper_trading.py` | 78% | 90% | intake 全分支（skip 分类）、update 双市场、双 key helper |
| `service/watchlist_automation.py` | 71% | 90% | 进池/清池/alpha 闸分支 |
| `service/execution_source.py` | 80% | 90% | backfill、source 推断边界 |

**波 2 · B 级信号（→ 70-85%）**

| 文件 | 当前 | 目标 |
|------|------|------|
| `alpha_radar_pipeline.py` 33% / `process.py` 17% / `ingest.py` 24% / `mapping.py` 38% | — | 85% |
| `service/daily.py` 45% / `db/daily.py` 44% | — | 85%（数据层读路径） |
| `service/market_sentiment.py` 66% / `market_regime.py` 72% | — | 80% |
| `service/index_daily.py` 40% / `db/index_daily.py` 46% | — | 80% |

**波 3 · 同步/胶水（→ 总体 90%）**

| 文件 | 当前 | 目标 |
|------|------|------|
| `service/tv.py` 42% / `tv_chrome.py` 42% / `db/tv.py` 38% | — | 70% |
| `service/macro_daily.py` 35% / `index_basic.py` 32% / `adj_factor.py` 24% / `etf_daily.py` 20% | — | 70% |
| `service/broker.py` 39% / `trade_calendar.py` 33% / `stock_basic.py` 32% | — | 70% |

**豁免清单**（记录在案，不计入 gate）：`tv/capture.py`（8%，ego-lite fallback 已退役路径）、纯常量/配置模块（`system_prompts.py` 文案）、`tv/__init__.py` 等空模块。豁免需在 coverage_gate.py 中显式列出 + 注明原因。

### 8.3 前端 40% 实现

1. `vitest.config.ts`：coverage.include 从 `src/lib/**` 扩到 `src/**`（exclude 仅 `.test/.spec` + 纯样式/常量文件）
2. thresholds：lines/branches/functions/statements 统一 **≥40**
3. 补测优先级：
   - 新增 0% 文件：`lib/queries/backtest.ts`、`lib/queries/weekly-review.ts`、`lib/queries/execution-journal.ts`、`lib/queries/broker.ts`（每个 5-10 用例）
   - 核心逻辑组件：`components/decision/*`（AnalysisView/WeeklyReviewCard）、`components/watchlist/*`（WatchlistRow 决策 UI）
   - `lib/queries/decision.ts` 6.8% → 70%（决策 Agent 数据层）

### 8.4 可解释性要求

- `coverage_gate.py` 输出：`模块 × 覆盖率 × 未覆盖行号 × 缺口行数`（读 coverage.json），fail 时直接可定位
- **关键路径断言清单**：每个核心模块维护「决策路径 → 测试用例名」映射（执行 H4 时同步产出），随覆盖率报告一起输出
- **覆盖率绿 ≠ 逻辑正确**（K1 教训）：mock shape 必须与 db 真实返回一致——断言清单比百分比更有解释力，两者都要
- 每次跑全量测试必须带 gate：`pytest`（52%→分波提到 90%）；`npm run coverage`（FE）

### 8.5 节奏与验收

- 波 1 完成 → BE ≥75% 且 11 个核心模块全部 ≥85%（gate 升级到 75）
- 波 2 完成 → BE ≥85%（gate 升级到 85）
- 波 3 完成 → BE ≥90%（gate 升级到 90）+ FE ≥40%
- 每次 gate 升级 = 一次「补齐 + 全量测试 + 记录」的会话（遵循 AGENTS.md 纪律）
- 与 H1-H10 的关系：H4（前端决策链回归）产出 FE 核心覆盖；K1 修复（H1）顺带拉高 decision.py
