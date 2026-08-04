# Alpha Incubator 模块 (V4 双核捕猎器)

> Global Tech + Cycle/Policy 全天候事件驱动追踪器

---

## 模块定位

Alpha Incubator 从 **7 路英文 + 6 路中文 RSSHub** 采集硬核信号，经 **per-source 严苛过滤 → batch LLM V4 提纯（仅 S/A）→ 混合 A 股映射**。

### 驱动核心 (Driver_Type)

| 类型 | 数据源示例 |
|------|------------|
| `Global_Tech` | Stratechery、SemiAnalysis、TrendForce 等 7 路英文源 |
| `Domestic_Policy` | 财联社宏观电报、工信部/发改委官方 RSS |
| `Cycle_Reversal` | 华尔街见闻大宗、东方财富铜产业检索 |
| `consensus` 源 | 财联社深度研判（过滤后归入 Cycle 逻辑） |

## 调度架构（RSS 与 LLM 解耦）

```text
每 4h  alpha_radar_ingest_job   → run_alpha_radar_ingest
每 1h  alpha_radar_process_job → run_alpha_radar_process
每 12h alpha_radar_pipeline_job → run_alpha_radar_pipeline
```

| 阶段 | 实现 |
|------|------|
| RSS | `alpha_radar_ingest.py`（含 RSSHub 中文源） |
| Filter | `alpha_radar_filter.py`（per-source profile） |
| LLM V4 | `apps/ai-service/src/alphaRadarPrompts.ts` + `/alpha-radar/extract-batch` |
| 混合映射 | `alpha_radar_symbol_resolve.py` → fallback `map-cn` |
| UI | `AlphaIncubatorPage.tsx` |

## V4 趋势 Schema

LLM 输出字段（仅 `catalyst_grade` ∈ {S, A}，B 级丢弃）：

- `macro_theme` — 主题桶
- `driver_type` — Global_Tech | Domestic_Policy | Cycle_Reversal
- `event_focus` — 事实陈述
- `a_share_mapping` — 1–3 只 A 股龙头名称或 6 位代码
- `hk_mapping` — 0–3 只港股纯映射龙头名称或 5 位 ticker（**OPT-052**，可选；催化剂直接映射到港股时才填）
- `logic_summary` — 逻辑推演（≤30 字）

DB 额外列：`driver_type`、`event_focus`、`logic_summary`

### OPT-052：HK 标的识别（自 2026-08-01）

LLM 输出 `hk_mapping` 后，Python 端在 `alpha_radar_symbol_resolve.resolve_hk_mapping` 中按 5 位 ticker 或公司名解析，写到 `trend_json.hkSymbols`（**不**单立 DB 列——HK 命中频次低，避免 schema migration 噪音）。`aggregate_catalyst_stocks` 把 `cnSymbols` 和 `hkSymbols` 合并到同一个 bucket map，下游 `WATCH_SILENT` / `compute_alpha_additions` 无需特殊处理。

HK 标的进 watchlist 时**跳过 EM industry 闸门**（`missing_industry` / `is_defense_sector` / Top10）：EM 行业数据只覆盖 A 股。仍保留 `catalystScore` + `Max Grade=S` 上游闸门（OPT-052 没放宽这两道）。

## 中文 RSS 源（RSSHub）

| source_id | category | 默认路由 |
|-----------|----------|----------|
| `cls-policy` | policy | `/cls/telegraph` |
| `gov-miit-policy` | policy | `/gov/miit/zcjd` |
| `gov-ndrc-policy` | policy | `/gov/ndrc/xwdt` |
| `wallstreetcn-commodity` | cycle | `/wallstreetcn/live/global` |
| `eastmoney-copper` | cycle | `/eastmoney/search/铜` |
| `cls-depth` | consensus | `/cls/depth` |

> 旧路由 `/gov/zhengce/zuixin`、`/smm/news`、`/hbreport/report` 在 RSSHub 中已失效（503），启动时会自动禁用 legacy source_id。

URL = `{ALPHA_RADAR_RSSHUB_BASE_URL}{route}`，各源可用独立 env 覆盖。

## 过滤规则

- 全局 `EXCLUDE_RE`：biomed、crypto、股评等噪音
- 中文源 **profile 过滤**（`ALPHA_RADAR_FILTER_STRICT=1` 默认开启）
- 英文 trusted 源：仅 exclude，不强制 include

## 混合 A 股映射

1. LLM 输出 `a_share_mapping`
2. 本地解析 6 位代码 / 公司名（`alpha_radar_symbol_resolve.py`）
3. 解析失败 → Tavily + `/alpha-radar/map-cn` fallback

## 环境变量

| 变量 | 说明 |
|------|------|
| `ALPHA_RADAR_RSSHUB_BASE_URL` | RSSHub 基址，默认 `http://127.0.0.1:1200` |
| `ALPHA_RADAR_RSS_CLS_POLICY` 等 | 单源 URL 覆盖 |
| `ALPHA_RADAR_FILTER_STRICT` | `1` 严苛过滤；`0` shadow（中文源不过滤） |
| `ALPHA_RADAR_CATALYST_MAX_AGE_DAYS` | 催化打分窗口，默认 30 |
| `ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS` | 全 pipeline 冷却，默认 12 |

其余变量见上文调度/ingest 配置。

## 失败保护

- LLM 返回 0 trends（V4 铁血标准）：document 标记 `extracted`，1h process **不**视为 error
- **stored=0**（pipeline ingest）：不删旧趋势卡

## Watchlist 自动化进池闸（TIP-004）

盘后 / Run automation 将 Alpha 候选写入 Watchlist 前，在 `catalystScore > 85` 且含 S 之外还要求：

1. 东财行业名可解析（DB `stock_eastmoney_industry`）
2. 非防守板块（银行 / 电力 / 公用事业 / 中药 / 煤炭 / 高速公路；**不**误伤 `电力设备`）
3. 若行业名本身是申万一级：须 ∈ 5D 净流入 Top10；若为更细的东财板块名（如「半导体」）则 **跳过 Top10**（避免与 SW L1「电子」硬匹配系统性误拒）。Top10 数据缺失时整闸跳过。
4. **TIP-009 auto-QA 闸门**：候选分数被 `auto_qa_penalty`（0–0.6）折扣后再与 `CATALYST_SCORE_MIN=85` 比。惩罚来源见下方 §auto-qa-rules。

拒绝原因计入 automation run `meta.alphaRejected`。Max Grade=S 的 GC 豁免基于 **完整催化窗口**，不限于进池用的 score Top200。

---

## Auto-QA Rules (TIP-009 · 数据驱动 · 用户零操作)

5 类自动 penalty 信号，全部从已有数据计算（**没有** LLM 写种子词、**没有** 用户抽检反馈）：

| # | 信号 | penalty | 数据源 |
|---|------|---------|--------|
| 1 | **行业不匹配** | 0.6 | `data/seed/theme_industry_map.json` + `stock_eastmoney_industry` |
| 2 | **历史胜率低** | 0.5 | `paper_trades` 30D 命中/总数（macroTheme 维度）|
| 3 | **名称歧义** | 0.4 | `_lookup_by_name` 候选 top1/top2 字符前缀/子串重合 |
| 4 | **板块资金流背离** | 0.3 | `industry_fund_flow` 5D 净流出 vs trend 暗示方向 |
| 5 | **个股资金流背离** | 0.2 | `fund_flow` 5D 净流出 vs trend 暗示方向 |

penalty = max(signal1, signal2, ..., signal5)（不累加，避免过度惩罚）。

### 数据驱动映射（不是硬编码）

`scripts/build_theme_industry_map.py` 从历史 `alpha_radar_trends` 跑一次，统计每个 `macro_theme` 映射股票的真实行业分布：

- 阈值 ≥70% 的 cnSymbols 落在某 SW L1 / EM 板块 → 该主题加入 `themes`
- 主题未达阈值或样本不足（<3 映射）→ 进 `unmapped_themes`，auto-QA penalty=0（无信号）
- 季度跑一次更新即可

### 用户工作流

| 步骤 | 用户 | 系统 |
|------|------|------|
| ① | Sync | 拉数据 + alpha pipeline（含 auto_qa_penalty） |
| ② | Copy (Dashboard) | Copy markdown 末尾多 2 section：⚠ Mapping warnings + Theme historical win-rate |
| ③ | 给外部 AI agent | 决策时看到错映射警告 + 低胜率主题 |
| ④ | AI agent 写 watchlist | 已带 auto_qa_penalty 的 catalystScore 决定 BUY 信号强度 |

**用户日常操作**：仍是 Sync + Copy。0 增量。

### 后置观测 API

- `GET /api/alpha-radar/auto-qa-stats?sinceDays=7&limit=20` — 主动查询时用，不进 Dashboard
- Copy markdown 末尾新 section 是被动可见的展示层

### 不做的事（明确）

- ❌ 不加 UI ✓/✗/? 反馈按钮
- ❌ 不写周抽检 markdown 模板（用户个人流程）
- ❌ 不动 LLM prompt
- ❌ 不引入外部 catalog
- ❌ 不让用户做任何额外操作
