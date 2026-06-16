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
- `a_share_mapping` — 1–3 只龙头名称或代码
- `logic_summary` — 逻辑推演（≤30 字）

DB 额外列：`driver_type`、`event_focus`、`logic_summary`

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
