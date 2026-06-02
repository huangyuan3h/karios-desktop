# Alpha Incubator 模块

> 宏观/产业事件驱动追踪器（Event-Driven Industry Radar）

---

## 模块定位

Alpha Incubator 从 **7 路精选** 英文 RSS 采集半导体/AI 基建/财报信号，经 **主题过滤 → RSS 摘要（默认）→ batch LLM 提纯 → A 股映射**。

## 调度架构（RSS 与 LLM 解耦）

```text
每 4h  alpha_radar_ingest_job   → run_alpha_radar_ingest   （仅 RSS 入库，无 12h 冷却）
每 1h  alpha_radar_process_job → run_alpha_radar_process  （消化 raw 积压，无冷却）
每 12h alpha_radar_pipeline_job → run_alpha_radar_pipeline （ingest + process + 本批 meta，12h 冷却）
```

| 阶段 | 实现 |
|------|------|
| Cron 4h ingest | `alpha_radar_ingest_job` |
| Cron 1h process | `alpha_radar_process_job` |
| Cron 12h pipeline | `alpha_radar_fetch_job` → `run_alpha_radar_pipeline` |
| RSS | `alpha_radar_ingest.py` |
| Filter | `alpha_radar_filter.py` |
| LLM | `POST /alpha-radar/extract-batch` |
| Map | `alpha_radar_mapping.py` |
| UI | `AlphaIncubatorPage.tsx`（趋势 / 催化股票 / RSS 原文） |

手动「生成趋势」仍调用 `run-pipeline`（12h 冷却，可 `force` 跳过）。

## 智能重复入库

`upsert_document` 在 **内容未变** 时 **保留** `processing_status`（例如保持 `mapped`），避免催化股票因重复 RSS 条目被误删趋势。

仅在以下情况重置为 `raw`：

- `title` / `summary` / `full_text_md`（新全文非空且与旧值不同）有变化
- `POST /sync` 或 ingest 传入 `forceReprocess: true`

`ingestStats` 含 `new` / `requeued` / `unchanged` 计数。

## 数据存储

| 表 | 策略 |
|----|------|
| `alpha_radar_documents` | **默认永久** upsert，按 URL/id 去重 |
| `alpha_radar_trends` | **默认永久**；流水线成功时 **不再** 批量删除历史批次 |
| 可选修剪 | `ALPHA_RADAR_TREND_RETENTION_DAYS`：`0` 或未设置 = 不删；`>0` 时在流水线成功后修剪 |

同一文档 **内容变化** 或 **force reprocess** 后进入 `raw`；batch/single 处理前对该 `document_id` 执行 `delete_trends_for_document`，再写入新趋势（替换不叠卡）。

## 三视图

| 视图 | 说明 |
|------|------|
| **趋势视图** | 默认 **本批**；可切换 **全部历史** |
| **催化股票** | 打分窗口默认 30 天；**不删库** |
| **RSS 原文** | `alpha_radar_documents` 入库列表 |

## 催化打分

- **展示窗口**：`COALESCE(published_at, fetched_at)`，默认 **30 天**
- **单篇贡献**：`confidence × urgency_weight × recency_decay`（半衰期 14 天）
- **综合分（0–100）**：`0.55×primary + 0.30×min(secondary, 2×primary) + 0.15×breadth`

## API

- `GET /api/alpha-radar/status` — `lastIngestAt`、`lastProcessAt`、`rawBacklogCount`、`accumulatedTrendCount`
- `POST /api/alpha-radar/run-ingest` — 仅 RSS（`forceReprocess` 可选）
- `POST /api/alpha-radar/run-process` — 仅消化 raw（`maxRounds` 可选）
- `POST /api/alpha-radar/run-pipeline` — 完整流水线（`force: true` 跳过冷却）
- `POST /api/alpha-radar/sync` — 兼容 ingest（支持 `forceReprocess`）
- `GET /api/alpha-radar/trends` / `documents` / `catalyst-stocks` — 见上文

## 环境变量

| 变量 | 说明 |
|------|------|
| `ALPHA_RADAR_INGEST_INTERVAL_HOURS` | RSS 定时同步间隔，默认 4 |
| `ALPHA_RADAR_PROCESS_INTERVAL_HOURS` | raw 消化间隔，默认 1 |
| `ALPHA_RADAR_PROCESS_MAX_ROUNDS` | 单次 process job 最多 batch 轮数，默认 8 |
| `ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS` | 全 pipeline 冷却，默认 12 |
| `ALPHA_RADAR_CATALYST_MAX_AGE_DAYS` | 催化打分窗口，默认 30 |
| `ALPHA_RADAR_TREND_RETENTION_DAYS` | 可选趋势修剪；`0` = 永久 |
| `ALPHA_RADAR_DAILY_BATCH_ROUNDS` | 12h pipeline 内 batch 轮数，默认 3 |
| `ALPHA_RADAR_MAX_ITEMS_PER_SOURCE` | 每源 RSS 条数，默认 5 |
| `JINA_API_KEY` / `ALPHA_RADAR_ENRICH_FULLTEXT` | 可选全文 |

## 失败保护

- **stored=0**（pipeline）：不删旧趋势卡
- **LLM=0 trends**（仅 12h pipeline）：`delete_trends_since` 回滚**本批**；1h process job **不**回滚
