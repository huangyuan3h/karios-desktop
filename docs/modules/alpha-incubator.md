# Alpha Incubator 模块

> 宏观/产业事件驱动追踪器（Event-Driven Industry Radar）

---

## 模块定位

Alpha Incubator 从 **7 路精选** 英文 RSS 采集半导体/AI 基建/财报信号，经 **主题过滤 → RSS 摘要（默认）→ batch LLM 提纯 → A 股映射**，每 **12 小时** 自动刷新趋势卡片。

## 流水线

```text
RSS (7 feeds) → Filter (regex) → RSS summary (optional Jina) → Batch LLM → A-share map → UI
```

| 阶段 | 实现 |
|------|------|
| Cron 12h | `alpha_radar_fetch_job` → `run_alpha_radar_pipeline` |
| RSS | `alpha_radar_ingest.py` |
| Filter | `alpha_radar_filter.py` |
| LLM | `POST /alpha-radar/extract-batch` |
| Map | `alpha_radar_mapping.py` |
| UI | `AlphaIncubatorPage.tsx`（趋势 / 催化股票 / RSS 原文） |

## 数据存储

| 表 | 策略 |
|----|------|
| `alpha_radar_documents` | **默认永久** upsert，按 URL 去重 |
| `alpha_radar_trends` | **默认永久**；流水线成功时 **不再** 批量删除历史批次 |
| 可选修剪 | `ALPHA_RADAR_TREND_RETENTION_DAYS`：`0` 或未设置 = 不删；`>0` 时在流水线成功后按文档事件时间修剪 |

同一文档 re-ingest 后 batch/single 处理会先 `delete_trends_for_document` 再写入，避免重复叠卡。

## 三视图

| 视图 | 说明 |
|------|------|
| **趋势视图** | 默认 **本批**（`latest_batch=true`）；可切换 **全部历史**（`latest_batch=false`）。卡片 footer 显示文章年龄，超出催化窗口会标注 |
| **催化股票** | 以 A 股为主，**打分窗口** 默认 30 天（`ALPHA_RADAR_CATALYST_MAX_AGE_DAYS`）；仅影响聚合与排序，**不删库** |
| **RSS 原文** | 展示 `alpha_radar_documents` 入库记录（标题、摘要、状态、时间、外链） |

## 催化打分

- **展示窗口**：`COALESCE(published_at, fetched_at)`，默认 **30 天**（`ALPHA_RADAR_CATALYST_MAX_AGE_DAYS`）
- **单篇贡献**：`confidence × urgency_weight × recency_decay`（半衰期 14 天）
- **综合分（0–100）**：`0.55×primary + 0.30×min(secondary, 2×primary) + 0.15×breadth`，非线性，避免「篇数越多分越高」
- 同一 `(symbol, document_id)` 去重，保留最高贡献

## 默认信源（7 路）

| ID | 名称 | 备注 |
|----|------|------|
| stratechery | Stratechery | RSS 常含全文；短摘要时才尝试 Jina |
| mit-tech-review | MIT Technology Review | RSS 摘要 |
| ieee-spectrum | IEEE Spectrum | RSS 摘要 |
| seeking-alpha-tech | Seeking Alpha Tech | RSS 摘要 |
| semianalysis | SemiAnalysis | RSS 摘要 |
| next-platform | The Next Platform | RSS 摘要 |
| trendforce | TrendForce News | `news/feed_v2/` |

旧源在 `init-defaults` 时自动 **禁用**。

## 过滤规则

- **Exclude**：biomed/clinical/pharma/sport/crypto/politics 等
- **Include**（非白名单源）：semiconductor/GPU/datacenter/AI/earnings/HBM/算力/半导体 等
- **白名单源**（仅 exclude）：stratechery、semianalysis、next-platform、ieee-spectrum、trendforce

## API

- `GET /api/alpha-radar/status` — 上次运行、本批/库内趋势数（`accumulatedTrendCount`）、ingest 统计
- `GET /api/alpha-radar/trends?latest_batch=true` — 最新一批趋势卡片
- `GET /api/alpha-radar/trends?latest_batch=false&limit=100` — 全部历史（分页 `offset`）
- `GET /api/alpha-radar/trends?maxAgeDays=30` — 仅查询过滤，不删数据
- `GET /api/alpha-radar/documents?limit=100` — RSS 入库原文列表
- `GET /api/alpha-radar/catalyst-stocks?limit=50&maxAgeDays=30` — 催化股票排行（含文章列表）
- `POST /api/alpha-radar/run-pipeline` — 完整流水线（`force: true` 跳过 12h 冷却）
- `POST /api/alpha-radar/generate-daily` — 兼容别名

## Dashboard 导出

Dashboard **Copy all Markdown** 会追加 **Top 10 催化股票** 表格与摘要（与催化股票 Tab 共用 API）。

## 环境变量

| 变量 | 说明 |
|------|------|
| `ALPHA_RADAR_CATALYST_MAX_AGE_DAYS` | 催化打分展示窗口（天），默认 30 |
| `ALPHA_RADAR_TREND_RETENTION_DAYS` | 可选库内趋势修剪（天）；`0` = 永久，默认不删 |
| `JINA_API_KEY` | 可选；默认 RSS 摘要即可 batch LLM |
| `ALPHA_RADAR_ENRICH_FULLTEXT` | `0`（默认）关闭 Jina；`1` 仅 Stratechery 短摘要时尝试 |
| `ALPHA_RADAR_FULLTEXT_MAX_PER_SOURCE` | 优先源每轮 Jina 上限，默认 2 |
| `ALPHA_RADAR_RSS_TIMEOUT` | RSS 超时秒数，默认 60 |
| `http_proxy` / `https_proxy` | RSS/Jina 走代理 |
| `ALPHA_RADAR_MAX_ITEMS_PER_SOURCE` | 每源 RSS 条数上限，默认 5 |
| `ALPHA_RADAR_DAILY_BATCH_ROUNDS` | batch 轮数，默认 3 |
| `AI_SERVICE_BASE_URL` | LLM 服务 |
| `NODE_USE_ENV_PROXY=1` | ai-service 走代理 |

## 失败保护

- **stored=0**：不删旧卡片，返回详细 sourceErrors
- **LLM=0 trends**：`delete_trends_since` 回滚本批失败写入，保留历史趋势

## 定时任务

`alpha_radar_pipeline_job` — 每 **12 小时** 跑完整流水线。
