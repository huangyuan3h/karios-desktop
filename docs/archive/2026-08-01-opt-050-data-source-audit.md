# OPT-050 · 数据源质量审计（2026-08） · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §3 收益 P0` / `§6 数据源 P1` / `§12 实施清单 #4`](../../todo.md)
> **决策稿**：[`docs/designs/data-source-audit-2026-08.md`](../../designs/data-source-audit-2026-08.md)（**新**）
> **健康检查**：[`services/data-sync-service/scripts/data-source-healthcheck.sh`](../../services/data-sync-service/scripts/data-source-healthcheck.sh)（**新**）

## 当时的目标

按 todo §3 / §6 / §12 #4："现有源'非常杂，质量不高'，评估是否替换/补强"——**决定下年要不要续 Tushare 200 + 候选源 ROI 评估**。

## 实际做了什么

### A. 现有源矩阵（grep 自 codebase）

| 源 | 状态 | 用途 |
|----|------|------|
| Tushare Pro (¥200/年) | ✅ 主力 | CN daily / HK basic / fund_basic / industry / index / adj_factor — 7 个 cron 依赖 |
| akshare (Sina HK) | ✅ 主力 | hk_daily_ak（OPT-043 验证 30/30 0 失败）|
| akshare (其他) | ✅ 多源 | 行业资金流 / 市场情绪 / 个股详情 |
| yfinance | ⚠️ backup | HK 日线最后兜底（rate-limit 严重）|
| 东方财富 push2 | ✅ 实时报价 | HK 实时报价（云 IP 拉黑，本机 OK——恰好对上"不上云"决策）|
| 雪球 Xueqiu | ✅ HK industry | mbu 主营业务抓取 |
| RSSHub | ✅ Alpha Radar | 13 中文 + 7 英文源，本地实例 |

### B. 候选源评估（5 维度：价格 / 覆盖 / 限频 / 质量 / ROI）

| 候选 | 价格 | 决策 | 理由 |
|------|------|------|------|
| 聚宽 JQData | 200-3000/年 | ❌ 不引 | 与 Tushare Pro 覆盖重叠；无不可替代价值 |
| Wind mini | 5000-8000/年 | ❌ 不引 | 25× 贵于 Tushare；卫星仓无高频 / 衍生品需求 |
| Choice (EM 量化) | 3000-5000/年 | ❌ 不引 | 与免费 EM push2 同质化 |
| iFinD (同花顺) | 几千/年 | ❌ 不引 | 与 Tushare Pro 大量重叠 |
| 自建爬虫 (ego-lite) | 0 元（开发 50h）| 🔄 P2 调研 | todo §12 #8——先调研，再决定 |

### C. 健康检查脚本（`data-source-healthcheck.sh`）

轻量、**不连外部**：
- 检查 8 个 env var 是否配置（TU_SHARE_API_KEY / DATABASE_URL / KARIOS_API_VERSION / KARIOS_API_KEYS / AI_SERVICE_BASE_URL / RSSHub / OpenAI / Google AI）
- 检查 7 个 Python 包是否 import
- 检查 4 个本地端口（Postgres 5432 / FastAPI 4310 / AI 4310 / RSSHub 1200）
- 退出码 0/1/2（绿 / 危险 / 降级）

### D. ROI 数字（卫星仓场景）

| 方案 | 年成本 | ¥/笔 | 评估 |
|------|--------|------|------|
| 当前（Tushare 200）| ¥200 | **¥4/笔** | 极佳 |
| Wind mini | ¥5000+ | ¥100/笔 | 25× 贵，无可感价值 |
| 自建爬虫 | 0（开发 50h）| — | 维护负担 |

**结论**：当前方案 ROI 已经极高。**不要跟风买 Wind**。

## 验证 / 数据

- 13/13 test_data_source_audit 全绿
- 120 + 1 skip 全部测试（101 含 audit + 19 test_api）
- 文档含 5 必要节（TL;DR / 现有源 / 候选 / 决策 / ROI / 反原则）
- 文档涵盖 6 个现有源 + 5 个候选源（test 守住）
- 复审日期写入：**2026-12-01**（半年期）
- healthcheck 脚本语法 OK + 缺 key 时给清晰指引

## 后续影响 / 留给谁

### 给 Karios 本身

- **续 Tushare Pro**（2027-08 前）— 不续 → 7 个 cron 全废
- **每日跑 healthcheck**（todo §11 注意力预算里加一条）
- **2026-12-01 复审**：续不续？akshare API 变了？tushare 调价？
- **2027-Q1 复审 ego-lite**：todo §12 #8 调研结论

### 给外部 AI 助手

- `/v1/*` 数据质量不会变（继续基于 Tushare / akshare 源）
- 不会引入"新源数据**假象**"（多源同质数据 → 假精度）

### 给未来 review

- Wind mini 价格降到 1000/年以下 → 重新评估
- 任何**关键源**连续 7 天失败率 > 5% → 立即调查
- akshare 包大版本升级 → 跑全测试 + 人工 spot check

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 3（决策稿 + healthcheck + tests）|
| 改动文件 | 2（OPT-050 doc + todo.md）|
| 总测试 | 13 audit + 19 paper + 18 business + 17 discovery + 14 explain + 12 tunnel + 8 alembic + 19 test_api = **120/120 ✅** + 1 skip |
| 工期 | 1 个会话 |
| 预算 | $0（续 Tushare 200/年不变）|
