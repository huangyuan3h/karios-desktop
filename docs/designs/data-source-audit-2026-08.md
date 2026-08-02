# Data Source Audit · 2026-08 · 决策

> **关联 todo**：[§3 收益 P0](../todo.md) · [§6 数据源 P1](../todo.md) · [§12 实施清单 #4](../todo.md)
> **决议日**：2026-08-01
> **下次轻审**：2026-12-01（3 个月后；不要超过 6 个月）

---

## 0. TL;DR

| 源 | 决策 | 成本 / 收益 |
|----|------|-------------|
| **Tushare Pro** | ✅ 续（200/年）| 主力；断了 → 7 个 cron 全废 |
| **akshare (Sina HK)** | ✅ 保留 | OPT-043 验证最稳 |
| **akshare (其他)** | ✅ 保留 | 行业资金流主力 |
| **yfinance** | ⚠️ 降级 backup | rate-limit 严重 |
| **东方财富 push2** | ✅ 保留 | HK 实时报价兜底 |
| **雪球 Xueqiu** | ✅ 保留 | HK industry |
| **RSSHub** | ✅ 保留 | Alpha Radar |
| **聚宽 JQData** | ❌ 不引 | 无不可替代覆盖 |
| **Wind mini** | ❌ 不引 | 25× 贵，无 ROI |
| **Choice / iFinD** | ❌ 不引 | 同上 |
| **自建爬虫 (ego-lite)** | 🔄 P2 调研 | todo §12 #8 |

**总成本**：200/年（Tushare Pro） + $0（其他全是免费 / 自建）。
**结论**：卫星仓场景下，**当前源矩阵是 ROI 最优解**。不要跟风买 Wind。

---

## 1. 现有源矩阵（grep 自 codebase）

> 数据来自 `services/data-sync-service/src/data_sync_service/service/` 的实际 import 与调用。

### 1.1 Tushare Pro（主力，¥200/年）

| 覆盖 | 用途 | 调用文件 | 风险 |
|------|------|----------|------|
| CN A 股日线 | `daily` cron（17:10 收盘同步）| `service/daily.py` `service/close_sync.py` | rate-limit 200次/分钟 |
| CN 复权因子 | adj_factor 同步 | `service/adj_factor.py` | 同上 |
| HK 基础信息 | hk_basic 月度 | `service/hk_basic.py` | 同上 |
| ETF 基础 | fund_basic 月度 | `service/fund_basic.py` | 同上 |
| ETF 日线 | etf_daily 同步 | `service/etf_daily.py` | 同上 |
| 行业 / 指数基础 | index_basic 同步 | `service/index_basic.py` | 同上 |
| 龙虎榜 / 主力 | top_inst_flow | `service/top_inst_flow.py` | 同上 |
| 实时报价 | realtime_quote | `service/realtime_quote.py` | 1次/分钟（严重）|
| 财报 / 财务指标 | （未启用）| — | — |

**断 Tushare 影响**：7 个 cron 全废，CN 日线断了 → watchlist 整个崩。**必须续**。

### 1.2 akshare（免费 / 主力多源）

| 覆盖 | 用途 | 调用文件 |
|------|------|----------|
| HK 日线 Sina 源 | hk_daily_ak（OPT-043 验证）| `service/hk_daily_ak.py` |
| HK 行业 | hk_industry | `service/hk_industry.py` |
| 行业资金流 | industry_fund_flow | `service/industry_fund_flow.py` |
| 市场情绪 | market_sentiment | `service/market_sentiment.py` |
| 个股详情 | market_detail | `service/market_detail.py` |
| 期权 IV | option_iv | `service/option_iv.py` |

**Sina HK 源**（OPT-043 实测）：30/30 连续调用 0 失败，平均 0.12s/call → 已经是**最稳的 HK 日线源**。

**风险**：akshare 是社区维护的"瑞士军刀"，包大、版本变化快、特定 API 偶尔会被改。  
**应对**：akshare fallback 链路已经是 design——断了某接口，自动降级到 tushare / yfinance。

### 1.3 yfinance（免费 / backup 降级）

| 覆盖 | 用途 | 风险 |
|------|------|------|
| HK 日线（backup）| hk_daily_yf | rate-limit 严重（OPT-043 测过）|
| 港股指数 ^HSI | macro snapshot | 同上 |
| US 标的（未来）| 多市场 | 暂未启用 |

**现状**：作为 **3 级兜底**。`hk_daily_ak` 失败 → `tushare.hk_daily` → `yfinance`。日常不调用。

### 1.4 东方财富 push2（免费 / 实时报价兜底）

| 覆盖 | 用途 |
|------|------|
| HK 实时报价 | realtime_quote fallback |
| ETF 实时资金流 | etf_fund_flow_em |

**风险**：云厂商 IP 段被拉黑（实测过），**本机 IP 反而稳定**。这点恰好和"不上云"决策一致——**本地用 EM push2 反而最稳**。

### 1.5 雪球 Xueqiu（免费 / HK industry）

| 覆盖 | 用途 |
|------|------|
| HK 主营业务 (mbu) | hk_industry 抓取 |

**风险**：soft rate-limit（全 None → retry × 2）。

### 1.6 RSSHub（本地实例）

| 覆盖 | 用途 |
|------|------|
| 中文财经新闻 | alpha_radar_ingest（13 个中文源 + 7 个英文源）|

**现状**：本地跑 RSSHub 实例（`KARIOS_AUTO_START_RSSHUB=1`）。免费 + 自控。

---

## 2. 候选源对比

> 全部按**卫星仓 + 个人用户**场景评估。机构 / 多用户 / 美股高频不在范围。

### 2.1 聚宽 JQData

| 维度 | 评估 |
|------|------|
| 价格 | 200-3000/年（按数据范围）|
| 覆盖 | CN A 股 + 基金 + 指数 + 财务 + 行业 |
| 限频 | 高（官方宽松）|
| 数据质量 | 高（与 Tushare Pro 接近）|
| ROI | **负**：Tushare Pro 已覆盖；akshare 已覆盖；多一个源 = 维护成本翻倍 |

**结论**：❌ **不引**。覆盖与 Tushare 重叠，没有不可替代价值。

### 2.2 Wind mini

| 维度 | 评估 |
|------|------|
| 价格 | 5000-8000/年（个人版）|
| 覆盖 | 全市场（CN/HK/US/期货/期权/外汇/债券/基金/指数）|
| 限频 | 中（按订阅）|
| 数据质量 | 顶级（机构标配）|
| ROI | **极负**：25× 贵于 Tushare Pro；卫星仓只用 daily / quote / industry，没有高频 / 衍生品需求 |

**结论**：❌ **不引**。卫星仓场景下没有 ROI。

### 2.3 Choice（东方财富量化接口）

| 维度 | 评估 |
|------|------|
| 价格 | 3000-5000/年 |
| 覆盖 | CN 主力（与 EM push2 大量重叠）|
| 数据质量 | 中（与 akshare EM 接口同源）|
| ROI | **负**：和免费 EM push2 同质化，付费没意义 |

**结论**：❌ **不引**。免费源已覆盖。

### 2.4 iFinD（同花顺）

| 维度 | 评估 |
|------|------|
| 价格 | 几千/年（按套餐）|
| 覆盖 | CN 主力（与 Tushare Pro 大量重叠）|
| 数据质量 | 中 |
| ROI | **负** |

**结论**：❌ **不引**。

### 2.5 自建爬虫（ego-lite / 自写）

| 维度 | 评估 |
|------|------|
| 价格 | $0（开发时间除外）|
| 覆盖 | 取决于写什么 |
| 限频 | 自控（违规风险自负）|
| 数据质量 | 自控（**0 校验** = 高风险）|
| ROI | **短期负**：0 成本但维护累；**长期可能正**：去 Chrome 依赖 |

**结论**：🔄 **P2 调研**（todo §12 #8）。先调研 ego-lite，**确认能替代 Chrome TV 抓取再做**；否则不碰。

---

## 3. 决策

### 3.1 续费 / 不续

| 行动 | 标的 | 时间 |
|------|------|------|
| **续 Tushare Pro 200/年** | 2027-08 前 | ✅ 已续 |
| **不续任何付费升级** | 2027-08 前 | 决策 |
| **不引新源** | 持续 | 决策 |

### 3.2 优化动作

| 行动 | 优先级 | 工时 | 关联 todo |
|------|--------|------|----------|
| ego-lite 调研（替代 Chrome TV 抓取）| P2 | 2-3 天 | §12 #8 |
| akshare 包版本锁定（防 API 突然改）| P3 | 0.5 天 | §12 #8 |
| 每个源加 health check 脚本 | P2 | 0.5 天 | OPT-050 |
| 半年一次轻审 | — | 1 小时 | — |

### 3.3 监控指标

每天跑 `scripts/data-source-healthcheck.sh` 看：
- Tushare API key 是否配置
- akshare import 是否成功
- EM push2 域名解析是否通
- RSSHub 本地端口是否监听

**不**真连外部 API（避免 rate-limit 浪费配额）；只检查**配置**和**连通性**。

---

## 4. ROI 分析（数字）

> 按"个人 + 卫星仓 + 每月约 50 笔交易 + 2500 标的监控"场景。

| 项 | Tushare Pro | Wind mini | 自建 |
|----|-------------|-----------|------|
| 年度成本 | ¥200 | ¥5000+ | 0（开发 50h） |
| CN 日线 | ✅ | ✅ | 1 周写 |
| HK 日线 | ✅ | ✅ | 1 周写 |
| 实时报价 | ❌（1次/分）| ✅ | 维护累 |
| 财报 | ❌（未启用）| ✅ | 2 周写 |
| 行业资金流 | ❌ | ✅ | 1 周写 |
| 维护 / 年 | 0 | 0 | 50-200h |

**当前方案 ROI**：¥200 / 年 / 50 笔交易 = **¥4/笔**。无可比。

**Wind mini ROI**：¥5000 / 年 / 50 笔 = **¥100/笔**。25× 贵于当前，没有可感知的 25× 价值。

---

## 5. 反原则

- ❌ 看到同好推荐 Wind 就心动（**没有不可替代的覆盖**就贵 25×）
- ❌ 一年 365 天都自审（轻审即可，6 个月一次足够）
- ❌ 多源同质数据做"双保险"（维护成本翻倍；让 1 个主源 + 1 个真正互补的 backup）
- ❌ 自建爬虫做主力（0 成本但维护累；做兜底 OK）
- ❌ 把"高 ROI"误读为"买最贵的"——**当前方案 ROI 已经极高**（¥4/笔）
- ❌ 跟着 AI agent 跑（agent 不需要看付费报告；自己 LLM 解读已有数据就够）

---

## 6. 与 freelancer-arch 的关系

按设计稿，**Karios 是被动数据 + endpoint 服务**——数据源矩阵直接决定 `/v1/*` 暴露数据的质量。

- 上游（数据源）质量下降 → 下游（AI 助手）误判概率上升
- 但**不**意味着要换更贵的源；意味着**健康检查 + 备份链路**要更稳
- 当前健康检查脚本是这方向的**第一步**

---

## 7. 复审时间表

| 日期 | 触发条件 | 复审内容 |
|------|----------|----------|
| 2026-12-01 | 半年期 | 续 Tushare？akshare API 变了？ |
| 2026-08-15 | Tushare 调价公告 | 调价后 ROI 变 → 重算 |
| 2027-Q1 | ego-lite 调研结论 | 如果能替代 Chrome → 启动迁移；否则降级 |
| 任何 | 关键源连续 7 天失败率 > 5% | 立即调查 + 切 backup |
