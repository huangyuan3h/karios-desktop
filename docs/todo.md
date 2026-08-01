# Karios · 路线图（todo）

> 产品级路线图，**按领域分章节 + 优先级标记**。完成时把条目标 `[done]`，详情迁到 [`docs/archive/`](./archive/)。
>
> **对应关系**：
> - 本文件管「**做什么、为什么**」（产品/战略层）。
> - 怎么做（架构/工程债）→ [`docs/optimization-checklist.md`](./optimization-checklist.md) 的 `OPT-xxx`。
> - 怎么投（交易规则）→ [`docs/trading-improvement-checklist.md`](./trading-improvement-checklist.md) 的 `TIP-xxx` / `V6.x`。

---

## 0. 我的优先级（用户口径，不可漂移）

| 序 | 维度 | 为什么 | 当前 todo 域 |
|----|------|--------|--------------|
| 1 | **收益** | 立命之本 | §2 交易策略、§8 回测 |
| 2 | **API 开放 / AI 打通** | 让外部 AI 助手能调我的数据 | §3 API 开放 |
| 3 | **工程架构 / 部署** | 长期可持续（含云，但 DB 大概率不上云） | §4 工程与部署 |
| 4 | **浏览器 / 数据源优化** | 上云的硬约束 | §5 数据源 |

> 任何新需求先对一下表：放错了域就回正。

---

## 1. 状态看板

> 每行"完成归档"列 = 已在代码 / doc 中沉淀的位置。新完成的 todo 摘要迁到 `docs/archive/` 后**同时**在这里补一行。

| 领域 | 在做（P0） | 待办（P1-P4） | 完成归档 |
|------|------------|---------------|----------|
| §1 定位与形态 | — | Tauri vs 固定 URL 评估 | — |
| §2 收益 / 交易 | TIP-009 alpha 映射抽检 / TIP-011 开火归因 | 数据源质量审计 | TIP-001~008 + V6.2/3 已沉淀（`trading-improvement-checklist.md`） |
| §3 API 开放 | — | OpenAI 兼容 API、Key 管理 | — |
| §4 工程与部署 | — | DB 走向决策 / Docker 一键 / 穿透方案 | `OPT-032` ensure_table guard / `OPT-033-034` 指数批读去重 |
| §5 数据源 / 浏览器 | — | ego-lite 调研、付费 API 矩阵 | `OPT-043` akshare 优先链 / `OPT-041/044` HK 闸门 |
| §6 新闻 / 研报 | — | 研报源评估、是否独立 | `OPT-037/038/039` News Query 并行化 |
| §7 多市场 | — | 美股 / 加拿大时区 | `OPT-041/042/043/044` HK + ETF 已通 |
| §8 回测 | — | paper-trading 先于回测 | 历史 BacktestPage 已隐藏（隐式归档） |
| **doc 大扫除** | — | — | `archive/modules-legacy/`（2026-08-01：industry-flow / market-sentiment / news-brief 3 旧版模块文档） |

---

## 2. 产品定位与形态

> 一边用一边改的现状不可持续，必须先把"长期形态"定下来。

- **[P0] Tauri 桌面 vs 固定 URL**：用户明确倾向"固定 URL + 每张页面有专属链接"，桌面打包不是刚需 → 形态决策文档化到 `docs/archive/`。
- **[P0] 形态迁移路线**：保持当前 Next.js dev，把"对外可访问性"提到 P0；Tauri 不再作为主线交付形态（暂保留 build 配置但不做为发布目标）。
- **[P1] 可分享 / 可订阅 URL**：每张关键页面（Watchlist、Dashboard、特定 symbol）有 stable URL，方便 AI 助手 query、复盘自包含。
- **[P1] 基础 AI 能力保留**：内置 Chat Panel + 摘要生成（不依赖外部 AI 调用本地数据时也能用）。
- **[P4] 完整产品定位文档**：把"卫星仓纪律化操作工具"等核心定位写成一页式宣言。

---

## 3. 收益 / 交易策略（最高优先级 · 优先级 1）

> 架构层与技术债的工程任务 → `OPT-xxx`；交易规则调整 → `TIP-xxx` / `V6.x`。本节只放**产品/战略层**的收益相关决策。

- **[P0] 数据源质量审计**：现有源"非常杂，质量不高"。评估是否替换/补强：
  - Tushare（200/年，国内）：保留作为基线
  - HK：akshare（Sina）已验证最稳（`OPT-043`），不要再切
  - 评估：聚宽 / Wind mini / Choice / iFinD / 自建爬虫 中是否有 ROI 为正的补强
- **[P0] 重启回测系统**：原 BacktestPage 效果差已隐藏，待重做 → 见 §8
- **[P0] 漏斗转化率度量闭环**：`TIP-002` 已埋点，下一步是把 N 日表格/周报落地 → `archive/`。
- **[P1] 研报/新闻 α 来源**：见 §6。
- **[P1] 卫星仓上限 / 仓位管理复核**：当前 15% 单票 + 30% 板块 + 袖子上限体系是否仍合理（参考 §13 `positionPct` 复杂度）。
- **[P2] 开火来源归因（TV/Alpha/手动）**：`TIP-011` 已列未做 → 直接挂这里追踪。
- **[P2] Alpha 映射质量抽检**：`TIP-009`。

---

## 4. API 开放与外部 AI 打通（优先级 2）

> 让个人 AI 助手等外部系统能调用本系统的数据 / 信号。

- **[P0] OpenAI 兼容 API**：把 `/v1/chat/completions` 风格暴露出来，至少覆盖：
  - `GET /v1/market/snapshot?symbols=...` → 实时价/分时/技术指标
  - `GET /v1/watchlist/items`
  - `POST /v1/decision-journal/query`
  - `POST /v1/explain/{symbol}` → 复用本地 Chat Panel 的 retrieval + LLM
- **[P1] API Key 配额管理**：多 Key、scope（read-only / write-journal）、限流、审计日志。
- **[P1] API 文档 + OpenAPI schema**：docs/ 下生成可访问的接口文档。
- **[P2] Webhook / Push**：信号触发推送（执行价到位、Watchlist 命中 Alpha S 等）。
- **[P3] MCP server 暴露**：把核心 endpoint 做成 MCP 工具，能被 Claude Desktop/Cursor 直接调。

---

## 5. 工程架构与部署（优先级 3）

> 大概率不上云（DB 贵），但**单 Docker 一键起 + 可内网穿透**是底线。

- **[P0] DB 走向决策**：写一份 `archive/YYYY-MM-db-direction.md`，明确：
  - 主体仍然本地 Postgres（`OPT-032` 后 schema 已干净）
  - 云上只放 **只读副本** 给 API 用，本地仍是权威源
  - 或：放弃云，全部本地 + frp/zerotier 对外暴露
- **[P0] Docker compose 一键起**：已有 `docker-compose.yml`，要保证：
  - 包含 data-sync + desktop-ui 静态构建 + ai-service
  - 包含 Postgres（本地持久卷）
  - `pnpm dev` 仍可用作开发模式
- **[P1] 频繁改代码 vs 部署**：开发模式用热重载；发布模式用 Docker 镜像。**不**做自动 CI/CD（手工 build 已够用，省钱）。
- **[P1] 内网穿透/反向代理**：研究 Tailscale / Cloudflare Tunnel / FRP 中最适合"经常改代码"的方案。
- **[P2] Tauri 构建降级**：保留但停止维护 desktop 形态的 bug 修复（与 §2 决策一致）。
- **[P2] Alembic 迁移纪律**：见 `AGENTS.md`，所有 schema 改必须经过 Alembic（已建立 baseline）。

---

## 6. 数据源 / 浏览器替代（优先级 4）

> chrome 后台抓 TV 池子 → 不能上云 + 重。需要更轻量的方案。

- **[P1] ego-lite 调研**：`https://github.com/citrolabs/ego-lite`
  - 验证：是否真的能替代 Chrome 抓 screener
  - 验证：HTTP/无头、是否支持并发、是否合规反爬
  - 验证：能否容器化跑（云部署前提）
  - 写调研结论到 `archive/YYYY-MM-ego-lite-eval.md`
- **[P1] 付费 API 矩阵**：对比候选源 → 写 `archive/YYYY-MM-datasource-matrix.md`：
  - Tushare Pro / Tushare HK / 聚宽 / 通达信 L2 / iFinD / Wind mini / Choice
  - 维度：覆盖范围（CN/HK/US）、价格、限频、数据质量、稳定性
- **[P2] 自建爬虫兜底**：仅在上述都不可行时启动，最低优先级。
- **[P2] 资讯 RSS 源扩张**：参考现有 `OPT-038` 已并行化，源不够时再加；不要超过 20 个源（噪音 vs 收益边际递减）。

---

## 7. 新闻 / 研报

> 当前 News Brief 主要是 RSS + 摘要，研报是另一个量级的信息。

- **[P1] 研报源评估**：可用性 / 合规 / 价格
  - 巨潮资讯网（公开，免费但有限）
  - 慧博 / 进门策略（会员）
  - Wind/Choice 研报（最全，最贵）
  - 第三方聚合：萝卜投研 / 研报客
- **[P2] 是否独立**：决定是否单独抽一个 `karios-research` 子项目，避免污染主仓的卫星仓交易逻辑。
- **[P2] 研报 → α 通道**：研报里的标的 + 评级如何进 Watchlist 旁路（参考 Alpha Radar 流程 `TIP-004`）。

---

## 8. 回测（重启）

> 原 BacktestPage 效果差已隐藏。重启前先定假设，否则又是一次无效投入。

- **[P0] 重启前置条件**：
  1. 必须与 live 的 Execution Gate 同口径（不要重写一份规则）
  2. 必须能拉历史 bars（HK ≥5y 已通过 `OPT-043`；CN 5y+ 已有 `daily` 表）
  3. 纸面交易（paper-trading）先于纯回测——回测容易过拟合，paper 不会
- **[P1] Paper-trading daily 跑**：把当前 BUY/ADD 信号在收盘后假买入，跟踪 N 日后的实际表现
- **[P1] 单策略回测能力**：保留但**不作为发布决策依据**，只为理解参数敏感度
- **[P2] BacktestPage 重写**：产品形态（不写到本 todo 的 P0，因为要先有数字）

---

## 9. 多市场 / 远景（最低优先级）

> 加拿大生活规划会推高美股优先级，但当下数据不足决策。

- **[P3] 美股 symbol 闸门**：参考 HK 闸门（`OPT-041`）的 8 处 `symbol→ts_code` 改造范式
- **[P3] 时区 / 交易日适配**：美股 vs CN vs HK 时区不同，scheduler (`scheduler/*.job.py`) 需要按市场分别触发
- **[P3] 美股数据源**：yfinance 已被 rate-limit 实测（`OPT-043` 注）；评估 Polygon / Alpaca / IEX Cloud / Tiingo
- **[P4] 加拿大税务/账户模型**：完全 P4，先保持基础 symbol 闸门就够

---

## 相关执行清单（不在本文件更新范围内）

| 清单 | 命名 | 用途 | 状态 |
|------|------|------|------|
| 架构优化 | `OPT-001` ~ `OPT-044+` | 工程实现债 / 性能 / 兼容 | 滚动维护 |
| 交易改进 | `TIP-001` ~ `TIP-011` | 业务规则校准 | 大部分完成 |
| 交易中枢 | `V6.2-*` / `V6.3-*` | Execution Gate 子规则 | 完成 |

> **规则**：新任务先在 `todo.md` 起条；明确属于工程债 → 转 OPT-xxx；属于交易规则 → 转 TIP-xxx。todo 本体不写实现细节。

---

## 数据源 / 系统现状速览（备忘）

- **CN 行情**：Tushare（200/年）为主基线，akshare 多源兜底；港股走 tushare.hk_basic + akshare(stock_hk_daily) + yfinance(^HSI 指数)
- **ETF**：fund_basic 已同步（2102+），fund_daily 已启用，`OPT-042` 完成
- **新闻**：RSS + AI 摘要（`OPT-038` 并行化）
- **指数**：index_daily + index_dailybasic（`OPT-033` 批量读 + `OPT-034` 去重）
- **回测**：BacktestPage 隐藏，效果差；paper-trading 缺位
- **执行**：Execution Gate + Action Card + Decision Journal 三件套已上线（V6.x）

> 本节只用来"对账"，变更频繁请改 `optimization-checklist.md` / `trading-improvement-checklist.md`，本文件保持指向。

---

## 维护规则

1. **新增条目** 用 `[Px] {一句话动词 + 名词}`，必要时给 2-3 行补充。
2. **完成判定**：标 `[done] YYYY-MM-DD`，同时把摘要迁到 `archive/YYYY-MM-{slug}.md`，todo 上保留一行完成链接。
3. **优先级漂移**：如果某条用户口径变了，**先动 §0，再动该节**，避免局部拧。
4. **每 30 天回顾一次 §1 状态看板**，把长期 P0 但未动的项目显式降级或归档。

---

## 10. 已沉淀到 archive/

> "沉淀"指"重大判断 + 不希望被忘记"的事件，不是所有实现细节。一个事件通常一条独立归档（见 `archive/README.md`）。

| 日期 | 事件 | 归档位置 |
|------|------|----------|
| 2026-08-01 | doc 大扫除：3 个旧模块文档迁移至 `archive/modules-legacy/`（与 V6.x 规则脱节） | `archive/modules-legacy/README.md` |
| 2026-07-27 | V6.3 极端资金流豁免 `INTRADAY_OVERFLOW_OVERRIDE` + Alpha S TrendOK recovering | 见 `trading-improvement-checklist.md` V6.3 节 |
| 2026-07-24 | V6.2 14:30 尾盘时间锁 + 防守双轨袖子 + Zero-Pos 归零清场 | 见 `trading-improvement-checklist.md` V6.2 节 |
| 2026-07-22 | 漏斗转化率 / Pullback 主宇宙校准 / Alpha 进池闸 / Alpha GC 对称化 | 见 `trading-improvement-checklist.md` TIP-001~006 |
| 2026-07-29 | HK + ETF 闸门全打通（OPT-041~044） | 见 `optimization-checklist.md` |

---

## 11. 注意力预算（自用）

> 散点信息太多时容易自乱。这节规定每天 / 每周的"读哪里 / 改哪里"。

| 周期 | 必读 | 可选 |
|------|------|------|
| 每天开工前 | 本 todo §1 状态看板 | — |
| 每天开工前 | `modules/watchlist.md` Execution Gate 节（确认 live 闸与纸面一致） | `modules/screener.md` 若今天改了 screener |
| 每周一次（周末） | §0 优先级表 → 是不是要漂移 | §10 沉淀表 → 是不是有重大事件该归档 |
| 改动 schema / 新依赖前 | `AGENTS.md` + `optimization-checklist.md`（OPT-xxx 进行中列表） | — |
| 修改交易规则前 | `trading-improvement-checklist.md` 最新一条 → 沿革 | — |
| 想做 idea 但排不上 P0 | 起一份草稿到 `designs/`（不要污染 todo） | — |

**反模式**：

- ❌ 把 §1 看板改满 ✓ 之后没有任何 archive 落地 —— todo 不能"假装完成"。
- ❌ 没有拍板就长期留在 todo P0；要么降级要么归档。
- ❌ 新建散点 markdown 文档（"会议纪要" / "杂记"）—— docs/ 只允许本 todo + 真值模块 + 设计草稿 + 归档。
