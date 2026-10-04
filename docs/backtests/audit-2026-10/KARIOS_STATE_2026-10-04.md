# Karios 当前状态与最新发现（2026-10-04，给接手的 agent 先读这份）

## 决策与规则（Yuan 定的）
- 基准 = 母港 M30（70% 港湾选股 + 30% B3 五ETF逆波动，月度调仓）。新策略必须在 long、valid、holdout 三段都跑赢 M30 才算数。M30 数字：long +152.6% / valid +35.0% / holdout −8.8%（连续切片口径 +152.6/+12.4/−7.3）。
- 星舰B = 观察（复活条件：近40笔滚动收益转正且大票月盈亏转正）。实盘由 Yuan 决定。
- 任何方法先过严格检验（前视、相邻参数、随机对照、成本×2/+1天、分段滚动、2021年前、蒙特卡洛、压力与容量）才给名字、做决定。
- 先查 `STRATEGY_LEDGER.md`，测过的不重测；每个新测试（过或不过）都记进账本。赚钱线索写 `profit-leads.md`。
- 资金 ≤200万 深耕，利用小资金优势；不碰没有分析框架的敞口（裸商品）。
- 2026-10-03 Yuan 决定停车方案"先不改，节后再看"（首个交易日约 10/9，结合伊朗/霍尔木兹消息）。

## 10-03 ~ 10-04 新发现（报告都在本目录）
- VOLT_AUDIT / VOLT_ROBUST：「波动封顶」VOLT15 没过严格检验（随机对照 valid 40.6%、holdout 19.8%，滚动只赢 4/10 块，月调即失效）。定位：防崩盘（油 −30% 损失减半），不是收益增强。不起名、观察。
- QDII_PREMIUM：回测用场内收盘价，溢价已含在内，之前的 2%/5% 惩罚是重复扣，作废。M30 没高估；溢价扩张贡献约 +11.5pt（脆）。纳指 513100 溢价约 12% 处历史高位。真实滑点 120万 <1pt。
- CAPACITY_MAP：200万以下 M30/港湾/B3 几乎不受影响（50→500万 仅降 1~3pt）。瓶颈只有：原油 513350（规则：当日成交 <5000万 或参与度 >5% 停车仓降50%）与星舰B原版（150~200万优势腰斩）。
- THIRD_LEG：6 个新方向全 REJECT（最接近 ETF 五日反转：延迟一天即崩）。国债 MA60 相关≈0 但年化 3%。组合不变。
- NICHE_QDII：32 只小众品种扫描、7 个快筛全 REJECT。陷阱：QDII 溢价反转用 lag1 是前视（净值 T+2 才公布），honest lag3 亏钱。线索：30年国债 511090 与 M30 相关 −0.27；溢价 >10% 可作减仓闸（未测）。
- 我们的策略基本都是趋势/动量（港湾停车、星舰B、S-gap），B3 是风险平价例外。第三条腿应找非趋势的结构性收益。
- 已完成 BACKFILL_LEGS（2026-10-04，报告 `BACKFILL_LEGS_report.md`，账本已记）：红利低波个股、港股中小盘/AH、LOF 溢价/折价、可转债强赎过滤共 9 个新测全部 REJECT，组合不动，基线仍为 M30。最接近的是 DIV_YIELD10（纯股息率前十只、月持）：五年 +198.8%（超 M30 约 46pt），与 M30 日相关 0.18、月相关 0.10，成本×2 和延迟 +1 天都稳；但 OOS2 +32.7%（输 16pt）、valid +0.5%（输 12pt），列为观察、仓位 0%，需全市场样本加 50 笔以上再裁。LOF_DISCOUNT3 表面最亮但延迟一天即崩、诚实口径收益腰斩，属偷看陷阱，不观察。

## 代码与分支
- karios-desktop：`feat/fleet-strategy` 含 M30 基准切换（50c4188d）、舰队/冷库/S-gap 页、组合层 PR1、审计文档（docs/backtests/audit-2026-10/）。2026-10-04 正在整合进 main（integrate/m30-main），全部页面默认/实盘改为 M30，测试全绿才推送。
- 文档同步脚本：`~/Projects/wealth-ideas/sync_docs_to_karios_desktop.sh`（把本目录 md/png、账本、profit-leads 复制进 karios-desktop 并本地提交）。

## 报告索引（按新到旧）
NICHE_QDII, CAPACITY_MAP, QDII_PREMIUM, THIRD_LEG, VOLT_ROBUST, VOLT_AUDIT, PARKCAP, NOCOMMOD, M30_BASELINE, REGIME_LEAD, M30_REGIME, BOLLKDJ, FINAL_REPORT, COMPARE_report。
