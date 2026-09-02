import { z } from 'zod';

/** A single run record from sync_job_record. */
export const SyncJobRecordSchema = z.object({
  id: z.number(),
  job_type: z.string(),
  sync_at: z.string(),
  success: z.boolean(),
  last_ts_code: z.string().nullable().optional(),
  error_message: z.string().nullable().optional(),
});
export type SyncJobRecord = z.infer<typeof SyncJobRecordSchema>;

/** Per-job status block returned by GET /sync/jobs. */
export const SchedulerJobStatusSchema = z.object({
  todayRun: SyncJobRecordSchema.nullable(),
  lastSuccess: SyncJobRecordSchema.nullable(),
});
export type SchedulerJobStatus = z.infer<typeof SchedulerJobStatusSchema>;

/** HK industry coverage metrics (mapped / total / pct). */
export const HkIndustryCoverageSchema = z.object({
  ok: z.boolean().optional(),
  totalHk: z.number(),
  mappedHk: z.number(),
  missingHk: z.number(),
  coveragePct: z.number(),
  jobType: z.string().optional(),
  error: z.string().optional(),
});
export type HkIndustryCoverage = z.infer<typeof HkIndustryCoverageSchema>;

/** Alpha Radar pipeline extras (subset of /api/alpha-radar/status). */
export const AlphaRadarSnapshotSchema = z.object({
  jobType: z.string().optional(),
  todayRun: SyncJobRecordSchema.nullable().optional(),
  lastSuccess: SyncJobRecordSchema.nullable().optional(),
  lastRunAt: z.string().nullable().optional(),
  lastIngestAt: z.string().nullable().optional(),
  lastProcessAt: z.string().nullable().optional(),
  lastTrendCount: z.number().optional(),
  currentTrendCount: z.number().optional(),
  accumulatedTrendCount: z.number().optional(),
  rawBacklogCount: z.number().optional(),
  withinCooldown: z.boolean().optional(),
  cooldownHours: z.number().optional(),
  lastIngestStats: z
    .object({
      fetched: z.number().optional(),
      filteredOut: z.number().optional(),
      stored: z.number().optional(),
      new: z.number().optional(),
      requeued: z.number().optional(),
      unchanged: z.number().optional(),
    })
    .nullable()
    .optional(),
  error: z.string().optional(),
});
export type AlphaRadarSnapshot = z.infer<typeof AlphaRadarSnapshotSchema>;

/** Watchlist automation latest run (shape mirrors /watchlist/automation/latest). */
export const WatchlistAutomationSnapshotSchema = z
  .object({
    runId: z.string().optional(),
    tradeDate: z.string().nullable().optional(),
    skipped: z.boolean().optional(),
    skipReason: z.string().nullable().optional(),
    removeItems: z.array(z.unknown()).optional(),
    alphaAdd: z.array(z.unknown()).optional(),
    screenerAdded: z.number().nullable().optional(),
    createdAt: z.string().nullable().optional(),
    appliedAt: z.string().nullable().optional(),
    error: z.string().optional(),
  })
  .nullable();
export type WatchlistAutomationSnapshot = z.infer<typeof WatchlistAutomationSnapshotSchema>;

/** Full aggregate response from GET /sync/jobs. */
export const SchedulerJobsResponseSchema = z.object({
  ok: z.boolean(),
  jobs: z.record(z.string(), SchedulerJobStatusSchema),
  hkIndustryCoverage: HkIndustryCoverageSchema.nullable(),
  alphaRadar: AlphaRadarSnapshotSchema.nullable(),
  watchlistAutomation: WatchlistAutomationSnapshotSchema,
});
export type SchedulerJobsResponse = z.infer<typeof SchedulerJobsResponseSchema>;

/* -------------------------------------------------------------------------- */
/*  Job catalog (frontend-only metadata; the backend stays the source of     */
/*  truth for status rows. Group + Chinese title + schedule description are    */
/*  presentation, so they live here to avoid drift across UI surfaces).       */
/* -------------------------------------------------------------------------- */

export const SchedulerJobGroupSchema = z.enum([
  'coreClose',
  'cnBasic',
  'cnIndustry',
  'hk',
  'etf',
  'indexMacro',
  'eastmoneyIndustry',
  'watchlistAutomation',
  'alphaRadar',
  'news',
]);
export type SchedulerJobGroup = z.infer<typeof SchedulerJobGroupSchema>;

/** Trigger interval family — drives the UI pill color. */
export const SchedulerTriggerKindSchema = z.enum([
  'cron',
  'interval',
  'manual',
]);
export type SchedulerTriggerKind = z.infer<typeof SchedulerTriggerKindSchema>;

/** Optional metadata for jobs that can be triggered manually from the UI. */
export const SchedulerJobActionSchema = z.object({
  endpoint: z.string(),
  method: z.enum(['POST', 'GET']),
  label: z.string(),
  confirmForce: z.boolean().optional(),
});
export type SchedulerJobAction = z.infer<typeof SchedulerJobActionSchema>;

/** Static metadata for a single scheduled job. */
export const SchedulerJobMetaSchema = z.object({
  jobType: z.string(),
  group: SchedulerJobGroupSchema,
  titleCn: z.string(),
  descriptionCn: z.string(),
  scheduleCn: z.string(),
  scheduleCron: z.string().nullable(),
  trigger: SchedulerTriggerKindSchema,
  /** When true, the job writes to sync_job_record; status comes from API. */
  tracked: z.boolean(),
  action: SchedulerJobActionSchema.optional(),
  /** Lower numbers render first within a group. */
  sortOrder: z.number(),
});
export type SchedulerJobMeta = z.infer<typeof SchedulerJobMetaSchema>;

/** Helper: build a meta entry inline. Reduces repetition in the catalog. */
function meta(
  jobType: string,
  group: SchedulerJobGroup,
  titleCn: string,
  descriptionCn: string,
  scheduleCn: string,
  scheduleCron: string | null,
  trigger: SchedulerTriggerKind,
  tracked: boolean,
  sortOrder: number,
  action?: SchedulerJobAction,
): SchedulerJobMeta {
  return {
    jobType,
    group,
    titleCn,
    descriptionCn,
    scheduleCn,
    scheduleCron,
    trigger,
    tracked,
    sortOrder,
    action,
  };
}

/** Authoritative catalog of every scheduled job the UI surfaces. */
export const SCHEDULER_JOB_CATALOG: readonly SchedulerJobMeta[] = [
  /* core close-time sync -------------------------------------------------- */
  meta(
    'stock_close_sync',
    'coreClose',
    'A 股收盘同步',
    '收盘后按交易日窗口更新 daily + adj_factor；同步成功后链式触发指数/宏观/行业/ETF 流向/Top 机构/期权 IV。',
    '每日 17:10 Asia/Shanghai',
    '10 17 * * *',
    'cron',
    true,
    10,
    { endpoint: '/sync/close?force=true', method: 'POST', label: '立即同步收盘' },
  ),
  meta(
    'stock_close_catchup',
    'coreClose',
    '收盘同步补跑',
    '工作日 17:00–23:00 每 10 分钟检查；收盘同步失败或漏跑时强制重跑。',
    '工作日 17:00–23:00 每 10 分钟',
    '*/10 17-23 * * 1-5',
    'cron',
    true,
    20,
  ),
  meta(
    'stock_daily_full',
    'coreClose',
    'A 股全量日线（备用）',
    '已废弃：每周五 17:00 兜底同步；正常路径由 stock_close_sync 覆盖。',
    '每周五 17:00 (已废弃)',
    '0 17 * * 5',
    'cron',
    true,
    30,
  ),
  meta(
    'stock_adj_factor_full',
    'coreClose',
    'A 股复权因子（备用）',
    '每周五 17:00 全量复权因子同步，作为正常收盘同步的兜底。',
    '每周五 17:00',
    '0 17 * * 5',
    'cron',
    true,
    40,
    { endpoint: '/sync/adj-factor', method: 'POST', label: '立即同步' },
  ),

  /* CN basic data --------------------------------------------------------- */
  meta(
    'stock_basic_sync',
    'cnBasic',
    'A 股股票列表',
    '从 tushare 同步 A 股 stock_basic，幂等 upsert。',
    '每周五 18:00',
    '0 18 * * 5',
    'cron',
    true,
    10,
    { endpoint: '/sync/stock-basic', method: 'POST', label: '立即同步' },
  ),

  /* HK sync (NEW) --------------------------------------------------------- */
  meta(
    'hk_basic_sync',
    'hk',
    '港股股票列表',
    '从 tushare 同步港股 hk_basic 至 stock_basic (market=HK)，每月一次；保留已填的行业信息。',
    '每月 1 日 03:30',
    '30 3 1 * *',
    'cron',
    true,
    10,
    { endpoint: '/sync/hk-basic', method: 'POST', label: '立即同步' },
  ),
  meta(
    'hk_daily_full',
    'hk',
    '港股日线 K 线',
    'akshare (Sina) → yfinance → tushare 三级回落，每交易日 17:30 增量同步；按失败断点续跑。',
    '每日 17:30',
    '30 17 * * *',
    'cron',
    true,
    20,
    { endpoint: '/sync/hk-daily', method: 'POST', label: '立即同步' },
  ),
  meta(
    'hk_industry_sync',
    'hk',
    '港股行业映射',
    '从雪球 mbu 拉取缺失行业的港股代码，每天最多补 200 条。',
    '每日 02:00',
    '0 2 * * *',
    'cron',
    true,
    30,
    { endpoint: '/sync/hk-industry', method: 'POST', label: '立即同步' },
  ),

  /* ETF ------------------------------------------------------------------- */
  meta(
    'etf_fund_basic_sync',
    'etf',
    'ETF 基金列表',
    '从 tushare fund_basic(market=E) 同步至 stock_basic (market=ETF)，每月一次。',
    '每月 1 日 04:00',
    '0 4 1 * *',
    'cron',
    true,
    10,
    { endpoint: '/sync/etf-fund-basic', method: 'POST', label: '立即同步' },
  ),
  meta(
    'etf_daily_full',
    'etf',
    'ETF 日线 K 线',
    '全量 ETF 日线同步至 daily table，月度运行。',
    '每月 1 日 19:00',
    '0 19 1 * *',
    'cron',
    true,
    20,
    { endpoint: '/sync/etf-daily', method: 'POST', label: '立即同步' },
  ),

  /* Index + Macro --------------------------------------------------------- */
  meta(
    'index_daily_full',
    'indexMacro',
    '指数日线',
    '主要指数日线同步 (如 000001.SH / 000300.SH / 000905.SH)。',
    '工作日 16:30',
    '30 16 * * 1-5',
    'cron',
    true,
    10,
    { endpoint: '/sync/index-daily', method: 'POST', label: '立即同步' },
  ),
meta(
    'index_basic_sync',
    'indexMacro',
    '指数每日指标（市宽）',
    '独立调度 pro.index_dailybasic（turnover_rate / float_mv），保证 macro_snapshot 市宽无需用户触发 Dashboard。',
    '工作日 17:15',
    '15 17 * * 1-5',
    'cron',
    true,
    11,
    { endpoint: '/sync/index-basic', method: 'POST', label: '立即同步' },
  ),
  meta(
    'sleeve_etf_daily_sync',
    'indexMacro',
    '核心腿 ETF 日线（机会双子星）',
    '增量同步机会双子星核心腿 5 只 ETF（金/油/纳×2/债）日线——mom60/MA200 决策依赖；修复原每月一次 cron 导致 GOLD/BOND10 停更 7+ 天的缺陷。',
    '工作日 17:25',
    '25 17 * * 1-5',
    'cron',
    true,
    13,
    { endpoint: '/sync/sleeve-etfs', method: 'POST', label: '立即同步' },
  ),
  meta(
    'stock_daily_basic_sync',
    'indexMacro',
    '个股估值（市值）',
    '增量同步 stock_dailybasic（total_mv / circ_mv / turnover_rate）——机会双子星卫星 S-gap 低波候选的每日市值依赖；2026-08-07 起曾停更，本任务补齐并每日维护。',
    '工作日 17:20',
    '20 17 * * 1-5',
    'cron',
    true,
    12,
    { endpoint: '/sync/daily-basic', method: 'POST', label: '立即同步' },
  ),
  meta(
    'etf_daily_full_sync',
    'indexMacro',
    '全市场 ETF 日线（低频）',
    '全市场 fund_daily 月频补全（限速 200 次/分钟，非决策路径；决策路径走核心腿 ETF 每日同步）。',
    '每月 1 日 19:00',
    '0 19 1 * *',
    'cron',
    true,
    14,
  ),
  meta(
    'macro_daily_full',
    'indexMacro',
    '宏观/全球数据',
    '美股收盘后同步全球宏观日线序列。',
    '周二–周六 07:00',
    '0 7 * * 2-6',
    'cron',
    true,
    20,
    { endpoint: '/sync/macro-daily', method: 'POST', label: '立即同步（force）', confirmForce: true },
  ),

  /* Eastmoney industry ---------------------------------------------------- */
  meta(
    'eastmoney_industry_sync',
    'eastmoneyIndustry',
    '东方财富行业映射',
    '增量同步 A 股东方财富行业分类，每天最多补 1000 条。',
    '工作日 18:00',
    '0 18 * * 1-5',
    'cron',
    true,
    10,
    { endpoint: '/sync/eastmoney-industry', method: 'POST', label: '立即同步' },
  ),

  /* Watchlist automation -------------------------------------------------- */
  meta(
    'watchlist_automation',
    'watchlistAutomation',
    '自选股收盘自动化',
    '工作日 17:30 收盘后，自动剔除弱势从 screener 导入新标的并附加 Alpha Radar 强信号。',
    '工作日 17:30',
    '30 17 * * 1-5',
    'cron',
    true,
    10,
  ),
  meta(
    'intraday_score',
    'watchlistAutomation',
    '盘中分数刷新（实时价）',
    '交易日 10:30 / 14:00 用实时行情合并最后一根 K 线重算 score_daily（落当日 trade_date），让盘中 S-3 体检立即出候选；17:30 收盘任务会用收盘价覆盖同一批行。',
    '工作日 10:30 / 14:00',
    '30 10,14 * * 1-5',
    'cron',
    true,
    5,
    { endpoint: '/watchlist/automation/intraday-scores', method: 'POST', label: '立即刷新' },
  ),
  meta(
    'watchlist_funnel_health',
    'watchlistAutomation',
    '漏斗健康检查',
    '工作日 18:10 用最新 TV 快照 + K 线回放入池漏斗（TV 命中 → 52W 回撤）；回撤关连续 3 天 0 通过则记失败并出现在健康页 Job Failures。',
    '工作日 18:10',
    '10 18 * * 1-5',
    'cron',
    true,
    10,
    { endpoint: '/watchlist/automation/funnel-health/check', method: 'POST', label: '立即检查' },
  ),

  /* CN industry + sentiment (post-close) ----------------------------------- */
  meta(
    'cn_industry_post_close_sync',
    'cnIndustry',
    '盘后行业/情绪/主线',
    '工作日 17:35 同步 industry_fund_flow + industry_mainline + cn_sentiment，让 Dashboard 顶部始终保持新，无需点 Sync all。',
    '工作日 17:35',
    '35 17 * * 1-5',
    'cron',
    true,
    10,
     { endpoint: '/market/cn/industry-fund-flow/sync', method: 'POST', label: '立即同步' },
   ),

  /* Alpha Radar ----------------------------------------------------------- */
  meta(
    'alpha_radar_pipeline',
    'alphaRadar',
    'Alpha Radar 主流程',
    'RSS 抓取 → 过滤 → 全文 → 批量 LLM → A 股映射；19:30 每天（平峰价，OPT-108）。',
    '19:30 每天',
    '30 19 * * *',
    'cron',
    true,
    10,
    { endpoint: '/api/alpha-radar/run-pipeline', method: 'POST', label: '强制运行主流程', confirmForce: true },
  ),
  meta(
    'alpha_radar_ingest',
    'alphaRadar',
    'Alpha Radar RSS 抓取',
    '仅 RSS 抓取与入库；与主流程解耦，缩短新增信号延迟。',
    '每 4 小时',
    null,
    'interval',
    true,
    20,
    { endpoint: '/api/alpha-radar/run-ingest', method: 'POST', label: '立即抓取' },
  ),
  meta(
    'alpha_radar_process',
    'alphaRadar',
    'Alpha Radar 文档处理',
    '处理 raw 状态文档为趋势卡片，包括 LLM 抽取与映射；20:30/23:30/02:30/05:30（平峰价，OPT-108）。',
    '20:30/23:30/02:30/05:30',
    '30 20,23,2,5 * * *',
    'cron',
    true,
    30,
    { endpoint: '/api/alpha-radar/run-process', method: 'POST', label: '立即处理' },
  ),

  /* News ------------------------------------------------------------------ */
  meta(
    'news_fetch_job',
    'news',
    '财经新闻 RSS',
    '每 4 小时从 RSS 源抓取财经新闻。',
    '每 4 小时',
    null,
    'interval',
    true,
    10,
  ),
  meta(
    'news_enrich_job',
    'news',
    '新闻 LLM 富化',
    '对新抓取的新闻进行 LLM 分析，提取关联个股、板块、事件类型、重要性与相关性评分；20:00/23:00/05:00（平峰价，OPT-108）。',
    '20:00/23:00/05:00',
    '0 20,23,5 * * *',
    'cron',
    true,
    11,
    { endpoint: '/api/news/enrichment/run', method: 'POST', label: '运行富化' },
  ),
  meta(
    'morning_brief_am',
    'news',
    '早间简报',
    '工作日 08:30 生成当日早间投资简报，精选 5–7 条重要新闻。',
    '工作日 08:30',
    null,
    'cron',
    true,
    12,
    { endpoint: '/api/news/brief/generate?brief_type=morning', method: 'POST', label: '生成早间简报' },
  ),
  meta(
    'morning_brief_pm',
    'news',
    '午间简报',
    '工作日 12:30 生成午间投资简报，精选上午时段重要新闻。',
    '工作日 12:30',
    null,
    'cron',
    true,
    13,
     { endpoint: '/api/news/brief/generate?brief_type=midday', method: 'POST', label: '生成午间简报' },
  ),
  meta(
    'trading_brief_open',
    'news',
    '开盘简报 (10:00)',
    '工作日 10:00 生成开盘简报：Regime/恐慌 + S-3 候选 + 隔夜新闻 Top5（30 秒读完）。',
    '工作日 10:00',
    null,
    'cron',
    true,
    14,
    { endpoint: '/api/news/brief/generate?brief_type=trading-open', method: 'POST', label: '生成开盘简报' },
  ),
  meta(
    'trading_brief_midday',
    'news',
    '午间简报 (12:00)',
    '工作日 12:00 生成午间简报：候选漂移 + 持仓接近止损线 + 新闻 Top5。',
    '工作日 12:00',
    null,
    'cron',
    true,
    15,
    { endpoint: '/api/news/brief/generate?brief_type=trading-midday', method: 'POST', label: '生成午间简报' },
  ),
  meta(
    'trading_brief_action',
    'news',
    '操作卡 (14:00)',
    '工作日 14:00 生成操作卡：买入卡列表 + 条件单清单 + 预警 + 执行卡推送（闸门状态/买入候选/退出持仓，webhook 直达手机）。',
    '工作日 14:00',
    null,
    'cron',
    true,
    16,
    { endpoint: '/api/news/brief/generate?brief_type=trading-action', method: 'POST', label: '生成操作卡' },
  ),
  meta(
    'research_report_sync',
    'alphaRadar',
     '研报同步',
     '每 2 小时从东方财富研报中心抓取最新个股研报（评级/目标价/EPS），供研报 α 通道入池。',
     '每 2 小时',
     null,
     'interval',
     true,
     18,
     { endpoint: '/api/research/sync', method: 'POST', label: '立即同步' },
  ),
  meta(
    'paper_chain_watchdog',
    'coreClose',
    'Paper 链看门狗',
    '工作日 18:05 自检收盘链（17:30 算分 / 17:42 S-3 intake / 17:45 update），缺失且 close_sync 成功则自动补跑。',
    '工作日 18:05',
    null,
    'cron',
    true,
     19,
  ),
  meta(
    'weekly_review',
    'coreClose',
    '周度复盘',
    '周一 07:40 聚合上周（周一至周五）决策量 / paper 实绩 / 卖出归因 / 回测对账，生成周度决策质量报告（morning_briefs: weekly-review）。',
    '周一 07:40',
    null,
    'cron',
    true,
     20,
  ),
  meta(
    'intraday_alarm',
    'coreClose',
    '盘中 -8% 巡检',
    '工作日 10-14 点整点拉取 open paper 仓实时价，跌破入场价 -8% 触发一次性 webhook 警报（券商条件单兜底，此为兜底提醒）。',
    '工作日 10/11/12/13/14 点',
    null,
    'cron',
    true,
     21,
  ),
  meta(
    'webhook_delivery',
    'coreClose',
    'Webhook 投递',
    '每分钟扫 pending webhook 事件并投递（HMAC 签名 · 失败退避 5/15/60 分钟 x3 · 单订阅 30 条/分钟限频）。',
    '每分钟',
    null,
    'cron',
    true,
     22,
  ),
  meta(
    'candidate_diff',
    'coreClose',
    '候选新增对比',
    '工作日 17:35 对比今日与上一交易日 S-3 候选，新增符号推送 webhook（候选消失=闸门关闭属正常，不推）。',
    '工作日 17:35',
    null,
    'cron',
    true,
     23,
  ),
  meta(
    'behavior_audit',
    'coreClose',
    '行为对账 (18:45)',
    '工作日 18:45 收盘后自动跑真实持仓 vs S-3 回测行为对账（simulate 数分钟），watchlist 横幅免手动刷新；发现不符项推送 audit_issues webhook。',
    '工作日 18:45',
    null,
    'cron',
    true,
     24,
  ),
  meta(
    'twin_star_reminder',
    'watchlistAutomation',
    '机会双子星 14:30 前提醒',
    '工作日 14:20 先拉全市场当日行情再推送：核心择强 + 卫星买卖（涨停给出备选），供 14:30 执行。',
    '工作日 14:20',
    null,
    'cron',
    true,
     25,
  ),
  meta(
    'twin_star_intraday',
    'watchlistAutomation',
    '机会双子星盘中快照',
    '交易时段每分钟拉全市场行情，把当日快照当作最后一根 K 线筛卫星。15:00 冻结，保留到次日 09:00。',
    '工作日 09:30–15:00 每分钟',
    null,
    'cron',
    true,
    26,
  ),
  meta(
    'paper_twin_star',
    'watchlistAutomation',
    '机会双子星 paper 簿',
    '工作日 17:43 写入 clip4 卫星 paper（最多 4 槽 × 12.5% NAV，body=3 / −5% 保护止损）。与 S-3 paper 拆开，引擎回放只作对照。',
    '工作日 17:43',
    '43 17 * * 1-5',
    'cron',
    true,
    27,
  ),
];

/** Group display order in the UI. */
export const SCHEDULER_GROUP_ORDER: readonly SchedulerJobGroup[] = [
  'coreClose',
  'cnBasic',
  'cnIndustry',
  'hk',
  'etf',
  'indexMacro',
  'eastmoneyIndustry',
  'watchlistAutomation',
  'alphaRadar',
  'news',
];

export const SCHEDULER_GROUP_META: Record<
  SchedulerJobGroup,
  { titleCn: string; descriptionCn: string }
> = {
  coreClose: {
    titleCn: '核心收盘同步',
    descriptionCn: 'A 股收盘日线、复权因子及后置链式同步。',
  },
  cnBasic: {
    titleCn: 'A 股基础数据',
    descriptionCn: '股票列表、上市状态等基础信息。',
  },
  cnIndustry: {
    titleCn: '盘后行业/情绪',
    descriptionCn: 'A 股盘后行业资金流向、主线与情绪指标同步。',
  },
  hk: {
    titleCn: '港股同步',
    descriptionCn: '港股股票列表、日线 K 线及行业映射。',
  },
  etf: {
    titleCn: 'ETF 同步',
    descriptionCn: '基金列表与日线 K 线数据。',
  },
  indexMacro: {
    titleCn: '指数与宏观',
    descriptionCn: '指数日线、宏观/全球数据。',
  },
  eastmoneyIndustry: {
    titleCn: '东方财富行业映射',
    descriptionCn: 'A 股东方财富行业分类的增量同步。',
  },
  watchlistAutomation: {
    titleCn: '自选股自动化',
    descriptionCn: '收盘后自选股清理、screener 导入与 Alpha Radar 追加。',
  },
  alphaRadar: {
    titleCn: 'Alpha Radar 情报',
    descriptionCn: 'RSS 抓取 → LLM 抽取 → A 股映射的端到端 pipeline。',
  },
  news: {
    titleCn: '财经新闻',
    descriptionCn: 'RSS 抓取的财经新闻。',
  },
};

/** Pre-sorted jobs grouped by their group, ready for rendering. */
export function groupSchedulerJobs(
  meta: readonly SchedulerJobMeta[] = SCHEDULER_JOB_CATALOG,
): Array<{ group: SchedulerJobGroup; jobs: SchedulerJobMeta[] }> {
  const buckets = new Map<SchedulerJobGroup, SchedulerJobMeta[]>();
  for (const m of meta) {
    const arr = buckets.get(m.group) ?? [];
    arr.push(m);
    buckets.set(m.group, arr);
  }
  return SCHEDULER_GROUP_ORDER.filter((g) => buckets.has(g)).map((group) => ({
    group,
    jobs: (buckets.get(group) ?? []).slice().sort((a, b) => a.sortOrder - b.sortOrder),
  }));
}
