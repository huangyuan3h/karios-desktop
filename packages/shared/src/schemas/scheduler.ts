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

  /* Alpha Radar ----------------------------------------------------------- */
  meta(
    'alpha_radar_pipeline',
    'alphaRadar',
    'Alpha Radar 主流程',
    'RSS 抓取 → 过滤 → 全文 → 批量 LLM → A 股映射；每 12 小时一次，12h 冷却。',
    '每 12 小时',
    null,
    'interval',
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
    '处理 raw 状态文档为趋势卡片，包括 LLM 抽取与映射。',
    '每 1 小时',
    null,
    'interval',
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
];

/** Group display order in the UI. */
export const SCHEDULER_GROUP_ORDER: readonly SchedulerJobGroup[] = [
  'coreClose',
  'cnBasic',
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
