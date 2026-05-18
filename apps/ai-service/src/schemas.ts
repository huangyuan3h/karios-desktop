import { z } from 'zod';

export const ConfigProfileCreateSchema = z.object({
  name: z.string().min(1),
  provider: z.enum(['openai', 'google', 'ollama']),
  modelId: z.string().min(1),
  setActive: z.boolean().optional(),
  openai: z
    .object({
      apiKey: z.string().optional(),
      baseUrl: z.string().optional(),
    })
    .optional(),
  google: z
    .object({
      apiKey: z.string().optional(),
    })
    .optional(),
  ollama: z
    .object({
      baseUrl: z.string().optional(),
      apiKey: z.string().optional(),
    })
    .optional(),
});

export const ConfigProfileUpdateSchema = ConfigProfileCreateSchema.partial().extend({
  setActive: z.boolean().optional(),
});

export const ConfigSetActiveSchema = z.object({
  profileId: z.string().min(1),
});

export const ConfigTestSchema = z.object({
  profileId: z.string().min(1).optional(),
});

export const TitleRequestSchema = z.object({
  text: z.string().min(1).max(8000),
  systemPrompt: z.string().optional(),
});

export const BrokerExtractRequestSchema = z.object({
  imageDataUrl: z.string().min(1),
});

export const BrokerExtractResponseSchema = z.object({
  kind: z.enum([
    'account_overview',
    'positions',
    'conditional_orders',
    'trades',
    'settlement_statement',
    'unknown',
  ]),
  broker: z.literal('pingan'),
  extractedAt: z.string(),
  data: z.record(z.any()).optional(),
});

export const StrategyDailyRequestSchema = z.object({
  date: z.string().min(1),
  accountId: z.string().min(1),
  accountTitle: z.string().optional(),
  accountPrompt: z.string().optional(),
  context: z.record(z.any()),
});

export const StrategyCandidatesRowSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  rank: z.number().int().min(1).max(5),
  why: z.string(),
  scoreBreakdown: z
    .object({
      trend: z.number().min(0).max(40),
      flow: z.number().min(0).max(30),
      structure: z.number().min(0).max(20),
      risk: z.number().min(0).max(10),
    })
    .optional(),
});

export const StrategyCandidatesResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  candidates: z.array(StrategyCandidatesRowSchema).max(5),
  leader: z.object({
    symbol: z.string(),
    reason: z.string(),
  }),
  riskNotes: z.array(z.string()).optional(),
  model: z.string(),
});

export const StrategyDailyMarkdownResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  markdown: z.string(),
  model: z.string(),
});

export const LeaderDailyRequestSchema = z.object({
  date: z.string().min(1),
  context: z.record(z.any()),
});

export const LeaderBuyZoneSchema = z.object({
  low: z.union([z.number(), z.string()]),
  high: z.union([z.number(), z.string()]),
  note: z.string().optional(),
});

export const LeaderTriggerSchema = z.object({
  kind: z.enum(['breakout', 'pullback']),
  condition: z.string(),
  value: z.union([z.number(), z.string()]).optional(),
});

export const LeaderTargetPriceSchema = z.object({
  primary: z.union([z.number(), z.string()]),
  stretch: z.union([z.number(), z.string()]).optional(),
  note: z.string().optional(),
});

export const LeaderPickSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  reason: z.string(),
  whyBullets: z.array(z.string()).min(3).max(6),
  expectedDurationDays: z.number().int().min(1).max(10),
  buyZone: LeaderBuyZoneSchema,
  triggers: z.array(LeaderTriggerSchema).min(1).max(4),
  invalidation: z.string(),
  targetPrice: LeaderTargetPriceSchema,
  probability: z.number().int().min(1).max(5),
  risks: z.array(z.string()).min(2).max(4),
  sourceSignals: z
    .object({
      industries: z.array(z.string()).optional(),
      screeners: z.array(z.string()).optional(),
      notes: z.array(z.string()).optional(),
    })
    .optional(),
  riskPoints: z.array(z.string()).optional(),
});

export const LeaderDailyResponseSchema = z.object({
  date: z.string(),
  leaders: z.array(LeaderPickSchema).max(2),
  model: z.string(),
});

export const MainlineThemeInputSchema = z.object({
  kind: z.enum(['industry', 'concept']),
  name: z.string().min(1),
  evidence: z.record(z.any()),
});

export const MainlineExplainRequestSchema = z.object({
  date: z.string().min(1),
  themes: z.array(MainlineThemeInputSchema).min(1).max(20),
  context: z.record(z.any()).optional(),
});

export const MainlineThemeExplainSchema = z.object({
  kind: z.enum(['industry', 'concept']),
  name: z.string(),
  logicScore: z.number().min(0).max(100),
  logicGrade: z.enum(['S', 'A', 'B']).optional(),
  logicSummary: z.string().optional(),
  catalysts: z.array(z.string()).optional(),
});

export const MainlineExplainResponseSchema = z.object({
  date: z.string(),
  themes: z.array(MainlineThemeExplainSchema),
  model: z.string(),
});

export const QuantRankCandidateInputSchema = z.object({
  symbol: z.string().min(1),
  ticker: z.string().min(1),
  name: z.string().optional(),
  evidence: z.record(z.any()),
});

export const QuantRankExplainRequestSchema = z.object({
  asOfTs: z.string().min(1),
  asOfDate: z.string().min(1),
  horizon: z.literal('2d'),
  objective: z.literal('profit_probability'),
  candidates: z.array(QuantRankCandidateInputSchema).min(1).max(30),
  context: z.record(z.any()).optional(),
});

export const QuantRankWhyBulletSchema = z.object({
  text: z.string().min(1).max(200),
  evidenceRefs: z.array(z.string().min(1)).min(1).max(4),
});

export const QuantRankExplainItemSchema = z.object({
  symbol: z.string().min(1),
  llmScoreAdj: z.number().min(-5).max(5),
  whyBullets: z.array(QuantRankWhyBulletSchema).min(2).max(5),
  riskNotes: z.array(z.string()).max(4).optional(),
});

export const QuantRankExplainResponseSchema = z.object({
  asOfTs: z.string(),
  asOfDate: z.string(),
  items: z.array(QuantRankExplainItemSchema),
  model: z.string(),
});

export const StrategyCandidateSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  rank: z.number().int().min(1),
  why: z.string(),
});

export const StrategyOrderSchema = z.object({
  kind: z.string(),
  side: z.enum(['buy', 'sell']),
  trigger: z.string(),
  qty: z.string(),
  timeInForce: z.string().nullable(),
  notes: z.string().nullable(),
});

export const StrategyRecommendationSchema = z.object({
  symbol: z.string(),
  ticker: z.string(),
  name: z.string(),
  thesis: z.string(),
  levels: z.object({
    support: z.array(z.string()),
    resistance: z.array(z.string()),
    invalidations: z.array(z.string()),
  }),
  orders: z.array(StrategyOrderSchema),
  positionSizing: z.string(),
  riskNotes: z.array(z.string()),
});

export const StrategyDailyResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  candidates: z.array(StrategyCandidateSchema).max(5),
  leader: z.object({
    symbol: z.string(),
    reason: z.string(),
  }),
  recommendations: z.array(StrategyRecommendationSchema).max(3),
  riskNotes: z.array(z.string()),
  model: z.string(),
});

export const NewsSummaryRequestSchema = z.object({
  items: z.array(
    z.object({
      title: z.string(),
      sourceId: z.string().optional(),
      publishedAt: z.string().optional(),
    }),
  ),
  hours: z.number().optional(),
});

/** Dashboard "Copy all Markdown" payload for AI investment daily report. */
export const InvestmentDailyReportRequestSchema = z.object({
  markdown: z.string().min(1).max(100_000),
});

export const InvestmentDailyReportStockItemSchema = z.object({
  symbol: z.string().min(1).max(32),
  name: z.string().min(1).max(64),
  rationale: z.string().min(1).max(2000),
});

export const InvestmentDailyReportNewsItemSchema = z.object({
  title: z.string().min(1).max(500),
  summary: z.string().min(1).max(1200),
});

/** Structured sections for PDF rendering (Chinese prose + fixed counts). */
export const InvestmentDailyReportResponseSchema = z.object({
  /** Narrative after dashboard traffic-light tables (仓位与情绪解读). */
  trafficLightPositionAndSentiment: z.string().min(1).max(8000),
  /** 3–8 bullet lines (Markdown "- " ok) distilling 市场环境摘要. */
  marketEnvironmentHighlights: z.string().min(1).max(4000),
  /** Formal written analysis of Hot industries workflow (书面化). */
  hotIndustriesFormalAnalysis: z.string().min(1).max(10000),
  /** Mainline + fund flow synthesis referencing industry tables. */
  capitalFlowAndMainline: z.string().min(1).max(12000),
  topStocks: z.array(InvestmentDailyReportStockItemSchema).length(3),
  topNews: z.array(InvestmentDailyReportNewsItemSchema).length(5),
});
