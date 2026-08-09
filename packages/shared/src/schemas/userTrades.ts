import { z } from 'zod';

/** One leg of the user's real trade journal (POST /trades). */
export const UserTradeSideSchema = z.enum(['BUY', 'ADD', 'SELL']);
export type UserTradeSide = z.infer<typeof UserTradeSideSchema>;

/** Stored trade leg returned by GET /trades and POST /trades. */
export const UserTradeSchema = z.object({
  id: z.string(),
  symbol: z.string(),
  side: UserTradeSideSchema,
  tradeDate: z.string(),
  price: z.number(),
  positionPct: z.number(),
  costBasis: z.number().nullable().optional(),
  entryDate: z.string().nullable().optional(),
  pnlPct: z.number().nullable().optional(),
  holdingDays: z.number().nullable().optional(),
  source: z.string().nullable().optional(),
  market: z.string().optional(),
  note: z.string().nullable().optional(),
  createdAt: z.string().nullable().optional(),
});
export type UserTrade = z.infer<typeof UserTradeSchema>;

/** Request body for POST /trades (fields mirror Python TradeLegRequest). */
export const UserTradeRequestSchema = z.object({
  symbol: z.string(),
  side: UserTradeSideSchema,
  price: z.number(),
  positionPct: z.number(),
  tradeDate: z.string().optional(),
  costBasis: z.number().optional(),
  entryDate: z.string().optional(),
  source: z.string().optional(),
  market: z.string().optional(),
  note: z.string().optional(),
});
export type UserTradeRequest = z.infer<typeof UserTradeRequestSchema>;

export const UserTradeResponseSchema = z.object({
  ok: z.boolean(),
  trade: UserTradeSchema,
});
export type UserTradeResponse = z.infer<typeof UserTradeResponseSchema>;

export const UserTradesListResponseSchema = z.object({
  ok: z.boolean(),
  trades: z.array(UserTradeSchema),
  count: z.number(),
});
export type UserTradesListResponse = z.infer<typeof UserTradesListResponseSchema>;

/** Per-bucket stats (all trades, bySource, bySymbol). */
export const TradeBucketStatsSchema = z.object({
  count: z.number(),
  wins: z.number(),
  losses: z.number(),
  winRate: z.number().nullable(),
  avgWinPct: z.number().nullable(),
  avgLossPct: z.number().nullable(),
  expectancyPct: z.number().nullable(),
  netExpectancyPct: z.number().nullable(),
  profitFactor: z.number().nullable(),
  avgHoldingDays: z.number().nullable(),
});
export type TradeBucketStats = z.infer<typeof TradeBucketStatsSchema>;

export const UserTradesStatsSchema = TradeBucketStatsSchema.extend({
  total: z.number(),
  roundTripCostPct: z.number(),
  bySource: z.record(z.string(), TradeBucketStatsSchema),
  bySymbol: z.record(z.string(), TradeBucketStatsSchema),
});
export type UserTradesStats = z.infer<typeof UserTradesStatsSchema>;

export const UserTradesStatsResponseSchema = z.object({
  ok: z.boolean(),
  stats: UserTradesStatsSchema,
});
export type UserTradesStatsResponse = z.infer<typeof UserTradesStatsResponseSchema>;
