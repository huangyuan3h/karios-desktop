import { z } from 'zod';

export const WatchlistSourceSchema = z.enum(['manual', 'screener', 'screener_fallback', 'alpha_radar']);
export type WatchlistSource = z.infer<typeof WatchlistSourceSchema>;

export const WatchlistNameStatusSchema = z.enum(['resolved', 'not_found']);
export type WatchlistNameStatus = z.infer<typeof WatchlistNameStatusSchema>;

/** API contract for POST/GET /watchlist/registry (matches Python WatchlistRegistryItem). */
export const WatchlistRegistryItemSchema = z.object({
  symbol: z.string(),
  name: z.string().nullable().optional(),
  addedAt: z.string().nullable().optional(),
  source: WatchlistSourceSchema.nullable().optional(),
  color: z.string().nullable().optional(),
  positionPct: z.number().nullable().optional(),
  costPrice: z.number().nullable().optional(),
  maxPrice: z.number().nullable().optional(),
  /** Shanghai calendar YYYY-MM-DD when the position was opened (T+1 lock source). */
  entryDate: z.string().nullable().optional(),
});
export type WatchlistRegistryItem = z.infer<typeof WatchlistRegistryItemSchema>;

/** Client-side watchlist row; extends registry with optional name resolution status. */
export const WatchlistItemSchema = WatchlistRegistryItemSchema.extend({
  addedAt: z.string(),
  nameStatus: WatchlistNameStatusSchema.optional(),
});
export type WatchlistItem = z.infer<typeof WatchlistItemSchema>;

export const WatchlistRegistryResponseSchema = z.object({
  ok: z.boolean(),
  items: z.array(WatchlistRegistryItemSchema),
  count: z.number(),
});
export type WatchlistRegistryResponse = z.infer<typeof WatchlistRegistryResponseSchema>;

/** TIP-002 funnel counts persisted in automation run meta (ack `meta.funnel`). */
export const AutomationFunnelSchema = z.object({
  tvHit: z.number(),
  passPullback: z.number(),
  passTrendOk: z.number(),
  addedNew: z.number(),
  droppedByPullback: z.number().optional(),
  fallbackUsed: z.boolean().optional(),
  fallbackHit: z.number().optional(),
  fallbackTrendOk: z.number().optional(),
  fallbackAdded: z.number().optional(),
});
export type AutomationFunnel = z.infer<typeof AutomationFunnelSchema>;

/** One row of the N-day funnel history (GET /watchlist/automation/runs). */
export const AutomationRunHistoryRowSchema = z.object({
  runId: z.string(),
  tradeDate: z.string(),
  trigger: z.string(),
  skipped: z.boolean(),
  funnel: AutomationFunnelSchema.nullable().optional(),
  screenerAdded: z.number().nullable().optional(),
  createdAt: z.string().nullable().optional(),
});
export type AutomationRunHistoryRow = z.infer<typeof AutomationRunHistoryRowSchema>;

export const FunnelHistoryResponseSchema = z.object({
  ok: z.boolean(),
  runs: z.array(AutomationRunHistoryRowSchema),
  asOfDate: z.string(),
});
export type FunnelHistoryResponse = z.infer<typeof FunnelHistoryResponseSchema>;
