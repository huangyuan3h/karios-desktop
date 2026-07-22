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
