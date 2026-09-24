import { z } from 'zod';

export const B3WeightSchema = z.object({
  symbol: z.string(),
  name: z.string(),
  targetPct: z.number(),
  driftPct: z.number(),
  deltaPct: z.number(),
  /** Tradable (raw) last close for lot sizing; null when unavailable. */
  px: z.number().nullable().optional(),
});
export type B3Weight = z.infer<typeof B3WeightSchema>;

export const B3TradeSchema = z.object({
  symbol: z.string(),
  name: z.string(),
  side: z.enum(['BUY', 'SELL']),
  deltaPct: z.number(),
});
export type B3Trade = z.infer<typeof B3TradeSchema>;

export const B3StateSchema = z.object({
  ok: z.boolean(),
  error: z.string().optional(),
  asOf: z.string().optional(),
  rebalanceDate: z.string().optional(),
  month: z.string().optional(),
  universe: z.array(B3WeightSchema).optional(),
  trades: z.array(B3TradeSchema).optional(),
  note: z.string().optional(),
});
export type B3State = z.infer<typeof B3StateSchema>;
