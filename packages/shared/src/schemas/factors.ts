import { z } from 'zod';

export const FactorNameSchema = z.enum(['strong_scoop_exhaustion']);
export type FactorName = z.infer<typeof FactorNameSchema>;

export const FactorDirectionSchema = z.enum(['short', 'long']);
export type FactorDirection = z.infer<typeof FactorDirectionSchema>;

export const FactorStatusSchema = z.enum(['pending', 'active', 'hit_target', 'hit_stop', 'expired']);
export type FactorStatus = z.infer<typeof FactorStatusSchema>;

export const FactorSignalSchema = z.object({
  symbol: z.string(),
  name: z.string().nullable().optional(),
  tradeDate: z.string(),
  factorName: FactorNameSchema,
  direction: FactorDirectionSchema,
  entryPrice: z.number(),
  targetPrice: z.number(),
  stopPrice: z.number(),
  probability: z.number(),
  holdDays: z.number(),
  status: FactorStatusSchema,
  ret60: z.number().nullable().optional(),
  volRatio: z.number().nullable().optional(),
  industry: z.string().nullable().optional(),
  board: z.string().nullable().optional(),
});
export type FactorSignal = z.infer<typeof FactorSignalSchema>;

export const FactorSignalsResponseSchema = z.object({
  asOfDate: z.string(),
  signals: z.array(FactorSignalSchema),
});
export type FactorSignalsResponse = z.infer<typeof FactorSignalsResponseSchema>;
