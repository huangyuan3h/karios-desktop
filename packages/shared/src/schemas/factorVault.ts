import { z } from 'zod';

export const FactorVaultHistoryPointSchema = z.object({
  window: z.string(),
  date: z.string().nullable(),
  percentile: z.number().nullable(),
  net: z.number(),
  trades: z.number(),
  revived: z.boolean(),
});
export type FactorVaultHistoryPoint = z.infer<typeof FactorVaultHistoryPointSchema>;

export const FactorVaultRowSchema = z.object({
  id: z.string(),
  name: z.string(),
  family: z.string(),
  source: z.string(),
  doc: z.string(),
  unit: z.string(),
  status: z.enum(['cold', 'watch', 'revived']),
  badge: z.string(),
  percentile: z.number().nullable(),
  net: z.number(),
  win_rate: z.number().nullable(),
  trades: z.number(),
  trades_raw: z.number(),
  rolling_window: z.string().nullable(),
  spark: z.array(z.number().nullable()),
  history: z.array(FactorVaultHistoryPointSchema),
  position: z.number(),
  confirmations: z.number(),
  days_since_change: z.number(),
  updated: z.string().nullable(),
});
export type FactorVaultRow = z.infer<typeof FactorVaultRowSchema>;

export const FactorVaultSchema = z.object({
  ok: z.boolean(),
  vault: z.object({
    meta: z.object({
      source: z.string(),
      n_factors: z.number(),
      generated_at: z.string().nullable().optional(),
      revival: z.object({
        rule: z.string(),
        N: z.number(),
        X: z.number(),
        off_below: z.number(),
        steps: z.array(z.number()),
        min_trades: z.number(),
        step_gap: z.number(),
        note: z.string(),
      }),
      cost: z.string(),
    }),
    summary: z.object({
      n_factors: z.number(),
      n_cold: z.number(),
      n_watch: z.number(),
      n_revived: z.number(),
    }),
    factors: z.array(FactorVaultRowSchema),
  }),
  history: z.array(z.object({ date: z.string(), states: z.record(z.string()) })).optional(),
});
export type FactorVault = z.infer<typeof FactorVaultSchema>;
