import { z } from 'zod';

export const FleetWindowMetricsSchema = z.object({
  total: z.number().nullable().optional(),
  cagr: z.number().nullable().optional(),
  mdd: z.number().nullable().optional(),
  sharpe: z.number().nullable().optional(),
  calmar: z.number().nullable().optional(),
  worst_month: z.string().nullable().optional(),
  worst_month_ret: z.number().nullable().optional(),
  recover_days: z.number().nullable().optional(),
});
export type FleetWindowMetrics = z.infer<typeof FleetWindowMetricsSchema>;

export const FleetSchema = z.object({
  ok: z.boolean(),
  fleet: z.object({
    meta: z
      .object({
        source: z.string().optional(),
        generated_at: z.string().nullable().optional(),
        note: z.string().optional(),
      })
      .passthrough(),
    windows: z
      .object({
        fleet: z.record(FleetWindowMetricsSchema).optional(),
        base: z.record(FleetWindowMetricsSchema).optional(),
      })
      .passthrough(),
    defense: z
      .object({
        days: z.number().optional(),
        L1_days: z.number().optional(),
        L2_days: z.number().optional(),
        L3_days: z.number().optional(),
        L4_days: z.number().optional(),
        recommendation: z.string().optional(),
      })
      .passthrough()
      .optional(),
    equity: z
      .object({
        dates: z.array(z.string()).optional(),
        fleet: z.array(z.number().nullable()).optional(),
        base: z.array(z.number().nullable()).optional(),
        starship_b: z.array(z.number().nullable()).optional(),
        hs300: z.array(z.number().nullable()).optional(),
      })
      .passthrough()
      .optional(),
  }),
});
export type Fleet = z.infer<typeof FleetSchema>;
