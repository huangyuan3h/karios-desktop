import { z } from 'zod';

export const SgapDecayWindowSchema = z.object({
  start: z.string(),
  end: z.string(),
});
export type SgapDecayWindow = z.infer<typeof SgapDecayWindowSchema>;

export const SgapRollingPointSchema = z.object({
  i: z.number(),
  date: z.string(),
  r40: z
    .object({
      mean: z.number(),
      sum: z.number(),
      win_rate: z.number(),
      t: z.number(),
      percentile: z.number(),
    })
    .nullable(),
  r60: z
    .object({
      mean: z.number(),
      sum: z.number(),
      win_rate: z.number(),
      t: z.number(),
      percentile: z.number(),
    })
    .nullable(),
  crowd_amt_w: z.number().nullable(),
  crowd_circ: z.number().nullable(),
  large_edge: z.number().nullable(),
  revival_pos: z.number().nullable(),
});
export type SgapRollingPoint = z.infer<typeof SgapRollingPointSchema>;

export const SgapMonthlySchema = z.object({
  month: z.string(),
  count: z.number(),
  sum: z.number(),
});
export type SgapMonthly = z.infer<typeof SgapMonthlySchema>;

export const SgapEquitySchema = z.object({
  i: z.number(),
  date: z.string(),
  cum: z.number(),
  drawdown: z.number(),
});
export type SgapEquity = z.infer<typeof SgapEquitySchema>;

export const SgapDecaySummarySchema = z.object({
  n_trades: z.number(),
  date_start: z.string().nullable(),
  date_end: z.string().nullable(),
  cum_total: z.number(),
  max_drawdown: z.number(),
  latest_r40_mean: z.number().nullable(),
  latest_r40_win_rate: z.number().nullable(),
  latest_r40_percentile: z.number().nullable(),
  latest_r40_t: z.number().nullable(),
  revival_pos: z.number(),
});
export type SgapDecaySummary = z.infer<typeof SgapDecaySummarySchema>;

export const SgapDecaySchema = z.object({
  ok: z.boolean(),
  decay: z.object({
    meta: z.object({
      source: z.string(),
      n_trades: z.number(),
      windows: z.object({
        OOS2: SgapDecayWindowSchema,
        train: SgapDecayWindowSchema,
        valid: SgapDecayWindowSchema,
        holdout: SgapDecayWindowSchema,
      }),
      generated_at: z.string().optional(),
      source_path: z.string().optional(),
    }),
    summary: SgapDecaySummarySchema,
    rolling: z.array(SgapRollingPointSchema),
    monthly: z.array(SgapMonthlySchema),
    equity: z.array(SgapEquitySchema),
  }),
});
export type SgapDecay = z.infer<typeof SgapDecaySchema>;
