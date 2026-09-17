import { z } from 'zod';

/** Paper-gate status of the H2 shadow ledger (OPT-216, display only). */
export const HarborH2ShadowStatusSchema = z.enum(['tracking', 'watch', 'rollback']);
export type HarborH2ShadowStatus = z.infer<typeof HarborH2ShadowStatusSchema>;

/** H2 sleeve action on the day (close-to-close parking overlay). */
export const HarborH2ShadowActionSchema = z.enum(['hold', 'enter', 'rotate', 'trail_exit']);
export type HarborH2ShadowAction = z.infer<typeof HarborH2ShadowActionSchema>;

/** One paper day: NAVs re-based to 1.0 at inception (paper window only). */
export const HarborH2ShadowRowSchema = z.object({
  date: z.string(),
  navLive: z.number(),
  navH2: z.number(),
  peakLive: z.number(),
  peakH2: z.number(),
  dayLivePct: z.number(),
  dayH2Pct: z.number(),
  spreadPt: z.number(),
  ddLivePct: z.number(),
  ddH2Pct: z.number(),
  mddGapPt: z.number(),
  pickLive: z.string(),
  pickH2: z.string(),
  actionH2: HarborH2ShadowActionSchema,
  idlePct: z.number(),
  status: HarborH2ShadowStatusSchema,
});
export type HarborH2ShadowRow = z.infer<typeof HarborH2ShadowRowSchema>;

/** GET /api/backtest/harbor-h2-shadow/latest (404 until the first 18:35 run). */
export const HarborH2ShadowReportSchema = z.object({
  ok: z.literal(true),
  inception: z.string(),
  generatedAt: z.string(),
  rows: z.array(HarborH2ShadowRowSchema),
  latest: HarborH2ShadowRowSchema,
  appended: z.number(),
  thresholds: z.object({
    spreadWatchPt: z.number(),
    spreadRollbackPt: z.number(),
    mddGapWatchPt: z.number(),
    mddGapRollbackPt: z.number(),
  }),
  note: z.string(),
});
export type HarborH2ShadowReport = z.infer<typeof HarborH2ShadowReportSchema>;
