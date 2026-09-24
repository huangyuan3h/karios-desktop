import { z } from 'zod';

/**
 * OPT-228: satellite forward paper book (OPT-186 slice 2a).
 *
 * Read-only projection of the frozen habit replay since the strategy freeze;
 * powers the watchlist card's paper book + the ``paper 20 笔`` go-live counter.
 */

export const SatellitePaperClosedSchema = z.object({
  ts: z.string(),
  entryDate: z.string().nullable(),
  exitDate: z.string().nullable(),
  entryPxSrc: z.string().nullable().optional(),
  exitPxSrc: z.string().nullable().optional(),
  grossPnlPct: z.number().nullable(),
  netPnlPct: z.number().nullable(),
  heldDays: z.number().nullable(),
  closeReason: z.string(),
  ampPct: z.number().nullable().optional(),
  ampRank: z.number().nullable().optional(),
});
export type SatellitePaperClosed = z.infer<typeof SatellitePaperClosedSchema>;

export const SatellitePaperOpenLegSchema = z.object({
  ts: z.string(),
  entryDate: z.string().nullable(),
  entryPrice: z.number().nullable(),
  close: z.number().nullable(),
  heldDays: z.number().nullable(),
  daysLeft: z.number().nullable(),
  exitDue: z.string().nullable(),
  pnlPct: z.number().nullable(),
  /** Journal-sourced book only: the recorded position size (% of assets). */
  positionPct: z.number().nullable().optional(),
});
export type SatellitePaperOpenLeg = z.infer<typeof SatellitePaperOpenLegSchema>;

export const SatellitePaperStatsSchema = z.object({
  closedCount: z.number(),
  openCount: z.number(),
  winCount: z.number(),
  winRate: z.number().nullable(),
  avgNetPnlPct: z.number().nullable(),
  bestNetPnlPct: z.number().nullable(),
  worstNetPnlPct: z.number().nullable(),
  paperPct: z.number().nullable(),
  paperMaxDdPct: z.number().nullable(),
  avgHeldDays: z.number().nullable(),
  closeReasons: z.record(z.string(), z.number()),
});
export type SatellitePaperStats = z.infer<typeof SatellitePaperStatsSchema>;

export const SatellitePaperPrereqSchema = z.object({
  closedCount: z.number(),
  target: z.number(),
  met: z.boolean(),
});
export type SatellitePaperPrereq = z.infer<typeof SatellitePaperPrereqSchema>;

export const SatellitePaperSchema = z.object({
  ok: z.boolean(),
  start: z.string(),
  end: z.string(),
  inception: z.string(),
  strategyMode: z.string().optional(),
  decisionAvailable: z.boolean().optional(),
  reason: z.string().optional(),
  prereq: SatellitePaperPrereqSchema,
  stats: SatellitePaperStatsSchema,
  closed: z.array(SatellitePaperClosedSchema),
  openLegs: z.array(SatellitePaperOpenLegSchema),
});
export type SatellitePaper = z.infer<typeof SatellitePaperSchema>;

export const SatellitePaperResponseSchema = z.object({
  ok: z.boolean(),
  error: z.string().optional(),
  paper: SatellitePaperSchema.optional(),
});
export type SatellitePaperResponse = z.infer<typeof SatellitePaperResponseSchema>;
