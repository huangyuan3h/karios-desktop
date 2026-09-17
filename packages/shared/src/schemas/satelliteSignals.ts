import { z } from 'zod';

export const SatelliteSignalNameSchema = z.object({
  ts: z.string(),
  gapPct: z.number().nullable(),
  amp1430Pct: z.number().nullable(),
  px1430: z.number().nullable(),
  ampRank: z.number(),
  inBucket: z.boolean(),
  skipReason: z.string().nullable(),
  skipKind: z.string().nullable(),
  fillable: z.boolean(),
  unfillableReason: z.string().nullable(),
  wouldFill: z.boolean(),
});
export type SatelliteSignalName = z.infer<typeof SatelliteSignalNameSchema>;

export const SatelliteSignalsSchema = z.object({
  ok: z.boolean(),
  date: z.string(),
  decisionAvailable: z.boolean(),
  reason: z.string().optional(),
  gateOpen: z.boolean().optional(),
  breadth1430: z.number().nullable().optional(),
  gapCount: z.number().optional(),
  bucketSize: z.number().optional(),
  poolSize: z.number().optional(),
  ranked: z.array(SatelliteSignalNameSchema).optional(),
  basis: z.string().optional(),
});
export type SatelliteSignals = z.infer<typeof SatelliteSignalsSchema>;

export const SatelliteSignalsResponseSchema = z.object({
  ok: z.boolean(),
  error: z.string().optional(),
  signals: SatelliteSignalsSchema.optional(),
});
export type SatelliteSignalsResponse = z.infer<typeof SatelliteSignalsResponseSchema>;

/** OPT-222: one ranked row of the persisted 14:30 live snapshot. */
export const SatelliteLivePanelNameSchema = z.object({
  ts: z.string(),
  ampRank: z.number(),
  inBucket: z.boolean(),
  gapPct: z.number().nullable(),
  amp1430Pct: z.number().nullable(),
  px1430: z.number().nullable(),
  skipReason: z.string().nullable(),
  fillable: z.boolean(),
  wouldFill: z.boolean(),
});
export type SatelliteLivePanelName = z.infer<typeof SatelliteLivePanelNameSchema>;

/** OPT-222: the 14:30 job's persisted panel (card shows it for today). */
export const SatelliteLiveLegSchema = z.object({
  ts: z.string(),
  entryDate: z.string().nullable().optional(),
  exitDue: z.string().nullable().optional(),
  daysLeft: z.number().nullable().optional(),
});
export type SatelliteLiveLeg = z.infer<typeof SatelliteLiveLegSchema>;

export const SatelliteLivePanelSchema = z.object({
  tradeDate: z.string(),
  generatedAt: z.string(),
  decisionAvailable: z.boolean(),
  reason: z.string().nullable().optional(),
  gateOpen: z.boolean().nullable().optional(),
  breadth1430: z.number().nullable().optional(),
  gapCount: z.number().nullable().optional(),
  bucketSize: z.number().nullable().optional(),
  poolSize: z.number().nullable().optional(),
  coverage: z.number().optional(),
  quoted: z.number().optional(),
  universe: z.number().optional(),
  wouldFill: z.array(z.string()).optional(),
  ranked: z.array(SatelliteLivePanelNameSchema).optional(),
  /** OPT-223: legs due today / still held, from the pre-injection replay. */
  exits: z.array(SatelliteLiveLegSchema).optional(),
  heldLegs: z.array(SatelliteLiveLegSchema).optional(),
  basis: z.string().optional(),
});
export type SatelliteLivePanel = z.infer<typeof SatelliteLivePanelSchema>;

export const SatelliteLivePanelResponseSchema = z.object({
  ok: z.boolean(),
  panel: SatelliteLivePanelSchema.nullable().optional(),
  /** True when the snapshot belongs to an earlier session (card shows fallback). */
  stale: z.boolean().optional(),
});
export type SatelliteLivePanelResponse = z.infer<typeof SatelliteLivePanelResponseSchema>;
