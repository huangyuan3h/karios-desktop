import { z } from 'zod';

/**
 * Frozen opportunity twin-star v3.1 clip4.
 *
 * Do not change without a 3-window walk-forward pass. Live sizing, paper book,
 * and GET /api/backtest/twin-star/action `clip4` must stay lockstep with
 * `state_bucket_track.MAX_POS` / `POSITION_PCT` / `BODY`.
 */
export const TWIN_STAR_CLIP4 = {
  maxPos: 4,
  slotOfSleeve: 0.25,
  satSleevePct: 50,
  coreIdlePct: 100,
  coreSatActivePct: 50,
  /** Each sat name as % of NAV when the sleeve is 50/50 (4 × 12.5%). */
  satSlotNavPct: 12.5,
  body: 3,
  protectStopPct: 0.05,
  rWideThreshold: 0.5,
  bucketQ: 3,
} as const;

export type TwinStarClip4 = typeof TWIN_STAR_CLIP4;

/** Python `clip4` block on the action payload — literals so 10%×10 cannot sneak in. */
export const TwinStarClip4Schema = z.object({
  maxPos: z.literal(TWIN_STAR_CLIP4.maxPos),
  slotOfSleeve: z.literal(TWIN_STAR_CLIP4.slotOfSleeve),
  satSlotNavPct: z.literal(TWIN_STAR_CLIP4.satSlotNavPct),
  body: z.literal(TWIN_STAR_CLIP4.body),
  protectStopPct: z.literal(TWIN_STAR_CLIP4.protectStopPct),
});

export const TwinStarCoreSchema = z.object({
  pick: z.string().nullable().optional(),
  symbol: z.string().nullable().optional(),
  label: z.string().nullable().optional(),
  action: z.string().nullable().optional(),
  message: z.string().nullable().optional(),
  active: z.boolean().nullable().optional(),
});
export type TwinStarCore = z.infer<typeof TwinStarCoreSchema>;

export const TwinStarSatCandidateSchema = z.object({
  ts: z.string(),
  name: z.string().nullable().optional(),
  amp: z.number().nullable(),
  gapPct: z.number().nullable(),
  close: z.number().nullable(),
  limitLocked: z.boolean().nullable().optional(),
});
export type TwinStarSatCandidate = z.infer<typeof TwinStarSatCandidateSchema>;

export const TwinStarSatHoldingSchema = z.object({
  ts: z.string(),
  symbol: z.string().nullable().optional(),
  name: z.string().nullable().optional(),
  entryDate: z.string().nullable().optional(),
  entryPrice: z.number().nullable().optional(),
  costPrice: z.number().nullable().optional(),
  close: z.number().nullable().optional(),
  lastClose: z.number().nullable().optional(),
  heldDays: z.number().nullable().optional(),
  daysLeft: z.number().nullable().optional(),
  exitDue: z.string().nullable().optional(),
  pnlPct: z.number().nullable().optional(),
  due: z.boolean().nullable().optional(),
  positionPct: z.number().nullable().optional(),
  missingEntry: z.boolean().nullable().optional(),
});
export type TwinStarSatHolding = z.infer<typeof TwinStarSatHoldingSchema>;

export const TwinStarSatBookSchema = z.object({
  asOf: z.string().nullable().optional(),
  holdings: z.array(TwinStarSatHoldingSchema).optional(),
  exitsDue: z.array(TwinStarSatHoldingSchema).optional(),
  body: z.number().nullable().optional(),
  liveHoldings: z.array(TwinStarSatHoldingSchema).optional(),
  liveExitsDue: z.array(TwinStarSatHoldingSchema).optional(),
  liveHeld: z.number().int().nonnegative().optional(),
  liveFreeSlots: z.number().int().nonnegative().optional(),
  engineHeld: z.number().int().nonnegative().optional(),
  error: z.string().optional(),
});
export type TwinStarSatBook = z.infer<typeof TwinStarSatBookSchema>;

export const TwinStarSatSchema = z.object({
  asOf: z.string().nullable().optional(),
  gateOpen: z.boolean().nullable().optional(),
  breadth: z.number().nullable().optional(),
  gapCount: z.number().nullable().optional(),
  candidates: z.array(TwinStarSatCandidateSchema).nullable().optional(),
  blocked: z.array(TwinStarSatCandidateSchema).nullable().optional(),
  alternates: z.array(TwinStarSatCandidateSchema).nullable().optional(),
  note: z.string().nullable().optional(),
  approx: z.boolean().nullable().optional(),
  snapshotAt: z.string().nullable().optional(),
  frozen: z.boolean().nullable().optional(),
  heldOvernight: z.boolean().nullable().optional(),
  snapshotMissing: z.boolean().nullable().optional(),
  snapshotStale: z.boolean().nullable().optional(),
  snapshotAgeSec: z.number().nullable().optional(),
  snapshotReason: z.string().nullable().optional(),
  coreTargetPct: z.union([
    z.literal(TWIN_STAR_CLIP4.coreIdlePct),
    z.literal(TWIN_STAR_CLIP4.coreSatActivePct),
  ]),
  satTargetPct: z.union([
    z.literal(0),
    z.literal(TWIN_STAR_CLIP4.satSleevePct),
  ]),
  book: TwinStarSatBookSchema.nullable().optional(),
});
export type TwinStarSat = z.infer<typeof TwinStarSatSchema>;

/** GET /api/backtest/twin-star/action and POST /api/backtest/twin-star/refresh. */
export const TwinStarActionResponseSchema = z.object({
  ok: z.boolean(),
  refreshed: z.boolean().optional(),
  core: TwinStarCoreSchema,
  sat: TwinStarSatSchema,
  clip4: TwinStarClip4Schema,
});
export type TwinStarAction = z.infer<typeof TwinStarActionResponseSchema>;

export function parseTwinStarAction(raw: unknown): TwinStarAction {
  return TwinStarActionResponseSchema.parse(raw);
}
