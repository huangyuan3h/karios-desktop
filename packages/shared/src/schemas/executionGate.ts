import { z } from 'zod';

export const ExecutionGateModeSchema = z.enum([
  'ATTACK',
  'WEAK_ATTACK',
  'HOLD_ONLY',
  'DEFEND',
]);
export type ExecutionGateMode = z.infer<typeof ExecutionGateModeSchema>;

export const MarketRegimeLabelSchema = z.enum(['Strong', 'Diverging', 'Weak']);
export type MarketRegimeLabel = z.infer<typeof MarketRegimeLabelSchema>;

export const ExecutionGateSchema = z.object({
  mode: ExecutionGateModeSchema,
  allowNewEntries: z.boolean(),
  marketRegime: MarketRegimeLabelSchema,
  indexLight: z.string(),
  srvLevel: z.string().nullable().optional(),
  srvOverlapCount: z.number().nullable().optional(),
  downCount: z.number().nullable().optional(),
  upCount: z.number().nullable().optional(),
  riskMode: z.string().nullable().optional(),
  reasons: z.array(z.string()).default([]),
  positionRangeHint: z.string().optional(),
  satelliteNote: z.string().optional(),
  /** V6.3: sector that triggered Intraday Overflow Override. */
  overflowSector: z.string().nullable().optional(),
  /** V6.3: max 1D sector inflow in 亿 (CNY / 1e8). */
  overflowInflowYi: z.number().nullable().optional(),
});
export type ExecutionGate = z.infer<typeof ExecutionGateSchema>;

export const ExecutionActionSchema = z.enum([
  'EXIT',
  'TRIM',
  'HOLD',
  'ADD',
  'BUY',
  'WATCH',
  'WATCH_SILENT',
  'PURGE',
]);
export type ExecutionAction = z.infer<typeof ExecutionActionSchema>;

export const MainlineTagSchema = z.enum(['MOMENTUM', '5D_TOP3']);
export type MainlineTag = z.infer<typeof MainlineTagSchema>;

export const ExecutionActionCardSchema = z.object({
  symbol: z.string(),
  action: ExecutionActionSchema,
  trailArmed: z.boolean(),
  peak: z.number().nullable().optional(),
  hardStop: z.number().nullable().optional(),
  trailStop: z.number().nullable().optional(),
  /**
   * Compat alias: held → exitStop; flat → entryTrigger.
   * Prefer entryTrigger / exitStop for new consumers.
   */
  trigger: z.number().nullable().optional(),
  /** Buy/sniper level for flat (Pos%=0) names — typically TrendOK buyZoneHigh. */
  entryTrigger: z.number().nullable().optional(),
  /** Defensive exit (max hardStop, trailStop) for held names. */
  exitStop: z.number().nullable().optional(),
  /**
   * Flat: (entryTrigger - current) / current * 100 (distance to sniper).
   * Held: (current - exitStop) / current * 100 (cushion to stop).
   */
  distPct: z.number().nullable().optional(),
  why: z.string().optional(),
  mainlineOk: z.boolean().optional(),
  mainlineTag: MainlineTagSchema.nullable().optional(),
  /** Suggested add to sleeve weight for BUY/ADD (pct points), after caps. */
  suggestAddPct: z.number().nullable().optional(),
  /** Binding constraint: clip | single | sector | sleeve */
  suggestSizeNote: z.string().nullable().optional(),
});
export type ExecutionActionCard = z.infer<typeof ExecutionActionCardSchema>;
