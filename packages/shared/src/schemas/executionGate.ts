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

/**
 * Per-market execution gate (A-share vs HK independent position budget).
 * Backend emits a flat CN gate (top-level fields) plus nested cnGate/hkGate.
 */
export const MarketGateSubsetSchema = z.object({
  mode: ExecutionGateModeSchema,
  allowNewEntries: z.boolean(),
  marketRegime: MarketRegimeLabelSchema,
  indexLight: z.string(),
  riskMode: z.string().nullable().optional(),
  reasons: z.array(z.string()).default([]),
  positionRangeHint: z.string().optional(),
  satelliteNote: z.string().optional(),
});
export type MarketGateSubset = z.infer<typeof MarketGateSubsetSchema>;

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
  /** Alias of the flat CN gate, for symmetric per-market access. */
  cnGate: MarketGateSubsetSchema.nullable().optional(),
  /** Independent HK position budget driven by HK index lights. */
  hkGate: MarketGateSubsetSchema.nullable().optional(),
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

/**
 * Provenance of a BUY/ADD signal (TIP-011).
 * 'TV' = TV screener funnel, 'ALPHA' = Alpha Radar catalyst, 'MANUAL' = user/AI.
 */
export const ExecutionSourceSchema = z.enum(['TV', 'ALPHA', 'MANUAL']);
export type ExecutionSource = z.infer<typeof ExecutionSourceSchema>;

export const ExecutionActionCardSchema = z.object({
  symbol: z.string(),
  action: ExecutionActionSchema,
  /** TIP-011: provenance of the signal; null = pre-TIP-011 / unknown. */
  source: ExecutionSourceSchema.nullable().optional(),
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
