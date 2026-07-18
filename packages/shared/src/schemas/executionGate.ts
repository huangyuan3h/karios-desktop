import { z } from 'zod';

export const ExecutionGateModeSchema = z.enum(['ATTACK', 'HOLD_ONLY', 'DEFEND']);
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
  riskMode: z.string().nullable().optional(),
  reasons: z.array(z.string()).default([]),
  positionRangeHint: z.string().optional(),
  satelliteNote: z.string().optional(),
});
export type ExecutionGate = z.infer<typeof ExecutionGateSchema>;

export const ExecutionActionSchema = z.enum(['EXIT', 'TRIM', 'HOLD', 'ADD', 'BUY', 'WATCH']);
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
  trigger: z.number().nullable().optional(),
  distPct: z.number().nullable().optional(),
  why: z.string().optional(),
  mainlineOk: z.boolean().optional(),
  mainlineTag: MainlineTagSchema.nullable().optional(),
});
export type ExecutionActionCard = z.infer<typeof ExecutionActionCardSchema>;
