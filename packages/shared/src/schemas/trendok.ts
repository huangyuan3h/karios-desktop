import { z } from 'zod';

export const WatchlistRiskAlertSeveritySchema = z.enum(['block', 'warn']);
export type WatchlistRiskAlertSeverity = z.infer<typeof WatchlistRiskAlertSeveritySchema>;

export const WatchlistRiskAlertSchema = z.object({
  code: z.string(),
  severity: WatchlistRiskAlertSeveritySchema,
  message: z.string(),
});
export type WatchlistRiskAlert = z.infer<typeof WatchlistRiskAlertSchema>;

export const InstFlowSeatSchema = z.object({
  name: z.string(),
  buyAmt: z.number().optional(),
  isLhasa: z.boolean().optional(),
  isInst: z.boolean().optional(),
});
export type InstFlowSeat = z.infer<typeof InstFlowSeatSchema>;

export const InstFlowSchema = z.object({
  tradeDate: z.string().optional(),
  onBoard: z.boolean(),
  synced: z.boolean().optional(),
  instNetBuyYi: z.number().optional(),
  label: z.string().optional(),
  lhasaDominant: z.boolean().optional(),
  display: z.string(),
  topBuySeats: z.array(InstFlowSeatSchema).optional(),
});
export type InstFlow = z.infer<typeof InstFlowSchema>;

export const MacroLockSchema = z.object({
  active: z.boolean(),
  reason: z.string().optional(),
  riskMode: z.string().nullable().optional(),
  downCount: z.number().nullable().optional(),
});
export type MacroLock = z.infer<typeof MacroLockSchema>;

export const TrendOkResultSchema = z.object({
  symbol: z.string(),
  name: z.string().nullable().optional(),
  asOfDate: z.string().nullable().optional(),
  trendOk: z.boolean().nullable().optional(),
  score: z.number().nullable().optional(),
  scoreParts: z.record(z.number()).optional(),
  stopLossPrice: z.number().nullable().optional(),
  stopLossParts: z.record(z.unknown()).optional(),
  buyMode: z.string().nullable().optional(),
  buyAction: z.string().nullable().optional(),
  buyZoneLow: z.number().nullable().optional(),
  buyZoneHigh: z.number().nullable().optional(),
  buyRefPrice: z.number().nullable().optional(),
  buyWhy: z.string().nullable().optional(),
  buyChecks: z.record(z.unknown()).optional(),
  macroLock: MacroLockSchema.optional(),
  marketRegime: z.string().nullable().optional(),
  rs: z.number().nullable().optional(),
  intradayChgPct: z.number().nullable().optional(),
  gapUp: z.boolean().nullable().optional(),
  riskAlerts: z.array(WatchlistRiskAlertSchema).optional(),
  riskMetricsLive: z.boolean().nullable().optional(),
  instFlow: InstFlowSchema.nullable().optional(),
  checks: z.record(z.unknown()).nullable().optional(),
  values: z.record(z.unknown()).nullable().optional(),
  missingData: z.array(z.string()).optional(),
});
export type TrendOkResult = z.infer<typeof TrendOkResultSchema>;
