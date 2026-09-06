import { z } from 'zod';

/** One row of GET /api/health/datasources `sources` (TIP-013 + OPT-126). */
export const DatasourceStatusSchema = z.object({
  source: z.string(),
  label: z.string(),
  group: z.string().nullable().optional(),
  lastSyncedAt: z.string().nullable(),
  ageMinutes: z.number().nullable(),
  thresholdMinutes: z.number(),
  stale: z.boolean(),
  /** OPT-126 eastmoney_probe extras (absent on other sources). */
  banLatched: z.boolean().optional(),
  cooldownRemainingS: z.number().optional(),
  failStreak: z.number().optional(),
  proxyDegraded: z.boolean().optional(),
  hosts: z.array(z.record(z.unknown())).nullable().optional(),
  failingHosts: z.array(z.string()).optional(),
});
export type DatasourceStatus = z.infer<typeof DatasourceStatusSchema>;

/** Per-key quota snapshot inside `tushare_quota` (suffix = last 4 chars only). */
export const TushareQuotaKeySchema = z.object({
  index: z.number(),
  suffix: z.string(),
  minuteUsed: z.number(),
  minuteLimit: z.number(),
  coolingSeconds: z.number(),
});
export type TushareQuotaKey = z.infer<typeof TushareQuotaKeySchema>;

/** GET /api/health/datasources `tushare_quota` (OPT-124). */
export const TushareQuotaSchema = z.object({
  configured: z.boolean(),
  keyCount: z.number().optional(),
  rotations: z.number().optional(),
  keys: z.array(TushareQuotaKeySchema).optional(),
  daily: z.record(z.object({ limit: z.number(), usedTotal: z.number() })).optional(),
});
export type TushareQuota = z.infer<typeof TushareQuotaSchema>;

/** GET /api/health/datasources full response. */
export const DatasourcesResponseSchema = z.object({
  ok: z.boolean(),
  generatedAt: z.string(),
  sources: z.array(DatasourceStatusSchema),
  tushare_quota: TushareQuotaSchema.optional(),
});
export type DatasourcesResponse = z.infer<typeof DatasourcesResponseSchema>;
