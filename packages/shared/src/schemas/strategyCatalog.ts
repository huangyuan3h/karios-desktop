import { z } from 'zod';

export const StrategyCatalogWindowSchema = z.object({
  total: z.number(),
  cagr: z.number(),
  mdd: z.number(),
  sharpe: z.number(),
});
export type StrategyCatalogWindow = z.infer<typeof StrategyCatalogWindowSchema>;

export const StrategyCatalogKeySchema = z.enum([
  'harbor',
  'homeport',
  'starport',
  'starship',
  'twin_star',
]);
export type StrategyCatalogKey = z.infer<typeof StrategyCatalogKeySchema>;

export const StrategyCatalogStatusSchema = z.enum([
  'live',
  'product_candidate',
  'product_candidate_increment',
  'parallel_candidate',
  'aggressive_pending',
  'rejected',
]);
export type StrategyCatalogStatus = z.infer<typeof StrategyCatalogStatusSchema>;

/**
 * Regime fit map (OPT-202): when the strategy's edge is present vs absent,
 * with the supporting evidence rows. Descriptive only — never a trading gate.
 */
export const StrategyCatalogRegimeSchema = z.object({
  fit: z.array(z.string()),
  unfit: z.array(z.string()),
  evidence: z.array(z.object({ label: z.string(), value: z.string() })),
  note: z.string().optional(),
});
export type StrategyCatalogRegime = z.infer<typeof StrategyCatalogRegimeSchema>;

export const StrategyCatalogEntrySchema = z.object({
  key: StrategyCatalogKeySchema,
  name: z.string(),
  structure: z.string(),
  status: StrategyCatalogStatusSchema,
  statusLabel: z.string(),
  /** Timeline API strategy when one exists; null only for legacy rows. */
  timelineStrategy: z.enum(['harbor', 'homeport', 'starport', 'starship', 'twin_star']).nullable(),
  doc: z.string(),
  tag: z.string(),
  updated: z.string(),
  windows: z.object({
    OOS2: StrategyCatalogWindowSchema,
    train: StrategyCatalogWindowSchema,
    valid: StrategyCatalogWindowSchema,
    long: StrategyCatalogWindowSchema,
  }),
  pros: z.array(z.string()),
  cons: z.array(z.string()),
  /** Optional per-strategy regime map (starship v2 currently). */
  regime: StrategyCatalogRegimeSchema.optional(),
});
export type StrategyCatalogEntry = z.infer<typeof StrategyCatalogEntrySchema>;

export const StrategyCatalogSchema = z.object({
  ok: z.boolean(),
  strategies: z.array(StrategyCatalogEntrySchema),
});
export type StrategyCatalog = z.infer<typeof StrategyCatalogSchema>;
