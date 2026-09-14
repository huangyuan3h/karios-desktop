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
  'aggressive_unaudited',
  'rejected',
]);
export type StrategyCatalogStatus = z.infer<typeof StrategyCatalogStatusSchema>;

export const StrategyCatalogEntrySchema = z.object({
  key: StrategyCatalogKeySchema,
  name: z.string(),
  structure: z.string(),
  status: StrategyCatalogStatusSchema,
  statusLabel: z.string(),
  /** Timeline API strategy when one exists; null for retired strategies. */
  timelineStrategy: z.enum(['harbor', 'homeport', 'starport', 'starship']).nullable(),
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
});
export type StrategyCatalogEntry = z.infer<typeof StrategyCatalogEntrySchema>;

export const StrategyCatalogSchema = z.object({
  ok: z.boolean(),
  strategies: z.array(StrategyCatalogEntrySchema),
});
export type StrategyCatalog = z.infer<typeof StrategyCatalogSchema>;
