import { z } from 'zod';

export const PortfolioSnapshotSchema = z.object({
  asOf: z.string(),
  baseCurrency: z.string(),
  totalValue: z.number(),
  positions: z.array(
    z.object({
      symbol: z.string(),
      quantity: z.number(),
      price: z.number().optional(),
      currency: z.string().optional(),
    }),
  ),
});

export type PortfolioSnapshot = z.infer<typeof PortfolioSnapshotSchema>;


