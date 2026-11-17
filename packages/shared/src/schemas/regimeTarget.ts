import { z } from 'zod';

export const RegimeTargetSchema = z.object({
  ok: z.boolean(),
  asOf: z.string().optional(),
  confirmedAt: z.string().optional(),
  close: z.number().nullable().optional(),
  ma200: z.number().nullable().optional(),
  distancePct: z.number().nullable().optional(),
  state: z.enum(['bull', 'bear']).optional(),
  stateLabel: z.string().optional(),
  target: z.enum(['starship_robust', 'starport']).optional(),
  targetLabel: z.string().optional(),
  sgapExposure: z.enum(['high', 'low']).optional(),
  nextReview: z.string().optional(),
  rule: z.string().optional(),
  note: z.string().optional(),
  error: z.string().optional(),
});
export type RegimeTarget = z.infer<typeof RegimeTargetSchema>;
