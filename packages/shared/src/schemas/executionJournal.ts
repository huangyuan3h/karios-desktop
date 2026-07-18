import { z } from 'zod';

import { ExecutionActionCardSchema } from './executionGate';

export const ExecutionSnapshotSourceSchema = z.enum([
  'sync_all',
  'poll',
  'registry',
  'manual',
  'eod',
]);
export type ExecutionSnapshotSource = z.infer<typeof ExecutionSnapshotSourceSchema>;

export const ExecutionJournalCardSchema = ExecutionActionCardSchema.extend({
  positionPct: z.number().nullable().optional(),
  costPrice: z.number().nullable().optional(),
  currentPrice: z.number().nullable().optional(),
  industry: z.string().nullable().optional(),
});
export type ExecutionJournalCard = z.infer<typeof ExecutionJournalCardSchema>;

export const ExecutionSnapshotSchema = z.object({
  id: z.string(),
  tradeDate: z.string(),
  capturedAt: z.string().nullable().optional(),
  source: z.string(),
  gate: z.record(z.string(), z.unknown()),
  cards: z.array(ExecutionJournalCardSchema.passthrough()),
  contentHash: z.string().optional(),
  meta: z.record(z.string(), z.unknown()).optional(),
});
export type ExecutionSnapshot = z.infer<typeof ExecutionSnapshotSchema>;

export const ExecutionDecisionChangeSchema = z.object({
  id: z.string(),
  tradeDate: z.string().nullable().optional(),
  changedAt: z.string().nullable().optional(),
  fromSnapshotId: z.string().nullable().optional(),
  toSnapshotId: z.string().nullable().optional(),
  scope: z.string(),
  symbol: z.string().nullable().optional(),
  field: z.string(),
  oldValue: z.string().nullable().optional(),
  newValue: z.string().nullable().optional(),
});
export type ExecutionDecisionChange = z.infer<typeof ExecutionDecisionChangeSchema>;

export const ExecutionSnapshotIngestRequestSchema = z.object({
  source: ExecutionSnapshotSourceSchema,
  tradeDate: z.string(),
  gate: z.record(z.string(), z.unknown()),
  cards: z.array(ExecutionJournalCardSchema.passthrough()),
  meta: z.record(z.string(), z.unknown()).optional().nullable(),
});
export type ExecutionSnapshotIngestRequest = z.infer<typeof ExecutionSnapshotIngestRequestSchema>;

export const ExecutionSnapshotIngestResponseSchema = z.object({
  snapshotId: z.string(),
  changed: z.boolean(),
  heartbeat: z.boolean().optional(),
  snapshot: ExecutionSnapshotSchema.passthrough(),
  changes: z.array(ExecutionDecisionChangeSchema.passthrough()).default([]),
});
export type ExecutionSnapshotIngestResponse = z.infer<
  typeof ExecutionSnapshotIngestResponseSchema
>;

export const ExecutionSnapshotListResponseSchema = z.object({
  items: z.array(ExecutionSnapshotSchema.passthrough()),
});
export type ExecutionSnapshotListResponse = z.infer<typeof ExecutionSnapshotListResponseSchema>;

export const ExecutionChangeListResponseSchema = z.object({
  items: z.array(ExecutionDecisionChangeSchema.passthrough()),
});
export type ExecutionChangeListResponse = z.infer<typeof ExecutionChangeListResponseSchema>;
