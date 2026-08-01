import { z } from 'zod';

export const TvCaptureJobStatusSchema = z.enum([
  'queued',
  'running',
  'done',
  'failed',
  'cancelled',
]);
export type TvCaptureJobStatus = z.infer<typeof TvCaptureJobStatusSchema>;

export const TvCaptureJobSchema = z.object({
  jobId: z.string(),
  screenerId: z.string(),
  status: TvCaptureJobStatusSchema,
  trigger: z.string().optional(),
  createdAt: z.string().nullable().optional(),
  startedAt: z.string().nullable().optional(),
  finishedAt: z.string().nullable().optional(),
  snapshotId: z.string().nullable().optional(),
  rowCount: z.number().nullable().optional(),
  error: z.string().nullable().optional(),
});
export type TvCaptureJob = z.infer<typeof TvCaptureJobSchema>;

export const TvScreenerSchema = z.object({
  id: z.string(),
  name: z.string(),
  url: z.string(),
  enabled: z.boolean(),
  updatedAt: z.string(),
  mode: z.enum(['api', 'chrome']).default('chrome'),
  market: z.string().nullable().optional(),
  filterJson: z.union([z.record(z.unknown()), z.array(z.unknown())]).nullable().optional(),
  apiColumns: z.array(z.string()).nullable().optional(),
});
export type TvScreener = z.infer<typeof TvScreenerSchema>;

export const TvScreenerTemplateSchema = z.object({
  templateId: z.string(),
  displayName: z.string(),
  market: z.string(),
  description: z.string(),
  nestedFilterValidated: z.boolean(),
  screenTitleSubstr: z.string(),
});
export type TvScreenerTemplate = z.infer<typeof TvScreenerTemplateSchema>;

export const TvScreenerTemplateListResponseSchema = z.object({
  items: z.array(TvScreenerTemplateSchema),
});
export type TvScreenerTemplateListResponse = z.infer<typeof TvScreenerTemplateListResponseSchema>;

export const TvScreenerListResponseSchema = z.object({
  items: z.array(TvScreenerSchema),
});
export type TvScreenerListResponse = z.infer<typeof TvScreenerListResponseSchema>;

export const TvSnapshotSummarySchema = z.object({
  id: z.string(),
  screenerId: z.string(),
  capturedAt: z.string(),
  rowCount: z.number(),
});
export type TvSnapshotSummary = z.infer<typeof TvSnapshotSummarySchema>;

export const TvSnapshotListResponseSchema = z.object({
  items: z.array(TvSnapshotSummarySchema),
});
export type TvSnapshotListResponse = z.infer<typeof TvSnapshotListResponseSchema>;

export const TvSnapshotDetailSchema = TvSnapshotSummarySchema.extend({
  screenTitle: z.string().nullable(),
  filters: z.array(z.string()),
  url: z.string(),
  headers: z.array(z.string()),
  rows: z.array(z.record(z.string(), z.string())),
});
export type TvSnapshotDetail = z.infer<typeof TvSnapshotDetailSchema>;
