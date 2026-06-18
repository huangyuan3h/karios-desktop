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
