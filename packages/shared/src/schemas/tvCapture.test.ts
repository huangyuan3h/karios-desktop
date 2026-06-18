import { describe, expect, it } from 'vitest';
import { TvCaptureJobSchema, TvCaptureJobStatusSchema } from './tvCapture';

describe('TvCaptureJobStatusSchema', () => {
  it('accepts all job statuses', () => {
    for (const status of ['queued', 'running', 'done', 'failed', 'cancelled'] as const) {
      expect(TvCaptureJobStatusSchema.parse(status)).toBe(status);
    }
  });

  it('rejects unknown status', () => {
    expect(() => TvCaptureJobStatusSchema.parse('pending')).toThrow();
  });
});

describe('TvCaptureJobSchema', () => {
  it('validates enqueued job', () => {
    const job = TvCaptureJobSchema.parse({
      jobId: 'job-123',
      screenerId: 'falcon',
      status: 'queued',
      trigger: 'api',
      createdAt: '2026-06-18T10:00:00+00:00',
    });
    expect(job.status).toBe('queued');
  });

  it('validates completed job', () => {
    const job = TvCaptureJobSchema.parse({
      jobId: 'job-123',
      screenerId: 'falcon',
      status: 'done',
      snapshotId: 'snap-1',
      rowCount: 42,
      finishedAt: '2026-06-18T10:01:00+00:00',
    });
    expect(job.rowCount).toBe(42);
  });

  it('validates failed job', () => {
    const job = TvCaptureJobSchema.parse({
      jobId: 'job-123',
      screenerId: 'falcon',
      status: 'failed',
      error: 'CDP is not available',
    });
    expect(job.error).toBe('CDP is not available');
  });
});
