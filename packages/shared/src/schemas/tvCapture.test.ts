import { describe, expect, it } from 'vitest';
import {
  TvCaptureJobSchema,
  TvCaptureJobStatusSchema,
  TvScreenerListResponseSchema,
  TvSnapshotDetailSchema,
  TvSnapshotListResponseSchema,
} from './tvCapture';

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

describe('TvScreenerListResponseSchema', () => {
  it('validates screener list', () => {
    const resp = TvScreenerListResponseSchema.parse({
      items: [
        {
          id: 'sc-1',
          name: 'Momentum',
          url: 'https://www.tradingview.com/screener/',
          enabled: true,
          updatedAt: '2026-06-18T10:00:00+00:00',
        },
      ],
    });
    expect(resp.items).toHaveLength(1);
    expect(resp.items[0]?.enabled).toBe(true);
  });
});

describe('TvSnapshotListResponseSchema', () => {
  it('validates snapshot summary list', () => {
    const resp = TvSnapshotListResponseSchema.parse({
      items: [
        {
          id: 'snap-1',
          screenerId: 'sc-1',
          capturedAt: '2026-06-18T10:00:00+00:00',
          rowCount: 42,
        },
      ],
    });
    expect(resp.items[0]?.rowCount).toBe(42);
  });
});

describe('TvSnapshotDetailSchema', () => {
  it('validates snapshot detail', () => {
    const snap = TvSnapshotDetailSchema.parse({
      id: 'snap-1',
      screenerId: 'sc-1',
      capturedAt: '2026-06-18T10:00:00+00:00',
      rowCount: 2,
      screenTitle: 'US Stocks',
      filters: ['Market cap > 1B'],
      url: 'https://www.tradingview.com/screener/',
      headers: ['Ticker', 'Price'],
      rows: [{ Ticker: 'AAPL', Price: '200' }],
    });
    expect(snap.headers).toEqual(['Ticker', 'Price']);
    expect(snap.rows[0]?.Ticker).toBe('AAPL');
  });
});
