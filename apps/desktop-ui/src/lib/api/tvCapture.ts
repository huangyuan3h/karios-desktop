import type { TvCaptureJob } from '@karios/shared';
import { apiGetJson, apiPostJson } from './client';

export type { TvCaptureJob };

export async function enqueueTvScreenerSync(screenerId: string): Promise<TvCaptureJob> {
  return apiPostJson<TvCaptureJob>(
    `/integrations/tradingview/screeners/${encodeURIComponent(screenerId)}/sync`,
  );
}

export async function getTvCaptureJob(jobId: string): Promise<TvCaptureJob> {
  return apiGetJson<TvCaptureJob>(
    `/integrations/tradingview/capture-jobs/${encodeURIComponent(jobId)}`,
  );
}

export async function waitForTvCaptureJob(
  jobId: string,
  options: { pollMs?: number; timeoutMs?: number } = {},
): Promise<TvCaptureJob> {
  const pollMs = options.pollMs ?? 1500;
  const timeoutMs = options.timeoutMs ?? 120_000;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await getTvCaptureJob(jobId);
    if (job.status === 'done' || job.status === 'failed' || job.status === 'cancelled') {
      if (job.status === 'failed') {
        throw new Error(job.error || 'Capture failed');
      }
      return job;
    }
    await new Promise((r) => window.setTimeout(r, pollMs));
  }
  throw new Error('Capture timed out');
}

export async function syncTvScreenerAndWait(screenerId: string): Promise<TvCaptureJob> {
  const enqueued = await enqueueTvScreenerSync(screenerId);
  return waitForTvCaptureJob(enqueued.jobId);
}
