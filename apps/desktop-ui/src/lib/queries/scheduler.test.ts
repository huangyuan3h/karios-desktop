import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGetJson, apiPostJson } from '@/lib/api/client';

import { fetchSchedulerJobs, schedulerJobsQueryKey, triggerSchedulerAction } from './scheduler';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));

beforeEach(() => {
  vi.mocked(apiGetJson).mockReset();
  vi.mocked(apiPostJson).mockReset();
});

describe('schedulerJobsQueryKey', () => {
  it('returns stable scheduler key', () => {
    expect(schedulerJobsQueryKey()).toEqual(['scheduler', 'jobs']);
  });
});

describe('fetchSchedulerJobs', () => {
  it('hits /sync/jobs and parses with Zod schema', async () => {
    const { apiGetJson } = await import('@/lib/api/client');
    const mockedGet = vi.mocked(apiGetJson);
    mockedGet.mockResolvedValueOnce({
      ok: true,
      jobs: {
        hk_basic_sync: {
          todayRun: {
            id: 1,
            job_type: 'hk_basic_sync',
            sync_at: '2026-07-29T10:00:00Z',
            success: true,
            last_ts_code: null,
            error_message: null,
          },
          lastSuccess: null,
        },
      },
      hkIndustryCoverage: {
        ok: true,
        totalHk: 2803,
        mappedHk: 100,
        missingHk: 2703,
        coveragePct: 3.57,
        jobType: 'hk_industry_sync',
      },
      alphaRadar: null,
      watchlistAutomation: null,
    });

    const result = await fetchSchedulerJobs();

    expect(mockedGet).toHaveBeenCalledTimes(1);
    expect(String(mockedGet.mock.calls[0][0])).toBe('/sync/jobs');
    expect(result.jobs.hk_basic_sync.todayRun?.success).toBe(true);
    expect(result.hkIndustryCoverage?.coveragePct).toBe(3.57);
  });

  it('rejects malformed payload via Zod', async () => {
    const { apiGetJson } = await import('@/lib/api/client');
    const mockedGet = vi.mocked(apiGetJson);
    mockedGet.mockResolvedValueOnce({ ok: true, jobs: 'not-a-dict' });

    await expect(fetchSchedulerJobs()).rejects.toBeDefined();
  });
});

describe('triggerSchedulerAction', () => {
  it('POSTs JSON body and forwards the response', async () => {
    const { apiPostJson } = await import('@/lib/api/client');
    const mockedPost = vi.mocked(apiPostJson);
    mockedPost.mockResolvedValueOnce({ ok: true, updated: 5 });

    const result = await triggerSchedulerAction('/sync/hk-daily', 'POST', undefined);

    expect(mockedPost).toHaveBeenCalledTimes(1);
    expect(String(mockedPost.mock.calls[0][0])).toBe('/sync/hk-daily');
    expect(result.ok).toBe(true);
    expect(result.updated).toBe(5);
  });

  it('uses GET path when method is GET', async () => {
    const { apiGetJson } = await import('@/lib/api/client');
    const mockedGet = vi.mocked(apiGetJson);
    mockedGet.mockResolvedValueOnce({ ok: true });

    const result = await triggerSchedulerAction('/sync/hk-daily/status', 'GET');

    expect(mockedGet).toHaveBeenCalledTimes(1);
    expect(String(mockedGet.mock.calls[0][0])).toBe('/sync/hk-daily/status');
    expect(result.ok).toBe(true);
  });
});
