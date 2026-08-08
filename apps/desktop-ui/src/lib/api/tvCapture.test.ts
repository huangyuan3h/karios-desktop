import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));

import { apiGetJson, apiPostJson } from '@/lib/api/client';

import {
  enqueueTvScreenerSync,
  getTvCaptureJob,
  syncTvScreenerAndWait,
  waitForTvCaptureJob,
} from './tvCapture';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedApiPostJson = vi.mocked(apiPostJson);

const RUNNING = { jobId: 'j1', screenerId: 'sc-a', status: 'running', error: null };
const DONE = { jobId: 'j1', screenerId: 'sc-a', status: 'done', error: null };

describe('enqueueTvScreenerSync', () => {
  beforeEach(() => {
    mockedApiPostJson.mockReset();
  });

  it('posts to screener sync endpoint', async () => {
    mockedApiPostJson.mockResolvedValue(RUNNING);
    const out = await enqueueTvScreenerSync('sc-a');
    expect(out.jobId).toBe('j1');
    expect(String(mockedApiPostJson.mock.calls[0][0])).toBe(
      '/integrations/tradingview/screeners/sc-a/sync',
    );
  });
});

describe('getTvCaptureJob', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetches capture job by id', async () => {
    mockedApiGetJson.mockResolvedValue(DONE);
    await getTvCaptureJob('j1');
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/integrations/tradingview/capture-jobs/j1',
    );
  });
});

describe('waitForTvCaptureJob', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockedApiGetJson.mockReset();
    vi.stubGlobal('window', globalThis);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('returns immediately when job is done', async () => {
    mockedApiGetJson.mockResolvedValue(DONE);
    await expect(waitForTvCaptureJob('j1')).resolves.toMatchObject({ status: 'done' });
    expect(mockedApiGetJson).toHaveBeenCalledTimes(1);
  });

  it('polls until done', async () => {
    mockedApiGetJson
      .mockResolvedValueOnce(RUNNING)
      .mockResolvedValueOnce(RUNNING)
      .mockResolvedValueOnce(DONE);
    const promise = waitForTvCaptureJob('j1', { pollMs: 50, timeoutMs: 10_000 });
    await vi.advanceTimersByTimeAsync(200);
    await expect(promise).resolves.toMatchObject({ status: 'done' });
    expect(mockedApiGetJson).toHaveBeenCalledTimes(3);
  });

  it('throws when job failed', async () => {
    mockedApiGetJson.mockResolvedValue({
      ...RUNNING,
      status: 'failed',
      error: 'capture crashed',
    });
    await expect(waitForTvCaptureJob('j1')).rejects.toThrow('capture crashed');
  });

  it('throws on timeout', async () => {
    mockedApiGetJson.mockResolvedValue(RUNNING);
    const promise = waitForTvCaptureJob('j1', { pollMs: 50, timeoutMs: 120 });
    const assertion = expect(promise).rejects.toThrow('Capture timed out');
    await vi.advanceTimersByTimeAsync(1000);
    await assertion;
  });
});

describe('syncTvScreenerAndWait', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockedApiPostJson.mockReset();
    mockedApiGetJson.mockReset();
    vi.stubGlobal('window', globalThis);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('enqueues then waits', async () => {
    mockedApiPostJson.mockResolvedValue(RUNNING);
    mockedApiGetJson.mockResolvedValueOnce(RUNNING).mockResolvedValueOnce(DONE);
    const promise = syncTvScreenerAndWait('sc-a');
    const assertion = expect(promise).resolves.toMatchObject({ status: 'done' });
    await vi.advanceTimersByTimeAsync(4000);
    await assertion;
  });
});
