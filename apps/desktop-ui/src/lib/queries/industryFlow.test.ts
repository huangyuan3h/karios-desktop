import { beforeEach, describe, expect, it, vi } from 'vitest';

import { apiGetJson, apiPostJson } from '@/lib/api/client';

import {
  fetchIndustryFlowBundle,
  industryFundFlowQueryKey,
  industryFundFlowQueryOptions,
  industryMainlineQueryKey,
  industryMainlineQueryOptions,
  invalidateIndustryFlowQueries,
  INDUSTRY_FLOW_DAYS,
  INDUSTRY_FLOW_UNIVERSE_TOP_N,
} from './industryFlow';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
  apiPostJson: vi.fn(),
}));

vi.mock('@/lib/market-hours', () => ({
  isShanghaiSyncWindow: () => true,
}));

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedApiPostJson = vi.mocked(apiPostJson);

describe('industryFundFlowQueryKey', () => {
  it('returns stable key for days and topN', () => {
    expect(industryFundFlowQueryKey(10, 200)).toEqual(['industry', 'fundFlow', 10, 200]);
  });
});

describe('industryMainlineQueryKey', () => {
  it('returns stable mainline key', () => {
    expect(industryMainlineQueryKey()).toEqual(['industry', 'mainline']);
  });
});

describe('industryFundFlowQueryOptions', () => {
  it('uses default universe params', () => {
    const options = industryFundFlowQueryOptions();
    expect(options.queryKey).toEqual(
      industryFundFlowQueryKey(INDUSTRY_FLOW_DAYS, INDUSTRY_FLOW_UNIVERSE_TOP_N),
    );
    expect(typeof options.queryFn).toBe('function');
  });
});

describe('industryMainlineQueryOptions', () => {
  it('uses mainline query key', () => {
    const options = industryMainlineQueryOptions();
    expect(options.queryKey).toEqual(industryMainlineQueryKey());
    expect(typeof options.queryFn).toBe('function');
  });
});

describe('fetchIndustryFlowBundle', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetches fund flow and mainline in parallel', async () => {
    const inFlight = new Set<string>();
    let maxInFlight = 0;

    mockedApiGetJson.mockImplementation(async (url: string) => {
      inFlight.add(url);
      maxInFlight = Math.max(maxInFlight, inFlight.size);
      await new Promise((resolve) => setTimeout(resolve, 20));
      inFlight.delete(url);

      if (url.includes('/industry-fund-flow')) {
        return { asOfDate: '2024-06-18', days: 10, topN: 200, dates: [], top: [] };
      }
      if (url.includes('/industry-mainline')) {
        return { asOfDate: '2024-06-18', dates: [], allScores: [], currentMainline: [] };
      }
      throw new Error(`Unexpected url: ${url}`);
    });

    const result = await fetchIndustryFlowBundle();

    expect(mockedApiGetJson).toHaveBeenCalledTimes(2);
    expect(maxInFlight).toBeGreaterThan(1);
    expect(result.fundFlow.asOfDate).toBe('2024-06-18');
    expect(result.mainline.asOfDate).toBe('2024-06-18');
  });
});

describe('invalidateIndustryFlowQueries', () => {
  it('invalidates industry and dashboard summary keys', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    const queryClient = { invalidateQueries } as never;

    await invalidateIndustryFlowQueries(queryClient);

    expect(invalidateQueries).toHaveBeenCalledTimes(2);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['industry'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['dashboard', 'summary'] });
  });
});

describe('syncIndustryFundFlow', () => {
  beforeEach(() => {
    mockedApiPostJson.mockReset();
  });

  it('posts fund flow sync payload', async () => {
    mockedApiPostJson.mockResolvedValueOnce({ rowsUpserted: 1 });
    const { syncIndustryFundFlow } = await import('./industryFlow');
    await syncIndustryFundFlow({ force: true });
    expect(mockedApiPostJson).toHaveBeenCalledWith('/market/cn/industry-fund-flow/sync', {
      days: INDUSTRY_FLOW_DAYS,
      topN: 10,
      force: true,
    });
  });
});
