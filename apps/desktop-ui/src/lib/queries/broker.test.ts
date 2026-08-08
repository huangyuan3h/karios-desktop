import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { apiGetJson } from '@/lib/api/client';
import { useQuery } from '@tanstack/react-query';

import {
  brokerAccountsQueryKey,
  brokerAccountStateQueryKey,
  brokerAccountsQueryOptions,
  brokerAccountStateQueryOptions,
  fetchBrokerAccounts,
  fetchBrokerAccountState,
  invalidateBrokerQueries,
  useBrokerAccountsQuery,
  useBrokerAccountStateQuery,
} from './broker';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedUseQuery = vi.mocked(useQuery);

type CapturedOptions = {
  queryKey: unknown;
  queryFn: () => Promise<unknown>;
  enabled?: boolean;
  staleTime?: number;
  refetchInterval?: number | boolean | ((...args: unknown[]) => unknown);
  refetchIntervalInBackground?: boolean;
};

function lastOptions(): CapturedOptions {
  return mockedUseQuery.mock.calls[mockedUseQuery.mock.calls.length - 1][0] as CapturedOptions;
}

describe('query keys', () => {
  it('builds stable keys', () => {
    expect(brokerAccountsQueryKey()).toEqual(['broker', 'accounts', 'pingan']);
    expect(brokerAccountsQueryKey('xyz')).toEqual(['broker', 'accounts', 'xyz']);
    expect(brokerAccountStateQueryKey('pingan', 'a1')).toEqual([
      'broker',
      'state',
      'pingan',
      'a1',
    ]);
  });
});

describe('fetch functions', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('fetchBrokerAccounts encodes broker name', async () => {
    mockedApiGetJson.mockResolvedValue([]);
    await fetchBrokerAccounts();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/broker/accounts?broker=pingan',
    );
  });

  it('fetchBrokerAccountState builds state path', async () => {
    mockedApiGetJson.mockResolvedValue({ accountId: 'a1' });
    const out = await fetchBrokerAccountState('pingan', 'a1');
    expect(out.accountId).toBe('a1');
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe(
      '/broker/pingan/accounts/a1/state',
    );
  });
});

describe('query options', () => {
  beforeEach(() => {
    mockedApiGetJson.mockReset();
  });

  it('brokerAccountsQueryOptions wires key, fn and stale time', async () => {
    mockedApiGetJson.mockResolvedValue([{ id: 'a1' }]);
    const opts = brokerAccountsQueryOptions();
    expect(opts.queryKey).toEqual(['broker', 'accounts', 'pingan']);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toContain('/broker/accounts');
  });

  it('brokerAccountStateQueryOptions disables when accountId blank', () => {
    const blank = brokerAccountStateQueryOptions('pingan', '  ');
    expect(blank.enabled).toBe(false);
    const ok = brokerAccountStateQueryOptions('pingan', 'a1');
    expect(ok.enabled).toBe(true);
  });
});

describe('hooks', () => {
  beforeEach(() => {
    mockedUseQuery.mockClear();
  });

  it('useBrokerAccountsQuery forwards options', () => {
    useBrokerAccountsQuery('icbc');
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['broker', 'accounts', 'icbc']);
  });

  it('useBrokerAccountStateQuery forwards options', () => {
    useBrokerAccountStateQuery('pingan', 'a1');
    expect(lastOptions().queryKey).toEqual(['broker', 'state', 'pingan', 'a1']);
  });
});

describe('invalidateBrokerQueries', () => {
  it('invalidates the broker subtree', async () => {
    const invalidateQueries = vi.fn().mockResolvedValue(undefined);
    await invalidateBrokerQueries({ invalidateQueries } as never);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['broker'] });
  });
});
