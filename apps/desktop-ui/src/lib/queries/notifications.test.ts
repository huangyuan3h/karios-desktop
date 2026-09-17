import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn(),
}));
vi.mock('@tanstack/react-query', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@tanstack/react-query')>();
  return { ...actual, useQuery: vi.fn(() => ({ data: undefined })) };
});

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

import { useNotificationsQuery } from './notifications';

const mockedApiGetJson = vi.mocked(apiGetJson);
const mockedUseQuery = vi.mocked(useQuery);

type CapturedOptions = { queryKey: unknown; queryFn: () => Promise<unknown> };

function lastOptions(): CapturedOptions {
  return mockedUseQuery.mock.calls[mockedUseQuery.mock.calls.length - 1][0] as CapturedOptions;
}

beforeEach(() => {
  mockedApiGetJson.mockReset();
  mockedUseQuery.mockClear();
  window.localStorage.removeItem('karios.strategyMode.v2');
});

describe('useNotificationsQuery', () => {
  it('follows the reactive strategy mode (OPT-223)', async () => {
    useNotificationsQuery(true, 'starship');
    const opts = lastOptions();
    expect(opts.queryKey).toEqual(['notifications', 'starship']);
    await opts.queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/notifications?mode=starship');
  });

  it('falls back to the stored mode when the caller does not pass one', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('twin_star'));
    useNotificationsQuery(true);
    await lastOptions().queryFn();
    expect(String(mockedApiGetJson.mock.calls[0][0])).toBe('/api/notifications?mode=twin_star');
  });
});
