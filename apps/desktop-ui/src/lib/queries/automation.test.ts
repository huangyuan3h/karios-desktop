import { describe, expect, it, vi } from 'vitest';

import {
  AUTOMATION_POLL_MS,
  automationPendingQueryKey,
  automationPendingQueryOptions,
} from './automation';

vi.mock('@/lib/watchlist-automation', () => ({
  fetchAutomationPending: vi.fn(),
  isAutomationPollWindow: vi.fn(() => true),
}));

describe('automationPendingQueryKey', () => {
  it('returns stable pending key', () => {
    expect(automationPendingQueryKey()).toEqual(['watchlist', 'automation', 'pending']);
  });
});

describe('automationPendingQueryOptions', () => {
  it('uses stable query key and poll interval staleTime', () => {
    const options = automationPendingQueryOptions();
    expect(options.queryKey).toEqual(automationPendingQueryKey());
    expect(options.staleTime).toBe(AUTOMATION_POLL_MS);
    expect(typeof options.queryFn).toBe('function');
  });
});
