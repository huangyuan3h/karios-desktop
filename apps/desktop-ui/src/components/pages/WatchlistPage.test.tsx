import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { WatchlistPage } from './WatchlistPage';

const mocks = vi.hoisted(() => ({
  pushMode: vi.fn(),
  regime: {
    data: { ok: true, target: 'starport', targetLabel: '星港 0.2' },
    isLoading: false,
    error: null,
  } as unknown,
}));

vi.mock('@/components/account/AccountBar', () => ({
  AccountBar: () => <div data-testid="account-bar" />,
}));
vi.mock('@/components/watchlist/RegimeAutoCard', () => ({
  RegimeAutoCard: ({ data }: { data?: { target?: string } }) => (
    <div data-testid="regime-auto-card-mock">{data?.target}</div>
  ),
}));
vi.mock('@/components/watchlist/SgapHealthCard', () => ({
  SgapHealthCard: () => <div data-testid="sgap-health-card-mock" />,
}));
vi.mock('@/components/watchlist/PortfolioHealthCard', () => ({
  PortfolioHealthCard: ({ mode }: { mode: string }) => (
    <div data-testid="health-mode">{mode}</div>
  ),
}));
vi.mock('@/components/watchlist/WatchlistTable', () => ({
  sortWatchlistItems: (items: unknown[]) => items,
  WatchlistTable: () => <div data-testid="watchlist-table" />,
}));
vi.mock('@/hooks/useExecutionJournalCapture', () => ({
  useExecutionJournalCapture: () => undefined,
}));
vi.mock('@/hooks/useWatchlistItems', () => ({
  useWatchlistItems: () => ({
    items: [],
    persist: vi.fn(),
    watchlistHydrating: false,
    error: null,
    setError: vi.fn(),
    code: '',
    setCode: vi.fn(),
    costPriceDrafts: {},
    positionPctDrafts: {},
    onAdd: vi.fn(),
    onRemove: vi.fn(),
    setItemColor: vi.fn(),
    setItemPositionPct: vi.fn(),
    setItemPositionPctDraft: vi.fn(),
    commitItemPositionPctDraft: vi.fn(),
    setItemCostPriceDraft: vi.fn(),
    setItemCostPriceValue: vi.fn(),
    commitItemCostPriceDraft: vi.fn(),
    applyTradeUpdate: vi.fn(),
  }),
}));
vi.mock('@/hooks/useWatchlistTrend', () => ({
  useWatchlistTrend: () => ({ trend: {}, quotes: {} }),
}));
vi.mock('@/lib/queries/behaviorAudit', () => ({
  useBehaviorAuditQuery: () => ({ data: [] }),
}));
vi.mock('@/lib/queries/alphaRadar', () => ({
  useAlphaRadarCatalystQuery: () => ({ data: null }),
}));
vi.mock('@/lib/queries/dashboard', () => ({
  useDashboardSummaryQuery: () => ({ data: null }),
}));
vi.mock('@/lib/queries/sentiment', () => ({
  useDashboardSentimentQuery: () => ({ data: null }),
}));
vi.mock('@/lib/queries/watchlist', () => ({
  useWatchlistRegimeTargetQuery: () => mocks.regime,
  useWatchlistRsRanksQuery: () => ({ data: { ranks: {} } }),
}));
vi.mock('@/lib/strategy-settings', () => ({
  pushStrategyModeToServer: mocks.pushMode,
}));

describe('WatchlistPage auto regime', () => {
  it('passes the auto target to PortfolioHealthCard and mirrors it to the server', () => {
    render(<WatchlistPage />);
    expect(screen.getByTestId('health-mode').textContent).toBe('starport');
    expect(screen.getByTestId('regime-auto-card-mock').textContent).toBe('starport');
    expect(mocks.pushMode).toHaveBeenCalledWith('starport');
    expect(screen.queryByTestId('strategy-mode-bar')).toBeNull();
  });
});
