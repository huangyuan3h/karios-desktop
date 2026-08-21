import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import * as React from 'react';

import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { WatchlistTable } from '@/components/watchlist/WatchlistTable';

vi.mock('@/lib/watchlist-storage', async () => {
  const actual = await vi.importActual<typeof import('@/lib/watchlist-storage')>(
    '@/lib/watchlist-storage',
  );
  return {
    ...actual,
    loadWatchlist: vi.fn(() => []),
    saveWatchlist: vi.fn(() => Promise.resolve({ ok: true, synced: true })),
    ensureWatchlistHydrated: vi.fn(() => Promise.resolve(undefined)),
  };
});

vi.mock('@/lib/queries/userTrades', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/userTrades')>(
    '@/lib/queries/userTrades',
  );
  return {
    ...actual,
    recordUserTrade: vi.fn(() => Promise.resolve({ id: 't1' })),
    invalidateUserTradesQueries: vi.fn(() => Promise.resolve()),
  };
});

vi.mock('@/lib/queries/backtest', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/backtest')>(
    '@/lib/queries/backtest',
  );
  return {
    ...actual,
    useCorrelationStatusQuery: vi.fn(() => ({ data: { clusters: {}, overLimit: [], topPairs: [] } })),
  };
});

vi.mock('@/lib/queries/watchlist', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/watchlist')>(
    '@/lib/queries/watchlist',
  );
  return {
    ...actual,
    useWatchlistRsRanksQuery: vi.fn(() => ({ data: null })),
  };
});

vi.mock('@/lib/chat/store', async () => {
  const actual = await vi.importActual<typeof import('@/lib/chat/store')>(
    '@/lib/chat/store',
  );
  return {
    ...actual,
    useChatStore: vi.fn(() => ({ addReference: vi.fn() })),
  };
});

import * as storage from '@/lib/watchlist-storage';
import { recordUserTrade } from '@/lib/queries/userTrades';

function Harness() {
  const {
    items,
    setItemPositionPct,
    setItemPositionPctDraft,
    commitItemPositionPctDraft,
    setItemCostPriceValue,
  } = useWatchlistItems();
  return (
    <WatchlistTable
      sortedItems={items}
      items={items}
      trend={{}}
      quotes={{}}
      costPriceDrafts={{}}
      positionPctDrafts={{}}
      scoreSortDir="desc"
      scoreSortEnabled={false}
      setScoreSortDir={() => {}}
      setScoreSortEnabled={() => {}}
      showHidden={false}
      setShowHidden={() => {}}
      setItemColor={() => {}}
      setItemPositionPct={setItemPositionPct}
      setItemPositionPctDraft={setItemPositionPctDraft}
      commitItemPositionPctDraft={commitItemPositionPctDraft}
      setItemCostPriceDraft={() => {}}
      setItemCostPriceValue={setItemCostPriceValue}
      commitItemCostPriceDraft={() => {}}
      onRemove={() => {}}
    />
  );
}

describe('repro: add-trade modal updates the row', () => {
  beforeEach(() => {
    vi.mocked(storage.loadWatchlist).mockReturnValue([
      {
        symbol: 'CN:300628',
        name: '亿联网络',
        addedAt: '2026-08-01',
        positionPct: 5.93,
        costPrice: 39.9,
        entryDate: '2026-08-04',
      },
    ] as never);
    vi.mocked(storage.saveWatchlist).mockClear();
    vi.mocked(recordUserTrade).mockClear();
  });

  it('click 加仓 -> enter pct -> confirm -> row positionPct updates even when the trade journal fails', async () => {
    vi.mocked(recordUserTrade).mockRejectedValueOnce(new Error('network down'));
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <Harness />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(screen.getByText('CN:300628')).toBeInTheDocument());

    const addBtn = screen.getByRole('button', { name: '加仓' });
    fireEvent.click(addBtn);

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /确认加仓/ })).toBeInTheDocument();
    });

    const dialog = screen.getByRole('button', { name: /确认加仓/ }).closest('div[class*="fixed inset-0"]') as HTMLElement;
    const pctInput = within(dialog).getByPlaceholderText('0');
    fireEvent.change(pctInput, { target: { value: '5' } });
    const priceInput = within(dialog).getByPlaceholderText('0.000');
    fireEvent.change(priceInput, { target: { value: '42.4' } });

    const confirmBtn = within(dialog).getByRole('button', { name: /确认加仓/ });
    fireEvent.click(confirmBtn);

    await waitFor(() => expect(recordUserTrade).toHaveBeenCalled());
    expect(recordUserTrade).toHaveBeenCalledWith({
      symbol: 'CN:300628',
      side: 'ADD',
      price: 42.4,
      positionPct: 5,
      source: expect.any(String),
      market: 'CN',
    });

    await waitFor(() => {
      const rows = screen.getAllByDisplayValue(/10\.93/);
      expect(rows.length).toBeGreaterThan(0);
    });
  });
});
