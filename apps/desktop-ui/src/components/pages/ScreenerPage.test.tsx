import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

const { useChatStore, useScreenerListQuery, useScreenerSnapshotsQuery } = vi.hoisted(() => ({
  useChatStore: vi.fn(),
  useScreenerListQuery: vi.fn(),
  useScreenerSnapshotsQuery: vi.fn(),
}));

vi.mock('@/lib/chat/store', () => ({ useChatStore }));
vi.mock('@/lib/queries/screener', () => ({
  useScreenerListQuery,
  useScreenerSnapshotsQuery,
  invalidateScreenerQueries: vi.fn().mockResolvedValue(undefined),
}));
vi.mock('@/lib/api/client', () => ({
  apiGetJson: vi.fn().mockResolvedValue({ items: [], status: null }),
}));

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ScreenerPage } from './ScreenerPage';

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <ScreenerPage />
    </QueryClientProvider>,
  );
}

describe('ScreenerPage tabs', () => {
  it('renders Snapshots tab by default and keeps TradingView mounted', () => {
    useChatStore.mockReturnValue({ addReference: vi.fn() });
    useScreenerListQuery.mockReturnValue({ data: [], isFetching: false });
    useScreenerSnapshotsQuery.mockReturnValue({ data: {}, isFetching: false });
    renderPage();
    expect(screen.getByText('Screener')).toBeInTheDocument();
    expect(screen.getByText('TradingView')).toBeInTheDocument();
    const panels = screen.getAllByText(/TradingView Integration|Sync TradingView screeners/);
    expect(panels.length).toBeGreaterThan(0);
  });

  it('switches to TradingView tab and back', () => {
    useChatStore.mockReturnValue({ addReference: vi.fn() });
    useScreenerListQuery.mockReturnValue({ data: [], isFetching: false });
    useScreenerSnapshotsQuery.mockReturnValue({ data: {}, isFetching: false });
    renderPage();
    const tvTab = screen.getByText('TradingView');
    expect(tvTab).toBeInTheDocument();
    fireEvent.click(tvTab);
    expect(screen.getByText(/Configure screeners and manage a dedicated Chrome/)).toBeInTheDocument();
    fireEvent.click(screen.getByText('Snapshots'));
    expect(screen.getByText(/Sync TradingView screeners/)).toBeInTheDocument();
  });
});
