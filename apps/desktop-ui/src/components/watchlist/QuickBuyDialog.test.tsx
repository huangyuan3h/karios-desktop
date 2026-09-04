import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { QuickBuyDialog } from '@/components/watchlist/QuickBuyDialog';

vi.mock('@/lib/watchlist-market', () => ({
  fetchWatchlistMarketSnapshot: vi.fn(),
}));

import { fetchWatchlistMarketSnapshot } from '@/lib/watchlist-market';

const mockedSnapshot = vi.mocked(fetchWatchlistMarketSnapshot);

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('QuickBuyDialog', () => {
  it('prefills instantly from initialPrice without fetching', () => {
    mockedSnapshot.mockClear();
    render(
      <QuickBuyDialog
        state={{ symbol: 'CN:600519', name: '贵州茅台' }}
        suggestPct={12.5}
        side="SELL"
        initialPrice={1599.5}
        onClose={() => {}}
        onConfirm={() => {}}
      />,
    );
    expect((screen.getByPlaceholderText('0.000') as HTMLInputElement).value).toBe('1599.5');
    expect(mockedSnapshot).not.toHaveBeenCalled();
  });

  it('keeps the price field editable while the snapshot loads, and never wipes typed input', async () => {
    const gate = deferred<{ quotes: Record<string, { price: number }> }>();
    mockedSnapshot.mockClear().mockReturnValueOnce(gate.promise as never);
    render(
      <QuickBuyDialog
        state={{ symbol: 'CN:600519', name: '贵州茅台' }}
        suggestPct={12.5}
        side="SELL"
        initialPrice={null}
        onClose={() => {}}
        onConfirm={() => {}}
      />,
    );
    const input = screen.getByPlaceholderText(/加载中/) as HTMLInputElement;
    // Loading must not block typing (2026-09-04 modal-freeze fix).
    expect(input.disabled).toBe(false);
    fireEvent.change(input, { target: { value: '1598' } });
    gate.resolve({ quotes: { 'CN:600519': { price: 1600 } } });
    await waitFor(() => expect(screen.getByPlaceholderText('0.000')).toBeTruthy());
    // Late snapshot must not overwrite what the user typed.
    expect((screen.getByPlaceholderText('0.000') as HTMLInputElement).value).toBe('1598');
  });
});
