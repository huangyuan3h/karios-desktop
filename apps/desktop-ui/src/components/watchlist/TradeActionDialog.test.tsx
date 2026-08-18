import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { TradeActionDialog } from '@/components/watchlist/TradeActionDialog';
import type { WatchlistItem } from '@/lib/watchlist-storage';

const ITEM: WatchlistItem = {
  symbol: 'CN:600519',
  name: '贵州茅台',
  costPrice: 1500,
  positionPct: 10,
  entryDate: '2026-07-01',
} as never;

function renderDialog(overrides?: { item?: WatchlistItem; kind?: 'sell' | 'buy' | 'add' }) {
  const onConfirm = vi.fn();
  const onClose = vi.fn();
  const kind = overrides?.kind ?? 'sell';
  render(
    <TradeActionDialog
      state={{
        kind,
        item: overrides?.item ?? ITEM,
        currentPrice: 1600,
      }}
      suggestPct={5}
      onClose={onClose}
      onConfirm={onConfirm}
    />,
  );
  return { onConfirm, onClose };
}

describe('TradeActionDialog', () => {
  it('shows no cost input when the holding has a cost price', () => {
    renderDialog();
    expect(screen.queryByText(/成本价（可选/)).toBeNull();
  });

  it('offers an optional cost fill when the holding lacks a cost price', () => {
    renderDialog({ item: { ...ITEM, costPrice: null } as never });
    expect(screen.getByText(/成本价（可选/)).toBeTruthy();
    expect(screen.getByPlaceholderText('留空 = 仅记录卖出')).toBeTruthy();
  });

  it('confirms without costPrice when the cost field is left empty', () => {
    const { onConfirm } = renderDialog({ item: { ...ITEM, costPrice: null } as never });
    fireEvent.click(screen.getByRole('button', { name: '确认卖出' }));
    expect(onConfirm).toHaveBeenCalledWith({ price: 1600, positionPct: 10 });
  });

  it('passes the filled cost price to the confirm handler', () => {
    const { onConfirm } = renderDialog({ item: { ...ITEM, costPrice: null } as never });
    fireEvent.change(screen.getByPlaceholderText('留空 = 仅记录卖出'), {
      target: { value: '1400' },
    });
    fireEvent.click(screen.getByRole('button', { name: '确认卖出' }));
    expect(onConfirm).toHaveBeenCalledWith({ price: 1600, positionPct: 10, costPrice: 1400 });
  });

  it('records sell without cost for a buy/add dialog too (no cost input shown)', () => {
    const { onConfirm } = renderDialog({ kind: 'buy' });
    fireEvent.click(screen.getByRole('button', { name: '确认买入' }));
    expect(onConfirm).toHaveBeenCalled();
  });
});
