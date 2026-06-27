import { describe, expect, it } from 'vitest';

import type { TrendOkResult } from '@/lib/api/types';

import { fmtBuyCell } from './watchlist-table-cells';

describe('fmtBuyCell macro lock', () => {
  it('renders macro lock text when macroLock.active is true', () => {
    const t = {
      symbol: 'CN:600000',
      buyMode: 'none',
      buyAction: 'avoid',
      macroLock: {
        active: true,
        riskMode: 'extreme_caution',
        downCount: 4600,
      },
    } as TrendOkResult;

    const cell = fmtBuyCell(t);
    expect(cell.text).toBe('宏观死锁');
    expect(cell.tone).toBe('avoid');
    expect(cell.forced).toBe(true);
  });

  it('renders macro lock text when blocked_macro_lock buyCheck is set', () => {
    const t = {
      symbol: 'CN:600000',
      buyMode: 'none',
      buyAction: 'avoid',
      buyChecks: { blocked_macro_lock: true },
    } as TrendOkResult;

    const cell = fmtBuyCell(t);
    expect(cell.text).toBe('宏观死锁');
    expect(cell.tone).toBe('avoid');
  });

  it('does not force buy when macro lock is inactive', () => {
    const t = {
      symbol: 'CN:600000',
      buyMode: 'B_momentum',
      buyAction: 'buy',
    } as TrendOkResult;

    const cell = fmtBuyCell(t);
    expect(cell.text).toContain('B 买');
    expect(cell.tone).toBe('buy');
  });
});
