import { describe, expect, it } from 'vitest';

import { render, screen } from '@testing-library/react';

import { HarborNavOverlay } from './HarborNavOverlay';
import type { TimelineRow } from '@/lib/queries/backtest';

function row(over: Partial<TimelineRow>): TimelineRow {
  return {
    date: '2026-08-01',
    deployedPct: 100,
    idlePct: 0,
    positions: 0,
    cnPositions: 0,
    hkPositions: 0,
    stockMarket: '',
    stockSymbols: [],
    stockMom: null,
    pick: 'STOCK',
    pickTs: null,
    navBase: 1,
    navSleeve: null,
    navSingle: 1.1,
    navMulti: 1.1,
    navBaseReturnPct: 0,
    navSingleReturnPct: 10,
    navMultiReturnPct: 10,
    exits: [],
    ...over,
  };
}

describe('HarborNavOverlay (recharts)', () => {
  it('renders the product curve with the frozen benchmark reference', () => {
    render(
      <HarborNavOverlay
        rows={[
          row({ date: '2026-08-01', navSimReturnPct: 5 }),
          row({
            date: '2026-08-04',
            navSingleReturnPct: 12,
            navSimReturnPct: 98.6,
          }),
        ]}
      />,
    );
    expect(screen.getByText('NAV 叠加')).toBeDefined();
    expect(screen.getByText('港湾 +98.6%')).toBeDefined();
    expect(screen.getByText('基准 +12.0%')).toBeDefined();
    expect(screen.getByText(/实盘口径港湾/)).toBeDefined();
    expect(screen.queryByText(/卫星/)).toBeNull();
  });

  it('falls back to benchmark-only mode without sim fields', () => {
    render(
      <HarborNavOverlay
        rows={[row({ date: '2026-08-01' }), row({ date: '2026-08-04', navSingleReturnPct: 12 })]}
      />,
    );
    expect(screen.getByText('港湾 +12.0%')).toBeDefined();
    expect(screen.queryByText('基准')).toBeNull();
  });
});
