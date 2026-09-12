import { describe, expect, it } from 'vitest';

import { render, screen } from '@testing-library/react';

import { TwinStarNavOverlay } from './TwinStarNavOverlay';
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
    coreNav: 1.08,
    coreNavReturnPct: 8,
    satNav: 1.02,
    satNavReturnPct: 2,
    satPositions: 1,
    satSlots: 1,
    satActive: true,
    exits: [],
    ...over,
  };
}

describe('TwinStarNavOverlay (recharts)', () => {
  it('renders product curve as main series with benchmark reference', () => {
    render(
      <TwinStarNavOverlay
        rows={[
          row({ date: '2026-08-01', navSimMultiReturnPct: 6, navSimReturnPct: 5 }),
          row({
            date: '2026-08-04',
            navSingleReturnPct: 12,
            navSimMultiReturnPct: 96.8,
            navSimReturnPct: 98.6,
            coreNavReturnPct: 9,
            satNavReturnPct: 1,
            satActive: false,
            satSlots: 0,
          }),
        ]}
      />,
    );
    expect(screen.getByText('NAV 叠加')).toBeDefined();
    expect(screen.getByText('双子星 +96.8%')).toBeDefined();
    expect(screen.getByText('核心 +98.6%')).toBeDefined();
    expect(screen.getByText('卫星 +1.0%')).toBeDefined();
    expect(screen.getByText('基准 +12.0%')).toBeDefined();
    expect(screen.getByText(/实盘口径/)).toBeDefined();
  });

  it('falls back to benchmark-only mode without sim fields', () => {
    render(
      <TwinStarNavOverlay
        rows={[row({ date: '2026-08-01' }), row({ date: '2026-08-04', navSingleReturnPct: 12 })]}
      />,
    );
    expect(screen.getByText('双子星 +12.0%')).toBeDefined();
    expect(screen.queryByText('基准')).toBeNull();
    expect(screen.getByText(/satActive/)).toBeDefined();
  });
});
