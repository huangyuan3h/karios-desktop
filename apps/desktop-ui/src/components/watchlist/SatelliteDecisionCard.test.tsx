import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SatelliteDecisionCard } from './SatelliteDecisionCard';

const { rowsRef } = vi.hoisted(() => ({ rowsRef: { value: [] as unknown[] } }));

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useTimelineQuery: () => ({ data: { rows: rowsRef.value } }),
  };
});

beforeEach(() => {
  rowsRef.value = [
    {
      date: '2026-09-11',
      satActive: true,
      satPositions: 2,
      satSlots: 4,
      gateOpen: true,
      filledToday: 1,
    },
  ];
});

describe('SatelliteDecisionCard', () => {
  it('renders the starship decision surface with the last-session state', () => {
    render(<SatelliteDecisionCard />);
    expect(screen.getByText('星舰 · 今日决策')).toBeDefined();
    expect(screen.getByText('有仓 2/4 槽')).toBeDefined();
    expect(screen.getByText(/最近交易日 2026-09-11/)).toBeDefined();
    expect(screen.getByText(/当日成交 1/)).toBeDefined();
    expect(screen.getByText(/OPT-178 重接 \+ OPT-186/)).toBeDefined();
  });

  it('labels an active day with no open positions as 今日有成交', () => {
    rowsRef.value = [
      {
        date: '2026-09-11',
        satActive: true,
        satPositions: 0,
        satSlots: 4,
        gateOpen: true,
      },
    ];
    render(<SatelliteDecisionCard />);
    expect(screen.getByText('今日有成交（现持 0/4 槽）')).toBeDefined();
  });
});
