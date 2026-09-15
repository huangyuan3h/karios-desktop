import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import type { TimelineRow } from '@/lib/queries/backtest';

import { SatelliteLegBlock } from './SatelliteLegBlock';

function row(over: Partial<TimelineRow> = {}): TimelineRow {
  return {
    date: '2026-09-11',
    satActive: true,
    satPositions: 4,
    satSlots: 8,
    satCapacity: 4,
    filledToday: 4,
    idleSlots: 0,
    gateOpen: true,
    ...over,
  } as unknown as TimelineRow;
}

describe('SatelliteLegBlock', () => {
  it('shows the capacity, not the recycle-day slot count, plus today churn', () => {
    render(<SatelliteLegBlock row={row()} />);
    expect(screen.getByText(/有仓 4\/4 槽/)).toBeDefined();
    expect(screen.getByText(/今日换 4/)).toBeDefined();
  });

  it('labels an unknown gate as — instead of claiming 开', () => {
    render(<SatelliteLegBlock row={row({ gateOpen: null })} />);
    expect(screen.getByText(/闸 —/)).toBeDefined();
  });

  it('falls back to idle slots when capacity is absent (legacy cached rows)', () => {
    render(
      <SatelliteLegBlock
        row={row({ satPositions: 2, satSlots: undefined, satCapacity: undefined, idleSlots: 2 })}
      />,
    );
    expect(screen.getByText(/有仓 2\/4 槽/)).toBeDefined();
  });

  it('renders the unavailable state without a row', () => {
    render(<SatelliteLegBlock row={undefined} />);
    expect(screen.getByText(/卫星腿：最近交易日状态不可用/)).toBeDefined();
  });
});
