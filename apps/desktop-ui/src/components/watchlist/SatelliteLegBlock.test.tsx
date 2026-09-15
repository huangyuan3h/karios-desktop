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

  it('shows starport operation hints: exits, slots, weights and the follow path', () => {
    render(
      <SatelliteLegBlock
        row={row({ date: '2026-09-14', satSlots: 4 })}
        strategy="starport"
        satWeight={1 / 3}
        openPositions={[
          { ts: '300906.SZ', entryDate: '2026-09-11', entryPrice: 28.12, daysLeft: 1 },
          { ts: '601872.SH', entryDate: '2026-09-11', entryPrice: 20.01, daysLeft: 2 },
        ]}
      />,
    );
    expect(screen.getByText(/操作提示（研究档 · 不进 Live）/)).toBeDefined();
    expect(screen.getByText(/星港：母港 2\/3 \+ 卫星 1\/3/)).toBeDefined();
    expect(screen.getByText(/卫星 33\.3% · 每槽 ≈8\.3%/)).toBeDefined();
    expect(screen.getByText(/14:30 到期卖出（余 1 日）：/)).toBeDefined();
    expect(screen.getByText(/300906\.SZ/)).toBeDefined();
    expect(screen.getByText(/继续持有（未到期）/)).toBeDefined();
    expect(screen.getByText(/跟法：卫星 1\/3/)).toBeDefined();
  });

  it('shows the starship parking leg (weight + code) and the full-satellite note', () => {
    render(
      <SatelliteLegBlock
        row={row({ date: '2026-09-14', satSlots: 4, parkedWeight: 0.84 })}
        strategy="starship"
        satWeight={1}
        openPositions={[
          { ts: '000839.SZ', entryDate: '2026-09-11', entryPrice: 2.88, daysLeft: 1 },
        ]}
        parkedHeld={{
          key: 'OIL',
          name: '富国油气QDII',
          ts: '513350.SH',
          since: '2026-09-02',
          price: 1.417,
          weight: 0.84,
        }}
      />,
    );
    expect(screen.getByText(/星舰 v2：卫星 100% \+ 闲钱停车/)).toBeDefined();
    expect(screen.getByText(/停车权重 84%/)).toBeDefined();
    expect(screen.getByText(/513350\.SH/)).toBeDefined();
    expect(screen.getByText(/全部资金按卫星 4 槽 ×25%/)).toBeDefined();
  });
});
