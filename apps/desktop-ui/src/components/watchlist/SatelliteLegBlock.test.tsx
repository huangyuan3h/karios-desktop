import { render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { TimelineRow } from '@/lib/queries/backtest';

import { SatelliteLegBlock } from './SatelliteLegBlock';

const marketHours = vi.hoisted(() => ({
  minutes: 10 * 60, // 10:00 Shanghai default (before the 14:30 slot)
  weekday: true,
  today: '2026-09-17',
}));
vi.mock('@/lib/market-hours', async () => {
  const actual =
    await vi.importActual<typeof import('@/lib/market-hours')>('@/lib/market-hours');
  return {
    ...actual,
    getShanghaiMinutes: () => marketHours.minutes,
    isWeekdayShanghai: () => marketHours.weekday,
    getShanghaiTodayIso: () => marketHours.today,
  };
});

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
  beforeEach(() => {
    marketHours.minutes = 10 * 60;
    marketHours.weekday = true;
  });
  afterEach(() => {
    marketHours.minutes = 10 * 60;
    marketHours.weekday = true;
  });

  it('shows the capacity, not the recycle-day slot count, plus today churn', () => {
    render(<SatelliteLegBlock row={row()} />);
    expect(screen.getByText(/有仓 4\/4 槽/)).toBeDefined();
    expect(screen.getByText(/今日换 4/)).toBeDefined();
  });

  it('labels an unknown replay gate without claiming 开/关', () => {
    render(<SatelliteLegBlock row={row({ gateOpen: null })} />);
    // The replay gate is no longer printed in the header at all; only the
    // time-aware tag (pending before 14:30) is shown.
    expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 待 14:30 判定');
    expect(screen.queryByText(/闸 开/)).toBeNull();
    expect(screen.queryByText(/闸 关/)).toBeNull();
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
        satWeight={0.2}
        openPositions={[
          { ts: '300906.SZ', entryDate: '2026-09-11', entryPrice: 28.12, daysLeft: 1 },
          { ts: '601872.SH', entryDate: '2026-09-11', entryPrice: 20.01, daysLeft: 2 },
        ]}
      />,
    );
    expect(screen.getByText(/操作提示（研究档 · 不进 Live）/)).toBeDefined();
    expect(screen.getByText(/星港：母港 0\.8 \+ 卫星 0\.2/)).toBeDefined();
    expect(screen.getByText(/卫星 20% · 每槽 ≈5%/)).toBeDefined();
    expect(screen.getByText(/14:30 到期卖出（余 1 日）：/)).toBeDefined();
    expect(screen.getByText(/300906\.SZ/)).toBeDefined();
    expect(screen.getByText(/继续持有（未到期）/)).toBeDefined();
    expect(screen.getByText(/跟法：卫星 0\.2/)).toBeDefined();
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
    expect(screen.getAllByText(/513350\.SH/).length).toBeGreaterThan(0);
    expect(screen.getByText(/全部资金按卫星 4 槽 ×25%/)).toBeDefined();
    // clarity: the exit bullet spells out where the proceeds go
    expect(screen.getByText(/卖出资金停入 H2 停车腿（原油 513350\.SH）/)).toBeDefined();
  });

  describe('OPT-222 live 14:30 panel', () => {
    const livePanel = {
      tradeDate: '2026-09-17',
      generatedAt: '2026-09-17T14:30:12+08:00',
      decisionAvailable: true,
      gateOpen: false,
      breadth1430: 0.2796,
      gapCount: 34,
      bucketSize: 11,
      poolSize: 11,
      coverage: 0.91,
      wouldFill: [],
      ranked: [
        {
          ts: '002128.SZ',
          ampRank: 3,
          inBucket: true,
          gapPct: 3.04,
          amp1430Pct: 2.92,
          px1430: 27.7,
          skipReason: null,
          fillable: true,
          wouldFill: false,
        },
        {
          ts: '001216.SZ',
          ampRank: 7,
          inBucket: true,
          gapPct: 7.86,
          amp1430Pct: 3.92,
          px1430: 19.88,
          skipReason: 'skip_t1_limit',
          fillable: false,
          wouldFill: false,
        },
      ],
    };

    it('shows a prominent closed gate and the would-be list when the gate is shut', () => {
      render(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16', gateOpen: true })}
          strategy="starship"
          livePanel={livePanel}
          openPositions={[
            { ts: '000978.SZ', entryDate: '2026-09-15', entryPrice: 8.1, exitDue: '2026-09-17' },
          ]}
        />,
      );
      expect(screen.getByTestId('satellite-live-panel')).toBeDefined();
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 关 · 今日 14:30');
      expect(screen.getByText('闸 关 · 今日只卖不买')).toBeDefined();
      expect(screen.getByText(/14:30 广度 28%/)).toBeDefined();
      expect(screen.getByText(/若开闸，今日本会买（1 只 · 不执行）：/)).toBeDefined();
      expect(screen.getByText('002128')).toBeDefined();
      expect(screen.getByText(/跳过：001216\(skip_t1_limit\)/)).toBeDefined();
      // exit anchored to today, not "next trading day"
      expect(screen.getByText('今日 2026-09-17 14:30 到期卖出：')).toBeDefined();
      expect(screen.queryByText(/下一交易日 14:30/)).toBeNull();
    });

    it('shows the buy list when the gate is open', () => {
      render(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16' })}
          strategy="starport"
          livePanel={{ ...livePanel, gateOpen: true, breadth1430: 0.62, wouldFill: ['002128.SZ'] }}
        />,
      );
      expect(screen.getByText('闸 开 · 今日 14:30 可补仓')).toBeDefined();
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 开 · 今日 14:30');
      expect(screen.getByText(/今日买入（1 只 · 每槽/)).toBeDefined();
    });

    it('falls back to the replay state when the snapshot is stale', () => {
      render(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16', gateOpen: true })}
          strategy="starport"
          livePanel={livePanel}
          livePanelStale
        />,
      );
      expect(screen.queryByTestId('satellite-live-panel')).toBeNull();
      // the replay gate must not read as today's: pending tag + replay label
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 待 14:30 判定');
      expect(screen.getByText(/回放 2026-09-16（非今日）/)).toBeDefined();
      expect(screen.queryByText(/回放 2026-09-16 · 闸/)).toBeNull();
      expect(screen.getByText(/待今日 14:30 现场面板/)).toBeDefined();
    });

    it('uses the panel exit list even without timeline positions (OPT-223)', () => {
      render(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16', gateOpen: true })}
          strategy="starship"
          livePanel={{ ...livePanel, exits: [{ ts: '000978.SZ', exitDue: '2026-09-17' }] }}
          openPositions={[]}
        />,
      );
      expect(screen.getByText('今日 2026-09-17 14:30 到期卖出：')).toBeDefined();
      expect(screen.getByText(/000978\.SZ/)).toBeDefined();
      expect(screen.queryByText(/回放 2026-09-16 · 闸/)).toBeNull();
    });

    it('switches to a missing-snapshot state after the 14:30 slot', () => {
      marketHours.minutes = 14 * 60 + 50; // 14:50, past the 14:40 grace
      render(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16', gateOpen: true })}
          strategy="starship"
        />,
      );
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 快照缺失');
      expect(screen.getAllByText(/今日 14:30 快照缺失/).length).toBeGreaterThan(0);
      expect(screen.queryByText(/待今日 14:30 现场面板/)).toBeNull();
    });

    it('shows a closed-market tag on weekends', () => {
      marketHours.weekday = false;
      render(<SatelliteLegBlock row={row({ date: '2026-09-16' })} strategy="starship" />);
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 今日休市');
      expect(screen.getByText(/今日休市，无补仓动作/)).toBeDefined();
    });
  });
});
