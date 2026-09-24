import * as React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { SatellitePaper } from '@karios/shared';

import { setAccountCapital } from '@/lib/account-settings';
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

const userTradesMock = vi.hoisted(() => ({
  recordUserTrade: vi.fn(async () => ({})),
  invalidateUserTradesQueries: vi.fn(async () => {}),
}));
vi.mock('@/lib/queries/userTrades', () => userTradesMock);

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


function userBook(open: number) {
  return {
    ok: true,
    start: '2026-09-18',
    end: '2026-09-23',
    inception: '2026-09-18',
    prereq: { closedCount: 0, target: 20, met: false },
    stats: {
      closedCount: 0,
      openCount: open,
      winCount: 0,
      winRate: null,
      avgNetPnlPct: null,
      bestNetPnlPct: null,
      worstNetPnlPct: null,
      paperPct: null,
      paperMaxDdPct: null,
      avgHeldDays: null,
      closeReasons: {},
    },
    closed: [],
    openLegs: Array.from({ length: open }, (_, i) => ({
      ts: `CN:60000${i}`,
      entryDate: '2026-09-23',
      entryPrice: 10,
      close: null,
      heldDays: null,
      daysLeft: null,
      exitDue: null,
      pnlPct: null,
      positionPct: 25,
    })),
  } as unknown as SatellitePaper;
}

function renderBlock(node: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{node}</QueryClientProvider>);
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
    renderBlock(<SatelliteLegBlock row={row()} />);
    expect(screen.getByText(/有仓 4\/4 槽/)).toBeDefined();
    expect(screen.getByText(/今日换 4/)).toBeDefined();
  });

  it('labels an unknown replay gate without claiming 开/关', () => {
    renderBlock(<SatelliteLegBlock row={row({ gateOpen: null })} />);
    // The replay gate is no longer printed in the header at all; only the
    // time-aware tag (pending before 14:30) is shown.
    expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 待 14:30 判定');
    expect(screen.queryByText(/闸 开/)).toBeNull();
    expect(screen.queryByText(/闸 关/)).toBeNull();
  });

  it('falls back to idle slots when capacity is absent (legacy cached rows)', () => {
    renderBlock(
      <SatelliteLegBlock
        row={row({ satPositions: 2, satSlots: undefined, satCapacity: undefined, idleSlots: 2 })}
      />,
    );
    expect(screen.getByText(/有仓 2\/4 槽/)).toBeDefined();
  });

  it('renders the unavailable state without a row', () => {
    renderBlock(<SatelliteLegBlock row={undefined} />);
    expect(screen.getByText(/卫星腿：策略参照/)).toBeDefined();
    expect(screen.getByText(/最近交易日状态不可用/)).toBeDefined();
  });

  it('keeps the user book visible when the timeline row is missing', () => {
    // 2026-09-23: the block used to early-return a one-line error, hiding the
    // user's own positions exactly when the timeline hiccupped.
    renderBlock(<SatelliteLegBlock row={undefined} userBook={userBook(4)} />);
    expect(screen.getByText(/卫星腿：策略参照/)).toBeDefined();
    expect(screen.getByText(/最近交易日状态不可用/)).toBeDefined();
    expect(screen.getByTestId('satellite-user-book')).toBeDefined();
    expect(screen.getByTestId('satellite-user-legs').textContent).toContain('我的持仓（4）');
  });

  it('shows my own slot count next to the strategy book', () => {
    renderBlock(<SatelliteLegBlock row={row()} userBook={userBook(4)} />);
    expect(screen.getByTestId('satellite-my-slots').textContent).toBe('我的 4/4 槽');
  });

  it('shows starport operation hints: exits, slots, weights and the follow path', () => {
    renderBlock(
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
    expect(screen.getByText(/策略参照（引擎 · 研究档）/)).toBeDefined();
    expect(screen.getByText(/星港：母港 0\.8 \+ 卫星 0\.2/)).toBeDefined();
    expect(screen.getByText(/卫星 20% · 每槽 ≈5%/)).toBeDefined();
    expect(screen.getByText(/14:30 到期卖出（余 1 日）：/)).toBeDefined();
    expect(screen.getByText(/300906\.SZ/)).toBeDefined();
    expect(screen.getByText(/继续持有（未到期）/)).toBeDefined();
    expect(screen.getByText(/跟法：卫星 0\.2/)).toBeDefined();
  });

  it('shows the starship parking leg (weight + code) and the full-satellite note', () => {
    renderBlock(
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

  it('shows the H2-a25 parking split (25% H2 ETF / 75% B3)', () => {
    renderBlock(
      <SatelliteLegBlock
        row={row({ date: '2026-09-14', satSlots: 4, parkedWeight: 1 })}
        strategy="starship_robust"
        satWeight={1}
        parkedHeld={{
          key: 'OIL',
          name: '富国油气QDII',
          ts: '513350.SH',
          since: '2026-09-02',
          price: 1.369,
          weight: 1,
        }}
      />,
    );
    expect(screen.getByText(/稳健星舰 H2-a25/)).toBeDefined();
    expect(screen.getByText(/闲置现金 25% 停 H2 ETF \/ 75% 停 B3 风险预算/)).toBeDefined();
    expect(screen.getByText(/ETF 腿持有 原油 513350\.SH/)).toBeDefined();
    expect(screen.getByText(/另 75% 停 B3/)).toBeDefined();
    expect(screen.getByText(/回撤只有激进版约 1\/4/)).toBeDefined();
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
          name: '电投能源',
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
      renderBlock(
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
      renderBlock(
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

    it('renders vertical buy rows with 100-lot share counts and records the buy', async () => {
      setAccountCapital(1_000_000);
      userTradesMock.recordUserTrade.mockClear();
      renderBlock(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16' })}
          strategy="starship"
          livePanel={{ ...livePanel, gateOpen: true, breadth1430: 0.62, wouldFill: ['002128.SZ'] }}
        />,
      );
      // 25% slot of ¥1,000,000 = ¥250,000 / 27.7 = 9025 → 9,000 shares (100-lot).
      const rows = screen.getAllByTestId('satellite-buy-row');
      expect(rows).toHaveLength(1);
      expect(rows[0].textContent).toContain('买 9,000 股');
      // Name shown and the row links out to Xueqiu for a quick check.
      expect(rows[0].textContent).toContain('电投能源');
      const link = rows[0].querySelector('a');
      expect(link?.getAttribute('href')).toBe('https://xueqiu.com/S/SZ002128');
      expect(link?.getAttribute('target')).toBe('_blank');
      fireEvent.click(screen.getByRole('button', { name: '买入' }));
      await vi.waitFor(() =>
        expect(userTradesMock.recordUserTrade).toHaveBeenCalledWith(
          expect.objectContaining({
            symbol: 'CN:002128',
            side: 'BUY',
            price: 27.7,
            positionPct: 25,
            leg: 'satellite',
          }),
        ),
      );
      expect(await screen.findByText('已记录')).toBeDefined();
      setAccountCapital(null);
    });

    it('points to the 账户 area (no inline capital field) when capital is unset', () => {
      setAccountCapital(null);
      renderBlock(
        <SatelliteLegBlock
          row={row({ date: '2026-09-16' })}
          strategy="starship"
          livePanel={{ ...livePanel, gateOpen: true, breadth1430: 0.62, wouldFill: ['002128.SZ'] }}
        />,
      );
      expect(screen.queryByLabelText('总资金')).toBeNull();
      expect(screen.getByText(/未设总资金 → 在上方「账户」里填/)).toBeDefined();
      expect(screen.getByText(/待设总资金/)).toBeDefined();
      expect((screen.getByRole('button', { name: '买入' }) as HTMLButtonElement).disabled).toBe(true);
    });

    it('falls back to the replay state when the snapshot is stale', () => {
      renderBlock(
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
      renderBlock(
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
      renderBlock(
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
      renderBlock(<SatelliteLegBlock row={row({ date: '2026-09-16' })} strategy="starship" />);
      expect(screen.getByTestId('satellite-gate-tag').textContent).toBe('闸 今日休市');
      expect(screen.getByText(/今日休市，无补仓动作/)).toBeDefined();
    });
  });

  describe('OPT-228 forward paper book', () => {
    it('shows the 20-trade prereq, open legs and recent closed trades', () => {
      renderBlock(
        <SatelliteLegBlock
          row={row({ date: '2026-09-24' })}
          strategy="starship"
          paper={{
            ok: true,
            start: '2026-09-18',
            end: '2026-09-24',
            inception: '2026-09-18',
            decisionAvailable: true,
            prereq: { closedCount: 3, target: 20, met: false },
            stats: {
              closedCount: 3,
              openCount: 1,
              winCount: 2,
              winRate: 0.667,
              avgNetPnlPct: 1.5,
              bestNetPnlPct: 4.7,
              worstNetPnlPct: -4.3,
              paperPct: 3.2,
              paperMaxDdPct: 1.1,
              avgHeldDays: 3,
              closeReasons: { body_exit: 3 },
            },
            closed: [
              {
                ts: '000978.SZ',
                entryDate: '2026-09-18',
                exitDate: '2026-09-22',
                grossPnlPct: 5,
                netPnlPct: 4.7,
                heldDays: 3,
                closeReason: 'body_exit',
              },
            ],
            openLegs: [
              {
                ts: '002128.SZ',
                entryDate: '2026-09-21',
                entryPrice: 27.7,
                close: 28.1,
                heldDays: 1,
                daysLeft: 2,
                exitDue: '2026-09-24',
                pnlPct: 1.44,
              },
            ],
          }}
        />,
      );
      expect(screen.getByTestId('satellite-paper-book')).toBeDefined();
      expect(screen.getByText(/前置 3\/20/)).toBeDefined();
      expect(screen.getByText(/胜率 66\.7%（2\/3）/)).toBeDefined();
      expect(screen.getByText(/单笔均净 \+1\.5%/)).toBeDefined();
      expect(screen.getByText('002128')).toBeDefined();
      expect(screen.getByText('000978')).toBeDefined();
      expect(screen.getByText(/body_exit/)).toBeDefined();
    });

    it('hides the book when the payload is unavailable', () => {
      renderBlock(<SatelliteLegBlock row={row({ date: '2026-09-24' })} strategy="starship" />);
      expect(screen.queryByTestId('satellite-paper-book')).toBeNull();
    });
  });
});
