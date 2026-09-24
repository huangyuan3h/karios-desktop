import * as React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { getShanghaiTodayIso } from '@/lib/market-hours';

import { A25TargetBlock } from './A25TargetBlock';

const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });

function renderBlock(ui: React.ReactElement) {
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>);
}

const TODAY = getShanghaiTodayIso();
const STALE_DAY = '2026-09-22';
const FAR = '2099-01-01';

const B3 = {
  ok: true,
  asOf: '2026-09-22',
  rebalanceDate: '2026-09-01',
  month: '2026-09',
  universe: [
    { symbol: '510300.SH', name: '沪深300', targetPct: 3.8, driftPct: 3.8, deltaPct: 0, px: 4.608 },
    {
      symbol: '511260.SH',
      name: '10年国债',
      targetPct: 86.5,
      driftPct: 86.5,
      deltaPct: 0,
      px: 134.757,
    },
  ],
  trades: [],
  note: 'B3 腿内部权重',
};

/** 星舰 B park leg = 3-leg {国债,黄金,纳指} inverse-vol (no H2 / no B3). */
const STAR_B = {
  ok: true,
  asOf: '2026-09-24',
  rebalanceDate: '2026-09-01',
  month: '2026-09',
  universe: [
    { symbol: '511260.SH', name: '10年国债', targetPct: 92.4, driftPct: 92.4, deltaPct: 0, px: 134.8 },
    { symbol: '518880.SH', name: '黄金', targetPct: 4.0, driftPct: 3.9, deltaPct: 0.1, px: 8.9 },
    { symbol: '513100.SH', name: '纳指100', targetPct: 3.6, driftPct: 3.7, deltaPct: -0.2, px: 2.28 },
  ],
  trades: [],
  note: '星舰 B 停放腿内部权重',
};

const dueMock = vi.hoisted(() => ({ value: {} as Record<string, string | null> }));
vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useB3StateQuery: () => ({ data: B3 }),
    useStarshipBStateQuery: () => ({ data: STAR_B }),
    useSatelliteExitDueQuery: () => ({ data: { ok: true, body: 3, exitDue: dueMock.value } }),
  };
});

vi.mock('@/lib/account-settings', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/account-settings')>();
  return { ...actual, useAccountSettings: () => ({ capital: 1_210_000, rate: 0.92 }) };
});

const userTradesMock = vi.hoisted(() => {
  const list = { value: [] as unknown[] };
  return {
    list,
    recordUserTrade: vi.fn(async () => ({})),
    invalidateUserTradesQueries: vi.fn(async () => {}),
    useUserTradesListQuery: () => ({ data: list.value }),
  };
});
vi.mock('@/lib/queries/userTrades', () => userTradesMock);

const watchlistMock = vi.hoisted(() => ({
  loadWatchlist: vi.fn(() => [] as unknown[]),
  saveWatchlist: vi.fn(async () => ({ ok: true, synced: true })),
  upsertWatchlistOpenTrade: vi.fn((items: unknown) => items),
}));
vi.mock('@/lib/watchlist-storage', () => watchlistMock);

/** A fresh panel (captured today) — its fills are today's orders. */
const PANEL = {
  tradeDate: TODAY,
  generatedAt: `${TODAY}T14:30:18+08:00`,
  decisionAvailable: true,
  gateOpen: true,
  ranked: [
    {
      ts: '603019.SH',
      name: '中科曙光',
      ampRank: 1,
      inBucket: true,
      gapPct: 3.53,
      amp1430Pct: 2.96,
      px1430: 84.77,
      skipReason: null,
      fillable: true,
      wouldFill: true,
    },
  ],
  heldLegs: [],
};

/**
 * `weight` is the ENGINE's parked weight (31.24% — 4/4 slots + a year of
 * accumulated profits). Targets must NOT use it: they size off the book's own
 * idle (unfilled satellite slots), so a 1-leg book parks 75%.
 */
const PARKED = { key: 'OIL', name: '富国油气QDII', ts: '513350.SH', price: 1.369, weight: 0.3124 };

describe('A25TargetBlock', () => {
  it('lists every order flat — satellite + sleeve + B3, sized off the book idle', () => {
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={PANEL} />);
    expect(screen.getByText('H2-a25 · 今日下单')).toBeDefined();
    const rows = screen.getAllByTestId('a25-order-row');
    expect(rows).toHaveLength(4); // 1 satellite + sleeve + 2 mocked B3 ETFs
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('603019');
    expect(t).toContain('中科曙光');
    expect(t).toContain('买 3,600 股'); // 25% × 1.21M / 84.77
    expect(t).toContain('513350');
    expect(t).toContain('买 221,000 份'); // idle 100% → sleeve 25%
    expect(t).toContain('10年国债');
    // % of total capital: satellite slot 25, B3 = 75% idle × 86.5%
    expect(t).toContain('25%');
    expect(t).toContain('64.88%');
    // strategy internals stay hidden
    expect(t).not.toContain('卫星腿');
    expect(t).not.toContain('停车 · B3');
  });

  it('星舰 B: parks 100% idle in the 3-leg blend (no H2 sleeve, no B3)', () => {
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={PANEL} parkMode="starship_b" />);
    expect(screen.getByText('星舰 B · 今日下单')).toBeDefined();
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    // 3-leg parking ETF names present…
    expect(t).toContain('10年国债');
    expect(t).toContain('黄金');
    expect(t).toContain('纳指100');
    // …and the H2-a25 sleeve ETF (mocked B3's 沪深300) is NOT offered.
    expect(t).not.toContain('沪深300');
    // 4 rows: 1 satellite + 3 park legs (no sleeve/B3 extra leg).
    expect(screen.getAllByTestId('a25-order-row')).toHaveLength(4);
  });

  it('caps the fills at the free slots (a full satellite books no new entry)', () => {
    // The USER's own 4 legs occupy the slots (2026-09-23: the book is the user's,
    // not the engine's — the engine's legs are a different cohort entirely).
    dueMock.value = { '2026-09-21': FAR };
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300220', name: '金运激光', positionPct: 25, costPrice: 11.23, entryDate: '2026-09-21' },
      { symbol: 'CN:300932', name: '三友联众', positionPct: 25, costPrice: 11.01, entryDate: '2026-09-21' },
      { symbol: 'CN:603137', name: '恒尚节能', positionPct: 25, costPrice: 19.81, entryDate: '2026-09-21' },
      { symbol: 'CN:001331', name: '胜通能源', positionPct: 25, costPrice: 45.9, entryDate: '2026-09-21' },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={PANEL} satCapacity={4} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).not.toContain('603019'); // no room → no phantom fill
    // Full satellite → idle 0 → no fill and no parking first-funding either.
    expect(screen.queryAllByTestId('a25-order-row')).toHaveLength(0);
  });

  it('counts a held off-book leg against the free slots (603019 case)', () => {
    // The engine booked no 603019 (all slots were full on 09-22), but the user
    // holds it — it must consume one of the 4 slots or the card over-offers.
    const panel4 = {
      ...PANEL,
      ranked: ['603125', '300990', '002982', '301058'].map((c, i) => ({
        ts: `${c}.SH`,
        name: c,
        ampRank: i + 1,
        inBucket: true,
        gapPct: 4,
        amp1430Pct: 3 + i,
        px1430: 10,
        skipReason: null,
        fillable: true,
        wouldFill: true,
      })),
    };
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77 },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={panel4} satCapacity={4} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('603125');
    expect(t).toContain('002982');
    expect(t).not.toContain('301058'); // 4th slot is taken by 603019
  });

  it('records the satellite row to the journal (leg=satellite) + watchlist', async () => {
    userTradesMock.recordUserTrade.mockClear();
    watchlistMock.saveWatchlist.mockClear();
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={PANEL} />);
    fireEvent.click(screen.getAllByRole('button', { name: '买入' })[0]!);
    await waitFor(() => expect(userTradesMock.recordUserTrade).toHaveBeenCalled());
    expect(watchlistMock.upsertWatchlistOpenTrade).toHaveBeenCalled();
    expect(watchlistMock.saveWatchlist).toHaveBeenCalled();
    expect(userTradesMock.recordUserTrade).toHaveBeenCalledWith(
      expect.objectContaining({ leg: 'satellite', symbol: 'CN:603019', positionPct: 25 }),
    );
  });

  it('rolls over to 当前持有 when the panel is not today (0-point rule)', () => {
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300220', name: '金运激光', positionPct: 25, costPrice: 11.23, entryDate: '2026-09-21' },
      { symbol: 'CN:300932', name: '三友联众', positionPct: 25, costPrice: 11.01, entryDate: '2026-09-21' },
    ]);
    dueMock.value = { '2026-09-21': TODAY };
    renderBlock(
      <A25TargetBlock
        parkedHeld={PARKED}
        panel={{ ...PANEL, tradeDate: STALE_DAY }}
        openPositions={[
          { ts: '300220.SZ', entryDate: '2026-09-21', entryPrice: 11.23, close: 11.24, pnlPct: 0.09, exitDue: FAR },
          { ts: '300932.SZ', entryDate: '2026-09-21', entryPrice: 11.01, close: 11.2, pnlPct: 1.73, exitDue: TODAY },
        ]}
      />,
    );
    expect(screen.getByText('H2-a25 · 当前持有')).toBeDefined();
    expect(screen.queryByText('H2-a25 · 今日下单')).toBeNull();
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('300220'); // held
    expect(t).toContain('300932'); // held
    expect(t).toContain('金运激光');
    expect(t).toContain('+1.73%');
    expect(t).toContain(`到期 ${TODAY}`); // entry + BODY sessions, from the rule
    // both legs are due today → both sellable
    expect(screen.getAllByRole('button', { name: '卖出' })).toHaveLength(2);
    expect(screen.queryByRole('button', { name: '买入' })).toBeNull();
  });

  it('hides a leg the registry only mirrors (no recorded size)', () => {
    // The satellite automation mirrors the strategy's legs with source=satellite
    // and no positionPct — that is not a user position (2026-09-23 incident),
    // so it must not appear as 持有 nor offer a sell.
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300932', name: '三友联众', source: 'satellite' },
    ]);
    renderBlock(
      <A25TargetBlock
        parkedHeld={PARKED}
        panel={{ ...PANEL, tradeDate: STALE_DAY }}
        openPositions={[
          { ts: '300932.SZ', entryDate: '2026-09-21', entryPrice: 11.01, close: 11.2, pnlPct: 1.73, exitDue: TODAY },
        ]}
      />,
    );
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).not.toContain('300932');
    expect(screen.queryByRole('button', { name: '卖出' })).toBeNull();
    expect(screen.getByText('当前无持仓')).toBeDefined();
  });

  it('shows the sell time on stocks and the target drift on ETFs', () => {
    dueMock.value = { '2026-09-21': TODAY };
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300932', name: '三友联众', positionPct: 25, costPrice: 11.01, entryDate: '2026-09-21' },
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77, entryDate: '2026-09-22' },
      { symbol: 'ETF:513350', name: '富国油气QDII', positionPct: 19, costPrice: 1.38 },
    ]);
    renderBlock(
      <A25TargetBlock
        parkedHeld={PARKED}
        panel={{ ...PANEL, tradeDate: STALE_DAY }}
        openPositions={[
          { ts: '300932.SZ', entryDate: '2026-09-21', entryPrice: 11.01, close: 11.2, pnlPct: 1.73, exitDue: TODAY },
        ]}
      />,
    );
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain(`到期 ${TODAY}`); // the strategy's exit day
    expect(t).toContain('14:30'); // …and the exit time
    // 2 held legs → 50% idle → sleeve target 12.5% vs the recorded 19%
    expect(t).toContain('目标 12.5%');
    expect(t).toContain('偏多');
    expect(t).toContain('需调整');
  });

  it('derives the sell time for a held leg from the frozen rule (entry + BODY)', () => {
    dueMock.value = { '2026-09-22': '2026-09-24' };
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77, entryDate: '2026-09-22' },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, tradeDate: STALE_DAY }} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('603019');
    expect(t).toContain('到期 2026-09-24'); // entry + BODY sessions
    expect(t).toContain('14:30');
  });

  it('offers a rebalance trade when a row is off its target', async () => {
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300932', name: '三友联众', positionPct: 25, costPrice: 11.01 },
      { symbol: 'ETF:513350', name: '富国油气QDII', positionPct: 30, costPrice: 1.38 },
    ]);
    userTradesMock.recordUserTrade.mockClear();
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, tradeDate: STALE_DAY }} />);
    // 1 leg held → idle 75% → sleeve target 18.75% vs recorded 30% → sell 11.25pt
    const btn = screen.getByRole('button', { name: /减仓/ });
    expect(btn.textContent ?? '').toContain('11.25%');
    fireEvent.click(btn);
    await waitFor(() => expect(userTradesMock.recordUserTrade).toHaveBeenCalled());
    expect(userTradesMock.recordUserTrade).toHaveBeenCalledWith(
      expect.objectContaining({ symbol: 'ETF:513350', side: 'SELL', positionPct: 11.25 }),
    );
  });

  it('does not tell a 1-leg book to sell its sleeve/B3 (own-idle sizing)', () => {
    // The 2026-09-23 user book: 1 satellite leg (25%) + OIL 19% + B3 56%.
    // Idle = 75% → sleeve 18.75% / B3 56.25% → the recorded weights are on
    // target and nothing should be flagged (the engine's 31.24% parked weight
    // would have said "sell", which is a backtest artifact).
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77, entryDate: '2026-09-22' },
      { symbol: 'ETF:513350', name: '富国油气QDII', positionPct: 19, costPrice: 1.38 },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, tradeDate: STALE_DAY }} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).not.toContain('目标 7.81%'); // not the engine's parked weight
    expect(t).not.toContain('目标 0%');
    expect(screen.queryByRole('button', { name: /减仓|加仓/ })).toBeNull();
  });

  it('liquidates the parking when every slot is filled (0 idle, small weight)', () => {
    // 4 satellite legs = 100% → idle 0 → the sleeve/B3 target is 0, so even a
    // 1.46% B3 row must offer 减仓. It used to sit under the 3pt drift threshold
    // (and `idle > 0` nulled the target outright) → unsellable (2026-09-23).
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77 },
      { symbol: 'CN:603125', name: '常青科技', positionPct: 25, costPrice: 24.54 },
      { symbol: 'CN:300990', name: '同飞股份', positionPct: 25, costPrice: 108.56 },
      { symbol: 'CN:002982', name: '湘佳股份', positionPct: 25, costPrice: 12.54 },
      { symbol: 'ETF:510300', name: '沪深300', positionPct: 1.46, costPrice: 4.608 },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, tradeDate: STALE_DAY }} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('目标 0%');
    expect(screen.getByRole('button', { name: '减仓 1.46%' })).toBeTruthy();
  });

  it('offers 卖出 for a due leg the user actually holds', () => {
    dueMock.value = { '2026-09-21': TODAY };
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:300932', name: '300932', positionPct: 25, entryDate: '2026-09-21' },
    ]);
    renderBlock(
      <A25TargetBlock
        parkedHeld={PARKED}
        panel={{ ...PANEL, tradeDate: STALE_DAY }}
        openPositions={[
          { ts: '300932.SZ', entryDate: '2026-09-21', entryPrice: 11.01, close: 11.2, pnlPct: 1.73, exitDue: TODAY },
        ]}
      />,
    );
    fireEvent.click(screen.getAllByRole('button', { name: '卖出' })[0]!);
    expect(watchlistMock.upsertWatchlistOpenTrade).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ symbol: 'CN:300932', side: 'SELL' }),
    );
  });

  it('falls back to the recorded position when the engine idle is unavailable', () => {
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'ETF:513350', name: '富国油气QDII', positionPct: 19 },
    ]);
    renderBlock(
      <A25TargetBlock
        parkedHeld={null}
        sleevePick={{ key: 'OIL', ts: '513350.SH', name: '富国油气QDII', close: 1.369 }}
        panel={{ ...PANEL, tradeDate: STALE_DAY }}
      />,
    );
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('513350');
    expect(t).toContain('19%');
  });

  it('re-syncs a journal-only row into the watchlist without duplicating the ledger', async () => {
    userTradesMock.list.value = [
      { symbol: 'CN:603019', tradeDate: TODAY, strategyMode: 'starship_robust' },
    ];
    userTradesMock.recordUserTrade.mockClear();
    watchlistMock.saveWatchlist.mockClear();
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={PANEL} />);
    fireEvent.click(screen.getAllByRole('button', { name: '买入' })[0]!);
    await waitFor(() => expect(watchlistMock.saveWatchlist).toHaveBeenCalled());
    expect(userTradesMock.recordUserTrade).not.toHaveBeenCalled(); // no duplicate
    userTradesMock.list.value = [];
  });

  it('shows no satellite buy when the gate is closed', () => {
    renderBlock(
      <A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, gateOpen: false, ranked: [] }} />,
    );
    expect(screen.getAllByTestId('a25-order-row')).toHaveLength(3); // sleeve + 2 B3
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).not.toContain('603019');
  });

  it('sells an off-engine leg using the portfolio-health last close (603019 case)', async () => {
    // The engine never booked 603019 (slots were full on 09-22) and it is not in
    // today's gap list, so neither `openPositions` nor the panel carries a mark.
    // Without the last-close fallback the row read "不足 1 手" and 卖出 did nothing.
    dueMock.value = { '2026-09-22': TODAY };
    userTradesMock.recordUserTrade.mockClear();
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603019', name: '中科曙光', positionPct: 25, costPrice: 84.77, entryDate: '2026-09-22' },
    ]);
    renderBlock(
      <A25TargetBlock
        parkedHeld={PARKED}
        panel={{ ...PANEL, ranked: [] }}
        holdingLastClose={{ 'CN:603019': 83.5 }}
      />,
    );
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('603019');
    expect(t).toContain('卖 3,600 股'); // 25% × 1.21M / 83.5 → 100-share lot
    expect(t).not.toContain('不足 1 手');
    fireEvent.click(screen.getByRole('button', { name: '卖出' }));
    await waitFor(() => expect(userTradesMock.recordUserTrade).toHaveBeenCalled());
    expect(userTradesMock.recordUserTrade).toHaveBeenCalledWith(
      expect.objectContaining({ symbol: 'CN:603019', side: 'SELL' }),
    );
  });

  it('re-funds the sleeve/B3 even when their registry rows sit at 0%', () => {
    // A zeroed row (bought then sold) or an automation-mirrored leg is NOT a
    // holding: it must not suppress the first funding (2026-09-24: OIL + B3 buys
    // vanished after selling 603019 because every ETF row already existed).
    dueMock.value = {};
    watchlistMock.loadWatchlist.mockReturnValueOnce([
      { symbol: 'CN:603125', name: '常青科技', positionPct: 25, costPrice: 24.54 },
      { symbol: 'CN:300990', name: '同飞股份', positionPct: 25, costPrice: 108.56 },
      { symbol: 'CN:002982', name: '湘佳股份', positionPct: 25, costPrice: 12.54 },
      { symbol: 'ETF:513350', name: '富国油气QDII', positionPct: 0, costPrice: 1.38 },
      { symbol: 'ETF:510300', name: '沪深300', positionPct: 0, costPrice: 4.608 },
    ]);
    renderBlock(<A25TargetBlock parkedHeld={PARKED} panel={{ ...PANEL, ranked: [] }} />);
    const t = screen.getByTestId('a25-target-block').textContent ?? '';
    expect(t).toContain('513350'); // idle 25% → sleeve 6.25%
    expect(t).toContain('6.25%');
    expect(t).toContain('510300'); // B3 re-funds too (18.75% total)
    expect(t).toContain('511260');
  });
});
