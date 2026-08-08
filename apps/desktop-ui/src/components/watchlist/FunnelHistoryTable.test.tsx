import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { FunnelHistoryTable, toFunnelRow } from './FunnelHistoryTable';

const { useFunnelHistoryQuery } = vi.hoisted(() => ({ useFunnelHistoryQuery: vi.fn() }));
vi.mock('@/lib/queries/funnel', () => ({ useFunnelHistoryQuery }));

describe('toFunnelRow', () => {
  it('computes conversion pct from tvHit when present', () => {
    const row = toFunnelRow({
      runId: 'r',
      runAt: 't',
      tradeDate: '2026-08-07',
      meta: { funnel: { tvHit: 10, passPullback: 5, passTrendOk: 3, addedNew: 2 } },
    } as never);
    expect(row).toEqual(
      expect.objectContaining({
        tradeDate: '2026-08-07',
        tvHit: 10,
        passPullback: 5,
        passTrendOk: 3,
        addedNew: 2,
        fallbackUsed: false,
        conversionPct: 30,
      }),
    );
  });

  it('uses fallback ratio when primary pool was empty', () => {
    const row = toFunnelRow({
      runId: 'r',
      runAt: 't',
      tradeDate: '2026-08-07',
      meta: {
        funnel: {
          tvHit: 0,
          passPullback: 0,
          passTrendOk: 0,
          addedNew: 0,
          fallbackUsed: true,
          fallbackHit: 7,
          fallbackTrendOk: 4,
        },
      },
    } as never);
    expect(row?.conversionPct).toBe(57);
    expect(row?.fallbackUsed).toBe(true);
    expect(row?.fallbackHit).toBe(7);
    expect(row?.fallbackTrendOk).toBe(4);
  });

  it('returns null when meta has no funnel', () => {
    const row = toFunnelRow({ runId: 'r', runAt: 't', tradeDate: '2026-08-07', meta: {} } as never);
    expect(row).toBeNull();
  });

  it('returns null conversion when no hits at all', () => {
    const row = toFunnelRow({
      runId: 'r',
      runAt: 't',
      tradeDate: '2026-08-07',
      meta: { funnel: { tvHit: 0, passPullback: 0, passTrendOk: 0, addedNew: 0, fallbackUsed: false, fallbackHit: 0, fallbackTrendOk: 0 } },
    } as never);
    expect(row?.conversionPct).toBeNull();
  });

  it('handles missing tradeDate and falsey fallback fields', () => {
    const row = toFunnelRow({
      runId: 'r',
      runAt: 't',
      tradeDate: undefined as never,
      meta: { funnel: { tvHit: 10, passPullback: 5, passTrendOk: 3, addedNew: 2 } },
    } as never);
    expect(row?.tradeDate).toBe('');
    expect(row?.fallbackUsed).toBe(false);
    expect(row?.conversionPct).toBe(30);
  });
});

describe('FunnelHistoryTable', () => {
  it('renders the section title and loading state on first frame', () => {
    useFunnelHistoryQuery.mockReturnValue({ isLoading: true, data: undefined });
    render(<FunnelHistoryTable />);
    expect(screen.getByText(/Funnel History/)).toBeInTheDocument();
    expect(screen.getByText('Loading…')).toBeInTheDocument();
  });

  it('renders funnel rows with conversion and fallback columns', () => {
    useFunnelHistoryQuery.mockReturnValue({
      isLoading: false,
      data: [
        {
          runId: 'r1',
          runAt: 't',
          tradeDate: '2026-08-07',
          meta: { funnel: { tvHit: 10, passPullback: 6, passTrendOk: 4, addedNew: 3, fallbackUsed: false } },
        },
        {
          runId: 'r2',
          runAt: 't',
          tradeDate: '2026-08-06',
          meta: { funnel: { tvHit: 0, passPullback: 0, passTrendOk: 0, addedNew: 0, fallbackUsed: true, fallbackHit: 7, fallbackTrendOk: 4 } },
        },
      ],
    });
    render(<FunnelHistoryTable />);
    expect(screen.getByText('2026-08-07')).toBeInTheDocument();
    expect(screen.getByText('40%')).toBeInTheDocument();
    expect(screen.getByText('57%')).toBeInTheDocument();
    expect(screen.getByText('7→OK 4')).toBeInTheDocument();
  });

  it('shows empty state with fallback message when not loading', () => {
    useFunnelHistoryQuery.mockReturnValue({ isLoading: false, data: [] });
    render(<FunnelHistoryTable />);
    expect(screen.getByText(/暂无漏斗数据/)).toBeInTheDocument();
  });

  it('drops rows without funnel meta', () => {
    useFunnelHistoryQuery.mockReturnValue({
      isLoading: false,
      data: [{ runId: 'r', runAt: 't', tradeDate: '2026-08-07', meta: {} }],
    });
    render(<FunnelHistoryTable />);
    expect(screen.getByText(/暂无漏斗数据/)).toBeInTheDocument();
  });
});
