import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { NationalTeamCard } from './NationalTeamCard';

const flowMock = vi.hoisted(() => ({ rows: [] as unknown[], isError: false, isFetching: false }));
vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useFundFlowSeriesQuery: () => ({
      data: { ok: true, start: '', end: '', rows: flowMock.rows },
      isError: flowMock.isError,
      isFetching: flowMock.isFetching,
      refetch: vi.fn(),
    }),
  };
});

const ROWS = [
  {
    date: '2026-11-05',
    etfShareYi: 1000,
    etfShareD20Pct: 1.2,
    gateOn: false,
    marginTrillion: 2.2,
    marginD20Pct: 0,
    northDailyYi: null,
    northD20Yi: 12,
    smNetPct: 1,
    natDailyYi: 2,
    natD20Yi: 30,
    marginDailyYi: null,
    marginD20Yi: 10,
  },
  {
    date: '2026-11-06',
    etfShareYi: 1022,
    etfShareD20Pct: 2.4,
    gateOn: false,
    marginTrillion: 2.27,
    marginD20Pct: 0,
    northDailyYi: 1,
    northD20Yi: 22,
    smNetPct: 2,
    natDailyYi: 3,
    natD20Yi: 44,
    marginDailyYi: null,
    marginD20Yi: 12,
  },
];

beforeEach(() => {
  flowMock.rows = ROWS;
  flowMock.isError = false;
});

describe('NationalTeamCard', () => {
  it('surfaces national-team context on Dashboard', () => {
    render(<NationalTeamCard />);
    expect(screen.getByTestId('national-team-card')).toBeDefined();
    expect(screen.getByText(/国家队 · 宽基 ETF 份额/)).toBeDefined();
    expect(screen.getByText('国家队净买')).toBeDefined();
    expect(screen.getByText(/1022 亿份/)).toBeDefined();
  });

  it('shows the B-gate retreat state when gateOn is true', () => {
    flowMock.rows = ROWS.map((r) => ({ ...r, gateOn: true }));
    render(<NationalTeamCard />);
    expect(screen.getByText(/国家队撤退 · B 闸 ON/)).toBeDefined();
  });

  it('opens the on-page full panel when the dashboard supplies a handler', () => {
    const onOpenPanel = vi.fn();
    render(<NationalTeamCard onOpenPanel={onOpenPanel} />);
    const btn = screen.getByRole('button', { name: /看全图（本页资金流全景）/ });
    btn.click();
    expect(onOpenPanel).toHaveBeenCalledTimes(1);
  });
});
