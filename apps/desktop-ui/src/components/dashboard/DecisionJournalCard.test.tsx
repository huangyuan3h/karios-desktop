import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import { DecisionJournalCard } from './DecisionJournalCard';

const { useWatchlistMarketQuery, useExecutionChangesQuery, useExecutionSnapshotsQuery } = vi.hoisted(() => ({
  useWatchlistMarketQuery: vi.fn(),
  useExecutionChangesQuery: vi.fn(),
  useExecutionSnapshotsQuery: vi.fn(),
}));
vi.mock('@/lib/queries/watchlist', () => ({ useWatchlistMarketQuery }));
vi.mock('@/lib/queries/execution-journal', () => ({ useExecutionChangesQuery, useExecutionSnapshotsQuery }));

const GATE = {
  mode: 'ATTACK',
  allowNewEntries: true,
  marketRegime: 'confirmed_uptrend',
  indexLight: 'deep_green',
  positionRangeHint: '50-100%',
  reasons: [],
} as unknown as ExecutionGate;

const SNAPSHOT = {
  id: 'snap-1',
  capturedAt: '2026-08-07T10:00:00+08:00',
  source: 'manual',
  gate: GATE,
  cards: [
    { symbol: 'CN:600519', action: 'BUY', why: 'REASON_TEST', positionPct: 12.3 },
    { symbol: 'CN:000858', action: 'WATCH', why: null },
  ],
};

const CHANGE = {
  id: 'c1',
  changedAt: '2026-08-07T10:05:00+08:00',
  scope: 'symbol',
  symbol: 'CN:600519',
  field: 'action',
  oldValue: 'WATCH',
  newValue: 'BUY',
};

describe('DecisionJournalCard', () => {
  const baseMocks = () => {
    useWatchlistMarketQuery.mockReturnValue({ isLoading: false, isFetching: false, data: null });
    useExecutionChangesQuery.mockReturnValue({ isLoading: false, data: { items: [] } });
    useExecutionSnapshotsQuery.mockReturnValue({ isLoading: false, data: { items: [] } });
  };

  it('shows Gate unavailable when no gate', () => {
    baseMocks();
    render(<DecisionJournalCard gate={null} />);
    expect(screen.getByText('Gate 不可用')).toBeInTheDocument();
    expect(screen.getByText(/今日暂无快照/)).toBeInTheDocument();
    expect(screen.getByText(/今日暂无决策变更记录/)).toBeInTheDocument();
  });

  it('renders live gate badge and mode change hint', () => {
    baseMocks();
    useExecutionSnapshotsQuery.mockReturnValue({
      isLoading: false,
      data: { items: [{ ...SNAPSHOT, gate: { mode: 'DEFEND' } }] },
    });
    render(<DecisionJournalCard gate={GATE} />);
    expect(screen.getByText(/实时 Gate: ATTACK/)).toBeInTheDocument();
    expect(screen.getByText(/vs snapshot DEFEND/)).toBeInTheDocument();
  });

  it('shows empty attention when no watchlist items', () => {
    baseMocks();
    render(<DecisionJournalCard gate={GATE} />);
    expect(screen.getAllByText('无').length).toBeGreaterThan(0);
  });

  it('renders snapshot card rows and latest action table', () => {
    baseMocks();
    useExecutionSnapshotsQuery.mockReturnValue({ isLoading: false, data: { items: [SNAPSHOT] } });
    useExecutionChangesQuery.mockReturnValue({ isLoading: false, data: { items: [CHANGE] } });
    render(<DecisionJournalCard gate={GATE} />);
    expect(screen.getByText(/最新快照: /)).toBeInTheDocument();
    expect(screen.getByText(/2 cards/)).toBeInTheDocument();
    expect(screen.getAllByText('CN:600519').length).toBeGreaterThan(0);
    expect(screen.getByText('12.3')).toBeInTheDocument();
  });

  it('renders decision changes list', () => {
    baseMocks();
    useExecutionChangesQuery.mockReturnValue({ isLoading: false, data: { items: [CHANGE] } });
    render(<DecisionJournalCard gate={GATE} />);
    expect(screen.getByText(/今日变更/)).toBeInTheDocument();
    expect(screen.getAllByText(/CN:600519/).length).toBeGreaterThan(0);
  });

  it('shows loading placeholders while queries load', () => {
    useWatchlistMarketQuery.mockReturnValue({ isLoading: true, isFetching: true, data: null });
    useExecutionChangesQuery.mockReturnValue({ isLoading: true, data: undefined });
    useExecutionSnapshotsQuery.mockReturnValue({ isLoading: false, data: { items: [] } });
    render(<DecisionJournalCard gate={null} />);
    expect(screen.getAllByText(/加载/).length).toBeGreaterThan(0);
  });

  it('disables snapshot button while capture busy and fires onSnapshotNow', () => {
    baseMocks();
    const onSnapshotNow = vi.fn();
    const { rerender } = render(<DecisionJournalCard gate={GATE} captureBusy onSnapshotNow={onSnapshotNow} />);
    expect(screen.getByText('保存中…')).toBeDisabled();
    rerender(<DecisionJournalCard gate={GATE} onSnapshotNow={onSnapshotNow} />);
    fireEvent.click(screen.getByText('立即快照'));
    expect(onSnapshotNow).toHaveBeenCalled();
  });

  it('navigates to watchlist via 打开持仓 button', () => {
    baseMocks();
    const onNavigate = vi.fn();
    render(<DecisionJournalCard gate={GATE} onNavigate={onNavigate} />);
    fireEvent.click(screen.getByText('打开持仓'));
    expect(onNavigate).toHaveBeenCalledWith('watchlist');
  });
});
