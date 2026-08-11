import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type { MorningBrief } from '@/lib/queries/news';

import { TradingBriefCard } from './TradingBriefCard';

const { useTradingBriefQuery } = vi.hoisted(() => ({ useTradingBriefQuery: vi.fn() }));
vi.mock('@/lib/queries/news', () => ({
  useTradingBriefQuery,
  TRADING_BRIEF_TYPES: ['open', 'midday', 'action'] as const,
}));

const brief = (partial: Partial<MorningBrief>): MorningBrief => ({
  id: '2026-08-11-trading-action',
  briefDate: '2026-08-11',
  briefType: 'trading-action',
  items: [],
  macroOverview: null,
  modelVersion: 'trading-brief-v1',
  sourceItemIds: null,
  markdown: '**Regime**\n- A股: Weak（强度 22.25）· panic 冷却\n\n**S-3 候选：无**',
  createdAt: '2026-08-11T06:30:00+00:00',
  ...partial,
});

const mockHook = (over: Partial<ReturnType<typeof useTradingBriefQuery>> = {}) => {
  vi.mocked(useTradingBriefQuery).mockReturnValue({
    data: { brief: brief({}) },
    isLoading: false,
    refetch: vi.fn(),
    ...over,
  } as never);
};

describe('TradingBriefCard', () => {
  it('renders three session tabs with action default', () => {
    mockHook();
    render(<TradingBriefCard />);
    expect(screen.getByText('今日操作简报')).toBeTruthy();
    expect(screen.getByText('开盘简报')).toBeTruthy();
    expect(screen.getByText('午间简报')).toBeTruthy();
    expect(screen.getByText('操作卡')).toBeTruthy();
    expect(useTradingBriefQuery).toHaveBeenCalledWith('action');
  });

  it('renders markdown content once loaded', async () => {
    mockHook();
    render(<TradingBriefCard />);
    await waitFor(() => {
      expect(screen.getByText('Regime')).toBeTruthy();
    });
    expect(screen.getByText(/A股: Weak/)).toBeTruthy();
  });

  it('switches session and refetches that brief type', () => {
    mockHook();
    render(<TradingBriefCard />);
    fireEvent.click(screen.getByText('开盘简报'));
    expect(useTradingBriefQuery).toHaveBeenCalledWith('open');
  });

  it('shows empty state when no brief yet', () => {
    mockHook({ data: { brief: null } });
    render(<TradingBriefCard />);
    expect(screen.getByText(/暂无操作卡/)).toBeTruthy();
  });

  it('shows loading state', () => {
    mockHook({ data: undefined, isLoading: true });
    render(<TradingBriefCard />);
    expect(screen.getByText('加载中…')).toBeTruthy();
  });
});
