import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import type { BriefItem, MorningBrief } from '@/lib/queries/news';

import { MorningBriefCard } from './MorningBriefCard';

const { useMorningBriefQuery } = vi.hoisted(() => ({ useMorningBriefQuery: vi.fn() }));
vi.mock('@/lib/queries/news', () => ({ useMorningBriefQuery }));

const item = (partial: Partial<BriefItem>): BriefItem => ({
  id: 'n1',
  title: '标题A',
  sourceId: null,
  publishedAt: null,
  tickers: [],
  sectors: [],
  eventType: null,
  importance: null,
  relevanceScore: null,
  aiSummary: null,
  actionability: null,
  link: null,
  score: 50,
  category: 'macro',
  ...partial,
});

const brief = (partial: Partial<MorningBrief>): MorningBrief => ({
  id: 'b1',
  briefDate: '2026-08-07',
  briefType: 'morning',
  items: [],
  macroOverview: null,
  modelVersion: null,
  sourceItemIds: null,
  createdAt: '2026-08-07T08:30:00+08:00',
  ...partial,
});

describe('MorningBriefCard', () => {
  it('shows loading state while pending', () => {
    useMorningBriefQuery.mockReturnValue({ isPending: true, data: undefined });
    render(<MorningBriefCard />);
    expect(screen.getByText('加载简报中…')).toBeInTheDocument();
  });

  it('shows empty state when brief is null', () => {
    useMorningBriefQuery.mockReturnValue({ isPending: false, data: { brief: null } });
    render(<MorningBriefCard />);
    expect(screen.getByText(/No brief yet/)).toBeInTheDocument();
  });

  it('groups items by category with counts and morning label', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: {
        brief: brief({
          items: [
            item({ id: 'w1', category: 'watchlist', title: '持仓项' }),
            item({ id: 'w2', category: 'watchlist', title: '持仓项2' }),
            item({ id: 'r1', category: 'risk', title: '风险项' }),
          ],
        }),
      },
    });
    render(<MorningBriefCard />);
    expect(screen.getByText(/早盘/)).toBeInTheDocument();
    expect(screen.getByText('持仓相关')).toBeInTheDocument();
    expect(screen.getByText('(2)')).toBeInTheDocument();
    expect(screen.getByText('风险提醒')).toBeInTheDocument();
    expect(screen.getByText('持仓项')).toBeInTheDocument();
    expect(screen.getByText('风险项')).toBeInTheDocument();
    expect(screen.queryByText('板块/宏观')).not.toBeInTheDocument();
  });

  it('shows midday label for midday briefs', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: { brief: brief({ briefType: 'midday' }) },
    });
    render(<MorningBriefCard />);
    expect(screen.getByText(/午间/)).toBeInTheDocument();
  });

  it('funnels missing categories into macro group', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: {
        brief: brief({
          items: [{ ...item({ id: 'u1', title: '未知项' }), category: undefined } as never],
        }),
      },
    });
    render(<MorningBriefCard />);
    expect(screen.getByText(/板块\/宏观/)).toBeInTheDocument();
    expect(screen.getByText('未知项')).toBeInTheDocument();
  });

  it('drops items with unknown non-union categories', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: {
        brief: brief({
          items: [{ ...item({ id: 'u1', title: '未知项' }), category: 'bogus' } as never],
        }),
      },
    });
    render(<MorningBriefCard />);
    expect(screen.queryByText('未知项')).not.toBeInTheDocument();
  });

  it('renders tickers and importance/relevance badges when thresholds met', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: {
        brief: brief({
          items: [
            item({
              tickers: ['CN:600519', 'CN:000858', 'CN:000001', 'CN:000002'],
              importance: 4,
              relevanceScore: 72,
            }),
          ],
        }),
      },
    });
    render(<MorningBriefCard />);
    expect(screen.getByText('CN:600519, CN:000858, CN:000001')).toBeInTheDocument();
    expect(screen.getByText('I4')).toBeInTheDocument();
    expect(screen.getByText('R72')).toBeInTheDocument();
  });

  it('omits badges below thresholds', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: {
        brief: brief({ items: [item({ importance: 2, relevanceScore: 30 })] }),
      },
    });
    render(<MorningBriefCard />);
    expect(screen.queryByText(/I\d/)).not.toBeInTheDocument();
    expect(screen.queryByText(/R\d/)).not.toBeInTheDocument();
  });

  it('renders AI summary states: busy, present, missing', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: { brief: brief({ items: [] }) },
    });
    const { rerender } = render(<MorningBriefCard newsSummaryBusy newsSummary={null} />);
    expect(screen.getByText(/生成AI摘要中…/)).toBeInTheDocument();
    expect(screen.getByText('生成中…')).toBeInTheDocument();

    rerender(<MorningBriefCard newsSummary="  摘要内容  " />);
    expect(screen.getByText('摘要内容')).toBeInTheDocument();
    expect(screen.queryByText(/生成AI摘要中/)).not.toBeInTheDocument();

    rerender(<MorningBriefCard />);
    expect(screen.getByText(/暂无摘要/)).toBeInTheDocument();
  });

  it('calls navigation and regenerate callbacks; disables regenerate while busy', () => {
    useMorningBriefQuery.mockReturnValue({
      isPending: false,
      data: { brief: brief({ items: [] }) },
    });
    const onNavigate = vi.fn();
    const onRegenerateNews = vi.fn();
    const { rerender } = render(
      <MorningBriefCard onNavigate={onNavigate} onRegenerateNews={onRegenerateNews} />,
    );
    fireEvent.click(screen.getByText('打开新闻'));
    expect(onNavigate).toHaveBeenCalledWith('news');
    fireEvent.click(screen.getByText('重新生成'));
    expect(onRegenerateNews).toHaveBeenCalled();

    rerender(
      <MorningBriefCard newsSummaryBusy onRegenerateNews={onRegenerateNews} />,
    );
    expect(screen.getByText('重新生成')).toBeDisabled();
  });
});
