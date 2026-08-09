import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { IndustryFundFlowCard } from './IndustryFundFlowCard';

const SUMMARY = {
  asOfDate: '2026-08-07',
  industryFundFlow: {
    dates: ['2026-08-03', '2026-08-04', '2026-08-05', '2026-08-06', '2026-08-07'],
    topByDate: [
      { date: '2026-08-03', top: ['AI', '半导体'] },
      { date: '2026-08-04', top: ['AI', '半导体'] },
      { date: '2026-08-05', top: ['AI', '半导体'] },
      { date: '2026-08-06', top: ['AI', '半导体'] },
      { date: '2026-08-07', top: ['AI', '半导体'] },
    ],
    flow5d: {
      dates: ['2026-08-03', '2026-08-04', '2026-08-05', '2026-08-06', '2026-08-07'],
      top: [
        {
          industryCode: 'BK1',
          industryName: '人工智能',
          sum5d: 1_500_000_000,
          series: [
            { date: '2026-08-07', netInflow: 300_000_000 },
            { date: '2026-08-06', netInflow: 200_000_000 },
          ],
        },
      ],
    },
  },
};

const onAddReference = vi.fn();

describe('IndustryFundFlowCard', () => {
  it('renders the top-5-by-date matrix with deduped dates', () => {
    render(
      <IndustryFundFlowCard
        summary={SUMMARY}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={null}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.getAllByText('08-03').length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText('AI')).toBeInTheDocument();
    expect(screen.getByText('半导体')).toBeInTheDocument();
  });

  it('reports collapsed duplicate non-trading snapshots', () => {
    const sameSig = SUMMARY.industryFundFlow.topByDate.map((t) => ({ ...t, top: ['X'] }));
    render(
      <IndustryFundFlowCard
        summary={{ industryFundFlow: { dates: SUMMARY.industryFundFlow.dates, topByDate: sameSig } }}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={null}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.getByText(/collapsed 4 duplicate non-trading snapshot/)).toBeInTheDocument();
  });

  it('renders 5D inflow table with industry names and sums', () => {
    render(
      <IndustryFundFlowCard
        summary={SUMMARY}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={null}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.getByText('5D net inflow (Top by 5D sum)')).toBeInTheDocument();
    expect(screen.getByText('人工智能')).toBeInTheDocument();
    expect(screen.getByText('15.00亿')).toBeInTheDocument();
    expect(screen.getByText('3.00亿')).toBeInTheDocument();
  });

  it('renders empty state when summary is empty', () => {
    render(
      <IndustryFundFlowCard
        summary={{}}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={null}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.queryByText('5D net inflow (Top by 5D sum)')).not.toBeInTheDocument();
  });

  it('navigates, copies markdown, and adds reference on button clicks', () => {
    const onNavigate = vi.fn();
    const onCopyIndustryMarkdown = vi.fn();
    render(
      <IndustryFundFlowCard
        summary={SUMMARY}
        hotIndustryPicks={[]}
        onNavigate={onNavigate}
        onAddReference={onAddReference}
        copyStatus={null}
        onCopyIndustryMarkdown={onCopyIndustryMarkdown}
      />,
    );
    fireEvent.click(screen.getByText('打开行业资金流'));
    expect(onNavigate).toHaveBeenCalledWith('industryFlow');
    fireEvent.click(screen.getByText('复制Markdown'));
    expect(onCopyIndustryMarkdown).toHaveBeenCalled();
    fireEvent.click(screen.getByText('参考'));
    expect(onAddReference).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'industryFundFlow', refId: '2026-08-07:5:10' }),
    );
  });

  it('renders copyStatus in ok/error colors', () => {
    const { rerender } = render(
      <IndustryFundFlowCard
        summary={SUMMARY}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={{ ok: true, text: 'OK' }}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.getByText('OK')).toHaveClass('text-emerald-600');
    rerender(
      <IndustryFundFlowCard
        summary={SUMMARY}
        hotIndustryPicks={[]}
        onAddReference={onAddReference}
        copyStatus={{ ok: false, text: 'NO' }}
        onCopyIndustryMarkdown={() => undefined}
      />,
    );
    expect(screen.getByText('NO')).toHaveClass('text-red-600');
  });
});
