import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { MarketSentimentCard } from './MarketSentimentCard';

const GATE = {
  mode: 'HOLD_ONLY',
  allowNewEntries: false,
  marketRegime: 'Weak',
  indexLight: 'yellow',
  positionRangeHint: '0-20%',
  reasons: ['REASON_TEST'],
  satelliteNote: '卫星仓说明',
};

const MS = {
  items: [
    {
      date: '2026-08-06',
      riskMode: 'caution',
      yesterdayLimitUpPremium: 1.234,
      failedLimitUpRate: 12.34,
      marketTurnoverCny: 1_200_000_000_000,
      upDownRatio: 1.5,
      upCount: 3000,
      downCount: 4000,
      flatCount: 500,
      rules: ['rule1', 'rule2'],
    },
  ],
  srvIndex: {
    level: 'Elevated',
    labelZh: '中等偏高风险',
    overlapSectors: ['AI', '银行'],
  },
  executionGate: GATE,
};

describe('MarketSentimentCard', () => {
  it('renders gate badge with mode, regime, light and reasons', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: MS }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/A股闸门/)).toBeInTheDocument();
    expect(screen.getByText(/允许开仓=false/)).toBeInTheDocument();
    expect(screen.getByText(/卫星仓说明/)).toBeInTheDocument();
    expect(screen.queryByText(/港股闸门/)).not.toBeInTheDocument();
  });

  it('renders HK gate when present', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: { ...MS, executionGate: { ...GATE, hkGate: GATE } } }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/港股闸门/)).toBeInTheDocument();
  });

  it('renders risk mode translation and rules', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: MS }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/rule1 · rule2/)).toBeInTheDocument();
  });

  it('shows breadth panic warning when down count exceeds threshold', () => {
    render(
      <MarketSentimentCard
        dash={{
          marketSentiment: {
            items: [{ ...MS.items[0], downCount: 5000 }],
          },
        }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/触发红色预警/)).toBeInTheDocument();
  });

  it('renders ETF flow signal with verdicts', () => {
    const base = {
      dash: { marketSentiment: { ...MS, etfFlowSignal: { verdict: 'confirm', broadDirection: 'buy', sectorDirection: 'buy' } } },
      summary: {},
      sentimentBusy: false,
      onSyncSentiment: vi.fn(),
      toastSentimentCopy: vi.fn(),
      sentimentCopyStatus: null,
      addReference: vi.fn(),
    };
    const { rerender } = render(<MarketSentimentCard {...base} />);
    expect(screen.getByText('确认净流入')).toBeInTheDocument();
    expect(screen.getAllByText((_, el) => el?.textContent?.includes('国家队净买') ?? false).length).toBeGreaterThan(0);
    rerender(
      <MarketSentimentCard
        {...base}
        dash={{
          marketSentiment: {
            ...MS,
            etfFlowSignal: { verdict: 'contradict', broadDirection: 'outflow', sectorDirection: 'outflow', incomplete: true, asOfDate: '2026-08-07' },
          },
        }}
      />,
    );
    expect(screen.getByText('背离净流出')).toBeInTheDocument();
    expect(screen.getByText(/数据不完整/)).toBeInTheDocument();
  });

  it('shows capitulation alert for capitulation_v_bottom risk', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: { items: [{ ...MS.items[0], riskMode: 'capitulation_v_bottom' }] } }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getAllByText(/恐慌冰点共振/).length).toBeGreaterThan(0);
  });

  it('renders index signals with featured star and quote error', () => {
    render(
      <MarketSentimentCard
        dash={{
          marketSentiment: {
            ...MS,
            indexSignals: [
              { tsCode: 'idx1', name: '沪深300', signal: 'deep_green', positionRange: '50%', pctChg: 1.5, close: 3800.5, ma5: 3700, ma20: 3600, realtime: true, source: 'eastmoney', tradeTime: '14:30', featured: true, rules: ['r1'], quoteError: 'quota' },
            ],
          },
        }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/★/)).toBeInTheDocument();
    expect(screen.getByText(/实时/)).toBeInTheDocument();
    expect(screen.getByText(/行情回退: quota/)).toBeInTheDocument();
    expect(screen.getByText(/规则: r1/)).toBeInTheDocument();
  });

  it('renders last-5-days table and empty state', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: MS }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText('2026-08-06')).toBeInTheDocument();
    expect(screen.getAllByText('1.50').length).toBeGreaterThan(0);
  });

  it('fires sync, copy markdown (with failure fallback), and add reference', () => {
    const onSyncSentiment = vi.fn();
    const toastSentimentCopy = vi.fn();
    const addReference = vi.fn();
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: MS, asOfDate: '2026-08-07' }}
        summary={{}}
        sentimentBusy
        onSyncSentiment={onSyncSentiment}
        toastSentimentCopy={toastSentimentCopy}
        sentimentCopyStatus={null}
        addReference={addReference}
      />,
    );
    expect(screen.getByText('同步情绪')).toBeDisabled();
    fireEvent.click(screen.getByText('参考'));
    expect(addReference).toHaveBeenCalledWith(expect.objectContaining({ kind: 'marketSentiment', refId: '2026-08-07:5' }));
  });

  it('renders copy status color', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: MS }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={{ ok: false, text: '复制失败' }}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText('复制失败')).toHaveClass('text-red-600');
  });

  it('renders empty state when no items', () => {
    render(
      <MarketSentimentCard
        dash={{ marketSentiment: {} }}
        summary={{}}
        sentimentBusy={false}
        onSyncSentiment={vi.fn()}
        toastSentimentCopy={vi.fn()}
        sentimentCopyStatus={null}
        addReference={vi.fn()}
      />,
    );
    expect(screen.getByText(/暂无情绪数据/)).toBeInTheDocument();
  });
});
