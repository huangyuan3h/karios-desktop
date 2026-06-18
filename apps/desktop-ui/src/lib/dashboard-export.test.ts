import { describe, expect, it } from 'vitest';

import { buildIndustryMarkdown, buildSentimentMarkdown } from './dashboard-export';

describe('buildIndustryMarkdown', () => {
  it('renders industry fund flow sections from fixture', () => {
    const summary = {
      asOfDate: '2026-06-18',
      industryFundFlow: {
        asOfDate: '2026-06-18',
        dates: ['2026-06-14', '2026-06-15', '2026-06-16', '2026-06-17', '2026-06-18'],
        topByDate: [
          { date: '2026-06-18', top: ['Semiconductors', 'AI', 'EV', 'Pharma', 'Banking'] },
        ],
        flow5d: {
          dates: ['2026-06-14', '2026-06-15', '2026-06-16', '2026-06-17', '2026-06-18'],
          top: [
            {
              industryName: 'Semiconductors',
              sum5d: 3_000_000_000,
              series: [{ date: '2026-06-18', netInflow: 500_000_000 }],
            },
          ],
        },
      },
    };

    const md = buildIndustryMarkdown(summary);

    expect(md).toContain('## Industry fund flow');
    expect(md).toContain('- asOfDate: 2026-06-18');
    expect(md).toContain('## Top5×Date hotspots (names only)');
    expect(md).toContain('Semiconductors');
    expect(md).toContain('## 5D net inflow (Top by 5D sum)');
    expect(md).toContain('30.00亿');
    expect(md).toContain('5.00亿');
  });

  it('returns minimal markdown for null summary', () => {
    const md = buildIndustryMarkdown(null);
    expect(md).toContain('## Industry fund flow');
    expect(md).not.toContain('Top5×Date');
  });
});

describe('buildSentimentMarkdown', () => {
  it('renders market sentiment and index signals from fixture', () => {
    const summary = {
      asOfDate: '2026-06-18',
      marketEnvironmentZh: '市场震荡，控制仓位。',
      marketSentiment: {
        asOfDate: '2026-06-18',
        indexSignals: [
          {
            name: '上证指数',
            signal: 'yellow',
            positionRange: 'mid',
            pctChg: 0.35,
            close: 3200.5,
            ma5: 3180.2,
            ma20: 3150.1,
            asOfDate: '2026-06-18',
          },
        ],
        items: [
          {
            date: '2026-06-18',
            upCount: 2500,
            downCount: 1800,
            flatCount: 200,
            upDownRatio: 1.39,
            marketTurnoverCny: 800_000_000_000,
            yesterdayLimitUpPremium: 2.5,
            failedLimitUpRate: 15.2,
            riskMode: 'neutral',
            rules: ['rule-a', 'rule-b'],
          },
        ],
      },
    };

    const md = buildSentimentMarkdown(summary);

    expect(md).toContain('## 市场环境摘要');
    expect(md).toContain('市场震荡，控制仓位。');
    expect(md).toContain('## Market sentiment');
    expect(md).toContain('- risk: neutral');
    expect(md).toContain('## Index traffic lights');
    expect(md).toContain('上证指数');
    expect(md).toContain('+0.35%');
    expect(md).toContain('| date | up | down |');
  });

  it('omits environment section when absent', () => {
    const md = buildSentimentMarkdown({ marketSentiment: { items: [] } });
    expect(md).not.toContain('市场环境摘要');
    expect(md).toContain('## Market sentiment');
  });
});
