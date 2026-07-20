import { describe, expect, it } from 'vitest';

import { parseInvestmentDailyReportAfterNormalize } from './investmentDailyReportNormalize';

describe('normalizeInvestmentDailyReportPayload', () => {
  it('pads stocks and news to required lengths', () => {
    const raw = {
      trafficLightPositionAndSentiment: 'a',
      marketEnvironmentHighlights: 'b',
      hotIndustriesFormalAnalysis: 'c',
      capitalFlowAndMainline: 'd',
      topStocks: [{ symbol: 'X', name: 'Y', rationale: 'z' }],
      topNews: [{ title: 't', summary: 's' }],
    };
    const out = parseInvestmentDailyReportAfterNormalize(raw);
    expect(out.success).toBe(true);
    if (!out.success) return;
    expect(out.data.topStocks).toHaveLength(3);
    expect(out.data.topNews).toHaveLength(5);
    expect(out.data.topStocks[1]?.symbol).toBe('—');
  });

  it('accepts snake_case keys', () => {
    const raw = {
      traffic_light_position_and_sentiment: 't',
      market_environment_highlights: 'm',
      hot_industries_formal_analysis: 'h',
      capital_flow_and_mainline: 'c',
      top_stocks: [
        { symbol: '1', name: 'n1', rationale: 'r1' },
        { symbol: '2', name: 'n2', rationale: 'r2' },
        { symbol: '3', name: 'n3', rationale: 'r3' },
      ],
      top_news: Array.from({ length: 5 }, (_, i) => ({
        title: `T${i}`,
        summary: `S${i}`,
      })),
    };
    const out = parseInvestmentDailyReportAfterNormalize(raw);
    expect(out.success).toBe(true);
  });

  it('replaces empty strings with fallbacks', () => {
    const out = parseInvestmentDailyReportAfterNormalize({
      trafficLightPositionAndSentiment: '   ',
      marketEnvironmentHighlights: '',
      hotIndustriesFormalAnalysis: 'x',
      capitalFlowAndMainline: 'y',
      topStocks: [],
      topNews: [],
    });
    expect(out.success).toBe(true);
    if (!out.success) return;
    expect(out.data.trafficLightPositionAndSentiment.length).toBeGreaterThan(5);
    expect(out.data.marketEnvironmentHighlights).toContain('依据');
  });
});
