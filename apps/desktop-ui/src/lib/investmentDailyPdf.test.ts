import { describe, expect, it } from 'vitest';
import {
  buildInvestmentDailyPdfLayout,
  INVESTMENT_DAILY_MARKDOWN_MAX_CHARS,
  parseInvestmentDailyReportResponse,
  truncateMarkdownForReport,
} from './investmentDailyPdf';

describe('truncateMarkdownForReport', () => {
  it('returns unchanged when under limit', () => {
    expect(truncateMarkdownForReport('hello')).toBe('hello');
  });

  it('truncates and appends notice when over limit', () => {
    const long = 'a'.repeat(INVESTMENT_DAILY_MARKDOWN_MAX_CHARS + 50);
    const out = truncateMarkdownForReport(long);
    expect(out).toContain('[Truncated:');
    expect(out.startsWith('a'.repeat(INVESTMENT_DAILY_MARKDOWN_MAX_CHARS))).toBe(true);
    expect(out.length).toBeGreaterThan(INVESTMENT_DAILY_MARKDOWN_MAX_CHARS);
  });
});

describe('parseInvestmentDailyReportResponse', () => {
  const valid = {
    trafficLightPositionAndSentiment: 's1',
    marketEnvironmentHighlights: 'mh',
    hotIndustriesFormalAnalysis: 'hi',
    capitalFlowAndMainline: 's2',
    topStocks: Array.from({ length: 3 }, (_, i) => ({
      symbol: `S${i}`,
      name: `Name${i}`,
      rationale: `r${i}`,
    })),
    topNews: Array.from({ length: 5 }, (_, i) => ({
      title: `t${i}`,
      summary: `n${i}`,
    })),
  };

  it('parses valid payload', () => {
    const r = parseInvestmentDailyReportResponse(valid);
    expect(r.trafficLightPositionAndSentiment).toBe('s1');
    expect(r.marketEnvironmentHighlights).toBe('mh');
    expect(r.hotIndustriesFormalAnalysis).toBe('hi');
    expect(r.topStocks).toHaveLength(3);
    expect(r.topStocks[0]?.name).toBe('Name0');
    expect(r.topNews).toHaveLength(5);
  });

  it('throws when topStocks length is wrong', () => {
    expect(() =>
      parseInvestmentDailyReportResponse({
        ...valid,
        topStocks: valid.topStocks.slice(0, 2),
      }),
    ).toThrow(/topStocks/);
  });
});

describe('buildInvestmentDailyPdfLayout', () => {
  const valid = {
    trafficLightPositionAndSentiment: 's1',
    marketEnvironmentHighlights: 'mh',
    hotIndustriesFormalAnalysis: 'hi',
    capitalFlowAndMainline: 's2',
    topStocks: Array.from({ length: 3 }, (_, i) => ({
      symbol: `S${i}`,
      name: `Name${i}`,
      rationale: `r${i}`,
    })),
    topNews: Array.from({ length: 5 }, (_, i) => ({
      title: `t${i}`,
      summary: `n${i}`,
    })),
  };

  it('fills layout and always includes hot picks table rows', () => {
    const report = parseInvestmentDailyReportResponse(valid);
    const layout = buildInvestmentDailyPdfLayout({
      report,
      subtitleTimeZh: '2026-05-06 12:00',
      summary: { asOfDate: '2026-05-05', marketEnvironmentZh: '市场环境原文' },
      hotIndustryPicks: [],
    });
    expect(layout.asOfDate).toBe('2026-05-05');
    expect(layout.envZh).toBe('市场环境原文');
    expect(layout.hotPicksTable.rows.length).toBeGreaterThanOrEqual(1);
    expect(layout.sentimentStaticNotes.length).toBeGreaterThan(10);
  });
});
