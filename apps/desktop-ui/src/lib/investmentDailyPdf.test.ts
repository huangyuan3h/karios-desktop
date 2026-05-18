import { describe, expect, it } from 'vitest';
import {
  buildInvestmentDailyPdfLayout,
  INVESTMENT_DAILY_MARKDOWN_MAX_CHARS,
  parseInvestmentDailyReportResponse,
  translateIndexSignalForPdf,
  translateRiskModeForPdf,
  translateSentimentSnippetForPdf,
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

describe('translateIndexSignalForPdf / translateRiskModeForPdf', () => {
  it('maps index signals to Chinese labels', () => {
    expect(translateIndexSignalForPdf('deep_green')).toBe('深绿灯');
    expect(translateIndexSignalForPdf('deep-green')).toBe('深绿灯');
    expect(translateIndexSignalForPdf('green')).toBe('绿灯');
    expect(translateIndexSignalForPdf('YELLOW')).toBe('黄灯');
    expect(translateIndexSignalForPdf('red')).toBe('红灯');
  });

  it('maps risk modes to Chinese labels', () => {
    expect(translateRiskModeForPdf('hot')).toBe('过热');
    expect(translateRiskModeForPdf('caution')).toBe('谨慎');
    expect(translateRiskModeForPdf('euphoric')).toBe('狂热');
    expect(translateRiskModeForPdf('normal')).toBe('常态');
  });

  it('translates mixed English tokens in rule snippets', () => {
    expect(translateSentimentSnippetForPdf('mode=caution and hot')).toContain('谨慎');
    expect(translateSentimentSnippetForPdf('mode=caution and hot')).toContain('过热');
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

  it('macro table has no 来源 column and six cells per row', () => {
    const report = parseInvestmentDailyReportResponse(valid);
    const layout = buildInvestmentDailyPdfLayout({
      report,
      subtitleTimeZh: '2026-05-06 12:00',
      summary: {
        asOfDate: '2026-05-05',
        macroSnapshot: {
          macro: [
            {
              name: '测试指数',
              seriesId: 'TEST',
              close: 100,
              pctChg: 0.5,
              ma5: 99,
              ma20: 98,
              asOfDate: '2026-05-05',
              source: 'SHOULD_NOT_APPEAR',
            },
          ],
        },
      },
      hotIndustryPicks: [],
    });
    expect(layout.macroTable?.title).toBe('宏观与外盘');
    expect(layout.macroTable?.headers).toEqual(['名称', '收盘', '涨跌%', 'MA5', 'MA20', '日期']);
    expect(layout.macroTable?.rows[0]).toHaveLength(6);
    expect(layout.macroTable?.rows[0]?.join('')).not.toContain('SHOULD_NOT_APPEAR');
  });

  it('daily sentiment table uses 风险评价 header and Chinese risk cell', () => {
    const report = parseInvestmentDailyReportResponse(valid);
    const layout = buildInvestmentDailyPdfLayout({
      report,
      subtitleTimeZh: 't',
      summary: {
        asOfDate: '2026-01-01',
        marketSentiment: {
          items: [
            {
              date: '2026-01-01',
              upDownRatio: 1.1,
              marketTurnoverCny: 1e10,
              yesterdayLimitUpPremium: 1.2,
              failedLimitUpRate: 10,
              riskMode: 'caution',
              rules: ['turnover below median -> hot'],
            },
          ],
        },
      },
      hotIndustryPicks: [],
    });
    expect(layout.sentimentDailyTable?.headers?.[5]).toBe('风险评价');
    expect(layout.sentimentDailyTable?.rows[0]?.[5]).toBe('谨慎');
    const rulesLine = layout.sentimentRuleLines.find((l) => l.startsWith('系统规则摘要'));
    expect(rulesLine).toBeDefined();
    expect(rulesLine).not.toMatch(/\bhot\b/i);
  });
});
