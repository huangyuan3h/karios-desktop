import { describe, expect, it } from 'vitest';

import {
  buildAlphaRadarTrendsMarkdown,
  buildCatalystStocksMarkdown,
  filterRecentArticles,
  formatCatalystStockSummaryLine,
  formatStructuredTrendJson,
  shouldShowCatalystNews,
  type CatalystArticle,
  type CatalystCopyContext,
  type CatalystStock,
} from './alpha-radar-catalyst';

const sampleArticle = (
  overrides: Partial<CatalystArticle> = {},
): CatalystArticle => ({
  trendId: 't1',
  trendName: 'Datacenter cooling',
  macroTheme: 'Optical Supercycle',
  catalystGrade: 'S',
  catalyst: 'Hyperscaler liquid-cooling demand rises.',
  globalTarget: 'NVDA',
  documentId: 'd1',
  relevance: 0.8,
  contribution: 0.5,
  documentTitle: 'The Download',
  documentUrl: 'https://example.com/download',
  summary: 'Raw RSS paragraph from MIT Download.',
  publishedAt: '2026-06-14T12:00:00+00:00',
  urgencyLevel: 'S',
  ...overrides,
});

const sampleStock = (overrides: Partial<CatalystStock> = {}): CatalystStock => ({
  symbol: '300308',
  name: '中际旭创',
  catalystScore: 78,
  articleCount: 1,
  latestArticleAt: '2026-06-14T12:00:00+00:00',
  articles: [sampleArticle()],
  ...overrides,
});

function makeContext(overrides: Partial<CatalystCopyContext> = {}): CatalystCopyContext {
  return {
    watchlistSymbols: new Set<string>(),
    watchlistScores: new Map<string, number>(),
    screenerTrendOkSymbols: new Set<string>(),
    trendMap: new Map(),
    ...overrides,
  };
}

describe('formatStructuredTrendJson', () => {
  it('emits Macro_Theme and Catalyst_Grade keys', () => {
    expect(
      formatStructuredTrendJson({
        macroTheme: 'Next-Gen Energy',
        catalystGrade: 'S',
      }),
    ).toBe('{"Macro_Theme":"Next-Gen Energy","Catalyst_Grade":"S"}');
  });
});

describe('formatCatalystStockSummaryLine', () => {
  it('formats one-line stock summary with max grade theme', () => {
    expect(formatCatalystStockSummaryLine(sampleStock())).toBe(
      'CN:300308 中际旭创 | Score: 78.0 | Max Grade: S (Optical Supercycle)',
    );
  });
});

describe('buildAlphaRadarTrendsMarkdown', () => {
  const trend = {
    id: 't1',
    trendName: 'Next-Gen Energy',
    macroTheme: 'Next-Gen Energy',
    catalystGrade: 'S',
    catalyst: 'Grid storage orders accelerate.',
    globalTarget: 'N/A',
    keywordsForMapping: ['液冷散热', '储能'],
    documentTitle: 'Stratechery headline',
    documentUrl: 'https://example.com/post',
    riskStatus: 'waiting_v2_flow',
    cnSymbols: [
      {
        symbol: 'CN:002837',
        name: '英维克',
        confidence: 0.72,
        rationale: '液冷龙头',
      },
    ],
  };

  it('includes structured JSON and catalyst text in full mode', () => {
    const md = buildAlphaRadarTrendsMarkdown([trend]);

    expect(md).toContain('## Alpha Radar · Structured Trends');
    expect(md).toContain('{"Macro_Theme":"Next-Gen Energy","Catalyst_Grade":"S"}');
    expect(md).toContain('- catalyst: Grid storage orders accelerate.');
    expect(md).toContain('英维克');
  });

  it('omits detail blocks in compact mode', () => {
    const md = buildAlphaRadarTrendsMarkdown([trend], { mode: 'compact' });

    expect(md).toContain('| Macro Theme | Catalyst Grade | Global Target | A-share Mapping |');
    expect(md).not.toContain('### Next-Gen Energy');
    expect(md).not.toContain('{"Macro_Theme":"Next-Gen Energy","Catalyst_Grade":"S"}');
    expect(md).not.toContain('- url: https://example.com/post');
  });
});

describe('buildCatalystStocksMarkdown', () => {
  it('prefers structured fields over raw source summary in full mode', () => {
    const md = buildCatalystStocksMarkdown(
      {
        stalenessBasis: 'published_then_fetched',
        maxAgeDays: 30,
        total: 1,
        items: [
          {
            symbol: 'CN:002837',
            name: '英维克',
            catalystScore: 44,
            articleCount: 1,
            latestArticleAt: '2026-05-28T00:00:00+00:00',
            articles: [
              {
                trendId: 't1',
                trendName: 'Datacenter cooling',
                macroTheme: 'Next-Gen Energy',
                catalystGrade: 'S',
                catalyst: 'Hyperscaler liquid-cooling demand rises.',
                globalTarget: 'NVDA',
                documentId: 'd1',
                relevance: 0.8,
                contribution: 0.5,
                documentTitle: 'The Download',
                documentUrl: 'https://example.com/download',
                summary: 'Raw RSS paragraph from MIT Download.',
                publishedAt: '2026-05-28T00:00:00+00:00',
                urgencyLevel: 'S',
              },
            ],
          },
        ],
      },
      { includeDetails: true },
    );

    expect(md).toContain('{"Macro_Theme":"Next-Gen Energy","Catalyst_Grade":"S"}');
    expect(md).toContain('- catalyst: Hyperscaler liquid-cooling demand rises.');
    expect(md).not.toContain('sourceSummary: Raw RSS paragraph');
  });

  it('renders compact summary without verbose fields', () => {
    const md = buildCatalystStocksMarkdown(
      {
        stalenessBasis: 'published_then_fetched',
        maxAgeDays: 30,
        total: 1,
        items: [sampleStock()],
      },
      { mode: 'compact' },
    );

    expect(md).toContain('CN:300308 中际旭创 | Score: 78.0 | Max Grade: S (Optical Supercycle)');
    expect(md).not.toContain('structured:');
    expect(md).not.toContain('globalTarget:');
    expect(md).not.toContain('https://example.com/download');
  });

  it('shows news for watchlist score > 80 and hides for score <= 80', () => {
    const now = new Date('2026-06-15T12:00:00+00:00').getTime();
    const resp = {
      stalenessBasis: 'published_then_fetched',
      maxAgeDays: 30,
      total: 1,
      items: [sampleStock()],
    };

    const eligible = buildCatalystStocksMarkdown(resp, {
      mode: 'compact',
      now,
      context: makeContext({
        watchlistSymbols: new Set(['CN:300308']),
        watchlistScores: new Map([['CN:300308', 85]]),
        trendMap: new Map([['CN:300308', { symbol: 'CN:300308', trendOk: true, score: 85 }]]),
      }),
    });
    expect(eligible).toContain('====');
    expect(eligible).toContain('- S · Optical Supercycle · Hyperscaler liquid-cooling demand rises.');

    const blocked = buildCatalystStocksMarkdown(resp, {
      mode: 'compact',
      now,
      context: makeContext({
        watchlistSymbols: new Set(['CN:300308']),
        watchlistScores: new Map([['CN:300308', 80]]),
        trendMap: new Map([['CN:300308', { symbol: 'CN:300308', trendOk: true, score: 80 }]]),
      }),
    });
    expect(blocked).not.toContain('====');
  });

  it('shows news for screener trendOk and hides when trendOk is false', () => {
    const now = new Date('2026-06-15T12:00:00+00:00').getTime();
    const resp = {
      stalenessBasis: 'published_then_fetched',
      maxAgeDays: 30,
      total: 1,
      items: [sampleStock()],
    };

    expect(
      shouldShowCatalystNews(
        'CN:300308',
        makeContext({
          screenerTrendOkSymbols: new Set(['CN:300308']),
          trendMap: new Map([['CN:300308', { symbol: 'CN:300308', trendOk: true, score: 70 }]]),
        }),
      ),
    ).toBe(true);

    const broken = buildCatalystStocksMarkdown(resp, {
      mode: 'compact',
      now,
      context: makeContext({
        screenerTrendOkSymbols: new Set(['CN:300308']),
        trendMap: new Map([['CN:300308', { symbol: 'CN:300308', trendOk: false, score: 90 }]]),
      }),
    });
    expect(broken).not.toContain('====');
  });
});

describe('filterRecentArticles', () => {
  it('keeps only articles within 72 hours and limits to 3', () => {
    const now = new Date('2026-06-15T12:00:00+00:00').getTime();
    const articles = [
      sampleArticle({ documentId: 'd1', contribution: 0.9, publishedAt: '2026-06-14T12:00:00+00:00' }),
      sampleArticle({ documentId: 'd2', contribution: 0.8, publishedAt: '2026-06-13T12:00:00+00:00' }),
      sampleArticle({ documentId: 'd3', contribution: 0.7, publishedAt: '2026-06-12T12:00:00+00:00' }),
      sampleArticle({ documentId: 'd4', contribution: 0.6, publishedAt: '2026-06-11T12:00:00+00:00' }),
      sampleArticle({ documentId: 'old', contribution: 1.0, publishedAt: '2026-06-01T12:00:00+00:00' }),
    ];

    const recent = filterRecentArticles(articles, 72, 3, now);
    expect(recent.map((a) => a.documentId)).toEqual(['d1', 'd2', 'd3']);
  });
});
