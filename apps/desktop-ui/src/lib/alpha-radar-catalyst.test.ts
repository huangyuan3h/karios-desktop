import { describe, expect, it } from 'vitest';

import {
  buildAlphaRadarTrendsMarkdown,
  buildAutoQaMarkdown,
  buildCatalystStocksMarkdown,
  filterRecentArticles,
  formatCatalystStockSummaryLine,
  formatStructuredTrendJson,
  shouldShowCatalystNews,
  type AutoQaStats,
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
    trendMap: new Map(),
    ...overrides,
  };
}

describe('formatStructuredTrendJson', () => {
  it('emits V4 structured keys', () => {
    const parsed = JSON.parse(
      formatStructuredTrendJson({
        macroTheme: 'Next-Gen Energy',
        catalystGrade: 'S',
        driverType: 'Domestic_Policy',
        eventFocus: '万亿国债落地',
        logicSummary: '政策驱动设备更新',
      }),
    );
    expect(parsed.Macro_Theme).toBe('Next-Gen Energy');
    expect(parsed.Driver_Type).toBe('Domestic_Policy');
    expect(parsed.Catalyst_Grade).toBe('S');
    expect(parsed.Event_Focus).toBe('万亿国债落地');
  });
});

describe('formatCatalystStockSummaryLine', () => {
  it('formats one-line stock summary with max grade theme', () => {
    expect(formatCatalystStockSummaryLine(sampleStock())).toBe(
      'CN:300308 中际旭创 | Score: 78.0 | Max Grade: S (Optical Supercycle)',
    );
  });

  it('appends QA flag and uses adjusted score when penalty > 0', () => {
    const stock = sampleStock({
      catalystScore: 100,
      autoQaPenalty: 0.6,
      adjustedCatalystScore: 40,
      autoQaSignals: { industry_mismatch: { industry: '半导体设备' } },
    });
    expect(formatCatalystStockSummaryLine(stock)).toBe(
      'CN:300308 中际旭创 | Score: 40.0 | Max Grade: S (Optical Supercycle) · ⚠QA -60%',
    );
  });
});

describe('buildAutoQaMarkdown', () => {
  it('returns empty string when no penalties and no low-win themes', () => {
    const stats: AutoQaStats = {
      sinceDays: 7,
      lookbackDays: 30,
      themesCovered: 5,
      recentPenalties: [],
      lowWinRateThemes: [],
    };
    expect(buildAutoQaMarkdown(stats)).toBe('');
  });

  it('returns empty string when stats is null/undefined', () => {
    expect(buildAutoQaMarkdown(null)).toBe('');
    expect(buildAutoQaMarkdown(undefined)).toBe('');
  });

  it('renders warnings + low-win-rate sections when both present', () => {
    const stats: AutoQaStats = {
      sinceDays: 7,
      lookbackDays: 30,
      themesCovered: 2,
      recentPenalties: [
        {
          trendId: 't1',
          trendName: 'HBM 涨价',
          macroTheme: 'HBM 涨价',
          symbol: 'CN:002371',
          symbolName: '北方华创',
          industry: '半导体设备',
          expectedIndustries: ['半导体'],
          penalty: 0.6,
        },
      ],
      lowWinRateThemes: [{ theme: '某某概念', wins: 1, total: 6, winRate: 0.167 }],
    };
    const md = buildAutoQaMarkdown(stats);
    expect(md).toContain('## Alpha Radar · Auto-QA');
    expect(md).toContain('HBM 涨价');
    expect(md).toContain('CN:002371');
    expect(md).toContain('半导体设备');
    expect(md).toContain('半导体');
    expect(md).toContain('60%');
    expect(md).toContain('某某概念');
    expect(md).toContain('1 / 6');
    expect(md).toContain('17%');
  });

  it('renders only penalties when no low-win themes', () => {
    const stats: AutoQaStats = {
      sinceDays: 7,
      lookbackDays: 30,
      themesCovered: 1,
      recentPenalties: [
        {
          trendId: 't1',
          trendName: 'AI',
          macroTheme: 'AI',
          symbol: 'CN:300033',
          symbolName: '同花顺',
          industry: '计算机',
          expectedIndustries: ['电子'],
          penalty: 0.6,
        },
      ],
      lowWinRateThemes: [],
    };
    const md = buildAutoQaMarkdown(stats);
    expect(md).toContain('## Alpha Radar · Auto-QA');
    expect(md).toContain('Mapping warnings');
    expect(md).not.toContain('Theme historical win-rate');
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
    expect(md).toContain('"Macro_Theme":"Next-Gen Energy"');
    expect(md).toContain('- catalyst: Grid storage orders accelerate.');
    expect(md).toContain('英维克');
  });

  it('omits detail blocks in compact mode', () => {
    const md = buildAlphaRadarTrendsMarkdown([trend], { mode: 'compact' });

    expect(md).toContain('| Macro Theme | Driver | Grade | A-share Mapping |');
    expect(md).not.toContain('### Next-Gen Energy');
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

    expect(md).toContain('"Macro_Theme":"Next-Gen Energy"');
    expect(md).toContain('"Catalyst_Grade":"S"');
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

  it('shows news for watchlist trendOk and hides when trendOk is false', () => {
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
          watchlistSymbols: new Set(['CN:300308']),
          watchlistScores: new Map([['CN:300308', 90]]),
          trendMap: new Map([['CN:300308', { symbol: 'CN:300308', trendOk: true, score: 90 }]]),
        }),
      ),
    ).toBe(true);

    const broken = buildCatalystStocksMarkdown(resp, {
      mode: 'compact',
      now,
      context: makeContext({
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
