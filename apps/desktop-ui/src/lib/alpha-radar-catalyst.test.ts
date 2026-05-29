import { describe, expect, it } from 'vitest';

import {
  buildAlphaRadarTrendsMarkdown,
  buildCatalystStocksMarkdown,
  formatStructuredTrendJson,
} from './alpha-radar-catalyst';

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

describe('buildAlphaRadarTrendsMarkdown', () => {
  it('includes structured JSON and catalyst text', () => {
    const md = buildAlphaRadarTrendsMarkdown([
      {
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
      },
    ]);

    expect(md).toContain('## Alpha Radar · Structured Trends');
    expect(md).toContain('{"Macro_Theme":"Next-Gen Energy","Catalyst_Grade":"S"}');
    expect(md).toContain('- catalyst: Grid storage orders accelerate.');
    expect(md).toContain('英维克');
  });
});

describe('buildCatalystStocksMarkdown', () => {
  it('prefers structured fields over raw source summary', () => {
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
});
