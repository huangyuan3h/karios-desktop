import { describe, expect, it } from 'vitest';

import {
  buildScreenerMarkdownRows,
  countMissingScores,
  extractSymbolsFromSnapshotRows,
  fetchTodayScreenerSymbolsByTitle,
  isHotspotTop3Industry,
  isTodayShanghai,
  matchesScreenerTitlePattern,
  normalizeScreenerSymbol,
  screenerMarkdownRowsToTable,
} from './screenerExport';

describe('normalizeScreenerSymbol', () => {
  it('normalizes CN tickers and TV exchange prefixes', () => {
    expect(normalizeScreenerSymbol('600000')).toBe('CN:600000');
    expect(normalizeScreenerSymbol('SSE:600000')).toBe('CN:600000');
    expect(normalizeScreenerSymbol('CN:000001')).toBe('CN:000001');
    expect(normalizeScreenerSymbol('HKEX:0700')).toBe('HK:0700');
  });
});

describe('buildScreenerMarkdownRows', () => {
  const headers = ['Ticker', 'Name', 'Price', 'Change %', 'Rel Volume', 'Flags'];
  const rows = [
    {
      Ticker: '600519',
      Name: '贵州茅台',
      Price: '1680.00',
      'Change %': '2.1%',
      'Rel Volume': '1.8',
      Flags: 'D',
    },
  ];

  it('merges TrendOK industry/score and sorts by score desc', () => {
    const trendMap = new Map([
      [
        'CN:600519',
        {
          symbol: 'CN:600519',
          score: 94,
          intradayChgPct: 3.2,
          gapUp: false,
          values: { industry: '白酒', industryFlowReasons: ['hotspots_today_top3'] },
        },
      ],
    ]);

    const enriched = buildScreenerMarkdownRows(rows, headers, trendMap);
    expect(enriched).toHaveLength(1);
    expect(enriched[0]?.symbol).toBe('CN:600519');
    expect(enriched[0]?.industry).toBe('白酒');
    expect(enriched[0]?.score).toBe(94);
    expect(enriched[0]?.intradayPct).toBe('+3.2%');
    expect(enriched[0]?.gapUp).toBe('No');
    expect(enriched[0]?.flags).toBe('D Top3');
  });

  it('counts missing scores', () => {
    const enriched = buildScreenerMarkdownRows(rows, headers, new Map());
    expect(countMissingScores(enriched)).toBe(1);
  });
});

describe('extractSymbolsFromSnapshotRows', () => {
  it('deduplicates symbols from snapshot rows', () => {
    const headers = ['Ticker', 'Name'];
    const rows = [{ Ticker: '600000', Name: 'A' }, { Ticker: '600000', Name: 'A' }];
    expect(extractSymbolsFromSnapshotRows(rows, headers)).toEqual(['CN:600000']);
  });
});

describe('isHotspotTop3Industry', () => {
  it('detects top3 from industryFlowReasons', () => {
    expect(
      isHotspotTop3Industry({
        symbol: 'CN:600519',
        values: { industryFlowReasons: ['hotspots_today_top3'] },
      }),
    ).toBe(true);
  });
});

describe('screenerMarkdownRowsToTable', () => {
  it('formats score as rounded integer', () => {
    const table = screenerMarkdownRowsToTable([
      {
        symbol: 'CN:600519',
        name: '贵州茅台',
        industry: '白酒',
        price: '1680.00',
        changePct: '2.1%',
        relVolume: '1.8',
        score: 94.2,
        intradayPct: '+3.2%',
        gapUp: '—',
        flags: 'D Top3',
      },
    ]);
    expect(table[0]?.[6]).toBe('94');
    expect(table[0]?.[7]).toBe('+3.2%');
  });
});

describe('matchesScreenerTitlePattern', () => {
  it('matches primary Pullback and legacy momentum titles case-insensitively', () => {
    expect(matchesScreenerTitlePattern('Karios Pullback')).toBe(true);
    expect(matchesScreenerTitlePattern('CN Pullback Filter')).toBe(false);
    expect(matchesScreenerTitlePattern('Falcon Launch Filter')).toBe(true);
    expect(matchesScreenerTitlePattern('institutional trend screener')).toBe(true);
    expect(matchesScreenerTitlePattern('Black Horse Filter')).toBe(false);
    expect(matchesScreenerTitlePattern('Legacy Falcon (momentum)')).toBe(false);
  });
});

describe('isTodayShanghai', () => {
  it('compares capturedAt against Shanghai local date', () => {
    expect(isTodayShanghai('2026-06-15T02:00:00+00:00', '2026-06-15')).toBe(true);
    expect(isTodayShanghai('2026-06-14T15:00:00+00:00', '2026-06-15')).toBe(false);
  });
});

describe('fetchTodayScreenerSymbolsByTitle', () => {
  it('returns symbols only for today snapshots with matching titles', async () => {
    const symbols = await fetchTodayScreenerSymbolsByTitle([{ id: 'falcon' }, { id: 'other' }], {
      todayIso: '2026-06-15',
      apiGetJson: (async (path: string) => {
        if (path.includes('/snapshots?limit=1')) {
          const sid = path.split('/screeners/')[1]?.split('/')[0];
          return { items: [{ id: `snap-${sid}`, capturedAt: '2026-06-15T08:00:00+00:00' }] };
        }
        if (path.includes('/snapshots/snap-falcon')) {
          return {
            id: 'snap-falcon',
            screenerId: 'falcon',
            capturedAt: '2026-06-15T08:00:00+00:00',
            rowCount: 1,
            screenTitle: 'Falcon Launch',
            filters: [],
            url: 'https://example.com',
            headers: ['Ticker', 'Name'],
            rows: [{ Ticker: '300308', Name: '中际旭创' }],
          };
        }
        return {
          id: 'snap-other',
          screenerId: 'other',
          capturedAt: '2026-06-15T08:00:00+00:00',
          rowCount: 1,
          screenTitle: 'Other Filter',
          filters: [],
          url: 'https://example.com',
          headers: ['Ticker', 'Name'],
          rows: [{ Ticker: '600519', Name: '贵州茅台' }],
        };
      }) as <T>(path: string) => Promise<T>,
    });

    expect([...symbols]).toEqual(['CN:300308']);
  });
});
