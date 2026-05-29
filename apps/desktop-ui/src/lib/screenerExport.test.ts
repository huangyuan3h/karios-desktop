import { describe, expect, it } from 'vitest';

import {
  buildScreenerMarkdownRows,
  countMissingScores,
  extractSymbolsFromSnapshotRows,
  isHotspotTop3Industry,
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
    expect(enriched[0]?.gapUp).toBe('—');
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
