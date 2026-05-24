import { describe, expect, it } from 'vitest';

import { buildDashboardHotIndustryPicks } from '@/lib/hot-industry-picks';

function makeSummary(args: {
  dailyRankings: Array<{
    date: string;
    ranked: Array<{ industryName: string; value: number; rank: number }>;
  }>;
  flow5dTop?: Array<{
    industryName: string;
    sum5d: number;
    series?: Array<{ date: string; netInflow: number }>;
  }>;
}) {
  const dates = args.dailyRankings.map((x) => x.date);
  return {
    industryFundFlow: {
      dates,
      dailyRankings: args.dailyRankings,
      flow5d: {
        dates,
        top: args.flow5dTop ?? [],
      },
    },
  };
}

describe('buildDashboardHotIndustryPicks', () => {
  it('computes rank delta when industry jumps from #15 to #2', () => {
    const summary = makeSummary({
      dailyRankings: [
        {
          date: '2026-05-22',
          ranked: [
            ...Array.from({ length: 14 }, (_, i) => ({
              industryName: `Industry-${i + 1}`,
              value: (20 - i) * 1e8,
              rank: i + 1,
            })),
            { industryName: 'Breakout-B', value: 5e8, rank: 15 },
            ...Array.from({ length: 5 }, (_, i) => ({
              industryName: `Tail-${i + 1}`,
              value: (4 - i) * 1e8,
              rank: 16 + i,
            })),
          ],
        },
        {
          date: '2026-05-23',
          ranked: [
            { industryName: 'Leader-A', value: 50e8, rank: 1 },
            { industryName: 'Breakout-B', value: 35e8, rank: 2 },
            ...Array.from({ length: 18 }, (_, i) => ({
              industryName: `Industry-${i + 1}`,
              value: (18 - i) * 1e8,
              rank: i + 3,
            })),
          ],
        },
      ],
      flow5dTop: [
        {
          industryName: 'Breakout-B',
          sum5d: 80e8,
          series: [
            { date: '2026-05-22', netInflow: 5e8 },
            { date: '2026-05-23', netInflow: 35e8 },
          ],
        },
      ],
    });

    const picks = buildDashboardHotIndustryPicks(summary);
    const breakout = picks.find((p) => p.industryName === 'Breakout-B');
    expect(breakout).toBeDefined();
    expect(breakout?.rankChange).toBe(13);
    expect(breakout?.momentumSignal).toBe(true);
  });

  it('flags momentum breakout with rank delta and net inflow threshold', () => {
    const summary = makeSummary({
      dailyRankings: [
        {
          date: '2026-05-22',
          ranked: [
            ...Array.from({ length: 24 }, (_, i) => ({
              industryName: `Industry-${i + 1}`,
              value: (25 - i) * 1e8,
              rank: i + 1,
            })),
            { industryName: 'NewMainline', value: 1e8, rank: 25 },
          ],
        },
        {
          date: '2026-05-23',
          ranked: [{ industryName: 'NewMainline', value: 25e8, rank: 1 }],
        },
      ],
      flow5dTop: [
        {
          industryName: 'NewMainline',
          sum5d: 30e8,
          series: [
            { date: '2026-05-22', netInflow: 1e8 },
            { date: '2026-05-23', netInflow: 25e8 },
          ],
        },
      ],
    });

    const picks = buildDashboardHotIndustryPicks(summary);
    expect(picks[0]?.industryName).toBe('NewMainline');
    expect(picks[0]?.rankChange).toBe(24);
    expect(picks[0]?.momentumSignal).toBe(true);
  });

  it('falls back to legacy topByDate when dailyRankings is missing', () => {
    const summary = {
      industryFundFlow: {
        dates: ['2026-05-22', '2026-05-23'],
        topByDate: [
          {
            date: '2026-05-22',
            top: ['Alpha', 'Beta'],
          },
          {
            date: '2026-05-23',
            top: [{ industryName: 'Alpha', value: 30e8 }],
          },
        ],
        flow5d: {
          dates: ['2026-05-22', '2026-05-23'],
          top: [{ industryName: 'Alpha', sum5d: 50e8, series: [] }],
        },
      },
    };

    const picks = buildDashboardHotIndustryPicks(summary);
    expect(picks[0]?.industryName).toBe('Alpha');
    expect(picks[0]?.rankChange).toBe(0);
  });

  it('computes rank delta when sector re-enters daily top after a negative day', () => {
    const summary = makeSummary({
      dailyRankings: [
        {
          date: '2026-05-22',
          ranked: [
            { industryName: 'Leader-A', value: 30e8, rank: 1 },
            { industryName: 'Rebound-B', value: -5e8, rank: 2 },
          ],
        },
        {
          date: '2026-05-23',
          ranked: [
            { industryName: 'Rebound-B', value: 35e8, rank: 1 },
            { industryName: 'Leader-A', value: 10e8, rank: 2 },
          ],
        },
      ],
      flow5dTop: [
        {
          industryName: 'Rebound-B',
          sum5d: 30e8,
          series: [
            { date: '2026-05-22', netInflow: -5e8 },
            { date: '2026-05-23', netInflow: 35e8 },
          ],
        },
      ],
    });

    const picks = buildDashboardHotIndustryPicks(summary);
    const rebound = picks.find((p) => p.industryName === 'Rebound-B');
    expect(rebound).toBeDefined();
    expect(rebound?.rankChange).toBe(1);
    expect(rebound?.momentumSignal).toBe(false);
  });
});
