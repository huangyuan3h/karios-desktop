import { describe, expect, it } from 'vitest';

import {
  collectWatchlistRiskAlerts,
  computePnLPct,
  computeVwap,
  formatGapUp,
  formatHotTop3,
  formatInstFlow,
  formatVolumeRatio,
  hasBlockingWatchlistRisk,
  industryDisplayName,
  isInstFlowRisk,
  isAboveVwapPremium,
  isHotTop3Industry,
  isIntradaySurge,
  resolveIntradayChgPct,
  resolveVolumeRatio,
  resolveWatchlistCurrentPrice,
  resolveWatchlistVwap,
  shouldRequireRealtimeQuote,
  volumeRatioTone,
} from './watchlist-metrics';

describe('computePnLPct', () => {
  it('returns null without cost', () => {
    expect(computePnLPct(null, 10)).toBeNull();
    expect(computePnLPct(0, 10)).toBeNull();
  });

  it('computes percentage gain and loss', () => {
    expect(computePnLPct(100, 105)).toBeCloseTo(5);
    expect(computePnLPct(100, 95)).toBeCloseTo(-5);
  });
});

describe('computeVwap', () => {
  it('computes realtime vwap from amount (CNY) and volume (shares)', () => {
    // amount=101200 CNY, volume=100 shares => VWAP = 1012.0
    expect(computeVwap(101200, 100, 'realtime')).toBeCloseTo(1012.0);
  });

  it('computes daily vwap with thousand-yuan amount', () => {
    expect(computeVwap(1012, 100, 'daily')).toBeCloseTo(101.2);
  });

  it('returns null for invalid inputs', () => {
    expect(computeVwap(null, 100)).toBeNull();
    expect(computeVwap(1000, 0)).toBeNull();
  });
});

describe('resolveWatchlistCurrentPrice', () => {
  const today = '2026-05-28';
  const yesterday = '2026-05-27';

  it('uses realtime quote during CN session when trend is for today', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: true,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: 10.5,
        quoteTradeTime: `${today} 14:30:00`,
        trendClose: 10.0,
      }),
    ).toBe(10.5);
  });

  it('uses realtime quote during CN session even when trend bar is stale', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: true,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: yesterday,
        quotePrice: 10.5,
        quoteTradeTime: `${today} 14:30:00`,
        trendClose: 10.0,
      }),
    ).toBe(10.5);
  });

  it('prefers daily close after hours when trend bar is for today', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: false,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: 10.2,
        quoteTradeTime: `${today} 15:00:00`,
        trendClose: 10.8,
      }),
    ).toBe(10.8);
  });

  it('uses closing quote after hours when daily bar is still stale', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: false,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: yesterday,
        quotePrice: 10.2,
        quoteTradeTime: `${today} 15:00:00`,
        trendClose: 10.0,
      }),
    ).toBe(10.2);
  });

  it('falls back to close when realtime quote is missing during session', () => {
    expect(
      resolveWatchlistCurrentPrice({
        tradingTime: true,
        todaySh: today,
        symbol: 'CN:600000',
        trendAsOfDate: today,
        quotePrice: null,
        quoteTradeTime: null,
        trendClose: 9.9,
      }),
    ).toBe(9.9);
  });
});

describe('resolveIntradayChgPct', () => {
  it('uses closing quote when trend bar is stale', () => {
    expect(
      resolveIntradayChgPct({
        fromTrend: -1.2,
        quotePrice: 10.5,
        quotePreClose: 10.0,
        quotePctChg: 5.0,
        quoteTradeDate: '2026-05-28',
        asOfDate: '2026-05-27',
      }),
    ).toBe(5.0);
  });
});

describe('volume ratio helpers', () => {
  it('resolves direct volumeRatio before avg volume fallback', () => {
    expect(resolveVolumeRatio({ volumeRatio: 1.34, avgVol5: 1, avgVol30: 10 })).toBe(1.34);
    expect(resolveVolumeRatio({ avgVol5: 150, avgVol30: 100 })).toBeCloseTo(1.5);
  });

  it('formats and tones volume ratio for risk display', () => {
    expect(formatVolumeRatio(1.234)).toBe('1.23x');
    expect(formatVolumeRatio(null)).toBe('—');
    expect(volumeRatioTone(1.5)).toBe('strong');
    expect(volumeRatioTone(0.99)).toBe('weak');
    expect(volumeRatioTone(1.2)).toBe('neutral');
  });
});

describe('shouldRequireRealtimeQuote', () => {
  it('is false outside session and true for CN symbols during session', () => {
    expect(
      shouldRequireRealtimeQuote({
        tradingTime: false,
        symbol: 'CN:600000',
        trendAsOfDate: '2026-05-28',
        todaySh: '2026-05-28',
      }),
    ).toBe(false);
    expect(
      shouldRequireRealtimeQuote({
        tradingTime: true,
        symbol: 'CN:600000',
        trendAsOfDate: '2026-05-27',
        todaySh: '2026-05-28',
      }),
    ).toBe(true);
    expect(
      shouldRequireRealtimeQuote({
        tradingTime: true,
        symbol: 'HK:0700',
        trendAsOfDate: '2026-05-28',
        todaySh: '2026-05-28',
      }),
    ).toBe(false);
  });
});

describe('industry helpers', () => {
  it('prefers emIndustry over tushare industry', () => {
    expect(industryDisplayName({ emIndustry: '集成电路封测', industry: '元器件' })).toBe('集成电路封测');
  });

  it('detects HotTop3 from industryFlowReasons', () => {
    const t = { values: { industryFlowReasons: ['hotspots_today_top3'] } };
    expect(isHotTop3Industry(t)).toBe(true);
    expect(formatHotTop3(t)).toBe('✓');
  });
});

describe('risk alerts', () => {
  it('detects intraday surge above threshold', () => {
    expect(isIntradaySurge(6.0)).toBe(false);
    expect(isIntradaySurge(6.1)).toBe(true);
  });

  it('detects price above VWAP premium band', () => {
    expect(isAboveVwapPremium(10.6, 10)).toBe(true);
    expect(isAboveVwapPremium(10.4, 10)).toBe(false);
  });

  it('formats gap-up false as No', () => {
    expect(formatGapUp(false)).toBe('No');
    expect(formatGapUp(true)).toBe('✓');
  });

  it('resolves intraday change from trend bars', () => {
    expect(
      resolveIntradayChgPct({
        fromTrend: 2.5,
        asOfDate: '2026-05-28',
      }),
    ).toBe(2.5);
  });

  it('resolves vwap when quote date matches as-of date', () => {
    expect(
      resolveWatchlistVwap({
        tradingTime: false,
        todaySh: '2026-05-29',
        symbol: 'CN:600000',
        trendAsOfDate: '2026-05-28',
        quoteAmount: 101200,
        quoteVolume: 100,
        quoteTradeTime: '2026-05-28 15:00:00',
      }),
    ).toBeCloseTo(1012.0);
  });

  it('aggregates server and client alerts without duplicates', () => {
    const alerts = collectWatchlistRiskAlerts({
      intradayChgPct: 7.2,
      gapUp: true,
      marketRegime: 'Weak',
      current: 11,
      vwap: 10,
      serverAlerts: [
        {
          code: 'intraday_surge',
          severity: 'block',
          message: 'Intraday change 7.2% exceeds 6.0%; no new positions',
        },
      ],
    });
    expect(alerts).toHaveLength(3);
    expect(hasBlockingWatchlistRisk(alerts)).toBe(true);
    expect(alerts.some((a) => a.code === 'above_vwap_premium')).toBe(true);
  });
});

describe('formatInstFlow', () => {
  it('formats display string from instFlow payload', () => {
    expect(
      formatInstFlow({
        onBoard: true,
        instNetBuyYi: 3.2,
        label: '机构主买',
        display: '+3.2亿 (机构主买)',
      }),
    ).toBe('+3.2亿 (机构主买)');
    expect(formatInstFlow(null)).toBe('—');
  });

  it('flags negative or lhasa-dominant flows as risk', () => {
    expect(
      isInstFlowRisk({
        onBoard: true,
        instNetBuyYi: -1.5,
        label: '机构净卖/拉萨主买',
        lhasaDominant: true,
        display: '-1.5亿 (机构净卖/拉萨主买)',
      }),
    ).toBe(true);
    expect(
      isInstFlowRisk({
        onBoard: true,
        instNetBuyYi: 2.0,
        label: '机构主买',
        display: '+2.0亿 (机构主买)',
      }),
    ).toBe(false);
  });
});
