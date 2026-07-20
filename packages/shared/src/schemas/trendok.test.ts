import { describe, expect, it } from 'vitest';
import { TrendOkResultSchema, WatchlistRiskAlertSchema } from './trendok';

describe('WatchlistRiskAlertSchema', () => {
  it('accepts block and warn severities', () => {
    expect(
      WatchlistRiskAlertSchema.parse({
        code: 'intraday_surge',
        severity: 'block',
        message: 'Intraday change 7.0% exceeds 6.0%; no new positions',
      }).severity,
    ).toBe('block');
    expect(
      WatchlistRiskAlertSchema.parse({
        code: 'gap_up_weak_market',
        severity: 'warn',
        message: 'Gap-up with diverging market; do not chase highs',
      }).severity,
    ).toBe('warn');
  });

  it('rejects invalid severity', () => {
    expect(() =>
      WatchlistRiskAlertSchema.parse({
        code: 'x',
        severity: 'info',
        message: 'm',
      }),
    ).toThrow();
  });
});

describe('TrendOkResultSchema', () => {
  const goldenSample = {
    symbol: 'CN:000001',
    name: 'Ping An Bank',
    asOfDate: '2026-06-18',
    trendOk: true,
    score: 72.5,
    scoreParts: { trend: 30, volume: 20 },
    stopLossPrice: 10.5,
    stopLossParts: { atr: 0.3 },
    buyMode: 'A',
    buyAction: 'watch',
    buyZoneLow: 10.0,
    buyZoneHigh: 11.0,
    buyRefPrice: 10.8,
    buyWhy: 'Above EMA20',
    buyChecks: { in_trend: true },
    marketRegime: 'risk_on',
    intradayChgPct: 1.2,
    gapUp: false,
    riskMetricsLive: true,
    riskAlerts: [
      {
        code: 'gap_up_weak_market',
        severity: 'warn',
        message: 'Gap-up with diverging market; do not chase highs',
      },
    ],
    instFlow: {
      tradeDate: '2026-06-18',
      onBoard: true,
      instNetBuyYi: 3.2,
      label: '机构主买',
      lhasaDominant: false,
      display: '+3.2亿 (机构主买)',
    },
    checks: { ema20: true },
    values: { close: 10.8 },
    rs: 12.5,
    missingData: [],
  };

  it('validates a full trendok payload', () => {
    const result = TrendOkResultSchema.parse(goldenSample);
    expect(result.symbol).toBe('CN:000001');
    expect(result.riskAlerts).toHaveLength(1);
    expect(result.scoreParts?.trend).toBe(30);
    expect(result.rs).toBe(12.5);
  });

  it('validates minimal trendok payload', () => {
    const result = TrendOkResultSchema.parse({
      symbol: 'CN:000001',
      missingData: ['no_bars'],
    });
    expect(result.symbol).toBe('CN:000001');
    expect(result.missingData).toEqual(['no_bars']);
  });

  it('validates macroLock payload', () => {
    const result = TrendOkResultSchema.parse({
      symbol: 'CN:000001',
      buyAction: 'avoid',
      buyMode: 'none',
      macroLock: {
        active: true,
        riskMode: 'extreme_caution',
        downCount: 4600,
      },
    });
    expect(result.macroLock?.active).toBe(true);
    expect(result.macroLock?.downCount).toBe(4600);
  });

  it('rejects missing symbol', () => {
    expect(() => TrendOkResultSchema.parse({ name: 'x' })).toThrow();
  });
});
