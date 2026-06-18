import { describe, expect, it, vi } from 'vitest';

import {
  getShanghaiTimeParts,
  isAutomationPollWindow,
  isShanghaiQuoteWindow,
  isShanghaiTradingTime,
} from './market-hours';

function mockShanghaiTime(isoLocal: string, weekday = 'Mon') {
  const date = new Date(isoLocal);
  vi.spyOn(globalThis, 'Date').mockImplementation(() => date as Date);
  vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
    if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
      return {
        formatToParts: () => [
          { type: 'weekday', value: weekday },
          { type: 'hour', value: '10' },
          { type: 'minute', value: '00' },
        ],
      } as Intl.DateTimeFormat;
    }
    if (options?.timeZone === 'Asia/Shanghai' && options.year === 'numeric') {
      return {
        formatToParts: () => [
          { type: 'year', value: '2026' },
          { type: 'month', value: '06' },
          { type: 'day', value: '18' },
        ],
      } as Intl.DateTimeFormat;
    }
    return new Intl.DateTimeFormat(locale, options);
  }) as typeof Intl.DateTimeFormat);
}

describe('market-hours', () => {
  it('detects trading time on weekday morning', () => {
    const parts = getShanghaiTimeParts(new Date('2026-06-18T02:00:00Z'));
    expect(typeof parts.hour).toBe('number');
    expect(isShanghaiTradingTime()).toBeTypeOf('boolean');
  });

  it('is false on Saturday for quote window', () => {
    mockShanghaiTime('2026-06-20T02:00:00Z', 'Sat');
    expect(isShanghaiQuoteWindow()).toBe(false);
    expect(isAutomationPollWindow()).toBe(false);
    vi.restoreAllMocks();
  });

  it('automation poll window on weekday evening', () => {
    vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
      if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
        return {
          formatToParts: () => [
            { type: 'weekday', value: 'Wed' },
            { type: 'hour', value: '18' },
            { type: 'minute', value: '00' },
          ],
        } as Intl.DateTimeFormat;
      }
      return new Intl.DateTimeFormat(locale, options);
    }) as typeof Intl.DateTimeFormat);
    expect(isAutomationPollWindow()).toBe(true);
    vi.restoreAllMocks();
  });
});
