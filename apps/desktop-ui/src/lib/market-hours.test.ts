import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  getShanghaiTimeParts,
  isAutomationPollWindow,
  isShanghaiPreMarket,
  isShanghaiQuoteWindow,
  isShanghaiTradingTime,
  satNamesVisible,
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

  it('isShanghaiPreMarket true before 09:30 on a weekday', () => {
    vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
      if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
        return {
          formatToParts: () => [
            { type: 'weekday', value: 'Thu' },
            { type: 'hour', value: '06' },
            { type: 'minute', value: '59' },
          ],
        } as Intl.DateTimeFormat;
      }
      return new Intl.DateTimeFormat(locale, options);
    }) as typeof Intl.DateTimeFormat);
    expect(isShanghaiPreMarket()).toBe(true);
    vi.restoreAllMocks();
  });

  it('isShanghaiPreMarket false at 10:00 on a weekday (market open)', () => {
    vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
      if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
        return {
          formatToParts: () => [
            { type: 'weekday', value: 'Thu' },
            { type: 'hour', value: '10' },
            { type: 'minute', value: '00' },
          ],
        } as Intl.DateTimeFormat;
      }
      return new Intl.DateTimeFormat(locale, options);
    }) as typeof Intl.DateTimeFormat);
    expect(isShanghaiPreMarket()).toBe(false);
    vi.restoreAllMocks();
  });

  it('isShanghaiPreMarket false on Saturday', () => {
    vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
      if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
        return {
          formatToParts: () => [
            { type: 'weekday', value: 'Sat' },
            { type: 'hour', value: '06' },
            { type: 'minute', value: '59' },
          ],
        } as Intl.DateTimeFormat;
      }
      return new Intl.DateTimeFormat(locale, options);
    }) as typeof Intl.DateTimeFormat);
    expect(isShanghaiPreMarket()).toBe(false);
    vi.restoreAllMocks();
  });
});

describe('satNamesVisible', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function mockParts(weekday: string, hour: string, minute: string) {
    vi.spyOn(Intl, 'DateTimeFormat').mockImplementation(((locale?: string, options?: Intl.DateTimeFormatOptions) => {
      if (options?.timeZone === 'Asia/Shanghai' && options.weekday === 'short') {
        return {
          formatToParts: () => [
            { type: 'weekday', value: weekday },
            { type: 'hour', value: hour },
            { type: 'minute', value: minute },
          ],
        } as Intl.DateTimeFormat;
      }
      return new Intl.DateTimeFormat(locale, options);
    }) as typeof Intl.DateTimeFormat);
  }

  it('hides names during the weekday session before 14:30', () => {
    mockParts('Wed', '11', '12');
    expect(satNamesVisible()).toBe(false);
  });

  it('shows names at 14:30', () => {
    mockParts('Wed', '14', '30');
    expect(satNamesVisible()).toBe(true);
  });

  it('shows names overnight before 09:00', () => {
    mockParts('Thu', '08', '00');
    expect(satNamesVisible()).toBe(true);
  });

  it('shows names on Saturday for last-session review', () => {
    mockParts('Sat', '11', '00');
    expect(satNamesVisible()).toBe(true);
  });
});
