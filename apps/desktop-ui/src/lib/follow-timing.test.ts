import { beforeEach, describe, expect, it } from 'vitest';

import {
  collectTrailExitSymbols,
  executionTiming,
  followSymbolKey,
  isWhipRebuy,
  mergeTrailExitKeys,
  recentTrailExits,
  rememberTrailExits,
  WHIP_ANNOTATION,
} from './follow-timing';

describe('follow-timing · achievable execution clock', () => {
  it('never suggests a close fill for a close-basis buy', () => {
    const t = executionTiming('close-buy', new Date('2026-11-06T20:00:00+08:00'));
    expect(t.label).toBe('明日开盘跟');
    expect(t.detail).toMatch(/次日开盘/);
  });

  it('uses 下一交易日 on weekends', () => {
    const weekend = new Date('2026-11-07T10:00:00+08:00'); // Saturday
    expect(executionTiming('close-buy', weekend).label).toBe('下一交易日开盘跟');
    expect(executionTiming('intraday-buy', weekend).label).toBe('下一交易日盘中可追');
    expect(executionTiming('close-sell-confirmed', weekend).label).toBe('下一交易日开盘卖');
  });

  it('keeps the 14:30 → close-confirm → next-open sell flow', () => {
    const t = executionTiming(
      'close-sell-pending',
      new Date('2026-11-05T14:30:00+08:00'),
    );
    expect(t.label).toBe('收盘待确认 → 次日开盘卖');
    expect(t.detail).toMatch(/14:30 现价触线只做预警/);
    expect(t.detail).toMatch(/收盘仍触发才卖/);
  });

  it('labels an intraday panel fill as tradable inside the session', () => {
    const t = executionTiming('intraday-buy', new Date('2026-11-05T14:31:00+08:00'));
    expect(t.label).toBe('盘中可追');
    expect(t.detail).toMatch(/收盘前/);
  });
});

describe('follow-timing · whip-saw guard', () => {
  it('collects sticky trail flags and the SELL_TO_REPO sleeve key', () => {
    const keys = collectTrailExitSymbols(
      { action: 'SELL_TO_REPO', symbol: 'ETF:513350.SH', pendingOpenSell: true },
      [
        { symbol: 'ETF:518880', trailExit: true },
        { symbol: 'CN:600519', pendingOpenSell: false },
      ],
    );
    expect(keys).toContain('513350');
    expect(keys).toContain('518880');
    expect(keys).not.toContain('600519');
  });

  it('matches BUY targets on the same canonical key only', () => {
    const keys = new Set(['513350']);
    expect(isWhipRebuy('BUY', 'ETF:513350.SH', keys)).toBe(true);
    expect(isWhipRebuy('SELL', 'ETF:513350.SH', keys)).toBe(false);
    expect(isWhipRebuy('BUY', 'ETF:518880', keys)).toBe(false);
    expect(WHIP_ANNOTATION).toBe('刚止损，回买=鞭打，请确认');
    expect(followSymbolKey('CN:600519.SH')).toBe('600519');
  });

  it('merges observed keys with the short display memory', () => {
    const merged = mergeTrailExitKeys(['ETF:513350'], ['518880.SH']);
    expect([...merged].sort()).toEqual(['513350', '518880']);
  });
});

describe('follow-timing · trail memory (display only)', () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it('remembers a stop-out so the next day reads as a potential whip', () => {
    rememberTrailExits(['ETF:513350.SH'], '2026-11-05');
    const recent = recentTrailExits(10, new Date('2026-11-07T10:00:00+08:00'));
    expect(recent.get('513350')).toBe('2026-11-05');
  });

  it('drops entries older than the display window', () => {
    rememberTrailExits(['ETF:513350.SH'], '2026-10-01');
    const recent = recentTrailExits(10, new Date('2026-11-07T10:00:00+08:00'));
    expect(recent.has('513350')).toBe(false);
  });
});
