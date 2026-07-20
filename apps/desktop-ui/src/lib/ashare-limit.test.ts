import { describe, expect, it } from 'vitest';

import { limitDownPrice, limitPctForAshare, roundAsharePrice } from './ashare-limit';

describe('limitPctForAshare', () => {
  it('uses board rules and ST name', () => {
    expect(limitPctForAshare('CN:000001', '平安银行')).toBe(10);
    expect(limitPctForAshare('CN:600000', '浦发银行')).toBe(10);
    expect(limitPctForAshare('CN:300001', '特锐德')).toBe(20);
    expect(limitPctForAshare('CN:688192', '迪哲医药')).toBe(20);
    expect(limitPctForAshare('CN:000001', 'ST某某')).toBe(5);
    expect(limitPctForAshare('CN:430001', '北交所票')).toBe(30);
  });
});

describe('limitDownPrice', () => {
  it('computes main-board limit-down', () => {
    expect(limitDownPrice('CN:002821', 179.99, '凯莱英')).toBe(roundAsharePrice(179.99 * 0.9));
  });

  it('computes STAR 20% limit-down', () => {
    expect(limitDownPrice('CN:688192', 70, '迪哲医药')).toBe(56);
  });

  it('returns null when preClose missing', () => {
    expect(limitDownPrice('CN:002821', null, '凯莱英')).toBeNull();
    expect(limitDownPrice('CN:002821', 0, '凯莱英')).toBeNull();
  });
});
