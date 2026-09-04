import { describe, expect, it } from 'vitest';

import {
  HOLDOUT_START,
  PRODUCT_YEAR,
  WALK_FORWARD,
  addCalendarYears,
  resolveTimelineWindows,
  roleBadge,
} from './timeline-windows';

describe('timeline-windows', () => {
  it('keeps gate windows frozen and labels trailing vs product year', () => {
    const wins = resolveTimelineWindows('2026-09-02');
    const byId = Object.fromEntries(wins.map((w) => [w.id, w]));
    expect(byId.trailing).toMatchObject({
      start: '2025-09-02',
      end: '2026-09-02',
      role: 'display',
    });
    expect(byId.product_year).toMatchObject({
      start: PRODUCT_YEAR.start,
      end: PRODUCT_YEAR.end,
      role: 'display',
    });
    expect(byId.OOS2.start).toBe(WALK_FORWARD.OOS2.start);
    expect(byId.train.end).toBe(WALK_FORWARD.train.end);
    expect(byId.valid.start).toBe(WALK_FORWARD.valid.start);
    expect(byId.holdout).toMatchObject({
      start: HOLDOUT_START,
      end: '2026-09-02',
      role: 'readonly',
    });
    expect(roleBadge('gate')).toBe('拒收闸');
    expect(roleBadge('display')).toBe('展示');
  });

  it('does not treat trailing as the product year', () => {
    const wins = resolveTimelineWindows('2026-09-02');
    const trailing = wins.find((w) => w.id === 'trailing')!;
    const product = wins.find((w) => w.id === 'product_year')!;
    expect(trailing.start).not.toBe(product.start);
    expect(trailing.end).not.toBe(product.end);
    expect(trailing.note).toMatch(/不当拒收闸/);
    expect(product.note).toMatch(/不当拒收闸/);
  });

  it('shifts calendar years without UTC-day drift', () => {
    expect(addCalendarYears('2026-03-01', -1)).toBe('2025-03-01');
  });
});
