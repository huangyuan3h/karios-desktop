import { describe, expect, it } from 'vitest';

import { groupNotifications, notificationLane } from './notification-lanes';

describe('notification lanes', () => {
  it('maps recon and OOS to research so they do not mix with today\'s orders', () => {
    expect(notificationLane({ type: 'recon_missing' })).toBe('research');
    expect(notificationLane({ type: 'oos_warning' })).toBe('research');
    expect(notificationLane({ type: 'pyramid_trigger' })).toBe('trade');
    expect(notificationLane({ type: 'cron_failed' })).toBe('system');
    expect(notificationLane({ type: 'sat_exit', lane: 'trade' })).toBe('trade');
  });

  it('groups a mixed list in trade → system → research order', () => {
    const groups = groupNotifications([
      { type: 'oos_warning', lane: 'research' },
      { type: 'sat_exit', lane: 'trade' },
      { type: 'cron_failed', lane: 'system' },
      { type: 'recon_missing' },
    ]);
    expect(groups.map((g) => g.lane)).toEqual(['trade', 'system', 'research']);
    expect(groups[2]?.items).toHaveLength(2);
  });
});
