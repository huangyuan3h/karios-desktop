export type NotificationLane = 'trade' | 'system' | 'research';

export type LaneItem = {
  type: string;
  lane?: string | null;
};

export const NOTIFICATION_LANE_ORDER: NotificationLane[] = ['trade', 'system', 'research'];

export const NOTIFICATION_LANE_META: Record<NotificationLane, { label: string; hint: string }> = {
  trade: { label: '今日交易', hint: '下单 / 改条件单' },
  system: { label: '系统', hint: '同步失败' },
  research: { label: '策略体检', hint: '不要求立刻下单' },
};

export function notificationLane(n: LaneItem): NotificationLane {
  if (n.lane === 'trade' || n.lane === 'system' || n.lane === 'research') return n.lane;
  if (n.type === 'cron_failed') return 'system';
  if (n.type === 'recon_missing' || n.type === 'oos_warning') return 'research';
  return 'trade';
}

export function groupNotifications<T extends LaneItem>(items: T[]): { lane: NotificationLane; items: T[] }[] {
  const buckets: Record<NotificationLane, T[]> = { trade: [], system: [], research: [] };
  for (const n of items) buckets[notificationLane(n)].push(n);
  return NOTIFICATION_LANE_ORDER.filter((lane) => buckets[lane].length > 0).map((lane) => ({
    lane,
    items: buckets[lane],
  }));
}
