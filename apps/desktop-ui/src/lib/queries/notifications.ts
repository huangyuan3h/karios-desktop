'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import { getStrategyMode } from '@/lib/strategy-settings';

/** Legacy S-3-only feed value; the live feed now follows the selected strategy. */
export const LEGACY_NOTIFY_MODE = 'single_track';

export type NotificationItem = {
  id: string;
  type: string;
  severity: 'high' | 'medium' | 'low';
  title: string;
  detail: string;
  anchor: string;
  createdAt: string;
  lane?: 'trade' | 'system' | 'research';
  book?: string;
};

export type NotificationsResponse = { ok: boolean; items: NotificationItem[] };

/**
 * OPT-223: the feed follows the selected strategy (satellite action item), so
 * switching modes refetches. Falls back to the localStorage value when the
 * caller does not pass the reactive mode.
 */
export function useNotificationsQuery(enabled = true, mode?: string) {
  const notifyMode = mode ?? getStrategyMode();
  return useQuery({
    queryKey: ['notifications', notifyMode],
    queryFn: () => apiGetJson<NotificationsResponse>(`/api/notifications?mode=${notifyMode}`),
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchOnWindowFocus: true,
    enabled,
  });
}

/** Bell/panel click → jump to the watchlist page and scroll to a block. */
export function openNotificationAnchor(anchor: string) {
  if (window.location.hash !== '#/watchlist') {
    window.location.hash = '#/watchlist';
  }
  window.dispatchEvent(new CustomEvent('karios-scroll-to', { detail: { anchor } }));
}
