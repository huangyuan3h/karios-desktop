'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import { useStrategyMode } from '@/lib/strategy-settings';

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

export function useNotificationsQuery(enabled = true) {
  const [mode] = useStrategyMode();
  return useQuery({
    queryKey: ['notifications', mode],
    queryFn: () => apiGetJson<NotificationsResponse>(`/api/notifications?mode=${mode}`),
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
