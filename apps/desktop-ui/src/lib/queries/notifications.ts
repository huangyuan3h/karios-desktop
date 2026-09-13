'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

/** Harbor core notifications ride the retired single-track lane on the backend. */
const HARBOR_NOTIFY_MODE = 'single_track';

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
  return useQuery({
    queryKey: ['notifications', HARBOR_NOTIFY_MODE],
    queryFn: () =>
      apiGetJson<NotificationsResponse>(`/api/notifications?mode=${HARBOR_NOTIFY_MODE}`),
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
