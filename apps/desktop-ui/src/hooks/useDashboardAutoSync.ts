'use client';

import * as React from 'react';

import { isShanghaiSyncWindow } from '@/lib/market-hours';
import { dashboardAutoSyncIntervalMs } from '@/lib/queries/intervals';

export type DashboardAutoSyncOptions = {
  /** Heavy sync entry (e.g. useDashboardSync.onSyncAll). Never called overlapping. */
  onSync: () => Promise<unknown>;
  /** When true the current tick is skipped (sync already running). */
  busy: boolean;
  /** Master switch, default true. */
  enabled?: boolean;
};

/**
 * While the Dashboard page is mounted and the Shanghai sync/quote window is
 * open, run the dashboard data sync on a timer — no human click needed.
 *
 * - Trading hours: 90s · after-hours quote window (until 20:00): 3min.
 * - Off-hours: no timer at all (no spam).
 * - Mounting inside the window triggers one catch-up sync shortly after mount.
 * - Never overlaps: ticks while `busy` are skipped.
 */
export function useDashboardAutoSync({ onSync, busy, enabled = true }: DashboardAutoSyncOptions) {
  const onSyncRef = React.useRef(onSync);
  const busyRef = React.useRef(busy);
  React.useEffect(() => {
    onSyncRef.current = onSync;
  });
  React.useEffect(() => {
    busyRef.current = busy;
  });

  const [lastAutoSyncAt, setLastAutoSyncAt] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!enabled) return;
    if (!isShanghaiSyncWindow()) return;
    const period = dashboardAutoSyncIntervalMs();
    if (period === false) return;

    let disposed = false;
    const runOnce = () => {
      if (disposed || busyRef.current) return;
      if (!isShanghaiSyncWindow()) return;
      void Promise.resolve()
        .then(() => onSyncRef.current())
        .then(
          () => {
            if (!disposed) setLastAutoSyncAt(new Date().toISOString());
          },
          () => {
            if (!disposed) setLastAutoSyncAt(new Date().toISOString());
          },
        );
    };

    // Catch-up shortly after mount so opening #/dashboard in-window syncs
    // without clicking (lets first paint finish first).
    const kickoff = window.setTimeout(runOnce, 2000);
    const timer = window.setInterval(runOnce, period);
    return () => {
      disposed = true;
      window.clearTimeout(kickoff);
      window.clearInterval(timer);
    };
  }, [enabled]);

  return { lastAutoSyncAt };
}
