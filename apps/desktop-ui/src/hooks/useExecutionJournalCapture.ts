'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { ExecutionGate, ExecutionSnapshotSource } from '@karios/shared';

import {
  captureAndPushExecutionSnapshot,
  flushExecutionSnapshotQueue,
} from '@/lib/execution-journal';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import {
  getShanghaiMinutes,
  getShanghaiTodayIso,
  isShanghaiTradingTime,
  isWeekdayShanghai,
} from '@/lib/market-hours';
import {
  executionChangesKey,
  executionSnapshotsKey,
} from '@/lib/queries/execution-journal';
import type { WatchlistItem } from '@/lib/watchlist-storage';

const POLL_MS = 5 * 60 * 1000;
const REGISTRY_DEBOUNCE_MS = 2000;

export type UseExecutionJournalCaptureOpts = {
  items: WatchlistItem[];
  gate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  /** When true, run poll + eod + initial flush */
  enabled?: boolean;
};

export function useExecutionJournalCapture(opts: UseExecutionJournalCaptureOpts) {
  const { items, gate, mainlineAllow, enabled = true } = opts;
  const queryClient = useQueryClient();
  const [busy, setBusy] = React.useState(false);
  const itemsRef = React.useRef(items);
  const gateRef = React.useRef(gate);
  const mainlineRef = React.useRef(mainlineAllow);
  itemsRef.current = items;
  gateRef.current = gate;
  mainlineRef.current = mainlineAllow;

  const invalidate = React.useCallback(() => {
    const td = getShanghaiTodayIso();
    void queryClient.invalidateQueries({ queryKey: executionChangesKey(td) });
    void queryClient.invalidateQueries({ queryKey: executionSnapshotsKey(td) });
    void queryClient.invalidateQueries({ queryKey: ['execution', 'snapshots', 'recent'] });
  }, [queryClient]);

  const capture = React.useCallback(
    async (source: ExecutionSnapshotSource) => {
      if (!gateRef.current) return null;
      setBusy(true);
      try {
        const res = await captureAndPushExecutionSnapshot(queryClient, {
          items: itemsRef.current,
          gate: gateRef.current,
          mainlineAllow: mainlineRef.current,
          source,
        });
        invalidate();
        return res;
      } finally {
        setBusy(false);
      }
    },
    [invalidate, queryClient],
  );

  // Flush offline queue + EOD / initial capture
  React.useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    void (async () => {
      await flushExecutionSnapshotQueue();
      if (cancelled || !gateRef.current) return;
      const mins = getShanghaiMinutes();
      const afterClose = isWeekdayShanghai() && mins >= 15 * 60;
      if (afterClose) {
        await capture('eod');
      } else if (itemsRef.current.length && gateRef.current) {
        // Light touch on mount so journal is not empty after restart
        await capture('poll');
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- mount once when enabled/gate ready
  }, [enabled, Boolean(gate)]);

  // Trading-hours poll every 5 minutes
  React.useEffect(() => {
    if (!enabled) return;
    const id = window.setInterval(() => {
      if (!isShanghaiTradingTime()) return;
      if (!gateRef.current || !itemsRef.current.length) return;
      void capture('poll');
    }, POLL_MS);
    return () => window.clearInterval(id);
  }, [capture, enabled]);

  // Registry debounce: watch items identity (position/cost changes)
  const registryTimer = React.useRef<number | null>(null);
  const itemsSig = React.useMemo(
    () =>
      items
        .map((i) => `${i.symbol}:${i.positionPct ?? ''}:${i.costPrice ?? ''}:${i.maxPrice ?? ''}`)
        .join('|'),
    [items],
  );
  React.useEffect(() => {
    if (!enabled || !gate) return;
    if (registryTimer.current) window.clearTimeout(registryTimer.current);
    registryTimer.current = window.setTimeout(() => {
      void capture('registry');
    }, REGISTRY_DEBOUNCE_MS);
    return () => {
      if (registryTimer.current) window.clearTimeout(registryTimer.current);
    };
  }, [capture, enabled, gate, itemsSig]);

  return { capture, busy, invalidate };
}
