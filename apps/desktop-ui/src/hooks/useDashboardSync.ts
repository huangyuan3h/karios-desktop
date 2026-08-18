/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';

import { DATA_SYNC_BASE_URL, AI_BASE_URL } from '@/lib/endpoints';
import type { DashboardSummary } from '@/lib/queries/dashboard';
import { stripModelThinking } from '@/lib/strip-model-thinking';

type DashboardSyncResp = Record<string, unknown>;

export type SyncStep = {
  name: string;
  ok: boolean | null;
  durationMs: number | null;
  message?: string | null;
};

export type DashboardSyncCallbacks = {
  applySummaryToCache: (s: DashboardSummary) => void;
  shouldRefreshNewsBrief: (lastUpdatedAt: string | null) => boolean;
  newsSummary: string | null;
  newsSummaryUpdatedAt: string | null;
  setNewsSummary: (v: string | null) => void;
  setNewsSummaryUpdatedAt: (v: string | null) => void;
  setNewsSummaryBusy: (v: boolean) => void;
  saveNewsBriefCache: (patch: object) => void;
  setError: (v: string | null) => void;
  /**
   * Optional: force-refresh watchlist K-lines after the backend SSE stream
   * finishes. Required so dashboard "Sync & Copy" also covers HK/ETF symbols
   * — the backend sync steps don't touch per-stock bars, so without this
   * callback watchlist HK tickers only get fresh data when the user opens
   * the watchlist page or via the daily hk_daily cron.
   */
  forceRefreshWatchlistOnSync?: () => Promise<unknown>;
  /** Fired after Sync All stream completes (success or soft failure). */
  onSyncComplete?: () => void;
};

export function useDashboardSync(callbacks: DashboardSyncCallbacks) {
  const callbacksRef = React.useRef(callbacks);
  React.useEffect(() => {
    callbacksRef.current = callbacks;
  });

  const [syncResp, setSyncResp] = React.useState<DashboardSyncResp | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [syncSteps, setSyncSteps] = React.useState<SyncStep[]>([]);
  const [syncProgress, setSyncProgress] = React.useState(0);
  const esRef = React.useRef<EventSource | null>(null);
  const timerRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  React.useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    };
  }, []);

  const SYNC_STREAM_TIMEOUT_MS = 5 * 60_000;

  async function onSyncAll(): Promise<{ ok: boolean; summary: DashboardSummary | null }> {
    setBusy(true);
    callbacksRef.current.setError(null);
    setSyncSteps([]);
    setSyncProgress(0);

    const stepNames = [
      'industryFundFlow',
      'marketSentiment',
      'macroDaily',
      'screeners',
      'news',
      // Final step: force-refresh watchlist K-lines (covers HK/ETF) which
      // the backend sync steps do not touch. This step runs after the SSE
      // 'done' event and is driven by the forceRefreshWatchlistOnSync callback.
      'watchlist',
    ];
    // Sync All is an explicit user action — always force catch-up for stale modules.
    const forceSync = true;

    return new Promise<{ ok: boolean; summary: DashboardSummary | null }>((resolve) => {
      if (esRef.current) {
        // A previous stream is still open (should not happen — cleanup closes it).
        esRef.current.close();
      }
      const es = new EventSource(
        `${DATA_SYNC_BASE_URL}/dashboard/sync/stream?force=${forceSync ? 'true' : 'false'}`,
      );
      esRef.current = es;

      // Overall guard: if the backend stalls mid-stream without erroring
      // (hung SQL, dead TCP), fail the sync instead of spinning forever.
      timerRef.current = setTimeout(() => {
        if (!esRef.current) return;
        es.close();
        esRef.current = null;
        callbacksRef.current.setError('Sync timed out after 5 minutes');
        setBusy(false);
        resolve({ ok: false, summary: null });
      }, SYNC_STREAM_TIMEOUT_MS);

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'start') {
            setSyncSteps(
              stepNames.map((name) => ({
                name,
                ok: null,
                durationMs: null,
                message: null,
              })),
            );
          } else if (data.type === 'step') {
            const step = data.step as {
              name: string;
              ok: boolean;
              durationMs: number;
              message?: string;
            };
            setSyncSteps((prev) => {
              const updated = prev.map((s) => (s.name === step.name ? { ...step } : s));
              const completed = updated.filter((s) => s.ok !== null).length;
              setSyncProgress(Math.round((completed / stepNames.length) * 100));
              return updated;
            });
          } else if (data.type === 'done') {
            if (timerRef.current) clearTimeout(timerRef.current);
            es.close();
            if (esRef.current === es) esRef.current = null;
            const result = data.result as DashboardSyncResp;
            setSyncResp(result);
            // Progress stays at backend progress until the optional watchlist
            // force-refresh finishes — otherwise the UI would jump to 100% and
            // appear "done" while HK/ETF bars are still being pulled.
            setSyncProgress(Math.round(((stepNames.length - 1) / stepNames.length) * 100));
            setSyncSteps((prev) =>
              prev.map((s) =>
                s.name === 'watchlist' && s.ok === null
                  ? { ...s, ok: null, message: 'pulling HK/ETF K-lines…' }
                  : s,
              ),
            );

            const cb = callbacksRef.current;
            const s = data.summary as DashboardSummary;
            const finalize = () => {
              setSyncSteps((prev) =>
                prev.map((st) =>
                  st.name === 'watchlist' && st.ok === null
                    ? { ...st, ok: true, durationMs: 0, message: 'done' }
                    : st,
                ),
              );
              setSyncProgress(100);
              setBusy(false);
              cb.onSyncComplete?.();
              resolve({ ok: true, summary: s });
            };

            const afterWatchlist = async () => {
              try {
                if (cb.forceRefreshWatchlistOnSync) {
                  await cb.forceRefreshWatchlistOnSync();
                }
              } catch (e) {
                console.warn('forceRefreshWatchlistOnSync failed:', e);
              }
              finalize();
            };

            if (s) {
              cb.applySummaryToCache(s);
              const newsData = (s as any)?.news;
              if (newsData && Array.isArray(newsData.items) && newsData.items.length > 0) {
                if (!cb.shouldRefreshNewsBrief(cb.newsSummaryUpdatedAt) && cb.newsSummary?.trim()) {
                  void afterWatchlist();
                  return;
                }
                cb.setNewsSummaryBusy(true);
                fetch(`${AI_BASE_URL}/news/summary`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ items: newsData.items, hours: 24 }),
                })
                  .then((aiRes) => {
                    if (aiRes.ok) {
                      return aiRes.json();
                    }
                    return null;
                  })
                  .then((aiData) => {
                    const summaryText =
                      typeof aiData?.summary === 'string'
                        ? stripModelThinking(aiData.summary)
                        : '';
                    if (summaryText) {
                      const updatedAt = new Date().toISOString();
                      cb.setNewsSummary(summaryText);
                      cb.setNewsSummaryUpdatedAt(updatedAt);
                      cb.saveNewsBriefCache({ summary: summaryText, updatedAt });
                    }
                  })
                  .catch((e) => {
                    console.warn('news summary generation failed:', e);
                  })
                  .finally(() => {
                    cb.setNewsSummaryBusy(false);
                    void afterWatchlist();
                  });
              } else {
                void afterWatchlist();
              }
            } else {
              void afterWatchlist();
            }
          }
        } catch {
          // ignore parse errors
        }
      };

      es.onerror = () => {
        if (timerRef.current) clearTimeout(timerRef.current);
        es.close();
        if (esRef.current === es) esRef.current = null;
        callbacksRef.current.setError('Connection error during sync');
        setBusy(false);
        resolve({ ok: false, summary: null });
      };
    });
  }

  return {
    syncResp,
    busy,
    syncSteps,
    syncProgress,
    onSyncAll,
    setError: callbacks.setError,
  };
}
