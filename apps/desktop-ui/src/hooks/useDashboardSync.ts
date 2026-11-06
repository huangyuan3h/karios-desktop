/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';

import { DATA_SYNC_BASE_URL, AI_BASE_URL } from '@/lib/endpoints';
import type { DashboardSummary } from '@/lib/queries/dashboard';
import { stripModelThinking } from '@/lib/strip-model-thinking';

type DashboardSyncResp = Record<string, unknown>;

export function isDashboardSyncResultOk(result: unknown): boolean {
  return Boolean(result && typeof result === 'object' && (result as { ok?: unknown }).ok === true);
}

function syncResultErrorMessage(result: unknown): string {
  // 2026-11-06: cap banner length — a raw upstream transport stack (EM
  // RemoteDisconnected × requests/curl/urllib) must never again render as an
  // enormous red banner. Full diagnostics stay in the per-step meta.
  const truncate = (s: string) =>
    s.length > 300 ? `${s.slice(0, 300)}…（详情见同步步骤）` : s;
  if (!result || typeof result !== 'object') return 'Sync returned an invalid result';
  const record = result as {
    error?: unknown;
    message?: unknown;
    steps?: unknown;
  };
  if (typeof record.error === 'string' && record.error) return truncate(record.error);
  if (typeof record.message === 'string' && record.message) return truncate(record.message);
  if (Array.isArray(record.steps)) {
    for (const step of record.steps) {
      if (!step || typeof step !== 'object') continue;
      const detail = step as { ok?: unknown; message?: unknown };
      if (detail.ok === false && typeof detail.message === 'string' && detail.message) {
        return truncate(detail.message);
      }
    }
  }
  return 'Sync failed';
}

/**
 * Chinese degraded note for a completed-with-degradation Sync All result
 * (2026-11-06: East Money upstream unavailable). Returns null when the
 * result is not degraded. Pure helper — never affects the ok contract.
 */
export function dashboardSyncDegradedNote(result: unknown): string | null {
  if (!result || typeof result !== 'object') return null;
  const record = result as { degraded?: unknown; degradedMessage?: unknown; warnings?: unknown };
  if (record.degraded === true) {
    if (typeof record.degradedMessage === 'string' && record.degradedMessage) {
      return record.degradedMessage;
    }
    return '同步完成（部分降级）：部分上游数据源暂时不可用，已有有效数据予以保留、缺失仍记为缺失；源恢复后自动恢复。';
  }
  if (Array.isArray(record.warnings) && record.warnings.length > 0) {
    const shown = record.warnings
      .filter((w): w is string => typeof w === 'string' && w.length > 0)
      .slice(0, 2);
    if (!shown.length) return null;
    const extra = record.warnings.length > 2 ? `（等 ${record.warnings.length} 项）` : '';
    return `同步完成（部分降级）：${shown.join('；')}${extra}`;
  }
  return null;
}

export type SyncStep = {
  name: string;
  ok: boolean | null;
  durationMs: number | null;
  message?: string | null;
  /** Soft-degraded sub-step notes (e.g. Put IV) — never flips overall ok. */
  warnings?: string[] | null;
};

/**
 * Collect soft warnings from a Sync All result without affecting ok.
 * Reads the top-level `warnings` list plus per-step `meta.warnings`
 * (e.g. `option_iv: no_iv_data` when Put IV falls back to the DB value).
 */
export function dashboardSyncWarnings(result: unknown): string[] {
  const out: string[] = [];
  if (!result || typeof result !== 'object') return out;
  const record = result as { warnings?: unknown; steps?: unknown };
  if (Array.isArray(record.warnings)) {
    for (const w of record.warnings) {
      if (typeof w === 'string' && w) out.push(w);
    }
  }
  if (Array.isArray(record.steps)) {
    for (const step of record.steps) {
      if (!step || typeof step !== 'object') continue;
      const meta = (step as { meta?: unknown }).meta;
      if (!meta || typeof meta !== 'object') continue;
      const warnings = (meta as { warnings?: unknown }).warnings;
      if (Array.isArray(warnings)) {
        for (const w of warnings) {
          if (typeof w === 'string' && w) out.push(w);
        }
      }
    }
  }
  return out;
}

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
  /** Fired after Sync All completes successfully. */
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
              meta?: { warnings?: unknown };
            };
            const stepWarnings = Array.isArray(step.meta?.warnings)
              ? (step.meta.warnings as unknown[]).filter(
                  (w): w is string => typeof w === 'string' && w.length > 0,
                )
              : null;
            setSyncSteps((prev) => {
              const updated = prev.map((s) =>
                s.name === step.name ? { ...step, warnings: stepWarnings } : s,
              );
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
            const cb = callbacksRef.current;
            const fail = (message: string) => {
              setSyncSteps((prev) =>
                prev.map((st) =>
                  st.name === 'watchlist' && st.ok === null
                    ? { ...st, ok: false, message: 'skipped after sync failure' }
                    : st,
                ),
              );
              setSyncProgress(100);
              setBusy(false);
              cb.setError(message);
              resolve({ ok: false, summary: null });
            };

            if (!isDashboardSyncResultOk(result)) {
              fail(syncResultErrorMessage(result));
              return;
            }

            const s = data.summary as DashboardSummary | null | undefined;
            if (!s) {
              fail('Sync returned no dashboard summary');
              return;
            }

            setSyncProgress(Math.round(((stepNames.length - 1) / stepNames.length) * 100));
            setSyncSteps((prev) =>
              prev.map((st) =>
                st.name === 'watchlist' && st.ok === null
                  ? { ...st, ok: null, message: 'pulling HK/ETF K-lines…' }
                  : st,
              ),
            );

            const finalize = (watchlistOk: boolean) => {
              if (!watchlistOk) {
                setSyncSteps((prev) =>
                  prev.map((st) =>
                    st.name === 'watchlist' && st.ok === null
                      ? { ...st, ok: false, durationMs: 0, message: 'refresh failed' }
                      : st,
                  ),
                );
                setSyncProgress(100);
                setBusy(false);
                cb.setError('Watchlist refresh failed after sync');
                resolve({ ok: false, summary: null });
                return;
              }
              setSyncSteps((prev) =>
                prev.map((st) =>
                  st.name === 'watchlist' && st.ok === null
                    ? { ...st, ok: true, durationMs: 0, message: 'done' }
                    : st,
                ),
              );
              setSyncProgress(100);
              setBusy(false);
              cb.applySummaryToCache(s);
              cb.onSyncComplete?.();
              resolve({ ok: true, summary: s });
            };

            const afterWatchlist = async () => {
              let watchlistOk = true;
              try {
                if (cb.forceRefreshWatchlistOnSync) {
                  await cb.forceRefreshWatchlistOnSync();
                }
              } catch (e) {
                watchlistOk = false;
                console.warn('forceRefreshWatchlistOnSync failed:', e);
              }
              finalize(watchlistOk);
            };

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
                    typeof aiData?.summary === 'string' ? stripModelThinking(aiData.summary) : '';
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
