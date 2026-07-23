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

  async function onSyncAll(): Promise<{ ok: boolean; summary: DashboardSummary | null }> {
    setBusy(true);
    callbacksRef.current.setError(null);
    setSyncSteps([]);
    setSyncProgress(0);

    const stepNames = ['industryFundFlow', 'marketSentiment', 'screeners', 'news'];
    // Sync All is an explicit user action — always force catch-up for stale modules.
    const forceSync = true;

    return new Promise<{ ok: boolean; summary: DashboardSummary | null }>((resolve) => {
      const es = new EventSource(
        `${DATA_SYNC_BASE_URL}/dashboard/sync/stream?force=${forceSync ? 'true' : 'false'}`,
      );

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
            es.close();
            const result = data.result as DashboardSyncResp;
            setSyncResp(result);
            setSyncProgress(100);

            const cb = callbacksRef.current;
            const s = data.summary as DashboardSummary;
            if (s) {
              cb.applySummaryToCache(s);
              const newsData = (s as any)?.news;
              if (newsData && Array.isArray(newsData.items) && newsData.items.length > 0) {
                if (!cb.shouldRefreshNewsBrief(cb.newsSummaryUpdatedAt) && cb.newsSummary?.trim()) {
                  setBusy(false);
                  cb.onSyncComplete?.();
                  resolve({ ok: true, summary: s });
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
                  .catch(() => {})
                  .finally(() => {
                    cb.setNewsSummaryBusy(false);
                    setBusy(false);
                    cb.onSyncComplete?.();
                    resolve({ ok: true, summary: s });
                  });
              } else {
                setBusy(false);
                cb.onSyncComplete?.();
                resolve({ ok: true, summary: s });
              }
            } else {
              setBusy(false);
              cb.onSyncComplete?.();
              resolve({ ok: true, summary: null });
            }
          }
        } catch {
          // ignore parse errors
        }
      };

      es.onerror = () => {
        es.close();
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
