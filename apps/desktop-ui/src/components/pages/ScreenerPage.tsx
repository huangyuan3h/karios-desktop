'use client';

import * as React from 'react';
import { ExternalLink, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';
import { QUANT_BASE_URL } from '@/lib/endpoints';

type TvScreener = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  updatedAt: string;
};

type TvSnapshotSummary = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
};

type TvSnapshotDetail = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function apiPostJson<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    try {
      const j = JSON.parse(txt) as { detail?: string };
      if (j && typeof j.detail === 'string' && j.detail) {
        throw new Error(j.detail);
      }
    } catch {
      // ignore
    }
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (await res.json()) as T;
}

function pickColumns(headers: string[]) {
  const preferred = [
    'Ticker',
    'Name',
    'Symbol',
    'Price',
    'Change %',
    'Rel Volume',
    'Rel Volume 1W',
    'Market cap',
    'Sector',
    'Analyst Rating',
    'RSI (14)',
  ];
  const set = new Set(headers);
  const picked = preferred.filter((h) => set.has(h));
  const rest = headers.filter((h) => !picked.includes(h));
  return [...picked, ...rest].slice(0, 8);
}

export function ScreenerPage() {
  const { addReference } = useChatStore();
  const [screeners, setScreeners] = React.useState<TvScreener[]>([]);
  const [snapshots, setSnapshots] = React.useState<Record<string, TvSnapshotDetail | null>>({});
  const [busyId, setBusyId] = React.useState<string | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const refreshAll = React.useCallback(async () => {
    setError(null);
    try {
      const s = await apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners');
      const enabled = s.items.filter((x) => x.enabled);
      setScreeners(enabled);

      const next: Record<string, TvSnapshotDetail | null> = {};
      for (const it of enabled) {
        const list = await apiGetJson<{ items: TvSnapshotSummary[] }>(
          `/integrations/tradingview/screeners/${encodeURIComponent(it.id)}/snapshots?limit=1`,
        );
        const latest = list.items[0];
        if (!latest) {
          next[it.id] = null;
          continue;
        }
        next[it.id] = await apiGetJson<TvSnapshotDetail>(
          `/integrations/tradingview/snapshots/${encodeURIComponent(latest.id)}`,
        );
      }
      setSnapshots(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refreshAll();
  }, [refreshAll]);

  async function syncOne(screener: TvScreener) {
    setBusyId(screener.id);
    setError(null);
    try {
      await apiPostJson<{ snapshotId: string }>(
        `/integrations/tradingview/screeners/${encodeURIComponent(screener.id)}/sync`,
      );
      await refreshAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Screener</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Sync TradingView screeners and review latest snapshots.
          </div>
        </div>
        <Button variant="secondary" size="sm" onClick={() => void refreshAll()}>
          Refresh
        </Button>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="grid gap-4">
        {screeners.map((it) => {
          const snap = snapshots[it.id] ?? null;
          const cols = snap ? pickColumns(snap.headers) : [];
          const busy = busyId === it.id;
          return (
            <section
              key={it.id}
              className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="font-medium">{it.name}</div>
                  <div className="mt-1 flex items-center gap-2">
                    <div className="truncate font-mono text-xs text-[var(--k-muted)]">{it.url}</div>
                    <a
                      className="text-[var(--k-muted)] hover:text-[var(--k-text)]"
                      href={it.url}
                      target="_blank"
                      rel="noreferrer"
                      title="Open in browser"
                    >
                      <ExternalLink className="h-4 w-4" />
                    </a>
                  </div>
                  {snap ? (
                    <div className="mt-1 text-xs text-[var(--k-muted)]">
                      Latest: {new Date(snap.capturedAt).toLocaleString()} • {snap.rowCount} rows
                    </div>
                  ) : (
                    <div className="mt-1 text-xs text-[var(--k-muted)]">No snapshot yet.</div>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    onClick={() => void syncOne(it)}
                    disabled={busy}
                    className="gap-2"
                  >
                    <RefreshCw className="h-4 w-4" />
                    Sync
                  </Button>
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={!snap}
                    onClick={() => {
                      if (!snap) return;
                      addReference({
                        snapshotId: snap.id,
                        screenerId: it.id,
                        screenerName: it.name,
                        capturedAt: snap.capturedAt,
                      });
                    }}
                  >
                    Reference to chat
                  </Button>
                </div>
              </div>

              {snap ? (
                <div className="mt-4 overflow-hidden rounded-lg border border-[var(--k-border)]">
                  <div className="max-h-[420px] overflow-auto">
                    <table className="w-full border-collapse text-sm">
                      <thead className="sticky top-0 bg-[var(--k-surface-2)]">
                        <tr className="text-left text-xs text-[var(--k-muted)]">
                          {cols.map((h) => (
                            <th key={h} className="whitespace-nowrap px-3 py-2">
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {snap.rows.map((r, idx) => (
                          <tr key={idx} className="border-t border-[var(--k-border)]">
                            {cols.map((h) => (
                              <td
                                key={h}
                                className="max-w-[280px] truncate px-3 py-2 font-mono text-xs"
                                title={r[h] ?? ''}
                              >
                                {r[h] ?? ''}
                              </td>
                            ))}
                          </tr>
                        ))}
                        {snap.rows.length === 0 ? (
                          <tr>
                            <td
                              className="px-3 py-6 text-center text-sm text-[var(--k-muted)]"
                              colSpan={cols.length || 1}
                            >
                              Empty snapshot.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
            </section>
          );
        })}

        {screeners.length === 0 ? (
          <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-6 text-center text-sm text-[var(--k-muted)]">
            No enabled screeners. Configure them in Settings first.
          </div>
        ) : null}
      </div>
    </div>
  );
}


