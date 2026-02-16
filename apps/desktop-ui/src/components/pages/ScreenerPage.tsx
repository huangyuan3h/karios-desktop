'use client';

import * as React from 'react';
import { ExternalLink, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

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
  filters: string[];
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

type TvHistoryCell = {
  snapshotId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  filters: string[];
};

type TvHistoryDayRow = {
  date: string;
  am: TvHistoryCell | null;
  pm: TvHistoryCell | null;
};

type TvHistoryResponse = {
  screenerId: string;
  screenerName: string;
  days: number;
  rows: TvHistoryDayRow[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

async function apiPostJson<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    const maybeDetail = (() => {
      try {
        const j = JSON.parse(txt) as { detail?: string };
        return j && typeof j.detail === "string" ? j.detail : null;
      } catch {
        return null;
      }
    })();
    if (maybeDetail) throw new Error(maybeDetail);
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

function escapeMarkdownCell(value: string): string {
  return value.replace(/\|/g, '\\|').replace(/\r?\n/g, '<br>').trim();
}

function toMarkdownTable(headers: string[], rows: Record<string, string>[]): string {
  if (!headers.length) return '';
  const safeHeaders = headers.map((h) => escapeMarkdownCell(h || ''));
  const headerLine = `| ${safeHeaders.join(' | ')} |`;
  const dividerLine = `| ${safeHeaders.map(() => '---').join(' | ')} |`;
  const body = rows.map((r) => {
    const cells = headers.map((h) => escapeMarkdownCell(r[h] ?? ''));
    return `| ${cells.join(' | ')} |`;
  });
  return [headerLine, dividerLine, ...body].join('\n');
}

export function ScreenerPage() {
  const { addReference } = useChatStore();
  const [screeners, setScreeners] = React.useState<TvScreener[]>([]);
  const [snapshots, setSnapshots] = React.useState<Record<string, TvSnapshotDetail | null>>({});
  const [history, setHistory] = React.useState<Record<string, TvHistoryResponse | null>>({});
  const [historyOpen, setHistoryOpen] = React.useState<Record<string, boolean>>({});
  const [busyId, setBusyId] = React.useState<string | null>(null);
  const [busyAll, setBusyAll] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [copyStatus, setCopyStatus] = React.useState<{ id: string; ok: boolean; text: string } | null>(null);
  const copyTimerRef = React.useRef<number | null>(null);

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
      // Keep history cache; user can open on demand.
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refreshAll();
  }, [refreshAll]);

  React.useEffect(
    () => () => {
      if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current);
    },
    [],
  );

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

  async function syncAll() {
    setBusyAll(true);
    setBusyId(null);
    setError(null);
    const failures: Array<{ id: string; name: string; error: string }> = [];
    try {
      // Serial sync to avoid overloading CDP/TradingView.
      for (const sc of screeners) {
        try {
          await apiPostJson<{ snapshotId: string }>(
            `/integrations/tradingview/screeners/${encodeURIComponent(sc.id)}/sync`,
          );
        } catch (e) {
          failures.push({
            id: sc.id,
            name: sc.name,
            error: e instanceof Error ? e.message : String(e),
          });
        }
      }
      await refreshAll();
      if (failures.length) {
        setError(
          `Sync all finished with ${failures.length} error(s): ` +
            failures
              .slice(0, 3)
              .map((x) => `${x.name}(${x.id}): ${x.error}`)
              .join(' | '),
        );
      }
    } finally {
      setBusyAll(false);
    }
  }

  async function ensureHistoryLoaded(screenerId: string) {
    if (history[screenerId] !== undefined) return;
    try {
      const h = await apiGetJson<TvHistoryResponse>(
        `/integrations/tradingview/screeners/${encodeURIComponent(screenerId)}/history?days=10`,
      );
      setHistory((prev) => ({ ...prev, [screenerId]: h }));
    } catch (e) {
      setHistory((prev) => ({ ...prev, [screenerId]: null }));
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function setCopyToast(id: string, ok: boolean, text: string) {
    setCopyStatus({ id, ok, text });
    if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current);
    copyTimerRef.current = window.setTimeout(() => setCopyStatus(null), 2400);
  }

  async function copySnapshotMarkdown(
    screenerId: string,
    headers: string[],
    rows: Record<string, string>[],
  ) {
    const md = toMarkdownTable(headers, rows);
    if (!md) {
      setCopyToast(screenerId, false, 'No data to copy.');
      return;
    }
    try {
      await navigator.clipboard.writeText(md);
      setCopyToast(screenerId, true, 'Copied Markdown table.');
    } catch {
      setCopyToast(screenerId, false, 'Copy failed. Please allow clipboard access.');
    }
  }

  async function copyAllMarkdown() {
    const parts: string[] = [];
    for (const sc of screeners) {
      const snap = snapshots[sc.id] ?? null;
      if (!snap) continue;
      const cols = pickColumns(snap.headers);
      if (!cols.length || !snap.rows.length) continue;
      parts.push(`## ${sc.name}`);
      parts.push(`- capturedAt: ${new Date(snap.capturedAt).toLocaleString()}`);
      parts.push(`- url: ${sc.url}`);
      parts.push('');
      parts.push(toMarkdownTable(cols, snap.rows));
      parts.push('');
    }
    const md = parts.join('\n').trim();
    if (!md) {
      setCopyToast('__all__', false, 'No tables to copy.');
      return;
    }
    try {
      await navigator.clipboard.writeText(md);
      setCopyToast('__all__', true, 'Copied all Markdown tables.');
    } catch {
      setCopyToast('__all__', false, 'Copy failed. Please allow clipboard access.');
    }
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-6 flex w-full items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-lg font-semibold">Screener</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Sync TradingView screeners and review latest snapshots.
          </div>
        </div>
        <div className="flex items-center justify-end gap-2">
          <Button variant="secondary" size="sm" onClick={() => void refreshAll()} disabled={busyAll}>
            Refresh
          </Button>
          <Button size="sm" onClick={() => void syncAll()} disabled={busyAll || screeners.length === 0} className="gap-2">
            <RefreshCw className={busyAll ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            {busyAll ? 'Syncing…' : 'Sync all'}
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void copyAllMarkdown()}
            disabled={screeners.length === 0}
          >
            Copy Markdown all
          </Button>
        </div>
      </div>

      {copyStatus?.id === '__all__' ? (
        <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm">
          <span className={copyStatus.ok ? 'text-emerald-600' : 'text-red-600'}>{copyStatus.text}</span>
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <div className="grid gap-4">
        {screeners.map((it) => {
          const snap = snapshots[it.id] ?? null;
          const cols = snap ? pickColumns(snap.headers) : [];
          const busy = busyAll || busyId === it.id;
          const showHist = Boolean(historyOpen[it.id]);
          const hist = history[it.id] ?? undefined;
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
                    onClick={() => {
                      setHistoryOpen((prev) => ({ ...prev, [it.id]: !prev[it.id] }));
                      void ensureHistoryLoaded(it.id);
                    }}
                    disabled={busyAll}
                  >
                    {showHist ? 'Hide history' : 'History'}
                  </Button>
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={!snap || cols.length === 0 || snap.rows.length === 0}
                    onClick={() => {
                      if (!snap) return;
                      void copySnapshotMarkdown(it.id, cols, snap.rows);
                    }}
                  >
                    Copy Markdown
                  </Button>
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={!snap}
                    onClick={() => {
                      if (!snap) return;
                      addReference({
                        kind: 'tv',
                        refId: snap.id,
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
                  {copyStatus?.id === it.id ? (
                    <div className="border-b border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs">
                      <span className={copyStatus.ok ? 'text-emerald-600' : 'text-red-600'}>{copyStatus.text}</span>
                    </div>
                  ) : null}
                  {snap.filters?.length ? (
                    <div className="border-b border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
                      <div className="flex flex-wrap gap-2">
                        {snap.filters.map((f) => (
                          <span
                            key={f}
                            className="max-w-full truncate rounded-full border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5 text-xs text-[var(--k-muted)]"
                            title={f}
                          >
                            {f}
                          </span>
                        ))}
                      </div>
                    </div>
                  ) : null}
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

              {showHist ? (
                <div className="mt-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                  <div className="mb-2 text-sm font-medium">History (last 10 days, AM/PM)</div>
                  {hist === undefined ? (
                    <div className="text-xs text-[var(--k-muted)]">Loading…</div>
                  ) : hist === null ? (
                    <div className="text-xs text-[var(--k-muted)]">Failed to load history.</div>
                  ) : (
                    <div className="overflow-auto rounded border border-[var(--k-border)]">
                      <table className="w-full border-collapse text-xs">
                        <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                          <tr className="text-left">
                            <th className="px-2 py-2">Date</th>
                            <th className="px-2 py-2">AM</th>
                            <th className="px-2 py-2">PM</th>
                          </tr>
                        </thead>
                        <tbody>
                          {hist.rows.map((r) => (
                            <tr key={r.date} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-2 font-mono">{r.date}</td>
                              {(['am', 'pm'] as const).map((slot) => {
                                const cell = slot === 'am' ? r.am : r.pm;
                                return (
                                  <td key={slot} className="px-2 py-2">
                                    {cell ? (
                                      <div className="flex items-center justify-between gap-2">
                                        <div className="min-w-0">
                                          <div className="truncate font-mono">
                                            {new Date(cell.capturedAt).toLocaleString()} • {cell.rowCount} rows
                                          </div>
                                          {cell.filters?.length ? (
                                            <div className="mt-1 truncate text-[var(--k-muted)]">
                                              {cell.filters.slice(0, 3).join(' | ')}
                                            </div>
                                          ) : null}
                                        </div>
                                        <Button
                                          size="icon"
                                          variant="ghost"
                                          className="h-7 w-7"
                                          title="Reference to chat"
                                          aria-label="Reference to chat"
                                          onClick={() => {
                                            addReference({
                                              kind: 'tv',
                                              refId: cell.snapshotId,
                                              snapshotId: cell.snapshotId,
                                              screenerId: it.id,
                                              screenerName: it.name,
                                              capturedAt: cell.capturedAt,
                                            });
                                          }}
                                        >
                                          <ExternalLink className="h-4 w-4" />
                                        </Button>
                                      </div>
                                    ) : (
                                      <div className="text-[var(--k-muted)]">—</div>
                                    )}
                                  </td>
                                );
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
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


