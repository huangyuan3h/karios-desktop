'use client';

import * as React from 'react';
import { Plus, RefreshCw, Trash2, Pencil } from 'lucide-react';

import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { Button } from '@/components/ui/button';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

type TradeJournal = {
  id: string;
  title: string;
  contentMd: string;
  createdAt: string;
  updatedAt: string;
};

type ListTradeJournalsResponse = {
  total: number;
  items: TradeJournal[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { method: 'DELETE' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
}

function fmtTsSimple(ts: string | null | undefined): string {
  const s = String(ts ?? '').trim();
  if (!s) return '—';
  const m = s.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})/);
  if (m) return `${m[1]} ${m[2]}`;
  return s.replace('T', ' ').replace(/\.\d+.*$/, '');
}

export function JournalReadPage({
  activeId,
  onEdit,
}: {
  activeId: string | null;
  onEdit: (id: string) => void;
}) {
  const [items, setItems] = React.useState<TradeJournal[]>([]);
  const [selectedId, setSelectedId] = React.useState<string | null>(activeId);
  const [selected, setSelected] = React.useState<TradeJournal | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const refreshList = React.useCallback(async () => {
    const r = await apiGetJson<ListTradeJournalsResponse>('/journals?limit=200&offset=0');
    const xs = Array.isArray(r.items) ? r.items : [];
    setItems(xs);
    return xs;
  }, []);

  async function onCreate() {
    setBusy(true);
    setError(null);
    try {
      const today = new Date().toISOString().slice(0, 10);
      const j = await apiPostJson<TradeJournal>('/journals', {
        title: `Trading Journal ${today}`,
        contentMd: '',
      });
      await refreshList();
      setSelectedId(j.id);
      setSelected(j);
      onEdit(j.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onDeleteSelected() {
    if (!selectedId) return;
    const ok = window.confirm('Delete this journal entry?');
    if (!ok) return;
    setBusy(true);
    setError(null);
    try {
      await apiDelete(`/journals/${encodeURIComponent(selectedId)}`);
      const xs = await refreshList();
      const nextId = xs[0]?.id ?? null;
      setSelectedId(nextId);
      setSelected(nextId ? xs.find((x) => x.id === nextId) ?? null : null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  React.useEffect(() => {
    void (async () => {
      setError(null);
      setBusy(true);
      try {
        const xs = await refreshList();
        const nextId = selectedId || activeId || xs[0]?.id || null;
        setSelectedId(nextId);
        const found = nextId ? xs.find((x) => x.id === nextId) ?? null : null;
        setSelected(found);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    if (!items.length) {
      setSelected(null);
      return;
    }
    const found = selectedId ? items.find((x) => x.id === selectedId) ?? null : null;
    setSelected(found);
  }, [items, selectedId]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Journal</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">Browse and read your past notes.</div>
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="secondary" onClick={() => void onCreate()} disabled={busy} className="gap-2">
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void refreshList()}
            disabled={busy}
            className="gap-2"
          >
            <RefreshCw className={busy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            Refresh
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onDeleteSelected()}
            disabled={busy || !selectedId}
            className="gap-2"
          >
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
          <Button
            size="sm"
            onClick={() => {
              if (!selectedId) return;
              onEdit(selectedId);
            }}
            disabled={!selectedId}
            className="gap-2"
          >
            <Pencil className="h-4 w-4" />
            Edit
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-12">
        <section className="md:col-span-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 flex items-center justify-between">
            <div className="text-sm font-medium">History</div>
            <div className="text-xs text-[var(--k-muted)]">{items.length} items</div>
          </div>
          <div className="overflow-auto rounded border border-[var(--k-border)]">
            <div className="divide-y divide-[var(--k-border)]">
              {items.map((it) => {
                const active = it.id === selectedId;
                return (
                  <button
                    key={it.id}
                    type="button"
                    className={[
                      'w-full px-3 py-2 text-left text-sm',
                      'hover:bg-[var(--k-surface-2)]',
                      active ? 'bg-[var(--k-surface-2)]' : 'bg-[var(--k-surface)]',
                    ].join(' ')}
                    onClick={() => setSelectedId(it.id)}
                  >
                    <div className="truncate font-medium">{it.title || 'Untitled'}</div>
                    <div className="mt-0.5 text-xs text-[var(--k-muted)]">{fmtTsSimple(it.updatedAt)}</div>
                  </button>
                );
              })}
              {!items.length ? (
                <div className="px-3 py-3 text-sm text-[var(--k-muted)]">No entries yet.</div>
              ) : null}
            </div>
          </div>
        </section>

        <section className="md:col-span-8 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          {selected ? (
            <>
              <div className="mb-2">
                <div className="text-base font-semibold">{selected.title || 'Untitled'}</div>
                <div className="mt-1 text-xs text-[var(--k-muted)]">
                  Updated: {fmtTsSimple(selected.updatedAt)} • Created: {fmtTsSimple(selected.createdAt)}
                </div>
              </div>
              <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                <MarkdownMessage content={selected.contentMd || ''} className="text-sm" />
              </div>
            </>
          ) : (
            <div className="text-sm text-[var(--k-muted)]">Select an entry to read.</div>
          )}
        </section>
      </div>
    </div>
  );
}

