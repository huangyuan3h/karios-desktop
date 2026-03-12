'use client';

import * as React from 'react';
import { Plus, Save, Trash2 } from 'lucide-react';

import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
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

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'PUT',
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

function fmtTs(ts: string | null | undefined): string {
  const s = String(ts ?? '').trim();
  return s || '—';
}

export function JournalPage() {
  const [items, setItems] = React.useState<TradeJournal[]>([]);
  const [selectedId, setSelectedId] = React.useState<string | null>(null);
  const [title, setTitle] = React.useState('');
  const [contentMd, setContentMd] = React.useState('');
  const [createdAt, setCreatedAt] = React.useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const refreshList = React.useCallback(async () => {
    const r = await apiGetJson<ListTradeJournalsResponse>('/journals?limit=200&offset=0');
    const xs = Array.isArray(r.items) ? r.items : [];
    setItems(xs);
    return xs;
  }, []);

  const loadOne = React.useCallback(async (id: string) => {
    const j = await apiGetJson<TradeJournal>(`/journals/${encodeURIComponent(id)}`);
    setSelectedId(j.id);
    setTitle(j.title || '');
    setContentMd(j.contentMd || '');
    setCreatedAt(j.createdAt || null);
    setUpdatedAt(j.updatedAt || null);
  }, []);

  React.useEffect(() => {
    void (async () => {
      setError(null);
      try {
        const xs = await refreshList();
        if (xs.length && !selectedId) {
          await loadOne(xs[0].id);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
      await loadOne(j.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onSave() {
    if (!selectedId) return;
    setBusy(true);
    setError(null);
    try {
      const j = await apiPutJson<TradeJournal>(`/journals/${encodeURIComponent(selectedId)}`, {
        title,
        contentMd,
      });
      setTitle(j.title || '');
      setContentMd(j.contentMd || '');
      setCreatedAt(j.createdAt || null);
      setUpdatedAt(j.updatedAt || null);
      await refreshList();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onDelete() {
    if (!selectedId) return;
    setBusy(true);
    setError(null);
    try {
      await apiDelete(`/journals/${encodeURIComponent(selectedId)}`);
      const xs = await refreshList();
      setSelectedId(null);
      setTitle('');
      setContentMd('');
      setCreatedAt(null);
      setUpdatedAt(null);
      if (xs.length) await loadOne(xs[0].id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Trading Journal</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Write your own trading diary. Markdown supported.
          </div>
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="secondary" onClick={() => void onCreate()} disabled={busy}>
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button size="sm" onClick={() => void onSave()} disabled={busy || !selectedId}>
            <Save className="h-4 w-4" />
            Save
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onDelete()}
            disabled={busy || !selectedId}
            title="Delete this entry"
          >
            <Trash2 className="h-4 w-4" />
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
                    onClick={() => void loadOne(it.id)}
                  >
                    <div className="truncate font-medium">{it.title || 'Untitled'}</div>
                    <div className="mt-0.5 text-xs text-[var(--k-muted)]">{fmtTs(it.updatedAt)}</div>
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
          <div className="grid gap-2">
            <div className="grid gap-2 md:grid-cols-12">
              <div className="md:col-span-8">
                <div className="text-xs text-[var(--k-muted)]">Title</div>
                <input
                  className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                  placeholder="Title"
                  value={title}
                  onChange={(e) => setTitle(e.target.value)}
                  disabled={!selectedId}
                />
              </div>
              <div className="md:col-span-4">
                <div className="text-xs text-[var(--k-muted)]">Time</div>
                <div className="mt-1 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
                  <div>Created: {fmtTs(createdAt)}</div>
                  <div>Updated: {fmtTs(updatedAt)}</div>
                </div>
              </div>
            </div>

            <Tabs defaultValue="write">
              <TabsList>
                <TabsTrigger value="write">Write</TabsTrigger>
                <TabsTrigger value="preview">Preview</TabsTrigger>
              </TabsList>
              <TabsContent value="write">
                <Textarea
                  className="min-h-[420px]"
                  placeholder="Write your journal in Markdown..."
                  value={contentMd}
                  onChange={(e) => setContentMd(e.target.value)}
                  disabled={!selectedId}
                />
                <div className="mt-2 text-xs text-[var(--k-muted)]">
                  Tip: Use Markdown. Preview tab renders exactly what you saved.
                </div>
              </TabsContent>
              <TabsContent value="preview">
                <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                  <MarkdownMessage content={contentMd || ''} className="text-sm" />
                </div>
              </TabsContent>
            </Tabs>
          </div>
        </section>
      </div>
    </div>
  );
}

