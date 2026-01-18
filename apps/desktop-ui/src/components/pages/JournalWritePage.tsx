'use client';

import * as React from 'react';
import { Plus, Save, Trash2 } from 'lucide-react';

import { PlateJournalEditor } from '@/components/journal/PlateJournalEditor';
import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';

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
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { method: 'DELETE' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
}

function fmtTs(ts: string | null | undefined): string {
  const s = String(ts ?? '').trim();
  return s || '—';
}

export function JournalWritePage({
  journalId,
  onJournalIdChange,
}: {
  journalId: string | null;
  onJournalIdChange: (id: string | null) => void;
}) {
  const [title, setTitle] = React.useState('');
  const [contentMd, setContentMd] = React.useState('');
  const [createdAt, setCreatedAt] = React.useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const editorKey = journalId || 'new';

  const loadOne = React.useCallback(async (id: string) => {
    const j = await apiGetJson<TradeJournal>(`/journals/${encodeURIComponent(id)}`);
    onJournalIdChange(j.id);
    setTitle(j.title || '');
    setContentMd(j.contentMd || '');
    setCreatedAt(j.createdAt || null);
    setUpdatedAt(j.updatedAt || null);
  }, [onJournalIdChange]);

  React.useEffect(() => {
    void (async () => {
      setError(null);
      try {
        if (journalId) {
          await loadOne(journalId);
          return;
        }
        // Default: load latest if exists, otherwise keep empty state.
        const r = await apiGetJson<ListTradeJournalsResponse>('/journals?limit=1&offset=0');
        const first = Array.isArray(r.items) ? r.items[0] : undefined;
        if (first?.id) {
          await loadOne(first.id);
        } else {
          onJournalIdChange(null);
          setTitle(`Trading Journal ${new Date().toISOString().slice(0, 10)}`);
          setContentMd('');
          setCreatedAt(null);
          setUpdatedAt(null);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    })();
  }, [journalId, loadOne, onJournalIdChange]);

  async function onCreate() {
    setBusy(true);
    setError(null);
    try {
      const today = new Date().toISOString().slice(0, 10);
      const j = await apiPostJson<TradeJournal>('/journals', {
        title: `Trading Journal ${today}`,
        contentMd: '',
      });
      await loadOne(j.id);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onSave() {
    setBusy(true);
    setError(null);
    try {
      if (!journalId) {
        const j = await apiPostJson<TradeJournal>('/journals', {
          title,
          contentMd,
        });
        await loadOne(j.id);
        return;
      }
      const j = await apiPutJson<TradeJournal>(`/journals/${encodeURIComponent(journalId)}`, {
        title,
        contentMd,
      });
      setTitle(j.title || '');
      setContentMd(j.contentMd || '');
      setCreatedAt(j.createdAt || null);
      setUpdatedAt(j.updatedAt || null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onDelete() {
    if (!journalId) return;
    setBusy(true);
    setError(null);
    try {
      await apiDelete(`/journals/${encodeURIComponent(journalId)}`);
      onJournalIdChange(null);
      setTitle(`Trading Journal ${new Date().toISOString().slice(0, 10)}`);
      setContentMd('');
      setCreatedAt(null);
      setUpdatedAt(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Write</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">Write your trading diary with a Plate toolbar.</div>
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="secondary" onClick={() => void onCreate()} disabled={busy} className="gap-2">
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button size="sm" onClick={() => void onSave()} disabled={busy} className="gap-2">
            <Save className="h-4 w-4" />
            Save
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onDelete()}
            disabled={busy || !journalId}
            className="gap-2"
            title="Delete this entry"
          >
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>

      <section className="mb-3 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="grid gap-2 md:grid-cols-12">
          <div className="md:col-span-8">
            <div className="text-xs text-[var(--k-muted)]">Title</div>
            <input
              className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
              placeholder="Title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
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
      </section>

      <PlateJournalEditor
        key={editorKey}
        initialMarkdown={contentMd || ''}
        onMarkdownChange={(md) => setContentMd(md)}
      />
    </div>
  );
}

