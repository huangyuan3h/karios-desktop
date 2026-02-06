'use client';

import * as React from 'react';
import { ArrowLeft, Save } from 'lucide-react';

import { PlateJournalEditor } from '@/components/journal/PlateJournalEditor';
import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';
import type { ChatReference } from '@/lib/chat/types';

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

function fmtTsSimple(ts: string | null | undefined): string {
  const s = String(ts ?? '').trim();
  if (!s) return '—';
  const m = s.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})/);
  if (m) return `${m[1]} ${m[2]}`;
  // Fallback: best-effort for already pretty strings.
  return s.replace('T', ' ').replace(/\.\d+.*$/, '');
}

export function JournalWritePage({
  journalId,
  onJournalIdChange,
  onExit,
}: {
  journalId: string | null;
  onJournalIdChange: (id: string | null) => void;
  onExit: () => void;
}) {
  const { addReference, setAgent } = useChatStore();
  const [title, setTitle] = React.useState('');
  const [contentMd, setContentMd] = React.useState('');
  const [createdAt, setCreatedAt] = React.useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = React.useState<string | null>(null);
  // Plate editor only uses initialMarkdown on mount; bump to remount after async load.
  const [editorRev, setEditorRev] = React.useState(0);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const editorKey = journalId || 'new';

  const loadOne = React.useCallback(
    async (id: string) => {
      const j = await apiGetJson<TradeJournal>(`/journals/${encodeURIComponent(id)}`);
      onJournalIdChange(j.id);
      setTitle(j.title || '');
      setContentMd(j.contentMd || '');
      setCreatedAt(j.createdAt || null);
      setUpdatedAt(j.updatedAt || null);
      setEditorRev((x) => x + 1);
    },
    [onJournalIdChange],
  );

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
          setEditorRev((x) => x + 1);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    })();
  }, [journalId, loadOne, onJournalIdChange]);

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
      setEditorRev((x) => x + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const handleReference = React.useCallback(
    (content: string) => {
      if (!content.trim()) return;

      const currentJournalId = journalId || 'new';
      const journalTitle = title.trim() || 'Untitled Journal';
      const now = new Date().toISOString();

      const reference: ChatReference = {
        kind: 'journal',
        refId: `journal:${currentJournalId}:${now}`,
        journalId: currentJournalId,
        title: journalTitle,
        content: content,
        capturedAt: now,
      };

      addReference(reference);

      // Open agent panel if not already visible
      setAgent((prev) => ({ ...prev, visible: true, historyOpen: false }));
    },
    [journalId, title, addReference, setAgent],
  );

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Write</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Write your trading diary with a Plate toolbar.
          </div>
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button
            size="sm"
            variant="secondary"
            onClick={() => onExit()}
            disabled={busy}
            className="gap-2"
          >
            <ArrowLeft className="h-4 w-4" />
            Exit
          </Button>
          <Button size="sm" onClick={() => void onSave()} disabled={busy} className="gap-2">
            <Save className="h-4 w-4" />
            Save
          </Button>
        </div>
      </div>

      <section className="mb-3 rounded-xl border border-[var(--k-border)] bg-white p-4">
        <div className="grid gap-2 md:grid-cols-12">
          <div className="md:col-span-8">
            <div className="text-xs text-[var(--k-muted)]">Title</div>
            <input
              className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm text-[var(--k-text)] outline-none"
              placeholder="Title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
            />
          </div>
          <div className="md:col-span-4">
            <div className="text-xs text-[var(--k-muted)]">Time</div>
            <div className="mt-1 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-xs">
              <div className="font-medium text-[var(--k-text)]">
                {fmtTsSimple(updatedAt || createdAt || new Date().toISOString())}
              </div>
            </div>
          </div>
        </div>
      </section>

      <PlateJournalEditor
        key={`${editorKey}:${editorRev}`}
        initialMarkdown={contentMd || ''}
        onMarkdownChange={(md) => setContentMd(md)}
        onReference={handleReference}
      />
    </div>
  );
}
