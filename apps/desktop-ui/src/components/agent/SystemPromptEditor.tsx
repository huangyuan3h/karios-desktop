'use client';

import * as React from 'react';
import { ChevronDown, Plus, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type PresetSummary = { id: string; title: string; updatedAt: string };

const NEW_VALUE = '__new__';
const LEGACY_VALUE = '__legacy__';

export function SystemPromptEditor() {
  const { state, setSystemPromptLocal } = useChatStore();
  const [open, setOpen] = React.useState(false);
  const [items, setItems] = React.useState<PresetSummary[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [selectedValue, setSelectedValue] = React.useState<string>(
    state.settings.systemPromptId ?? LEGACY_VALUE,
  );
  const [title, setTitle] = React.useState(state.settings.systemPromptTitle);
  const [draft, setDraft] = React.useState(state.settings.systemPrompt);

  React.useEffect(() => {
    // Do not override local "New preset" drafting state.
    if (selectedValue === NEW_VALUE) return;

    setSelectedValue(state.settings.systemPromptId ?? LEGACY_VALUE);
    setTitle(state.settings.systemPromptTitle);
    setDraft(state.settings.systemPrompt);
  }, [
    selectedValue,
    state.settings.systemPrompt,
    state.settings.systemPromptId,
    state.settings.systemPromptTitle,
  ]);

  async function refreshList() {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts`);
      if (!resp.ok) return;
      const data = (await resp.json()) as { items?: PresetSummary[] };
      const next = Array.isArray(data.items) ? data.items : [];
      setItems(next);
    } catch {
      // ignore (v0 best-effort)
    } finally {
      setLoading(false);
    }
  }

  async function refreshActive() {
    setError(null);
    try {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/active`);
      if (!resp.ok) return;
      const data = (await resp.json()) as { id?: string | null; title?: string; content?: string };
      const id = data.id === null || typeof data.id === 'string' ? (data.id ?? null) : null;
      const nextTitle = typeof data.title === 'string' ? data.title : 'Legacy';
      const content = typeof data.content === 'string' ? data.content : '';
      setSystemPromptLocal({ id, title: nextTitle, content });
    } catch {
      // ignore
    }
  }

  React.useEffect(() => {
    // Load presets + active selection (best-effort).
    void refreshList();
    void refreshActive();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const selectedId =
    selectedValue !== NEW_VALUE && selectedValue !== LEGACY_VALUE ? selectedValue : null;
  const isLegacy = selectedValue === LEGACY_VALUE;

  return (
    <div className="border-b border-[var(--k-border)]">
      <Button
        variant="ghost"
        size="sm"
        className="h-auto w-full justify-between px-3 py-2"
        onClick={() => setOpen((v) => !v)}
      >
        <div className="min-w-0">
          <div className="text-xs font-semibold uppercase tracking-wide text-[var(--k-muted)]">
            System prompt
          </div>
          <div className="truncate text-xs text-[var(--k-text)]">{state.settings.systemPromptTitle}</div>
        </div>
        <ChevronDown className={`h-4 w-4 text-[var(--k-muted)] transition-transform ${open ? 'rotate-180' : ''}`} />
      </Button>

      {open ? (
        <div className="space-y-2 px-3 pb-3">
          <div className="grid grid-cols-[1fr_auto] gap-2">
            <label className="min-w-0">
              <div className="mb-1 text-xs text-[var(--k-muted)]">Preset</div>
              <Select
                value={selectedValue}
                onValueChange={async (value) => {
                  setSelectedValue(value);

                  if (value === NEW_VALUE) {
                    setTitle('New prompt');
                    return;
                  }

                  const id = value === LEGACY_VALUE ? null : value;
                  setSaving(true);
                  setError(null);
                  try {
                    await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/active`, {
                      method: 'PUT',
                      headers: { 'content-type': 'application/json' },
                      body: JSON.stringify({ id }),
                    });
                    await refreshActive();
                  } catch (err) {
                    const msg = err instanceof Error ? err.message : String(err);
                    setError(msg);
                  } finally {
                    setSaving(false);
                  }
                }}
                disabled={saving}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select a preset" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={NEW_VALUE}>New preset…</SelectItem>
                  <SelectItem value={LEGACY_VALUE}>Legacy</SelectItem>
                  {items.map((p) => (
                    <SelectItem key={p.id} value={p.id}>
                      {p.title}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>

            <div className="flex items-end gap-2">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                disabled={saving}
                onClick={async () => {
                  setSaving(true);
                  setError(null);
                  try {
                    const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts`, {
                      method: 'POST',
                      headers: { 'content-type': 'application/json' },
                      body: JSON.stringify({ title: 'New prompt', content: '' }),
                    });
                    if (!resp.ok) {
                      setError(await resp.text().catch(() => 'Failed to create preset.'));
                      return;
                    }
                    await refreshList();
                    await refreshActive();
                  } catch (err) {
                    const msg = err instanceof Error ? err.message : String(err);
                    setError(msg);
                  } finally {
                    setSaving(false);
                  }
                }}
                title="New preset"
              >
                <Plus className="h-4 w-4" />
              </Button>
              <Button
                type="button"
                variant="secondary"
                size="sm"
                disabled={saving || !selectedId}
                onClick={async () => {
                  if (!selectedId) return;
                  if (!confirm('Delete this system prompt preset?')) return;
                  setSaving(true);
                  setError(null);
                  try {
                    const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/${selectedId}`, {
                      method: 'DELETE',
                    });
                    if (!resp.ok) {
                      setError(await resp.text().catch(() => 'Failed to delete preset.'));
                      return;
                    }
                    await refreshList();
                    await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/active`, {
                      method: 'PUT',
                      headers: { 'content-type': 'application/json' },
                      body: JSON.stringify({ id: null }),
                    });
                    await refreshActive();
                  } catch (err) {
                    const msg = err instanceof Error ? err.message : String(err);
                    setError(msg);
                  } finally {
                    setSaving(false);
                  }
                }}
                title="Delete preset"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          </div>

          <label className="block">
            <div className="mb-1 text-xs text-[var(--k-muted)]">Title</div>
            <input
              value={title}
              onChange={(e) => setTitle(e.currentTarget.value)}
              disabled={isLegacy}
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-sm text-[var(--k-text)] disabled:opacity-60"
              placeholder="e.g., Default"
            />
          </label>

          <label className="block">
            <div className="mb-1 text-xs text-[var(--k-muted)]">Prompt</div>
            <Textarea
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="e.g., You are Kairos, an AI-first investment assistant. Be concise and cite evidence."
              className="min-h-[96px]"
            />
          </label>

          <div className="flex items-center justify-between">
            <div className="text-xs text-[var(--k-muted)]">
              {loading ? 'Loading presets…' : 'Stored in Postgres via data-sync-service.'}
            </div>
            <Button
              size="sm"
              variant="secondary"
              disabled={saving}
              onClick={async () => {
                setSaving(true);
                setError(null);
                try {
                  if (selectedValue === NEW_VALUE) {
                    const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts`, {
                      method: 'POST',
                      headers: { 'content-type': 'application/json' },
                      body: JSON.stringify({ title: title.trim() || 'Untitled', content: draft }),
                    });
                    if (!resp.ok) {
                      setError(await resp.text().catch(() => 'Failed to create preset.'));
                      return;
                    }
                    const data = (await resp.json()) as { id?: string };
                    const id = typeof data.id === 'string' ? data.id : null;
                    if (id) setSelectedValue(id);
                    await refreshList();
                    await refreshActive();
                    return;
                  }

                  if (selectedId) {
                    const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/${selectedId}`, {
                      method: 'PUT',
                      headers: { 'content-type': 'application/json' },
                      body: JSON.stringify({ title: title.trim() || 'Untitled', content: draft }),
                    });
                    if (!resp.ok) {
                      setError(await resp.text().catch(() => 'Failed to save preset.'));
                      return;
                    }
                    setSystemPromptLocal({ id: selectedId, title: title.trim() || 'Untitled', content: draft });
                    await refreshList();
                    return;
                  }

                  const resp = await fetch(`${DATA_SYNC_BASE_URL}/settings/system-prompt`, {
                    method: 'PUT',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ value: draft }),
                  });
                  if (!resp.ok) {
                    setError(await resp.text().catch(() => 'Failed to save.'));
                    return;
                  }
                  setSystemPromptLocal({ id: null, title: 'Legacy', content: draft });
                } catch (err) {
                  const msg = err instanceof Error ? err.message : String(err);
                  setError(msg);
                } finally {
                  setSaving(false);
                }
              }}
            >
              Save
            </Button>
          </div>

          {error ? <div className="text-xs text-red-600 dark:text-red-400">{error}</div> : null}
        </div>
      ) : null}
    </div>
  );
}


