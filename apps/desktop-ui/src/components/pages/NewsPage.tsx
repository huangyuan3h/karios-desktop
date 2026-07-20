'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  RefreshCw,
  ExternalLink,
  Star,
  StarOff,
  Settings2,
  Plus,
  Trash2,
  Pencil,
  X,
  Check,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { apiDeleteJson, apiPatchJson, apiPostJson } from '@/lib/api/client';
import {
  invalidateNewsPageQueries,
  useNewsItemsQuery,
  useNewsSourcesQuery,
  type DashboardNewsItem,
  type NewsSource,
} from '@/lib/queries/news';

type NewsItem = DashboardNewsItem & { id: string };

export function NewsPage() {
  const queryClient = useQueryClient();
  const [error, setError] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [showSettings, setShowSettings] = React.useState(false);
  const [hours, setHours] = React.useState(24);
  const [showAddForm, setShowAddForm] = React.useState(false);
  const [addForm, setAddForm] = React.useState({ name: '', url: '' });
  const [editingId, setEditingId] = React.useState<string | null>(null);
  const [editForm, setEditForm] = React.useState({ name: '', url: '' });

  const itemsQuery = useNewsItemsQuery(hours);
  const sourcesQuery = useNewsSourcesQuery();
  const items = (itemsQuery.data?.items ?? []) as NewsItem[];
  const total = itemsQuery.data?.total ?? 0;
  const sources = sourcesQuery.data?.sources ?? [];

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      await invalidateNewsPageQueries(queryClient);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [queryClient]);

  async function fetchNews() {
    setBusy(true);
    setError(null);
    try {
      await apiPostJson('/api/news/refresh');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function initDefaults() {
    setBusy(true);
    setError(null);
    try {
      await apiPostJson('/api/news/init-defaults');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function toggleSource(sourceId: string, enabled: boolean) {
    try {
      await apiPatchJson(`/api/news/sources/${sourceId}`, { enabled });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function addSource(name: string, url: string) {
    setBusy(true);
    setError(null);
    try {
      await apiPostJson<{ source: NewsSource }>('/api/news/sources', {
        name,
        url,
        enabled: true,
      });
      setAddForm({ name: '', url: '' });
      setShowAddForm(false);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function deleteSource(sourceId: string) {
    try {
      await apiDeleteJson(`/api/news/sources/${sourceId}`);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function updateSource(sourceId: string, name: string) {
    try {
      await apiPatchJson(`/api/news/sources/${sourceId}`, { name });
      setEditingId(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function startEdit(src: NewsSource) {
    setEditingId(src.id);
    setEditForm({ name: src.name, url: src.url });
  }

  function cancelEdit() {
    setEditingId(null);
    setEditForm({ name: '', url: '' });
  }

  async function toggleImportant(item: NewsItem) {
    try {
      await apiPostJson(`/api/news/items/${item.id}/important`, { important: !item.isImportant });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function openLink(link: string) {
    window.open(link, '_blank', 'noopener,noreferrer');
  }

  const importantItems = items.filter((i) => i.isImportant);
  const regularItems = items.filter((i) => !i.isImportant);

  return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">News</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            24-hour aggregated news from RSS feeds.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Total: {total} items • {sources.filter((s) => s.enabled).length} sources enabled
          </div>
        </div>
        <div className="flex items-center gap-2">
          <select
            className="h-9 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-sm"
            value={hours}
            onChange={(e) => setHours(Number(e.target.value))}
          >
            <option value={6}>Last 6h</option>
            <option value={12}>Last 12h</option>
            <option value={24}>Last 24h</option>
            <option value={48}>Last 48h</option>
            <option value={72}>Last 72h</option>
          </select>
          <Button variant="secondary" size="sm" onClick={() => void refresh()} disabled={busy}>
            Refresh
          </Button>
          <Button size="sm" onClick={() => void fetchNews()} disabled={busy} className="gap-2">
            <RefreshCw className={`h-4 w-4 ${busy ? 'animate-spin' : ''}`} />
            Fetch
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowSettings((v) => !v)}
            className="gap-2"
          >
            <Settings2 className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {showSettings ? (
        <div className="mb-6 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-semibold">Sources</div>
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setShowAddForm(true)}
                disabled={busy || showAddForm}
                className="gap-1"
              >
                <Plus className="h-4 w-4" />
                Add Custom
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => void initDefaults()}
                disabled={busy}
              >
                Add Defaults
              </Button>
            </div>
          </div>

          {showAddForm ? (
            <div className="mb-3 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="mb-2 text-xs font-medium text-[var(--k-muted)]">
                Add Custom Source
              </div>
              <div className="flex gap-2">
                <input
                  type="text"
                  placeholder="Name"
                  value={addForm.name}
                  onChange={(e) => setAddForm((f) => ({ ...f, name: e.target.value }))}
                  className="h-9 flex-1 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm"
                />
                <input
                  type="text"
                  placeholder="RSS URL"
                  value={addForm.url}
                  onChange={(e) => setAddForm((f) => ({ ...f, url: e.target.value }))}
                  className="h-9 flex-[2] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 text-sm"
                />
                <Button
                  size="sm"
                  onClick={() => void addSource(addForm.name, addForm.url)}
                  disabled={busy || !addForm.name || !addForm.url}
                  className="gap-1"
                >
                  <Check className="h-4 w-4" />
                  Add
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setShowAddForm(false);
                    setAddForm({ name: '', url: '' });
                  }}
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ) : null}

          <div className="space-y-2">
            {sources.map((src) =>
              editingId === src.id ? (
                <div
                  key={src.id}
                  className="flex items-center gap-2 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2"
                >
                  <input
                    type="text"
                    value={editForm.name}
                    onChange={(e) => setEditForm((f) => ({ ...f, name: e.target.value }))}
                    className="h-8 flex-1 rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-sm"
                  />
                  <input
                    type="text"
                    value={editForm.url}
                    onChange={(e) => setEditForm((f) => ({ ...f, url: e.target.value }))}
                    className="h-8 flex-[2] rounded border border-[var(--k-border)] bg-[var(--k-surface)] px-2 text-sm opacity-60"
                    disabled
                    title="URL cannot be changed"
                  />
                  <Button
                    size="sm"
                    variant="secondary"
                    onClick={() => void updateSource(src.id, editForm.name)}
                    disabled={!editForm.name}
                    className="h-8 w-8 p-0"
                  >
                    <Check className="h-4 w-4" />
                  </Button>
                  <Button size="sm" variant="ghost" onClick={cancelEdit} className="h-8 w-8 p-0">
                    <X className="h-4 w-4" />
                  </Button>
                </div>
              ) : (
                <div
                  key={src.id}
                  className="flex items-center justify-between rounded-lg border border-[var(--k-border)] px-3 py-2"
                >
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium">{src.name}</div>
                    <div className="text-xs text-[var(--k-muted)] truncate">{src.url}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    {src.lastFetch ? (
                      <div className="text-xs text-[var(--k-muted)]">
                        Last: {new Date(src.lastFetch).toLocaleTimeString()}
                      </div>
                    ) : null}
                    <Switch
                      checked={src.enabled}
                      onCheckedChange={(checked) => void toggleSource(src.id, checked)}
                    />
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => startEdit(src)}
                      className="h-8 w-8 p-0"
                      title="Edit"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => void deleteSource(src.id)}
                      className="h-8 w-8 p-0 text-red-500 hover:text-red-600"
                      title="Delete"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>
              ),
            )}
            {sources.length === 0 && !showAddForm ? (
              <div className="rounded-lg border border-dashed border-[var(--k-border)] px-3 py-6 text-center text-sm text-[var(--k-muted)]">
                No sources configured. Click Add Defaults to add common finance news sources or Add
                Custom to add your own.
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {importantItems.length > 0 ? (
        <div className="mb-6">
          <div className="mb-2 text-sm font-semibold text-[var(--k-accent)]">
            Starred ({importantItems.length})
          </div>
          <div className="space-y-2">
            {importantItems.map((item) => (
              <NewsItemCard
                key={item.id}
                item={item}
                sources={sources}
                onToggleImportant={toggleImportant}
                onOpen={openLink}
              />
            ))}
          </div>
        </div>
      ) : null}

      <div className="mb-2 text-sm font-semibold">Latest ({regularItems.length})</div>
      <div className="space-y-2">
        {regularItems.map((item) => (
          <NewsItemCard
            key={item.id}
            item={item}
            sources={sources}
            onToggleImportant={toggleImportant}
            onOpen={openLink}
          />
        ))}
        {regularItems.length === 0 && importantItems.length === 0 ? (
          <div className="rounded-lg border border-dashed border-[var(--k-border)] px-3 py-10 text-center text-sm text-[var(--k-muted)]">
            No news. Click Fetch to fetch from RSS sources.
          </div>
        ) : null}
      </div>
    </div>
  );
}

function NewsItemCard({
  item,
  sources,
  onToggleImportant,
  onOpen,
}: {
  item: NewsItem;
  sources: NewsSource[];
  onToggleImportant: (item: NewsItem) => void;
  onOpen: (link: string) => void;
}) {
  const source = sources.find((s) => s.id === item.sourceId);
  const sourceName = source?.name ?? item.sourceId;
  const time = item.publishedAt
    ? new Date(item.publishedAt).toLocaleString(undefined, {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : new Date(item.fetchedAt).toLocaleString(undefined, {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });

  return (
    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 cursor-pointer" onClick={() => onOpen(item.link)}>
          <div className="text-sm font-medium leading-snug hover:underline">{item.title}</div>
          {item.summary ? (
            <div className="mt-1 line-clamp-2 text-xs text-[var(--k-muted)]">{item.summary}</div>
          ) : null}
          <div className="mt-1 flex items-center gap-2 text-xs text-[var(--k-muted)]">
            <span className="inline-flex items-center rounded-md bg-[var(--k-accent)]/10 px-1.5 py-0.5 text-xs font-medium text-[var(--k-accent)]">
              {sourceName}
            </span>
            <span>{time}</span>
          </div>
        </div>
        <div className="flex shrink-0 items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            onClick={() => onToggleImportant(item)}
            title={item.isImportant ? 'Remove star' : 'Star this'}
          >
            {item.isImportant ? (
              <Star className="h-4 w-4 fill-yellow-400 text-yellow-400" />
            ) : (
              <StarOff className="h-4 w-4 text-[var(--k-muted)]" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            onClick={() => onOpen(item.link)}
            title="Open link"
          >
            <ExternalLink className="h-4 w-4 text-[var(--k-muted)]" />
          </Button>
        </div>
      </div>
    </div>
  );
}
