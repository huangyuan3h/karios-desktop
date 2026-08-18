'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
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
  useMorningBriefQuery,
  useNewsItemsQuery,
  useNewsSourcesQuery,
  type DashboardNewsItem,
  type BriefCategory,
  type BriefItem,
  type NewsSource,
} from '@/lib/queries/news';

type NewsItem = DashboardNewsItem & {
  id: string;
  tickers?: string[] | null;
  sectors?: string[] | null;
  eventType?: string | null;
  importance?: number | null;
  relevanceScore?: number | null;
  aiSummary?: string | null;
  enrichmentStatus?: string | null;
};

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
  const [filterMode, setFilterMode] = React.useState<'all' | 'important'>('important');

  const itemsQuery = useNewsItemsQuery(hours);
  const sourcesQuery = useNewsSourcesQuery();
  const briefQuery = useMorningBriefQuery();
  const items = (itemsQuery.data?.items ?? []) as NewsItem[];
  const total = itemsQuery.data?.total ?? 0;
  const sources = sourcesQuery.data?.sources ?? [];
  const brief = briefQuery.data?.brief;

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      await invalidateNewsPageQueries(queryClient);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [queryClient]);

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
  // Important filter: keep (a) manually starred items, (b) anything the LLM
  // scored as ≥1 importance or ≥15 relevance, (c) anything tagged actionable.
  // Pure noise (importance=0 + relevance=0) still gets filtered out.
  const isImportantEnough = (item: NewsItem) =>
    (item.importance != null && item.importance >= 1) ||
    (item.relevanceScore != null && item.relevanceScore >= 15) ||
    item.actionability === 'actionable';
  const filteredItems =
    filterMode === 'important'
      ? items.filter((i) => i.isImportant || isImportantEnough(i))
      : items;
  const regularItems = filteredItems.filter((i) => !i.isImportant);

  return (
    <div className="mx-auto w-full max-w-4xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">News</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            24-hour aggregated news from RSS feeds.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Total: {total} items · {sources.filter((s) => s.enabled).length} sources
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
          <div className="flex items-center gap-1.5 text-xs text-[var(--k-muted)]">
            <Switch
              checked={filterMode === 'important'}
              onCheckedChange={(checked) => setFilterMode(checked ? 'important' : 'all')}
            />
            <span>Important only</span>
          </div>
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
                    <div className="flex items-center gap-2">
                      <span
                        className={
                          'inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-medium ' +
                          (src.tier === 'A'
                            ? 'bg-emerald-500/15 text-emerald-700'
                            : src.tier === 'B'
                              ? 'bg-sky-500/15 text-sky-700'
                              : src.tier === 'C'
                                ? 'bg-zinc-500/15 text-zinc-600'
                                : 'bg-rose-500/15 text-rose-700')
                        }
                        title={
                          src.tier === 'A'
                            ? 'Tier A — must-read, professional editorial (e.g. 财联社 / 华尔街见闻 / 财新)'
                            : src.tier === 'B'
                              ? 'Tier B — skim, market + sentiment (e.g. 雪球 / 同花顺 / 东方财富)'
                              : src.tier === 'C'
                                ? 'Tier C — macro / sector background (e.g. 36氪 / 第一财经 / 统计局)'
                                : 'Tier D — disabled / legacy / unclassified'
                        }
                      >
                        {src.tier ?? 'D'}
                      </span>
                      <span className="text-sm font-medium">{src.name}</span>
                      {src.category ? (
                        <span className="text-[10px] text-[var(--k-muted)]">· {src.category}</span>
                      ) : null}
                    </div>
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

      {brief && brief.items.length > 0 ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 flex items-center justify-between">
            <div className="text-sm font-semibold">
              {brief.briefType === 'morning' ? '早盘' : '午间'} Brief · {brief.briefDate}
            </div>
            <div className="text-xs text-[var(--k-muted)]">
              {brief.items.length} items · v{brief.modelVersion}
            </div>
          </div>
          <BriefSection items={brief.items} />
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
            {filterMode === 'important'
              ? 'No important news. Try "All" to see everything, or click Fetch to get new items.'
              : 'No news. Click Fetch to fetch from RSS sources.'}
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
            {item.eventType ? (
              <span className="inline-flex items-center rounded bg-violet-500/10 px-1 py-0.5 text-[10px] font-medium text-violet-600">
                {item.eventType}
              </span>
            ) : null}
            {item.importance != null && item.importance > 0 ? (
              <span
                className={`inline-flex items-center rounded px-1 py-0.5 text-[10px] font-medium ${
                  item.importance >= 4
                    ? 'bg-red-500/15 text-red-700'
                    : item.importance >= 3
                      ? 'bg-orange-500/15 text-orange-700'
                      : 'bg-zinc-500/10 text-zinc-600'
                }`}
                title={`Importance: ${item.importance}/5`}
              >
                I{item.importance}
              </span>
            ) : null}
            {item.relevanceScore != null && item.relevanceScore > 0 ? (
              <span
                className={`inline-flex items-center rounded px-1 py-0.5 text-[10px] font-medium ${
                  item.relevanceScore >= 60
                    ? 'bg-emerald-500/15 text-emerald-700'
                    : item.relevanceScore >= 30
                      ? 'bg-sky-500/15 text-sky-700'
                      : 'bg-zinc-500/10 text-zinc-600'
                }`}
                title={`Relevance: ${item.relevanceScore}/100`}
              >
                R{item.relevanceScore}
              </span>
            ) : null}
          </div>
          {item.tickers && item.tickers.length > 0 ? (
            <div className="mt-1 flex flex-wrap gap-1">
              {item.tickers.slice(0, 5).map((t) => (
                <span
                  key={t}
                  className="inline-flex items-center rounded bg-amber-500/10 px-1 py-0.5 text-[10px] font-medium text-amber-700"
                >
                  {t}
                </span>
              ))}
              {item.tickers.length > 5 ? (
                <span className="text-[10px] text-[var(--k-muted)]">+{item.tickers.length - 5}</span>
              ) : null}
            </div>
          ) : null}
          {item.aiSummary ? (
            <div className="mt-1 text-xs text-[var(--k-muted)] italic line-clamp-1">{item.aiSummary}</div>
          ) : null}
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

const BRIEF_CATEGORY_META: Record<BriefCategory, { label: string; dot: string }> = {
  watchlist: { label: '持仓相关', dot: 'bg-emerald-500' },
  risk: { label: '风险提醒', dot: 'bg-red-500' },
  macro: { label: '板块/宏观', dot: 'bg-blue-500' },
  sector: { label: '板块动态', dot: 'bg-amber-500' },
};

const BRIEF_CATEGORY_ORDER: BriefCategory[] = ['watchlist', 'risk', 'sector', 'macro'];

function BriefSection({ items }: { items: BriefItem[] }) {
  const grouped = new Map<BriefCategory, BriefItem[]>();
  for (const cat of BRIEF_CATEGORY_ORDER) {
    grouped.set(cat, []);
  }
  for (const item of items) {
    const cat = item.category || 'macro';
    const list = grouped.get(cat) || [];
    list.push(item);
    grouped.set(cat, list);
  }

  const nonEmpty = BRIEF_CATEGORY_ORDER.filter((cat) => (grouped.get(cat) || []).length > 0);

  return (
    <div className="space-y-2">
      {nonEmpty.map((cat) => {
        const meta = BRIEF_CATEGORY_META[cat];
        const catItems = grouped.get(cat) || [];
        return (
          <div key={cat}>
            <div className="mb-1 flex items-center gap-1.5 text-xs font-medium text-[var(--k-muted)]">
              <span className={`h-1.5 w-1.5 rounded-full ${meta.dot}`} />
              {meta.label}
            </div>
            <div className="space-y-1 pl-3">
              {catItems.map((item, idx) => (
                <div
                  key={item.id ?? `${item.title ?? 'item'}-${idx}`}
                  className="flex items-start gap-2 text-sm"
                >
                  <span className={`mt-1.5 h-1 w-1 shrink-0 rounded-full ${meta.dot}`} />
                  <div className="min-w-0 flex-1">
                    <span className="leading-snug">{item.title}</span>
                    {item.tickers?.length ? (
                      <span className="ml-2 rounded bg-amber-500/10 px-1 py-0.5 font-mono text-[10px] text-amber-700">
                        {item.tickers.slice(0, 2).join(', ')}
                      </span>
                    ) : null}
                  </div>
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
