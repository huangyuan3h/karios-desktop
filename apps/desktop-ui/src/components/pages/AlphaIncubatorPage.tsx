'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  Bot,
  ExternalLink,
  FileText,
  Rocket,
  Sparkles,
  Star,
  Trash2,
  AlertTriangle,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  articleAgeDays,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  displaySymbol,
  formatCatalystScore,
  formatRelevancePct,
  isStaleArticle,
  type CatalystStock,
} from '@/lib/alpha-radar-catalyst';
import { useChatStore } from '@/lib/chat/store';
import {
  deleteAlphaRadarTrend,
  invalidateAlphaRadarQueries,
  remapAlphaRadarTrend,
  runAlphaRadarPipeline,
  useAlphaRadarCatalystQuery,
  useAlphaRadarRssQuery,
  useAlphaRadarStatusQuery,
  useAlphaRadarTrendsQuery,
  type AlphaTrend,
  type DriverFilter,
  type PipelineStatus,
  type RssDocument,
  type TrendsScope,
  type ViewTab,
} from '@/lib/queries/alphaRadar';
import { loadWatchlist, saveWatchlist } from '@/lib/watchlist-storage';
import { cn } from '@/lib/utils';

function trendDisplayTitle(t: AlphaTrend): string {
  return (t.macroTheme || t.trendName || '').trim() || '—';
}

function trendCatalystGrade(t: AlphaTrend): string {
  return (t.catalystGrade || t.urgencyLevel || 'B').trim() || 'B';
}

function fmtWhen(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString();
}

function urgencyTone(level: string): string {
  if (level === 'S') return 'bg-red-600 text-white';
  if (level === 'A') return 'bg-orange-500 text-white';
  if (level === 'B') return 'bg-amber-500 text-white';
  return 'bg-[var(--k-muted)] text-white';
}

function driverTone(driver: string): string {
  if (driver === 'Global_Tech') return 'bg-blue-600/90 text-white';
  if (driver === 'Domestic_Policy') return 'bg-violet-600/90 text-white';
  if (driver === 'Cycle_Reversal') return 'bg-emerald-600/90 text-white';
  return 'bg-[var(--k-muted)] text-white';
}

function driverLabel(driver: string | null | undefined): string {
  const d = (driver || 'Global_Tech').trim();
  if (d === 'Domestic_Policy') return '政策';
  if (d === 'Cycle_Reversal') return '周期';
  return '科技';
}

function riskLabel(status: string): { text: string; className: string } {
  if (status === 'armed') {
    return {
      text: '允许狙击！',
      className: 'border-red-500 bg-red-50 text-red-700 animate-pulse',
    };
  }
  return {
    text: '等待 V2.0 资金流共振',
    className: 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]',
  };
}

async function addSymbolsToWatchlist(symbols: AlphaTrend['cnSymbols']) {
  const existing = loadWatchlist();
  const seen = new Set(existing.map((x) => x.symbol));
  const now = new Date().toISOString();
  const next = [...existing];
  for (const s of symbols) {
    const sym = String(s.symbol || '').trim();
    if (!sym || seen.has(sym)) continue;
    seen.add(sym);
    next.push({ symbol: sym, name: s.name, addedAt: now, source: 'alpha_radar' });
  }
  await saveWatchlist(next);
}

export function AlphaIncubatorPage() {
  const { addReference } = useChatStore();
  const queryClient = useQueryClient();
  const [viewTab, setViewTab] = React.useState<ViewTab>('trends');
  const [trendsScope, setTrendsScope] = React.useState<TrendsScope>('batch');
  const [driverFilter, setDriverFilter] = React.useState<DriverFilter>('all');
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [msg, setMsg] = React.useState<string | null>(null);

  const statusQuery = useAlphaRadarStatusQuery();
  const trendsQuery = useAlphaRadarTrendsQuery(trendsScope);
  const catalystQuery = useAlphaRadarCatalystQuery(DEFAULT_CATALYST_MAX_AGE_DAYS);
  const rssQuery = useAlphaRadarRssQuery({ enabled: viewTab === 'rss' });

  const status = (statusQuery.data ?? {}) as PipelineStatus;
  const trends = trendsQuery.data?.items ?? [];
  const trendsTotal = trendsQuery.data?.total ?? trends.length;
  const catalystStocks = (catalystQuery.data?.items ?? []) as CatalystStock[];
  const catalystMeta = {
    maxAgeDays: catalystQuery.data?.maxAgeDays ?? DEFAULT_CATALYST_MAX_AGE_DAYS,
    total: catalystQuery.data?.total ?? 0,
  };
  const rssDocuments = rssQuery.data?.documents ?? [];
  const rssTotal = rssQuery.data?.total ?? rssDocuments.length;
  const sourceNames = rssQuery.data?.sourceNames ?? {};

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      await invalidateAlphaRadarQueries(queryClient);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [queryClient]);

  const visibleTrends = React.useMemo(() => {
    if (driverFilter === 'all') return trends;
    return trends.filter((t) => (t.driverType || 'Global_Tech') === driverFilter);
  }, [trends, driverFilter]);

  async function runPipeline(force = false) {
    setError(null);
    setMsg(null);
    setBusy(true);
    try {
      const r = await runAlphaRadarPipeline(force);
      if (r.skipped) {
        setMsg(`12h 冷却中 · 当前 ${r.trendCount ?? 0} 张卡片`);
      } else if (r.ok === false) {
        setMsg(
          `生成失败 · 入库 ${r.ingestStats?.stored ?? 0} 条` +
            (r.keptPreviousTrends ? ' · 已保留上一批卡片' : ''),
        );
      } else {
        setMsg(
          `已生成 ${r.trendCount ?? 0} 张趋势卡片` +
            (r.processedHeadlines ? ` · 处理 ${r.processedHeadlines} 条标题` : '') +
            (r.ingestStats?.stored != null ? ` · 入库 ${r.ingestStats.stored} 条` : ''),
        );
      }
      if (r.errors?.length) {
        setError(r.errors.slice(0, 3).map((e) => e.error ?? 'unknown').join('\n'));
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function deleteTrend(trendId: string, trendName: string) {
    if (!window.confirm(`Delete trend card "${trendName}"?`)) return;
    setError(null);
    setMsg(null);
    setBusy(true);
    try {
      const r = await deleteAlphaRadarTrend(trendId);
      if (!r.ok) throw new Error(r.error || 'Delete failed');
      setMsg('Trend card deleted');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function remapTrend(trendId: string) {
    setError(null);
    setMsg(null);
    setBusy(true);
    try {
      const r = await remapAlphaRadarTrend(trendId);
      if (!r.ok) throw new Error(r.error || 'Remap failed');
      const n = r.cnSymbols?.length ?? 0;
      setMsg(n ? `Mapped ${n} A-share symbol(s)` : 'Remap done (no symbols — try Tavily or manual review)');
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const ingest = status.lastIngestStats;
  const withinCooldown = Boolean(status.withinCooldown);

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-4 p-4 md:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-lg font-semibold">
            <Rocket className="h-5 w-5" />
            Alpha Incubator
          </div>
          <p className="mt-1 max-w-2xl text-sm text-[var(--k-muted)]">
            7 路精选 RSS → 主题过滤 → 全文抓取 → batch LLM 提纯 → A 股映射（每 12h 自动刷新）。
          </p>
          <p className="mt-1 text-xs text-[var(--k-muted)]">
            {status.lastRunAt ? `上次生成 ${fmtWhen(status.lastRunAt)}` : '尚未生成'}
            {status.lastTrendCount != null ? ` · 本批 ${status.lastTrendCount} 张` : ''}
            {status.accumulatedTrendCount != null
              ? ` · 库内共 ${status.accumulatedTrendCount} 张趋势`
              : ''}
            {status.lastIngestAt ? ` · RSS 同步 ${fmtWhen(status.lastIngestAt)}` : ''}
            {status.rawBacklogCount != null ? ` · raw 待处理 ${status.rawBacklogCount}` : ''}
            {ingest?.stored != null ? ` · 入库 ${ingest.stored} 条` : ''}
            {ingest?.filteredOut ? ` · 过滤 ${ingest.filteredOut} 条` : ''}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            size="default"
            disabled={busy || (withinCooldown && !busy)}
            onClick={() => void runPipeline(false)}
          >
            <Sparkles className={cn('mr-2 h-4 w-4', busy && 'animate-pulse')} />
            {busy ? '生成中…' : withinCooldown ? '12h 冷却中' : '生成趋势'}
          </Button>
          <Button variant="outline" size="sm" disabled={busy} onClick={() => void runPipeline(true)}>
            重新生成
          </Button>
        </div>
      </div>

      {error ? (
        <div className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700 whitespace-pre-wrap">
          {error}
        </div>
      ) : null}
      {msg ? (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
          {msg}
        </div>
      ) : null}

      <div className="flex flex-wrap gap-2 border-b border-[var(--k-border)] pb-2">
        <button
          type="button"
          className={cn(
            'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
            viewTab === 'trends'
              ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
              : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
          )}
          onClick={() => setViewTab('trends')}
        >
          趋势视图
        </button>
        <button
          type="button"
          className={cn(
            'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
            viewTab === 'catalyst'
              ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
              : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
          )}
          onClick={() => setViewTab('catalyst')}
        >
          催化股票
          {catalystMeta.total ? (
            <span className="ml-1 text-xs text-[var(--k-muted)]">({catalystMeta.total})</span>
          ) : null}
        </button>
        <button
          type="button"
          className={cn(
            'rounded-md px-3 py-1.5 text-sm font-medium transition-colors',
            viewTab === 'rss'
              ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
              : 'text-[var(--k-muted)] hover:text-[var(--k-text)]',
          )}
          onClick={() => setViewTab('rss')}
        >
          <FileText className="mr-1 inline h-3.5 w-3.5" />
          RSS 原文
          {rssTotal ? (
            <span className="ml-1 text-xs text-[var(--k-muted)]">({rssTotal})</span>
          ) : null}
        </button>
        {viewTab === 'catalyst' ? (
          <span className="self-center text-xs text-[var(--k-muted)]">
            打分窗口 {catalystMeta.maxAgeDays} 天 · 历史趋势仍保存在库
          </span>
        ) : null}
      </div>

      {viewTab === 'trends' ? (
      <>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          className={cn(
            'rounded-md border px-2.5 py-1 text-xs font-medium transition-colors',
            trendsScope === 'batch'
              ? 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-text)]'
              : 'border-transparent text-[var(--k-muted)] hover:text-[var(--k-text)]',
          )}
          onClick={() => setTrendsScope('batch')}
        >
          本批
          {status.lastTrendCount != null ? ` (${status.lastTrendCount})` : ''}
        </button>
        <button
          type="button"
          className={cn(
            'rounded-md border px-2.5 py-1 text-xs font-medium transition-colors',
            trendsScope === 'all'
              ? 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-text)]'
              : 'border-transparent text-[var(--k-muted)] hover:text-[var(--k-text)]',
          )}
          onClick={() => setTrendsScope('all')}
        >
          全部历史 ({trendsTotal})
        </button>
        {(['all', 'Global_Tech', 'Domestic_Policy', 'Cycle_Reversal'] as const).map((key) => (
          <button
            key={key}
            type="button"
            className={cn(
              'rounded-md border px-2.5 py-1 text-xs font-medium transition-colors',
              driverFilter === key
                ? 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-text)]'
                : 'border-transparent text-[var(--k-muted)] hover:text-[var(--k-text)]',
            )}
            onClick={() => setDriverFilter(key)}
          >
            {key === 'all' ? '全部驱动' : driverLabel(key)}
          </button>
        ))}
      </div>
      <div className="grid grid-cols-1 gap-3">
        {visibleTrends.length === 0 ? (
          <div className="rounded-xl border border-dashed border-[var(--k-border)] p-8 text-center text-sm text-[var(--k-muted)]">
            暂无趋势卡片。点击「生成趋势」开始（约 1–3 分钟，需 ai-service + 网络）。
          </div>
        ) : (
          visibleTrends.map((t) => {
            const risk = riskLabel(t.riskStatus);
            const ageDays = articleAgeDays(t.documentPublishedAt, t.documentFetchedAt);
            const stale = isStaleArticle(t.documentPublishedAt, t.documentFetchedAt, catalystMeta.maxAgeDays);
            return (
              <section
                key={t.id}
                className={cn(
                  'rounded-xl border p-4',
                  t.riskStatus === 'armed'
                    ? 'border-red-400 bg-red-50/40 shadow-sm shadow-red-100'
                    : 'border-[var(--k-border)] bg-[var(--k-surface)]',
                )}
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span
                        className={cn(
                          'rounded px-1.5 py-0.5 text-xs font-semibold',
                          urgencyTone(trendCatalystGrade(t)),
                        )}
                        title="Catalyst grade"
                      >
                        {trendCatalystGrade(t)}
                      </span>
                      <span
                        className={cn(
                          'rounded px-1.5 py-0.5 text-xs font-medium',
                          driverTone(t.driverType || 'Global_Tech'),
                        )}
                        title="Driver type"
                      >
                        {driverLabel(t.driverType)}
                      </span>
                      <h3 className="font-semibold">{trendDisplayTitle(t)}</h3>
                    </div>
                    <div className="mt-2 text-sm text-[var(--k-muted)]">
                      <span className="font-medium text-[var(--k-text)]">【宏观主题】</span>
                      {trendDisplayTitle(t)}
                    </div>
                    <div className="mt-1 text-sm">
                      <span className="font-medium">【催化剂源】</span>
                      {t.documentTitle || '—'}
                      {t.documentUrl ? (
                        <a
                          href={t.documentUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="ml-2 inline-flex items-center text-blue-600 hover:underline"
                        >
                          原文 <ExternalLink className="ml-0.5 h-3 w-3" />
                        </a>
                      ) : null}
                    </div>
                    {t.eventFocus || t.catalyst ? (
                      <p className="mt-2 text-sm leading-relaxed text-[var(--k-text)]">
                        {t.eventFocus || t.catalyst}
                      </p>
                    ) : null}
                    {t.logicSummary ? (
                      <p className="mt-1 text-xs font-medium text-[var(--k-muted)]">
                        逻辑：{t.logicSummary}
                      </p>
                    ) : null}
                    <div className="mt-2 text-sm">
                      <span className="font-medium">【A股映射龙头】</span>
                      {t.cnSymbols?.length ? (
                        t.cnSymbols.map((s) => (
                          <span key={s.symbol} className="mr-2 font-mono">
                            {s.name} ({s.symbol.replace('CN:', '')})
                            <span className="text-xs text-[var(--k-muted)]">
                              {' '}
                              · {(s.confidence * 100).toFixed(0)}%
                            </span>
                          </span>
                        ))
                      ) : (
                        <span className="text-[var(--k-muted)]">待映射 / 待人工复核</span>
                      )}
                    </div>
                    {t.keywordsForMapping?.length ? (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {t.keywordsForMapping.map((kw) => (
                          <span
                            key={kw}
                            className="rounded-full border border-[var(--k-border)] px-2 py-0.5 text-xs"
                          >
                            {kw}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <div className={cn('rounded-lg border px-3 py-2 text-xs font-medium', risk.className)}>
                    {t.riskStatus === 'armed' ? (
                      <span className="inline-flex items-center gap-1">
                        <AlertTriangle className="h-3.5 w-3.5" />
                        【风控状态】{risk.text}
                      </span>
                    ) : (
                      <>【风控状态】{risk.text}</>
                    )}
                  </div>
                </div>

                <div className="mt-3 flex flex-wrap gap-2">
                  {ageDays != null ? (
                    <span
                      className={cn(
                        'self-center text-xs',
                        stale ? 'text-[var(--k-muted)] line-through decoration-[var(--k-muted)]' : 'text-[var(--k-muted)]',
                      )}
                    >
                      文章年龄 {ageDays} 天{stale ? ' · 已超出催化窗口' : ''}
                    </span>
                  ) : null}
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={busy}
                    onClick={() => void deleteTrend(t.id, trendDisplayTitle(t))}
                  >
                    <Trash2 className="mr-1 h-4 w-4" />
                    Delete
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={busy}
                    onClick={() => void remapTrend(t.id)}
                  >
                    Remap A-shares
                  </Button>
                  {t.cnSymbols?.length ? (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        void addSymbolsToWatchlist(t.cnSymbols).then(() => {
                          setMsg(`Added ${t.cnSymbols.length} symbol(s) to Watchlist`);
                        });
                      }}
                    >
                      <Star className="mr-1 h-4 w-4" />
                      Add to Watchlist
                    </Button>
                  ) : null}
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      addReference({
                        kind: 'alphaRadar',
                        refId: `alphaRadar:${t.id}`,
                        trendId: t.id,
                        trendName: trendDisplayTitle(t),
                        macroTheme: t.macroTheme ?? trendDisplayTitle(t),
                        catalystGrade: trendCatalystGrade(t),
                        driverType: t.driverType ?? undefined,
                        eventFocus: t.eventFocus ?? t.catalyst ?? undefined,
                        logicSummary: t.logicSummary ?? undefined,
                        catalyst: t.catalyst,
                        cnSymbols: t.cnSymbols,
                        riskStatus: t.riskStatus,
                        documentTitle: t.documentTitle,
                        capturedAt: new Date().toISOString(),
                      });
                      setMsg('Added trend to Agent context');
                    }}
                  >
                    <Bot className="mr-1 h-4 w-4" />
                    Ask Agent
                  </Button>
                </div>
              </section>
            );
          })
        )}
      </div>
      </>
      ) : viewTab === 'rss' ? (
        <div className="grid grid-cols-1 gap-3">
          {rssDocuments.length === 0 ? (
            <div className="rounded-xl border border-dashed border-[var(--k-border)] p-8 text-center text-sm text-[var(--k-muted)]">
              暂无 RSS 入库记录。运行「生成趋势」或等待 12h 定时任务同步信源。
            </div>
          ) : (
            rssDocuments.map((doc) => (
              <section
                key={doc.id}
                className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2 text-xs text-[var(--k-muted)]">
                      <span className="rounded border border-[var(--k-border)] px-1.5 py-0.5">
                        {sourceNames[doc.sourceId] || doc.sourceId}
                      </span>
                      <span className="rounded border border-[var(--k-border)] px-1.5 py-0.5">
                        {doc.processingStatus}
                      </span>
                      <span>{doc.category}</span>
                    </div>
                    <h3 className="mt-2 font-semibold">{doc.title}</h3>
                    {doc.summary ? (
                      <p className="mt-2 line-clamp-4 text-sm leading-relaxed text-[var(--k-text)]">
                        {doc.summary}
                      </p>
                    ) : (
                      <p className="mt-2 text-sm text-[var(--k-muted)]">无 RSS 摘要</p>
                    )}
                    <p className="mt-2 text-xs text-[var(--k-muted)]">
                      发布 {fmtWhen(doc.publishedAt)} · 入库 {fmtWhen(doc.fetchedAt)}
                    </p>
                  </div>
                  {doc.url ? (
                    <a
                      href={doc.url}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex shrink-0 items-center text-sm text-blue-600 hover:underline"
                    >
                      打开链接 <ExternalLink className="ml-1 h-3.5 w-3.5" />
                    </a>
                  ) : null}
                </div>
              </section>
            ))
          )}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3">
          {catalystStocks.length === 0 ? (
            <div className="rounded-xl border border-dashed border-[var(--k-border)] p-8 text-center text-sm text-[var(--k-muted)]">
              暂无催化股票（{catalystMeta.maxAgeDays} 天内无 A 股映射趋势）。
            </div>
          ) : (
            catalystStocks.map((stock) => (
              <section
                key={stock.symbol}
                className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div>
                    <h3 className="text-base font-semibold">
                      {stock.name}{' '}
                      <span className="font-mono text-sm text-[var(--k-muted)]">
                        ({displaySymbol(stock.symbol)})
                      </span>
                    </h3>
                    <p className="mt-1 text-xs text-[var(--k-muted)]">
                      {stock.articleCount} 篇相关文章
                      {stock.latestArticleAt ? ` · 最新 ${fmtWhen(stock.latestArticleAt)}` : ''}
                    </p>
                  </div>
                  <div className="rounded-lg border border-orange-200 bg-orange-50 px-3 py-2 text-center">
                    <div className="text-xs text-orange-700">催化分</div>
                    <div className="text-xl font-bold text-orange-800">
                      {formatCatalystScore(stock.catalystScore)}
                    </div>
                  </div>
                </div>

                <div className="mt-3 space-y-3">
                  {stock.articles.map((article) => (
                    <div
                      key={`${stock.symbol}-${article.documentId}-${article.trendId}`}
                      className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3"
                    >
                      <div className="flex flex-wrap items-center gap-2 text-sm">
                        <span
                          className={cn(
                            'rounded px-1.5 py-0.5 text-xs font-semibold',
                            urgencyTone(article.catalystGrade || article.urgencyLevel),
                          )}
                          title="Catalyst grade"
                        >
                          {article.catalystGrade || article.urgencyLevel}
                        </span>
                        <span className="font-medium">{article.documentTitle || article.macroTheme || article.trendName}</span>
                        <span className="text-xs text-[var(--k-muted)]">
                          相关度 {formatRelevancePct(article.relevance)}
                        </span>
                        {article.documentUrl ? (
                          <a
                            href={article.documentUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center text-xs text-blue-600 hover:underline"
                          >
                            原文 <ExternalLink className="ml-0.5 h-3 w-3" />
                          </a>
                        ) : null}
                      </div>
                      <div className="mt-1 text-xs text-[var(--k-muted)]">
                        {article.macroTheme || article.trendName}
                      </div>
                      {article.summary ? (
                        <p className="mt-2 text-sm leading-relaxed text-[var(--k-text)]">{article.summary}</p>
                      ) : null}
                      {article.publishedAt ? (
                        <div className="mt-1 text-xs text-[var(--k-muted)]">
                          {fmtWhen(article.publishedAt)}
                        </div>
                      ) : null}
                    </div>
                  ))}
                </div>

                <div className="mt-3">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      void addSymbolsToWatchlist([
                        {
                          symbol: stock.symbol.startsWith('CN:') ? stock.symbol : `CN:${stock.symbol}`,
                          name: stock.name,
                          confidence: stock.catalystScore / 100,
                          rationale: `Catalyst score ${formatCatalystScore(stock.catalystScore)}`,
                        },
                      ]).then(() => {
                        setMsg(`Added ${stock.name} to Watchlist`);
                      });
                    }}
                  >
                    <Star className="mr-1 h-4 w-4" />
                    Add to Watchlist
                  </Button>
                </div>
              </section>
            ))
          )}
        </div>
      )}
    </div>
  );
}
