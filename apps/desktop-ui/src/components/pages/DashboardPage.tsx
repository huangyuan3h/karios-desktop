/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { RefreshCw } from 'lucide-react';

import { DashboardHeader } from '@/components/dashboard/DashboardHeader';
import { DecisionJournalCard } from '@/components/dashboard/DecisionJournalCard';
import { EtfFundFlowCard } from '@/components/dashboard/EtfFundFlowCard';
import { IndustryFundFlowCard } from '@/components/dashboard/IndustryFundFlowCard';
import { MarketSentimentCard } from '@/components/dashboard/MarketSentimentCard';
import { MorningBriefCard } from '@/components/dashboard/MorningBriefCard';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { useDashboardSummary } from '@/hooks/useDashboardSummary';
import { useDashboardSync } from '@/hooks/useDashboardSync';
import { useExecutionJournalCapture } from '@/hooks/useExecutionJournalCapture';
import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { useWatchlistRisk } from '@/hooks/useWatchlistRisk';
import {
  buildDashboardHotIndustryPicks,
  buildMainlineAllowSet,
  isSectorOutflowBlock,
} from '@/lib/hot-industry-picks';
import { writeLastCopyAt } from '@/lib/copy-ai-brief';
import {
  buildDashboardCopyAllMarkdown,
  buildIndustryMarkdown,
} from '@/lib/dashboard-export';
import { refetchWatchlistMarket } from '@/lib/queries/watchlist';
import { loadWatchlist } from '@/lib/watchlist-storage';
import {
  fmtDateTime,
  loadCopyMode,
  saveCopyMode,
} from '@/lib/dashboard-format';
import { parseExecutionGate } from '@/lib/execution-action';
import { isShanghaiSyncWindow } from '@/lib/market-hours';
import { useChatStore } from '@/lib/chat/store';
import {
  formatGapUp,
  formatIntradayChgPct,
  formatVolumeRatio,
  isIntradaySurge,
  volumeRatioClassName,
} from '@/lib/watchlist-metrics';

export function DashboardPage({ onNavigate }: { onNavigate?: (pageId: string) => void }) {
  const queryClient = useQueryClient();
  const { addReference } = useChatStore();

  const {
    summary,
    summaryLoading,
    error,
    setError,
    applySummaryToCache,
    newsSummary,
    newsSummaryUpdatedAt,
    newsFallback,
    newsSummaryBusy,
    sentimentBusy,
    onSyncSentiment,
    regenerateNewsSummary,
    shouldRefreshNewsBrief,
    saveNewsBriefCache,
    setNewsSummary,
    setNewsSummaryUpdatedAt,
    setNewsSummaryBusy,
  } = useDashboardSummary();

  const { items: watchlistItems } = useWatchlistItems();
  const executionGate = React.useMemo(
    () => parseExecutionGate((summary as any)?.marketSentiment?.executionGate),
    [summary],
  );
  const mainlineAllow = React.useMemo(() => buildMainlineAllowSet(summary), [summary]);
  const sectorOutflowBlock = React.useMemo(() => isSectorOutflowBlock(summary), [summary]);
  const { capture: captureDecisionSnapshot, busy: decisionCaptureBusy } =
    useExecutionJournalCapture({
      items: watchlistItems,
      gate: executionGate,
      mainlineAllow,
      sectorOutflowBlock,
      enabled: true,
    });

  const { syncResp, busy, syncSteps, syncProgress, onSyncAll } = useDashboardSync({
    applySummaryToCache,
    shouldRefreshNewsBrief,
    newsSummary,
    newsSummaryUpdatedAt,
    setNewsSummary,
    setNewsSummaryUpdatedAt,
    setNewsSummaryBusy,
    saveNewsBriefCache,
    setError,
    forceRefreshWatchlistOnSync: async () => {
      // Cover HK/ETF watchlist symbols that the backend sync steps do not
      // touch. Reads from local watchlist so this also runs even when the
      // user is on Dashboard and not on the Watchlist page.
      const items = loadWatchlist();
      const symbols = items
        .map((it) => (typeof it?.symbol === 'string' ? it.symbol.trim().toUpperCase() : ''))
        .filter(Boolean);
      if (!symbols.length) return;
      await refetchWatchlistMarket(queryClient, symbols, { forceMarket: true });
    },
    onSyncComplete: () => {
      void captureDecisionSnapshot('sync_all');
    },
  });

  const {
    rows: watchlistRiskRows,
    busy: watchlistRiskBusy,
    updatedAt: watchlistRiskUpdatedAt,
    refetch: refetchWatchlistRisk,
  } = useWatchlistRisk();

  const [industryCopyStatus, setIndustryCopyStatus] = React.useState<{
    ok: boolean;
    text: string;
  } | null>(null);
  const [sentimentCopyStatus, setSentimentCopyStatus] = React.useState<{
    ok: boolean;
    text: string;
  } | null>(null);
  const [copyAllBusy, setCopyAllBusy] = React.useState(false);
  const [copyAllStatus, setCopyAllStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );

  const hotIndustryPicks = React.useMemo(() => buildDashboardHotIndustryPicks(summary), [summary]);

  // Loose JSON schema from API; cast for nested card rendering.
  const dash = summary as any;

  const industryCopyTimerRef = React.useRef<number | null>(null);
  const sentimentCopyTimerRef = React.useRef<number | null>(null);
  const copyAllTimerRef = React.useRef<number | null>(null);

  React.useEffect(() => {
    return () => {
      if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
      if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
      if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
    };
  }, []);

  function toastIndustryCopy(ok: boolean, text: string) {
    setIndustryCopyStatus({ ok, text });
    if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
    industryCopyTimerRef.current = window.setTimeout(() => setIndustryCopyStatus(null), 2400);
  }

  function toastSentimentCopy(ok: boolean, text: string) {
    setSentimentCopyStatus({ ok, text });
    if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
    sentimentCopyTimerRef.current = window.setTimeout(() => setSentimentCopyStatus(null), 2400);
  }

  function toastCopyAll(ok: boolean, text: string) {
    setCopyAllStatus({ ok, text });
    if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
    copyAllTimerRef.current = window.setTimeout(() => setCopyAllStatus(null), 2600);
  }

  async function onCopyIndustryMarkdown() {
    try {
      const ind = summary?.industryFundFlow as any;
      const hasTopByDate = Array.isArray(ind?.topByDate) && ind.topByDate.length > 0;
      const hasFlow5d = Array.isArray(ind?.flow5d?.top) && ind.flow5d.top.length > 0;
      const hasFlow5dOut = Array.isArray(ind?.flow5dOut?.top) && ind.flow5dOut.top.length > 0;
      if (!hasTopByDate && !hasFlow5d && !hasFlow5dOut) {
        toastIndustryCopy(false, 'Nothing to copy (no industry fund flow data).');
        return;
      }
      const md = buildIndustryMarkdown(summary, '#').trim();
      await navigator.clipboard.writeText(md);
      toastIndustryCopy(true, 'Copied Markdown to clipboard.');
    } catch (e) {
      toastIndustryCopy(false, e instanceof Error ? e.message : String(e));
    }
  }

  async function copyAllMarkdown() {
    setCopyAllBusy(true);
    setError(null);
    try {
      if (!summary) {
        toastCopyAll(false, 'No data available. Please refresh first.');
        return;
      }
      const text = await buildDashboardCopyAllMarkdown({
        summary,
        newsSummary,
        newsSummaryUpdatedAt,
        newsFallback,
        queryClient,
        mode: copyMode,
      });
      await navigator.clipboard.writeText(text);
      writeLastCopyAt(new Date().toISOString());
      toastCopyAll(true, `Copied ${copyMode === 'compact' ? 'compact' : 'full'} Markdown to clipboard.`);
    } catch (e) {
      toastCopyAll(false, e instanceof Error ? e.message : String(e));
    } finally {
      setCopyAllBusy(false);
    }
  }

  async function syncAndCopyMarkdown() {
    setCopyAllBusy(true);
    setError(null);
    try {
      const syncResult = await onSyncAll();
      if (!syncResult.ok) {
        toastCopyAll(false, 'Sync failed. Copy aborted.');
        return;
      }
      const s = syncResult.summary ?? summary;
      if (!s) {
        toastCopyAll(false, 'No data available after sync.');
        return;
      }
      const text = await buildDashboardCopyAllMarkdown({
        summary: s,
        newsSummary,
        newsSummaryUpdatedAt,
        newsFallback,
        queryClient,
        mode: copyMode,
      });
      await navigator.clipboard.writeText(text);
      writeLastCopyAt(new Date().toISOString());
      toastCopyAll(true, `Synced and copied ${copyMode === 'compact' ? 'compact' : 'full'} Markdown to clipboard.`);
    } catch (e) {
      toastCopyAll(false, e instanceof Error ? e.message : String(e));
    } finally {
      setCopyAllBusy(false);
    }
  }

  type DashCard = { id: string; title: string };
  const cardsById: Record<string, DashCard> = React.useMemo(
    () => ({
      industry: { id: 'industry', title: '行业资金流' },
      sentiment: { id: 'sentiment', title: '市场情绪' },
      brief: { id: 'brief', title: '新闻简报' },
      watchlistRisk: { id: 'watchlistRisk', title: 'Watchlist 风险警报' },
      decisions: { id: 'decisions', title: '执行日志' },
      etf: { id: 'etf', title: 'ETF资金流' },
    }),
    [],
  );

  const [copyMode, setCopyMode] = React.useState<'full' | 'compact'>('full');
  React.useEffect(() => {
    setCopyMode(loadCopyMode());
  }, []);
  React.useEffect(() => {
    saveCopyMode(copyMode);
  }, [copyMode]);

  const columnCards = React.useMemo(() => {
    const visible = (id: string) => id !== 'watchlistRisk' || watchlistRiskRows.length > 0;
    return {
      left: ['industry', 'watchlistRisk', 'etf', 'decisions'].filter(visible).map((id) => cardsById[id]),
      right: ['sentiment', 'brief'].filter(visible).map((id) => cardsById[id]),
    };
  }, [cardsById, watchlistRiskRows.length]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Dashboard</div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy}
            onClick={() => void syncAndCopyMarkdown()}
          >
            {busy || copyAllBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            Sync & Copy
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || !summary}
            onClick={() => void copyAllMarkdown()}
          >
            {copyAllBusy && !busy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            Copy for AI
          </Button>
          <div className="inline-flex items-center rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-0.5 text-xs">
            <button
              type="button"
              className={`rounded px-2 py-1 ${
                copyMode === 'compact'
                  ? 'bg-emerald-600/15 text-emerald-700 font-medium'
                  : 'text-[var(--k-muted)] hover:bg-[var(--k-surface-2)]'
              }`}
              onClick={() => setCopyMode('compact')}
              aria-pressed={copyMode === 'compact'}
              title="极速决策模式 — 剪贴板只保留核心 20% 数据"
            >
              Compact
            </button>
            <button
              type="button"
              className={`rounded px-2 py-1 ${
                copyMode === 'full'
                  ? 'bg-emerald-600/15 text-emerald-700 font-medium'
                  : 'text-[var(--k-muted)] hover:bg-[var(--k-surface-2)]'
              }`}
              onClick={() => setCopyMode('full')}
              aria-pressed={copyMode === 'full'}
              title="完整模式 — 盘后归档/存盘用"
            >
              Full
            </button>
          </div>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[var(--k-muted)]">
        <span>
          asOfDate: <span className="font-mono">{String(dash?.asOfDate ?? '—')}</span>
        </span>
        {summaryLoading ? (
          <span className="inline-flex items-center gap-1">
            <RefreshCw className="h-3 w-3 animate-spin" />
            Updating…
          </span>
        ) : null}
        {!summaryLoading && summary && !isShanghaiSyncWindow() ? (
          <span>盘后模式：仅读缓存，同步跳过实时抓取</span>
        ) : null}
      </div>
      {copyAllStatus ? (
        <div className={`mb-4 text-xs ${copyAllStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {copyAllStatus.text}
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {busy && syncSteps.length > 0 ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-medium">Syncing…</div>
            <div className="text-xs text-[var(--k-muted)]">{syncProgress}%</div>
          </div>
          <Progress value={syncProgress} className="mb-4" />
          <div className="space-y-2">
            {syncSteps.map((s) => (
              <div key={s.name} className="flex items-center gap-3 text-xs">
                {s.ok === null ? (
                  <span className="h-4 w-4 rounded-full bg-[var(--k-muted)]/30 animate-pulse" />
                ) : s.ok ? (
                  <span className="h-4 w-4 rounded-full bg-emerald-500" />
                ) : (
                  <span className="h-4 w-4 rounded-full bg-red-500" />
                )}
                <span className="font-mono">
                  {s.name === 'industryFundFlow'
                    ? 'Industry Fund Flow'
                    : s.name === 'marketSentiment'
                      ? 'Market Sentiment'
                      : s.name === 'screeners'
                        ? 'Screeners'
                        : s.name === 'news'
                          ? 'News'
                          : s.name}
                </span>
                {s.durationMs !== null ? (
                  <span className="text-[var(--k-muted)]">{s.durationMs}ms</span>
                ) : null}
                {s.message ? (
                  <span className="text-red-600 truncate">{s.message}</span>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {syncResp ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 flex flex-wrap items-center gap-3 text-xs">
            <span className="text-sm font-medium">Last sync</span>
            <span className="text-[var(--k-muted)]">
              {fmtDateTime(syncResp.startedAt as string)} → {fmtDateTime(syncResp.finishedAt as string)}
            </span>
            <span
              className={`rounded px-2 py-0.5 ${
                syncResp.ok
                  ? 'bg-emerald-500/15 text-emerald-700'
                  : 'bg-red-500/15 text-red-700'
              }`}
            >
              {syncResp.ok ? 'OK' : 'FAILED'}
            </span>
            {((syncResp.steps as any[]) ?? []).length ? (
              <span className="text-[var(--k-muted)]">
                {(syncResp.steps as any[]).map((s: any) => {
                  const ok = Boolean(s.ok);
                  const name = String(s.name ?? '');
                  const ms = Number(s.durationMs ?? 0);
                  return (
                    <span
                      key={name}
                      className={`ml-2 inline-flex items-center gap-1 font-mono ${
                        ok ? 'text-emerald-700' : 'text-red-700'
                      }`}
                      title={String(s.message ?? '')}
                    >
                      {ok ? '✓' : '✗'} {name} {ms}ms
                    </span>
                  );
                })}
              </span>
            ) : null}
          </div>
          {(syncResp.screener as any)?.failed?.length || (syncResp.screener as any)?.missing?.length ? (
            <div className="text-xs text-red-600">
              Screener issues: failed={(syncResp.screener as any)?.failed?.length ?? 0} missing=
              {(syncResp.screener as any)?.missing?.length ?? 0}
            </div>
          ) : null}
        </div>
      ) : null}

      {(() => {
        const left = columnCards.left;
        const right = columnCards.right;

        const renderCard = (c: any) => {
          const id = String(c.id);
          return (
            <section
              key={id}
              className="min-h-0 flex flex-col rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
            >
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="text-sm font-medium">{c.title}</div>
              </div>

              {id === 'decisions' ? (
                <DecisionJournalCard
                  gate={executionGate}
                  watchlistItems={watchlistItems}
                  mainlineAllow={mainlineAllow}
                  onNavigate={onNavigate}
                  captureBusy={decisionCaptureBusy}
                  onSnapshotNow={() => void captureDecisionSnapshot('manual')}
                />
              ) : null}

              {id === 'sentiment' ? (
                <MarketSentimentCard
                  dash={dash}
                  summary={summary}
                  sentimentBusy={sentimentBusy}
                  onSyncSentiment={() => void onSyncSentiment()}
                  toastSentimentCopy={toastSentimentCopy}
                  sentimentCopyStatus={sentimentCopyStatus}
                  addReference={addReference}
                />
              ) : id === 'etf' ? (
                <EtfFundFlowCard dash={dash} />
              ) : id === 'industry' ? (
                <IndustryFundFlowCard
                  summary={dash}
                  hotIndustryPicks={hotIndustryPicks}
                  onNavigate={onNavigate}
                  onAddReference={addReference}
                  copyStatus={industryCopyStatus}
                  onCopyIndustryMarkdown={onCopyIndustryMarkdown}
                />
              ) : id === 'brief' ? (
                <MorningBriefCard
                  onNavigate={onNavigate}
                  newsSummary={newsSummary}
                  newsSummaryBusy={newsSummaryBusy}
                  onRegenerateNews={() => void regenerateNewsSummary()}
                />
              ) : id === 'watchlistRisk' ? (
                <div>
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--k-muted)]">
                    <span>
                      Intraday &gt;6%、跳空缺口（弱势/震荡）、VWAP 溢价等建仓风险预警
                    </span>
                    <span>
                      {watchlistRiskUpdatedAt
                        ? `Updated ${fmtDateTime(watchlistRiskUpdatedAt)}`
                        : '—'}
                    </span>
                  </div>
                  {watchlistRiskBusy && !watchlistRiskRows.length ? (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      <RefreshCw className="mr-2 inline h-4 w-4 animate-spin" />
                      Loading watchlist risk alerts...
                    </div>
                  ) : watchlistRiskRows.length ? (
                    <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                      <table className="w-full border-collapse text-xs">
                        <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                          <tr className="text-left">
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.symbol" align="left" width={260} />
                            </th>
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.name" align="left" width={260} />
                            </th>
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.intradayPct" align="left" width={300} />
                            </th>
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.vr" align="left" width={300} />
                            </th>
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.gap" align="left" width={300} />
                            </th>
                            <th className="px-2 py-2 whitespace-nowrap">
                              <DashboardHeader helpId="risk.alerts" align="left" width={360} />
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {watchlistRiskRows.map((row) => {
                            const hasBlock = row.alerts.some((a) => a.severity === 'block');
                            return (
                              <tr
                                key={row.symbol}
                                className={`border-t border-[var(--k-border)] ${
                                  hasBlock ? 'bg-red-50/70' : 'bg-amber-50/50'
                                }`}
                              >
                                <td className="px-2 py-2 font-mono text-red-700">{row.symbol}</td>
                                <td className="px-2 py-2">{row.name}</td>
                                <td
                                  className={`px-2 py-2 font-mono ${
                                    isIntradaySurge(row.intradayChgPct)
                                      ? 'font-semibold text-red-600'
                                      : ''
                                  }`}
                                >
                                  {formatIntradayChgPct(row.intradayChgPct)}
                                </td>
                                <td
                                  className={`px-2 py-2 font-mono ${volumeRatioClassName(row.volumeRatio)}`}
                                >
                                  {formatVolumeRatio(row.volumeRatio)}
                                </td>
                                <td
                                  className={`px-2 py-2 font-mono ${
                                    row.gapUp === true ? 'font-semibold text-red-600' : ''
                                  }`}
                                >
                                  {formatGapUp(row.gapUp)}
                                </td>
                                <td className="px-2 py-2">
                                  {row.alerts.map((alert) => (
                                    <div
                                      key={alert.code}
                                      className={
                                        alert.severity === 'block'
                                          ? 'text-red-600'
                                          : 'text-amber-700'
                                      }
                                    >
                                      {alert.message}
                                    </div>
                                  ))}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      No watchlist risk alerts. Add symbols to Watchlist or refresh during session.
                    </div>
                  )}
                  <div className="mt-3 flex items-center gap-2">
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={watchlistRiskBusy}
                      onClick={() => void refetchWatchlistRisk()}
                    >
                      {watchlistRiskBusy ? (
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <RefreshCw className="mr-2 h-4 w-4" />
                      )}
                      Refresh alerts
                    </Button>
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('watchlist')}>
                      Open Watchlist
                    </Button>
                  </div>
                </div>
              ) : null}
            </section>
          );
        };

        return (
          <>
            <div className="space-y-4 lg:hidden">
              {[...left, ...right].map(renderCard)}
            </div>
            <div className="hidden lg:flex lg:items-start lg:gap-4">
              <div className="flex min-w-0 flex-1 flex-col gap-4">{left.map(renderCard)}</div>
              <div className="flex min-w-0 flex-1 flex-col gap-4">{right.map(renderCard)}</div>
            </div>
          </>
        );
      })()}
    </div>
  );
}
