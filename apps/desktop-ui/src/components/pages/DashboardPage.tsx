/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { RefreshCw } from 'lucide-react';

import { IndustryFundFlowCard } from '@/components/dashboard/IndustryFundFlowCard';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { useDashboardSummary } from '@/hooks/useDashboardSummary';
import { useDashboardSync } from '@/hooks/useDashboardSync';
import { useWatchlistRisk } from '@/hooks/useWatchlistRisk';
import { buildDashboardHotIndustryPicks } from '@/lib/hot-industry-picks';
import {
  buildDashboardCopyAllMarkdown,
  buildIndustryMarkdown,
  buildSentimentMarkdown,
} from '@/lib/dashboard-export';
import {
  BREADTH_PANIC_DOWN_THRESHOLD,
  buildIndexTrafficSummary,
  fmtAmountCn,
  fmtSignedAmountCn,
  formatSrvIndexLine,
  srvIndexBadgeClass,
  fmtDateTime,
  loadCardOrder,
  saveCardOrder,
} from '@/lib/dashboard-format';
import { AI_BASE_URL } from '@/lib/endpoints';
import {
  downloadInvestmentDailyPdf,
  parseInvestmentDailyReportResponse,
  truncateMarkdownForReport,
} from '@/lib/investmentDailyPdf';
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
    refetchSummary,
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
  const [pdfReportBusy, setPdfReportBusy] = React.useState(false);
  const [pdfReportStatus, setPdfReportStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [editLayout, setEditLayout] = React.useState(false);

  const hotIndustryPicks = React.useMemo(() => buildDashboardHotIndustryPicks(summary), [summary]);

  // Loose JSON schema from API; cast for nested card rendering.
  const dash = summary as any;

  const industryCopyTimerRef = React.useRef<number | null>(null);
  const sentimentCopyTimerRef = React.useRef<number | null>(null);
  const copyAllTimerRef = React.useRef<number | null>(null);
  const pdfReportTimerRef = React.useRef<number | null>(null);

  React.useEffect(() => {
    return () => {
      if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
      if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
      if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
      if (pdfReportTimerRef.current != null) window.clearTimeout(pdfReportTimerRef.current);
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

  function toastPdfReport(ok: boolean, text: string) {
    setPdfReportStatus({ ok, text });
    if (pdfReportTimerRef.current != null) window.clearTimeout(pdfReportTimerRef.current);
    pdfReportTimerRef.current = window.setTimeout(() => setPdfReportStatus(null), 3200);
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
      });
      await navigator.clipboard.writeText(text);
      toastCopyAll(true, 'Copied all Markdown to clipboard.');
    } catch (e) {
      toastCopyAll(false, e instanceof Error ? e.message : String(e));
    } finally {
      setCopyAllBusy(false);
    }
  }

  async function onDownloadInvestmentDailyPdf() {
    setPdfReportBusy(true);
    setError(null);
    try {
      if (!summary) {
        toastPdfReport(false, 'No data available. Please refresh first.');
        return;
      }
      const rawMd = await buildDashboardCopyAllMarkdown({
        summary,
        newsSummary,
        newsSummaryUpdatedAt,
        newsFallback,
        queryClient,
      });
      const markdown = truncateMarkdownForReport(rawMd);
      const aiRes = await fetch(`${AI_BASE_URL}/report/investment-daily`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ markdown }),
      });
      const rawText = await aiRes.text();
      let data: unknown = null;
      try {
        data = rawText ? JSON.parse(rawText) : null;
      } catch {
        throw new Error(rawText || `AI error (${aiRes.status})`);
      }
      if (!aiRes.ok) {
        const errMsg =
          data && typeof data === 'object' && 'error' in data
            ? String((data as { error?: unknown }).error)
            : rawText;
        throw new Error(errMsg || `AI error (${aiRes.status})`);
      }
      const report = parseInvestmentDailyReportResponse(data);
      const subtitleTimeZh = new Date().toLocaleString('zh-CN', { hour12: false });
      const datePart = new Date().toISOString().slice(0, 10);
      await downloadInvestmentDailyPdf({
        report,
        subtitleTimeZh,
        filename: `投资要点日报-${datePart}.pdf`,
        summary,
        hotIndustryPicks,
      });
      toastPdfReport(true, 'PDF downloaded.');
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toastPdfReport(false, msg);
    } finally {
      setPdfReportBusy(false);
    }
  }

  const defaultCards = React.useMemo(
    () => [
      { id: 'industry', title: 'Industry fund flow' },
      { id: 'sentiment', title: 'Market sentiment' },
      { id: 'watchlistRisk', title: 'Watchlist 风险警报' },
      { id: 'news', title: 'News brief' },
      { id: 'screeners', title: 'Screener sync' },
    ],
    [],
  );

  const [cardOrder, setCardOrder] = React.useState<string[]>(() => []);
  React.useEffect(() => {
    const loaded = loadCardOrder();
    const ids = defaultCards.map((c) => c.id);
    const next = loaded
      ? [...loaded.filter((x) => ids.includes(x)), ...ids.filter((x) => !loaded.includes(x))]
      : ids;
    const nextIds = next.includes('industry')
      ? ['industry', ...next.filter((x) => x !== 'industry')]
      : next;
    setCardOrder(nextIds);
    saveCardOrder(nextIds);
  }, [defaultCards]);

  const cardsById = React.useMemo(
    () => Object.fromEntries(defaultCards.map((c) => [c.id, c])),
    [defaultCards],
  );
  const orderedCards = cardOrder.map((id) => cardsById[id]).filter(Boolean);

  function moveCard(id: string, dir: -1 | 1) {
    const idx = cardOrder.indexOf(id);
    if (idx < 0) return;
    const j = idx + dir;
    if (j < 0 || j >= cardOrder.length) return;
    const next = [...cardOrder];
    const tmp = next[idx];
    next[idx] = next[j];
    next[j] = tmp;
    setCardOrder(next);
    saveCardOrder(next);
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Dashboard</div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => {
              setError(null);
              void refetchSummary();
            }}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => void copyAllMarkdown()}
          >
            {copyAllBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            Copy all Markdown
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => void onDownloadInvestmentDailyPdf()}
          >
            {pdfReportBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            下载 PDF
          </Button>
          <Button
            variant="secondary"
            size="sm"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => {
              const asOfDate = String(summary?.asOfDate ?? '');
              const capturedAt = new Date().toISOString();
              addReference({
                kind: 'dashboardAll',
                refId: `dashboardAll:${asOfDate}:${Date.now()}`,
                asOfDate,
                title: 'Dashboard Overview',
                capturedAt,
              } as any);
            }}
          >
            Reference all
          </Button>
          <Button size="sm" className="gap-2" disabled={busy} onClick={() => void onSyncAll()}>
            {busy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            {busy
              ? 'Syncing…'
              : isShanghaiSyncWindow()
                ? 'Sync all (force)'
                : 'Sync all (cached)'}
          </Button>
          <Button size="sm" variant="secondary" onClick={() => setEditLayout((v) => !v)}>
            {editLayout ? 'Done' : 'Edit layout'}
          </Button>
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
      {pdfReportStatus ? (
        <div
          className={`mb-4 text-xs ${pdfReportStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}
        >
          {pdfReportStatus.text}
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
          <div className="mb-2 text-sm font-medium">Last sync result</div>
          <div className="text-xs text-[var(--k-muted)]">
            started: {fmtDateTime(syncResp.startedAt as string)} • finished:{' '}
            {fmtDateTime(syncResp.finishedAt as string)} • ok: {String(Boolean(syncResp.ok))}
          </div>
          <div className="mt-3 overflow-auto rounded-lg border border-[var(--k-border)]">
            <table className="w-full border-collapse text-xs">
              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2">Step</th>
                  <th className="px-3 py-2">OK</th>
                  <th className="px-3 py-2">Duration</th>
                  <th className="px-3 py-2">Message</th>
                </tr>
              </thead>
              <tbody>
                {((syncResp.steps as any[]) ?? []).map((s: any) => (
                  <tr key={String(s.name)} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">{String(s.name)}</td>
                    <td className="px-3 py-2">{String(Boolean(s.ok))}</td>
                    <td className="px-3 py-2 font-mono">{String(s.durationMs ?? 0)}ms</td>
                    <td className="px-3 py-2 text-[var(--k-muted)]">{String(s.message ?? '')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {(syncResp.screener as any)?.failed?.length || (syncResp.screener as any)?.missing?.length ? (
            <div className="mt-3 text-xs text-red-600">
              Screener issues: failed={(syncResp.screener as any)?.failed?.length ?? 0} missing=
              {(syncResp.screener as any)?.missing?.length ?? 0}
            </div>
          ) : null}
        </div>
      ) : null}

      {(() => {
        const weightOf = (id: string) => {
          if (id === 'industry') return 6;
          if (id === 'sentiment') return 3;
          if (id === 'watchlistRisk') return 2;
          if (id === 'news') return 2;
          if (id === 'screeners') return 2;
          return 2;
        };
        const left: any[] = [];
        const right: any[] = [];
        let wl = 0;
        let wr = 0;
        for (const c of orderedCards) {
          const id = String(c.id);
          const w = weightOf(id);
          if (wl <= wr) {
            left.push(c);
            wl += w;
          } else {
            right.push(c);
            wr += w;
          }
        }

        const renderCard = (c: any) => {
          const id = String(c.id);
          return (
            <section
              key={id}
              className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
            >
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="text-sm font-medium">{c.title}</div>
                {editLayout ? (
                  <div className="flex items-center gap-1">
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, -1)}
                    >
                      ↑
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, 1)}
                    >
                      ↓
                    </Button>
                  </div>
                ) : null}
              </div>

              {id === 'sentiment' ? (
                <div>
                  {(() => {
                    const ms = dash?.marketSentiment ?? {};
                    const items: any[] = Array.isArray(ms.items) ? ms.items : [];
                    const latest = items.length ? items[items.length - 1] : null;
                    const indexSignals: any[] = Array.isArray(ms.indexSignals)
                      ? ms.indexSignals
                      : [];
                    const summaryLine = buildIndexTrafficSummary(indexSignals);
                    const risk = String(latest?.riskMode ?? '—');
                    const premium = Number.isFinite(latest?.yesterdayLimitUpPremium)
                      ? `${Number(latest.yesterdayLimitUpPremium).toFixed(2)}%`
                      : '—';
                    const failed = Number.isFinite(latest?.failedLimitUpRate)
                      ? `${Number(latest.failedLimitUpRate).toFixed(1)}%`
                      : '—';
                    const turnover = fmtAmountCn(latest?.marketTurnoverCny);
                    const ratio = Number.isFinite(latest?.upDownRatio)
                      ? Number(latest.upDownRatio).toFixed(2)
                      : '—';
                    const up = Number(latest?.upCount ?? 0);
                    const down = Number(latest?.downCount ?? 0);
                    const flat = Number(latest?.flatCount ?? 0);
                    const breadthPanic = down >= BREADTH_PANIC_DOWN_THRESHOLD;
                    const srvIndex: any = ms?.srvIndex ?? null;
                    const srvLine = formatSrvIndexLine(srvIndex);
                    const srvBadge = srvIndexBadgeClass(srvIndex?.level);
                    const overlapSectors: string[] = Array.isArray(srvIndex?.overlapSectors)
                      ? srvIndex.overlapSectors.map((x: any) => String(x)).filter(Boolean)
                      : [];
                    const badge =
                      risk === 'extreme_caution' || breadthPanic
                        ? 'border-red-600/40 bg-red-600/15 text-red-700'
                        : risk === 'no_new_positions'
                          ? 'border-red-500/30 bg-red-500/10 text-red-600'
                          : risk === 'caution'
                            ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                            : risk === 'hot'
                              ? 'border-green-500/30 bg-green-500/10 text-green-700'
                              : risk === 'euphoric'
                                ? 'border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-700'
                                : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
                    return (
                      <>
                        <div className="mb-2 flex flex-wrap items-center gap-2">
                          <div className={`rounded-md border px-2 py-1 text-xs ${badge}`}>
                            risk: {risk}
                          </div>
                          {Array.isArray(latest?.rules) && latest.rules.length ? (
                            <div className="text-xs text-[var(--k-muted)]">
                              {latest.rules
                                .slice(0, 2)
                                .map((x: any) => String(x))
                                .join(' • ')}
                            </div>
                          ) : null}
                        </div>

                        <div className={`mb-3 rounded-lg border px-3 py-2 text-sm ${srvBadge}`}>
                          <div className="font-medium">{srvLine}</div>
                          {srvIndex?.labelZh ? (
                            <div className="mt-1 text-xs opacity-90">{String(srvIndex.labelZh)}</div>
                          ) : null}
                          {overlapSectors.length ? (
                            <div className="mt-1 text-xs opacity-90">
                              Overlap: {overlapSectors.join(', ')}
                            </div>
                          ) : null}
                        </div>

                        <div
                          className={`mb-3 rounded-lg border px-3 py-2 text-sm ${
                            breadthPanic
                              ? 'border-red-500/40 bg-red-500/10'
                              : 'border-[var(--k-border)] bg-[var(--k-surface-2)]'
                          }`}
                        >
                          <div
                            className={`font-medium ${
                              breadthPanic ? 'text-red-700' : 'text-[var(--k-fg)]'
                            }`}
                          >
                            Market Breadth: {up.toLocaleString()} Up / {down.toLocaleString()}{' '}
                            Down
                          </div>
                          {breadthPanic ? (
                            <div className="mt-1 text-xs text-red-700">
                              Down &ge; {BREADTH_PANIC_DOWN_THRESHOLD.toLocaleString()}: force red
                              lights and extreme caution.
                            </div>
                          ) : null}
                        </div>

                        <div className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                          <div className="text-sm font-semibold text-amber-700">
                            {summaryLine.title}
                          </div>
                          <div className="mt-1 text-xs text-amber-800">{summaryLine.detail}</div>
                        </div>

                        <div className="grid grid-cols-2 gap-2 text-sm">
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Up/Down/Flat</div>
                            <div className="mt-1 font-mono">
                              {up}/{down}/{flat}
                            </div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">ratio: {ratio}</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              turnover: {turnover}
                            </div>
                          </div>
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Sentiment</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              yesterday limit-up premium
                            </div>
                            <div className="mt-0.5 font-mono">{premium}</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              failed limit-up rate
                            </div>
                            <div className="mt-0.5 font-mono">{failed}</div>
                          </div>
                        </div>

                        {indexSignals.length ? (
                          <div className="mt-3">
                            <div className="mb-2 text-xs text-[var(--k-muted)]">
                              Index traffic lights
                            </div>
                            <div className="mb-3 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
                              <div className="font-medium text-[var(--k-fg)]">信号规则（简版）</div>
                              <div className="mt-1">
                                🔴 Red: Price &lt; MA20 或 MA5 &lt; MA20，仓位 0%-10%。
                              </div>
                              <div className="mt-1">
                                🟡 Yellow: Price &gt; MA20 但 MA20 斜率向下 或 预估全天量 &lt;
                                MA5_Vol * 0.8 或 MA5 &lt; MA20，仓位 30%。
                              </div>
                              <div className="mt-1">
                                🟢 Green: Price &gt; MA20 且 MA5 &gt; MA20 且 MA20
                                向上，且预估全天量 &gt; MA5_Vol * 0.8，仓位 50%-60%。
                              </div>
                              <div className="mt-1">
                                ❇️ Deep Green: MA5 &gt; MA20 &gt; MA60 且 Price &gt; EMA10，全市场成交额连续
                                &gt; 1.5万亿，Breadth &gt; 50% 或 单一板块流入 &gt; 50亿，仓位
                                80%-100%。
                              </div>
                            </div>
                            <div className="grid gap-2 md:grid-cols-2">
                              {indexSignals.map((it: any) => {
                                const signal = String(it?.signal ?? 'unknown');
                                const signalBadge =
                                  signal === 'deep_green'
                                    ? 'border-emerald-600/40 bg-emerald-600/15 text-emerald-800'
                                    : signal === 'light_green' || signal === 'green'
                                      ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700'
                                      : signal === 'red'
                                        ? 'border-red-500/30 bg-red-500/10 text-red-600'
                                        : signal === 'yellow'
                                          ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                                          : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
                                return (
                                  <div
                                    key={String(it?.tsCode ?? it?.name)}
                                    className={`rounded-lg border px-3 py-2 text-xs ${signalBadge}`}
                                  >
                                    <div className="font-medium">
                                      {String(it?.name ?? it?.tsCode ?? '')}
                                    </div>
                                    <div className="mt-1 font-mono">
                                      {signal} • pos {String(it?.positionRange ?? '—')}
                                    </div>
                                    <div className="mt-1 text-[var(--k-muted)]">
                                      chg{' '}
                                      {Number.isFinite(it?.pctChg)
                                        ? `${Number(it.pctChg) >= 0 ? '+' : ''}${Number(it.pctChg).toFixed(2)}%`
                                        : '—'}{' '}
                                      • close{' '}
                                      {Number.isFinite(it?.close)
                                        ? Number(it.close).toFixed(2)
                                        : '—'}{' '}
                                      • MA5{' '}
                                      {Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—'} •
                                      MA20{' '}
                                      {Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—'}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        ) : null}

                        <div className="mt-3">
                          <div className="mb-2 text-xs text-[var(--k-muted)]">Last 5 days</div>
                          <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                            <table className="w-full border-collapse text-xs">
                              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                <tr className="text-left">
                                  <th className="px-2 py-2 font-mono">date</th>
                                  <th className="px-2 py-2 text-right">ratio</th>
                                  <th className="px-2 py-2 text-right">turnover</th>
                                  <th className="px-2 py-2 text-right">premium%</th>
                                  <th className="px-2 py-2 text-right">failed%</th>
                                  <th className="px-2 py-2">risk</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(items || []).slice(-5).map((it: any, idx: number) => (
                                  <tr key={idx} className="border-t border-[var(--k-border)]">
                                    <td className="px-2 py-2 font-mono">{String(it.date ?? '')}</td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.upDownRatio)
                                        ? Number(it.upDownRatio).toFixed(2)
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {fmtAmountCn(it.marketTurnoverCny)}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.yesterdayLimitUpPremium)
                                        ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.failedLimitUpRate)
                                        ? `${Number(it.failedLimitUpRate).toFixed(1)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2">{String(it.riskMode ?? '')}</td>
                                  </tr>
                                ))}
                                {!items.length ? (
                                  <tr>
                                    <td
                                      className="px-2 py-3 text-sm text-[var(--k-muted)]"
                                      colSpan={7}
                                    >
                                      No sentiment cached yet. Click “Sync all (force)”.
                                    </td>
                                  </tr>
                                ) : null}
                              </tbody>
                            </table>
                          </div>
                        </div>

                        {(() => {
                          const etfFlow: any = ms?.etfFundFlow ?? {};
                          const etfItems: any[] = Array.isArray(etfFlow?.items)
                            ? etfFlow.items
                            : [];
                          return (
                            <div className="mt-3">
                              <div className="mb-2 text-xs font-medium text-[var(--k-muted)]">
                                ETF Fund Flow (Top Watchlist)
                              </div>
                              {etfFlow?.shareLag ? (
                                <div className="mb-2 text-xs text-amber-600 dark:text-amber-400">
                                  Realtime East Money flow is incomplete. Missing rows are excluded from intraday signals
                                  {etfFlow?.intradaySafe === false ? ' — not safe for intraday decisions' : ''}.
                                </div>
                              ) : null}
                              <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                                <table className="w-full border-collapse text-xs">
                                  <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                    <tr className="text-left">
                                      <th className="px-2 py-2">ETF Name</th>
                                      <th className="px-2 py-2 font-mono">Symbol</th>
                                      <th className="px-2 py-2 text-right">Main Flow</th>
                                      <th className="px-2 py-2 text-right">Super/Large</th>
                                      <th className="px-2 py-2 text-right">3D Net Flow</th>
                                      <th className="px-2 py-2">Realtime AsOf</th>
                                      <th className="px-2 py-2">Source</th>
                                      <th className="px-2 py-2">Status</th>
                                      <th className="px-2 py-2">Signal</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {etfItems.map((it: any, idx: number) => {
                                      const flowStatus = String(
                                        it?.flowStatus ?? (it?.live === true ? 'Live' : '—'),
                                      );
                                      const live = it?.live === true || flowStatus === 'Live';
                                      const isMarketClosed = flowStatus === 'MarketClosed';
                                      const flow1dStale =
                                        !live &&
                                        !isMarketClosed &&
                                        it?.netFlow1d == null &&
                                        (it?.flowAsOfDate != null || it?.netFlow1dLagged != null);
                                      const flow1dDisplay = flow1dStale
                                        ? '— (stale)'
                                        : fmtSignedAmountCn(it?.netFlow1d);
                                      const superLargeFlow = fmtSignedAmountCn(it?.superLargeNetInflow);
                                      const largeFlow = fmtSignedAmountCn(it?.largeNetInflow);
                                      const signalText = String(
                                        it?.signalDisplay ?? it?.signal ?? '—',
                                      );
                                      const isDataLag = String(it?.signal ?? '') === 'Data Lag';
                                      const realtimeAsOf = String(
                                        it?.tradeTime ?? it?.flowAsOfDate ?? etfFlow?.asOfDate ?? '—',
                                      );
                                      return (
                                      <tr key={idx} className="border-t border-[var(--k-border)]">
                                        <td className="px-2 py-2">{String(it?.name ?? '')}</td>
                                        <td className="px-2 py-2 font-mono">
                                          {String(it?.symbol ?? '')}
                                        </td>
                                        <td className="px-2 py-2 text-right font-mono">
                                          {flow1dDisplay}
                                        </td>
                                        <td className="px-2 py-2 text-right font-mono">
                                          {superLargeFlow}/{largeFlow}
                                        </td>
                                        <td className="px-2 py-2 text-right font-mono">
                                          {fmtSignedAmountCn(it?.netFlow3d)}
                                        </td>
                                        <td className="px-2 py-2 font-mono">
                                          {realtimeAsOf}
                                        </td>
                                        <td className="px-2 py-2 font-mono">
                                          {String(it?.source ?? '—')}
                                        </td>
                                        <td
                                          className={`px-2 py-2 font-mono ${
                                            flowStatus === 'Live'
                                              ? 'font-semibold text-emerald-600'
                                              : isMarketClosed
                                                ? 'text-[var(--k-muted)]'
                                                : flowStatus === 'Stale' || flowStatus === 'Missing'
                                                  ? 'text-amber-600'
                                                  : 'text-[var(--k-muted)]'
                                          }`}
                                        >
                                          {isMarketClosed ? 'Market Closed' : flowStatus}
                                        </td>
                                        <td
                                          className={
                                            isDataLag
                                              ? 'px-2 py-2 text-[var(--k-muted)]'
                                              : 'px-2 py-2'
                                          }
                                        >
                                          {signalText}
                                        </td>
                                      </tr>
                                    );})}
                                    {!etfItems.length ? (
                                      <tr>
                                        <td
                                          className="px-2 py-3 text-sm text-[var(--k-muted)]"
                                          colSpan={9}
                                        >
                                          No ETF fund flow cached yet. Click &quot;Sync
                                          sentiment&quot;.
                                        </td>
                                      </tr>
                                    ) : null}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          );
                        })()}

                        <div className="mt-3 flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            disabled={sentimentBusy}
                            onClick={() => void onSyncSentiment()}
                          >
                            {sentimentBusy ? (
                              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                              <RefreshCw className="mr-2 h-4 w-4" />
                            )}
                            Sync sentiment
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              try {
                                const md = buildSentimentMarkdown(summary, '#');
                                void navigator.clipboard
                                  .writeText(md)
                                  .then(() => toastSentimentCopy(true, 'Copied Markdown.'))
                                  .catch(() =>
                                    toastSentimentCopy(
                                      false,
                                      'Copy failed. Please allow clipboard access.',
                                    ),
                                  );
                              } catch (e) {
                                toastSentimentCopy(
                                  false,
                                  e instanceof Error ? e.message : String(e),
                                );
                              }
                            }}
                          >
                            Copy Markdown
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              const asOfDate = String(ms.asOfDate ?? dash?.asOfDate ?? '');
                              addReference({
                                kind: 'marketSentiment',
                                refId: `${asOfDate}:5`,
                                asOfDate,
                                days: 5,
                                title: 'CN market sentiment (breadth & limit-up)',
                                createdAt: new Date().toISOString(),
                              } as any);
                            }}
                          >
                            Reference
                          </Button>
                        </div>
                        {sentimentCopyStatus ? (
                          <div
                            className={`mt-2 text-xs ${
                              sentimentCopyStatus.ok ? 'text-emerald-600' : 'text-red-600'
                            }`}
                          >
                            {sentimentCopyStatus.text}
                          </div>
                        ) : null}
                      </>
                    );
                  })()}
                </div>
              ) : id === 'industry' ? (
                <IndustryFundFlowCard
                  summary={dash}
                  hotIndustryPicks={hotIndustryPicks}
                  onNavigate={onNavigate}
                  onAddReference={addReference}
                  copyStatus={industryCopyStatus}
                  onCopyIndustryMarkdown={onCopyIndustryMarkdown}
                />
              ) : id === 'news' ? (
                <div>
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <div className="text-xs text-[var(--k-muted)]">
                      24-hour news summary (AI-generated, finance/stock focused)
                    </div>
                    {newsSummaryUpdatedAt ? (
                      <div className="text-xs text-[var(--k-muted)]">
                        Generated: {fmtDateTime(newsSummaryUpdatedAt)}
                      </div>
                    ) : null}
                  </div>
                  {newsSummaryBusy ? (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      <RefreshCw className="mr-2 inline h-4 w-4 animate-spin" />
                      Generating AI summary...
                    </div>
                  ) : newsSummary || newsFallback ? (
                    <div className="rounded-lg border border-blue-500/30 bg-blue-500/10 p-4 text-sm">
                      {newsSummary?.trim() || newsFallback?.trim()}
                    </div>
                  ) : (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      No summary yet. Click &quot;Sync all&quot; to fetch news and generate summary.
                    </div>
                  )}
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('news')}>
                      Open News
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={newsSummaryBusy}
                      onClick={() => void regenerateNewsSummary()}
                    >
                      {newsSummaryBusy ? (
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <RefreshCw className="mr-2 h-4 w-4" />
                      )}
                      Regenerate
                    </Button>
                  </div>
                </div>
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
                            <th className="px-2 py-2">Symbol</th>
                            <th className="px-2 py-2">Name</th>
                            <th className="px-2 py-2">Intraday%</th>
                            <th className="px-2 py-2">VR</th>
                            <th className="px-2 py-2">Gap</th>
                            <th className="px-2 py-2">Alerts</th>
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
              ) : id === 'screeners' ? (
                <div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    Enabled screeners (no content). Missing/rowCount=0 will be highlighted.
                  </div>
                  <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                    <table className="w-full border-collapse text-xs">
                      <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                        <tr className="text-left">
                          <th className="px-2 py-2">Name</th>
                          <th className="px-2 py-2">capturedAt</th>
                          <th className="px-2 py-2 text-right">rows</th>
                          <th className="px-2 py-2 text-right">filters</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(dash?.screeners ?? []).map((s: any) => {
                          const bad = !s.capturedAt || Number(s.rowCount ?? 0) <= 0;
                          return (
                            <tr key={String(s.id)} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-2">{String(s.name ?? s.id)}</td>
                              <td className={`px-2 py-2 font-mono ${bad ? 'text-red-600' : ''}`}>
                                {String(s.capturedAt ?? '—')}
                              </td>
                              <td
                                className={`px-2 py-2 text-right font-mono ${bad ? 'text-red-600' : ''}`}
                              >
                                {String(s.rowCount ?? 0)}
                              </td>
                              <td className="px-2 py-2 text-right font-mono">
                                {String(s.filtersCount ?? 0)}
                              </td>
                            </tr>
                          );
                        })}
                        {!(dash?.screeners ?? []).length ? (
                          <tr>
                            <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={4}>
                              No enabled screeners.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('screener')}>
                      Open Screener
                    </Button>
                  </div>
                </div>
              ) : null}
            </section>
          );
        };

        return (
          <>
            <div className="space-y-4 lg:hidden">{orderedCards.map(renderCard)}</div>
            <div className="hidden lg:grid lg:grid-cols-2 lg:gap-4">
              <div className="space-y-4">{left.map(renderCard)}</div>
              <div className="space-y-4">{right.map(renderCard)}</div>
            </div>
          </>
        );
      })()}

      {editLayout ? (
        <div className="mt-4 text-xs text-[var(--k-muted)]">
          Layout config is saved locally. Drag-and-drop UI can be added later; for now use ↑/↓.
        </div>
      ) : null}
    </div>
  );
}
