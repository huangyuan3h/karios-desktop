'use client';

import * as React from 'react';

import { FunnelHistoryTable } from '@/components/watchlist/FunnelHistoryTable';
import { TodayActionCard } from '@/components/watchlist/TodayActionCard';
import { BehaviorAuditBanner } from '@/components/watchlist/BehaviorAuditBanner';
import { PickStrongAlignBanner } from '@/components/watchlist/PickStrongAlignBanner';
import { EtfExecutionLogCard } from '@/components/watchlist/EtfExecutionLogCard';
import { PortfolioHealthCard } from '@/components/watchlist/PortfolioHealthCard';
import { ThirdAssetSleeveBanner } from '@/components/watchlist/ThirdAssetSleeveBanner';
import { TradingBriefCard } from '@/components/watchlist/TradingBriefCard';
import { TradeStatsPanel } from '@/components/watchlist/TradeStatsPanel';
import { WatchlistInsightsPanel } from '@/components/watchlist/WatchlistInsightsPanel';
import { sortWatchlistItems, WatchlistTable } from '@/components/watchlist/WatchlistTable';
import { WatchlistToolbar } from '@/components/watchlist/WatchlistToolbar';
import { Button } from '@/components/ui/button';
import { useExecutionJournalCapture } from '@/hooks/useExecutionJournalCapture';
import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';
import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { useWatchlistTrend } from '@/hooks/useWatchlistTrend';
import {
  buildCatalystPurgeMap,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
} from '@/lib/alpha-radar-catalyst';
import { useChatStore } from '@/lib/chat/store';
import { useStrategyMode } from '@/lib/strategy-settings';
import { executionGateBadgeClass } from '@/lib/dashboard-format';
import {
  buildSleeveExposurePct,
  countHeldMissingPositionPct,
  formatSleeveBudgetLabel,
  parseExecutionGate,
} from '@/lib/execution-action';
import { buildMainlineAllowSet, isSectorOutflowBlock } from '@/lib/hot-industry-picks';
import { useAlphaRadarCatalystQuery } from '@/lib/queries/alphaRadar';
import { useDashboardSummaryQuery } from '@/lib/queries/dashboard';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { useWatchlistRsRanksQuery, watchlistMarketKey } from '@/lib/queries/watchlist';
import { scoreExplainZhLines } from '@/lib/trendok-display';
import {
  fetchAutomationLatest,
  formatAutomationSummary,
  runManualAutomation,
  type AutomationRun,
} from '@/lib/watchlist-automation';
import { copyWatchlistMarkdown } from '@/lib/watchlist-export';
import { loadWatchlist } from '@/lib/watchlist-storage';

export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const sentimentQuery = useDashboardSentimentQuery();
  const liteSummaryQuery = useDashboardSummaryQuery();
  const catalystQuery = useAlphaRadarCatalystQuery(DEFAULT_CATALYST_MAX_AGE_DAYS);
  const catalystBySymbol = React.useMemo(
    () => buildCatalystPurgeMap(catalystQuery.data ?? null),
    [catalystQuery.data],
  );
  const executionGate = React.useMemo(
    () =>
      parseExecutionGate(
        (sentimentQuery.data as { marketSentiment?: { executionGate?: unknown } } | undefined)
          ?.marketSentiment?.executionGate,
      ),
    [sentimentQuery.data],
  );
  const mainlineAllow = React.useMemo(
    () => buildMainlineAllowSet(liteSummaryQuery.data ?? null),
    [liteSummaryQuery.data],
  );
  const sectorOutflowBlock = React.useMemo(
    () => isSectorOutflowBlock(liteSummaryQuery.data ?? null),
    [liteSummaryQuery.data],
  );
  const {
    items,
    setItems,
    persist,
    watchlistHydrating,
    error,
    setError,
    code,
    setCode,
    costPriceDrafts,
    positionPctDrafts,
    onAdd,
    onRemove,
    addSymbolToWatchlist,
    setItemColor,
    setItemPositionPct,
    setItemPositionPctDraft,
    commitItemPositionPctDraft,
    setItemCostPriceDraft,
    setItemCostPriceValue,
    commitItemCostPriceDraft,
    applyTradeUpdate,
  } = useWatchlistItems();

  useExecutionJournalCapture({
    items,
    gate: executionGate,
    mainlineAllow,
    sectorOutflowBlock,
    enabled: true,
  });

  const symbols = React.useMemo(
    () => items.map((x) => x.symbol).filter(Boolean),
    [items],
  );

  const {
    trend,
    quotes,
    trendBusy,
    trendUpdatedAt,
    syncMsg,
    setSyncMsg,
    onManualRefreshTrend,
    queryClient,
  } = useWatchlistTrend(symbols, items, persist);

  const [strategyMode] = useStrategyMode();
  const showSingleTrack = strategyMode !== 'twin_star';

  const [syncBusy, setSyncBusy] = React.useState(false);
  const [syncStage, setSyncStage] = React.useState<string | null>(null);
  const [syncProgress, setSyncProgress] = React.useState<{ cur: number; total: number } | null>(
    null,
  );
  const [syncLogs, setSyncLogs] = React.useState<string[]>([]);
  const [automationBusy, setAutomationBusy] = React.useState(false);
  const [automationStage, setAutomationStage] = React.useState<string | null>(null);
  const [automationLogs, setAutomationLogs] = React.useState<string[]>([]);
  const [automationMsg, setAutomationMsg] = React.useState<string | null>(null);
  const [latestAutomation, setLatestAutomation] = React.useState<AutomationRun | null>(null);
  const [automationSkipRun, setAutomationSkipRun] = React.useState<AutomationRun | null>(null);
  const [copyMdStatus, setCopyMdStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [copyMdBusy, setCopyMdBusy] = React.useState(false);
  const copyMdTimerRef = React.useRef<number | null>(null);


  const [scoreSortDir, setScoreSortDir] = React.useState<'desc' | 'asc'>('desc');
  const [scoreSortEnabled, setScoreSortEnabled] = React.useState(true);
  const [showHidden, setShowHidden] = React.useState(false);
  const [hideAuditExtra, setHideAuditExtra] = React.useState(false);
  // OPT-106: symbols flagged by the behavior audit (买了不该买/该卖没卖) —
  // shared query cache with the banner (no duplicate requests).
  const auditRows = useBehaviorAuditQuery().data ?? [];
  const auditExtraSymbols = React.useMemo(
    () => new Set(auditRows.flatMap((r) => (r.extraList ?? []).map((e) => e.symbol))),
    [auditRows],
  );

  React.useEffect(() => {
    void fetchAutomationLatest()
      .then((run) => {
        if (run) setLatestAutomation(run);
      })
      .catch(() => {
        // ignore
      });
  }, []);

  // Global notification hub scroll: jump to the anchored health block and
  // flash it so "提醒我做操作" lands on the details.
  React.useEffect(() => {
    function onScrollTo(e: Event) {
      const anchor = (e as CustomEvent<{ anchor: string }>).detail?.anchor;
      if (!anchor) return;
      const ids = [anchor, `${anchor}-hk`];
      const el = ids
        .map((id) => document.getElementById(id))
        .find((x): x is HTMLElement => Boolean(x));
      if (!el) return;
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      el.classList.add('health-flash');
      window.setTimeout(() => el.classList.remove('health-flash'), 1600);
    }
    window.addEventListener('karios-scroll-to', onScrollTo);
    return () => window.removeEventListener('karios-scroll-to', onScrollTo);
  }, []);

  React.useEffect(
    () => () => {
      if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    },
    [],
  );

  // RS percentiles (whole-market, /watchlist/rs-ranks) — tiebreaker for the
  // score sort; the table's own rs query shares the same query key/cache.
  const rsRanksQuery = useWatchlistRsRanksQuery(items.map((i) => i.symbol));

  const sortedItems = React.useMemo(
    () =>
      sortWatchlistItems(
        items,
        trend,
        scoreSortEnabled,
        scoreSortDir,
        rsRanksQuery.data?.ranks ?? null,
      ),
    [items, trend, scoreSortEnabled, scoreSortDir, rsRanksQuery.data],
  );

  const macroLockBanner = React.useMemo(() => {
    for (const t of Object.values(trend)) {
      if (t?.macroLock?.active) {
        return t.macroLock;
      }
      const checks = t?.buyChecks as Record<string, unknown> | null | undefined;
      if (checks?.blocked_macro_lock === true) {
        return {
          active: true,
          riskMode: String(checks.macroRiskMode ?? 'extreme_caution'),
          downCount:
            typeof checks.macroDownCount === 'number' ? checks.macroDownCount : undefined,
        };
      }
    }
    return null;
  }, [trend]);

  const watchlistSet = React.useMemo(() => new Set(items.map((x) => x.symbol)), [items]);

  async function onRunAutomation(force = true) {
    setError(null);
    setAutomationMsg(null);
    setAutomationBusy(true);
    setAutomationStage('Starting automation…');
    setAutomationLogs([]);
    const pushLog = (line: string) => {
      setAutomationLogs((prev) => [...prev, line].slice(-6));
    };
    try {
      const { run, result } = await runManualAutomation({
        force,
        onStage: (label) => {
          setAutomationStage(label);
          pushLog(label);
        },
      });
      setLatestAutomation({
        ...run,
        meta: {
          ...(run.meta && typeof run.meta === 'object' ? run.meta : {}),
        },
      });
      if (run.skipped) {
        setAutomationMsg(`Skipped: ${run.skipReason || 'unknown'}`);
        setAutomationSkipRun(run);
        return;
      }
      setAutomationSkipRun(null);
      setItems(loadWatchlist());
      void queryClient.invalidateQueries({ queryKey: watchlistMarketKey(symbols) });
      const summary = formatAutomationSummary(run, result ?? null);
      setAutomationMsg(summary);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setAutomationBusy(false);
      setAutomationStage(null);
    }
  }

  function referenceTable() {
    const capturedAt = new Date().toISOString();
    const rows = sortedItems.slice(0, 50).map((it) => {
      const t = trend[it.symbol];
      return {
        symbol: it.symbol,
        name: it.name ?? null,
        asOfDate: t?.asOfDate ?? null,
        close: typeof t?.values?.close === 'number' ? t.values.close : null,
        trendOk: t?.trendOk ?? null,
        score: t?.score ?? null,
        stopLossPrice: t?.stopLossPrice ?? null,
        buyMode: t?.buyMode ?? null,
        buyAction: t?.buyAction ?? null,
        buyZoneLow: t?.buyZoneLow ?? null,
        buyZoneHigh: t?.buyZoneHigh ?? null,
      };
    });
    addReference({
      kind: 'watchlistTable',
      refId: `${capturedAt}:${sortedItems.length}`,
      capturedAt,
      total: sortedItems.length,
      items: rows,
    });
  }

  function toastCopyMd(ok: boolean, text: string) {
    setCopyMdStatus({ ok, text });
    if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    copyMdTimerRef.current = window.setTimeout(() => setCopyMdStatus(null), 2400);
  }

  async function handleCopyWatchlistMarkdown() {
    setCopyMdBusy(true);
    try {
      const result = await copyWatchlistMarkdown({
        queryClient,
        sortedItems,
        trend,
        quotes,
        trendUpdatedAt,
        executionGate,
        mainlineAllow,
        sectorOutflowBlock,
      });
      if (!result.ok) {
        toastCopyMd(false, result.message);
        return;
      }
      try {
        await navigator.clipboard.writeText(result.markdown);
        toastCopyMd(true, 'Copied Markdown.');
      } catch {
        toastCopyMd(false, 'Copy failed. Please allow clipboard access.');
      }
    } catch (err) {
      console.warn('copy watchlist markdown failed:', err);
      toastCopyMd(false, 'Copy failed. Backend unavailable.');
    } finally {
      setCopyMdBusy(false);
    }
  }

  async function handleManualRefreshTrend() {
    setError(null);
    await onManualRefreshTrend();
  }

  return (
    <div className="box-border min-w-0 w-full max-w-full overflow-x-hidden p-6">
      {watchlistHydrating ? (
        <div className="text-sm text-[var(--k-muted)]">Loading watchlist…</div>
      ) : null}
      <div className={watchlistHydrating ? 'hidden' : undefined}>
        {macroLockBanner?.active ? (
          <div className="mb-4 rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-800 dark:text-red-200">
            🔒 宏观死锁生效中 — 全市场风险 {String(macroLockBanner.riskMode ?? 'extreme_caution')}
            {typeof macroLockBanner.downCount === 'number'
              ? `，下跌 ${macroLockBanner.downCount.toLocaleString()} 家`
              : ''}
            ，所有买入已强制拦截
          </div>
        ) : null}
        {showSingleTrack ? <TodayActionCard /> : null}
        {showSingleTrack ? (
          <>
            <PickStrongAlignBanner />
            <EtfExecutionLogCard />
          </>
        ) : null}
        {showSingleTrack ? (
        <details className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)]/30 px-3 py-2">
          <summary className="cursor-pointer text-xs text-[var(--k-muted)]">展开旧提醒（行为对账 / 轮动 / Gate 详情）</summary>
          <div className="mt-3">
            <BehaviorAuditBanner />
            {showSingleTrack ? <ThirdAssetSleeveBanner /> : null}
          </div>
        </details>
      ) : null}
        {executionGate && showSingleTrack ? (
          <div
            className={`mb-4 rounded-lg border px-4 py-3 text-sm ${executionGateBadgeClass(executionGate.mode)}`}
          >
            <div className="font-medium">
              Execution Gate: {executionGate.mode}
              <span className="ml-2 text-xs font-normal opacity-90">
                allowNewEntries={String(executionGate.allowNewEntries)} · {executionGate.marketRegime}
              </span>
            </div>
            <div className="mt-1 text-xs opacity-90">
              {formatSleeveBudgetLabel(
                buildSleeveExposurePct(items),
                executionGate.positionRangeHint,
              )}
            </div>
            <div className="mt-1 text-xs opacity-90">
              S-2 操作口径：{['Strong', 'Diverging'].includes(String(executionGate.marketRegime ?? ''))
                ? '✅ 非 Weak 可开仓'
                : '⏸ Weak 空仓等待'} · score≥70 · RS 前 50% · 移动止损 -8%
            </div>
            {(() => {
              const missingSize = countHeldMissingPositionPct(items);
              return missingSize > 0 ? (
                <div className="mt-1 text-xs font-medium text-amber-700 dark:text-amber-300">
                  {missingSize} held missing size
                </div>
              ) : null;
            })()}
            {executionGate.satelliteNote ? (
              <div className="mt-1 text-xs opacity-90">{executionGate.satelliteNote}</div>
            ) : null}
          </div>
        ) : null}
        <WatchlistToolbar
          trendUpdatedAt={trendUpdatedAt}
          latestAutomation={latestAutomation}
          syncBusy={syncBusy}
          syncStage={syncStage}
          syncProgress={syncProgress}
          syncLogs={syncLogs}
          automationBusy={automationBusy}
          automationStage={automationStage}
          automationLogs={automationLogs}
          automationMsg={automationMsg}
          automationSkipRun={automationSkipRun}
          syncMsg={syncMsg}
          copyMdStatus={copyMdStatus}
          error={error}
          trendBusy={trendBusy}
          itemsCount={items.length}
          sortedItemsCount={sortedItems.length}
          copyMdBusy={copyMdBusy}
          onManualRefreshTrend={() => void handleManualRefreshTrend()}
          onReferenceTable={referenceTable}
          onCopyMarkdown={() => void handleCopyWatchlistMarkdown()}
          onRunAutomation={() => void onRunAutomation(true)}
          onForceAutomationFromSkip={() => void onRunAutomation(true)}
        />

        <PortfolioHealthCard onOpenStock={onOpenStock} />

        {showSingleTrack ? <TradingBriefCard /> : null}


        <WatchlistInsightsPanel>
          <TradeStatsPanel />
          <FunnelHistoryTable limit={10} />

        </WatchlistInsightsPanel>

        <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">Add</div>
          <div className="grid gap-2 md:grid-cols-12">
            <input
              className="h-9 md:col-span-10 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
              placeholder="Ticker (e.g. 600000 / 0700 / CN:600000)"
              value={code}
              onChange={(e) => setCode(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') onAdd();
              }}
            />
            <div className="md:col-span-2 flex gap-2">
              <Button size="sm" onClick={onAdd} disabled={!code.trim()}>
                Add
              </Button>
              <Button
                size="sm"
                variant="secondary"
                onClick={() => {
                  setCode('');
                  setError(null);
                }}
                disabled={!code.trim() && !error}
              >
                Clear
              </Button>
            </div>
          </div>
          <div className="mt-2 text-xs text-[var(--k-muted)]">
            Supported inputs: CN 6-digit ticker, HK 4-5 digit ticker, or prefixed symbol (CN:/HK:).
          </div>
        </section>

        {showSingleTrack ? (
        <section className="mb-4 min-w-0 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="text-sm font-medium">Score（0–100）计分说明</div>
          <div className="mt-2 space-y-1.5 text-xs leading-relaxed text-[var(--k-text)]">
            {scoreExplainZhLines().map((line, i) => (
              <div key={i}>{line}</div>
            ))}
          </div>
          <div className="mt-3 text-[11px] leading-relaxed text-[var(--k-muted)]">
            鼠标悬停在列表「Score」数字上可查看该股各项得分（ema / macd / breakout / rsi / volume
            及加扣分）。
          </div>
        </section>
      ) : null}

        <WatchlistTable
          sortedItems={sortedItems}
          items={items}
          trend={trend}
          quotes={quotes}
          costPriceDrafts={costPriceDrafts}
          positionPctDrafts={positionPctDrafts}
          scoreSortDir={scoreSortDir}
          scoreSortEnabled={scoreSortEnabled}
          setScoreSortDir={setScoreSortDir}
          setScoreSortEnabled={setScoreSortEnabled}
          showHidden={showHidden}
          setShowHidden={setShowHidden}
          auditExtraSymbols={auditExtraSymbols}
          hideAuditExtra={hideAuditExtra}
          setHideAuditExtra={setHideAuditExtra}
          setItemColor={setItemColor}
          setItemPositionPct={setItemPositionPct}
          setItemPositionPctDraft={setItemPositionPctDraft}
          commitItemPositionPctDraft={commitItemPositionPctDraft}
          setItemCostPriceDraft={setItemCostPriceDraft}
          setItemCostPriceValue={setItemCostPriceValue}
          commitItemCostPriceDraft={commitItemCostPriceDraft}
          applyTradeUpdate={applyTradeUpdate}
          onRemove={onRemove}
          onOpenStock={onOpenStock}
          executionGate={executionGate}
          mainlineAllow={mainlineAllow}
          sectorOutflowBlock={sectorOutflowBlock}
          catalystBySymbol={catalystBySymbol}
        />
      </div>
    </div>
  );
}
