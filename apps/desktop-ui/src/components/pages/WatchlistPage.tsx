'use client';

import * as React from 'react';

import { AccountBar } from '@/components/account/AccountBar';
import { PortfolioHealthCard } from '@/components/watchlist/PortfolioHealthCard';
import { RegimeAutoCard } from '@/components/watchlist/RegimeAutoCard';
import { SgapHealthCard } from '@/components/watchlist/SgapHealthCard';
import { sortWatchlistItems, WatchlistTable } from '@/components/watchlist/WatchlistTable';
import { Button } from '@/components/ui/button';
import { useExecutionJournalCapture } from '@/hooks/useExecutionJournalCapture';
import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';
import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { useWatchlistTrend } from '@/hooks/useWatchlistTrend';
import { buildCatalystPurgeMap, DEFAULT_CATALYST_MAX_AGE_DAYS } from '@/lib/alpha-radar-catalyst';
import { parseExecutionGate } from '@/lib/execution-action';
import { buildMainlineAllowSet, isSectorOutflowBlock } from '@/lib/hot-industry-picks';
import { useAlphaRadarCatalystQuery } from '@/lib/queries/alphaRadar';
import { useDashboardSummaryQuery } from '@/lib/queries/dashboard';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { useWatchlistRegimeTargetQuery, useWatchlistRsRanksQuery } from '@/lib/queries/watchlist';
import { pushStrategyModeToServer, type StrategyMode } from '@/lib/strategy-settings';

/**
 * Watchlist follow page (母港M30 baseline).
 *
 * Top-to-bottom: 账户（最小一行，总资金是份数/金额换算的分母） →
 * 自动 regime 卡片（只读目标 + S-gap 健康度） → 操作引导（PortfolioHealthCard，
 * 顶部是今日结论 + 编号操作清单，次要细节在同一组件内弱化展示） →
 * watchlist 表格 + 添加输入框（页面本体）。
 *
 * TodayTodo / 跟仓 / 工具类卡片不在此页渲染（源码保留，以后可能复用，
 * 不要新建折叠“详情”区）。
 */
export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  // 2026-11-15: the watchlist is auto-regime, not a manual selector. The
  // month-end 000300-vs-MA200 target is the single source; the page mirrors it
  // to the backend so Bark/notification copy follows the same strategy.
  const regimeQ = useWatchlistRegimeTargetQuery();
  const autoMode = (regimeQ.data?.target ?? 'homeport') as StrategyMode;
  React.useEffect(() => {
    if (regimeQ.data?.target) pushStrategyModeToServer(regimeQ.data.target);
  }, [regimeQ.data?.target]);
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

  const symbols = React.useMemo(() => items.map((x) => x.symbol).filter(Boolean), [items]);

  const { trend, quotes } = useWatchlistTrend(symbols, items, persist);

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

  // Derived directly (no memo): React Compiler cannot preserve the manual
  // memo here, and the scan is a cheap pass over the trend map.
  const macroLockBanner = (() => {
    for (const t of Object.values(trend)) {
      if (t?.macroLock?.active) {
        return t.macroLock;
      }
      const checks = t?.buyChecks as Record<string, unknown> | null | undefined;
      if (checks?.blocked_macro_lock === true) {
        return {
          active: true,
          riskMode: String(checks.macroRiskMode ?? 'extreme_caution'),
          downCount: typeof checks.macroDownCount === 'number' ? checks.macroDownCount : undefined,
        };
      }
    }
    return null;
  })();

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
        <AccountBar compact />
        <RegimeAutoCard data={regimeQ.data} isLoading={regimeQ.isLoading} error={regimeQ.error} />
        <SgapHealthCard />
        <PortfolioHealthCard mode={autoMode} onOpenStock={onOpenStock} />

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
