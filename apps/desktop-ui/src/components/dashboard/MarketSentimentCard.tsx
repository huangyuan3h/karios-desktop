/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw, ChevronDown, ChevronRight } from 'lucide-react';

import { DashboardHeader } from '@/components/dashboard/DashboardHeader';
import { Button } from '@/components/ui/button';
import {
  BREADTH_PANIC_DOWN_THRESHOLD,
  buildIndexTrafficSummary,
  executionGateBadgeClass,
  fmtAmountCn,
  fmtSignedAmountCn,
  formatSrvIndexLine,
  srvIndexBadgeClass,
} from '@/lib/dashboard-format';
import { parseExecutionGate } from '@/lib/execution-action';
import { buildSentimentMarkdown } from '@/lib/dashboard-export';

type Props = {
  dash: any;
  summary: any;
  sentimentBusy: boolean;
  onSyncSentiment: () => void;
  toastSentimentCopy: (ok: boolean, text: string) => void;
  sentimentCopyStatus: { ok: boolean; text: string } | null;
  addReference: (ref: any) => void;
};

export function MarketSentimentCard({
  dash,
  summary,
  sentimentBusy,
  onSyncSentiment,
  toastSentimentCopy,
  sentimentCopyStatus,
  addReference,
}: Props) {
  const [detailsOpen, setDetailsOpen] = React.useState(false);

  const ms = dash?.marketSentiment ?? {};
  const items: any[] = Array.isArray(ms.items) ? ms.items : [];
  const latest = items.length ? items[items.length - 1] : null;
  const indexSignals: any[] = Array.isArray(ms.indexSignals) ? ms.indexSignals : [];
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
    risk === 'confirmed_uptrend'
      ? 'border-emerald-600/40 bg-emerald-600/15 text-emerald-700'
      : risk === 'capitulation_v_bottom'
        ? 'border-fuchsia-600/40 bg-fuchsia-600/15 text-fuchsia-700'
        : risk === 'extreme_caution' || breadthPanic
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

  const gate = parseExecutionGate(ms?.executionGate);

  return (
    <div className="flex flex-col gap-3">
      {/* Execution Gate */}
      {gate ? (
        <div className={`rounded-lg border px-3 py-2 text-sm ${executionGateBadgeClass(gate.mode)}`}>
          <div className="flex flex-wrap items-center gap-2 font-medium">
            <span>Execution Gate: {gate.mode}</span>
            <span className="text-xs opacity-90">allowNewEntries={String(gate.allowNewEntries)}</span>
            <span className="text-xs opacity-90">
              {gate.marketRegime} · {gate.indexLight} · pos {gate.positionRangeHint || '—'}
            </span>
          </div>
          {gate.satelliteNote ? (
            <div className="mt-1 text-xs opacity-90">{gate.satelliteNote}</div>
          ) : null}
          {gate.reasons.length ? (
            <div className="mt-1 text-xs opacity-80">reasons: {gate.reasons.join(' · ')}</div>
          ) : null}
        </div>
      ) : null}

      {/* Risk + SRV + Traffic — 3-column compact grid */}
      <div className="grid grid-cols-3 gap-2">
        <div className={`rounded-lg border px-3 py-2 text-xs ${badge}`}>
          <div className="mb-1 font-medium uppercase tracking-wide opacity-60">Risk</div>
          <div className="text-base font-bold">{risk}</div>
          {Array.isArray(latest?.rules) && latest.rules.length ? (
            <div className="mt-1 text-[10px] opacity-80">
              {latest.rules.slice(0, 2).map((x: any) => String(x)).join(' · ')}
            </div>
          ) : null}
        </div>

        <div className={`rounded-lg border px-3 py-2 text-xs ${srvBadge}`}>
          <div className="mb-1 font-medium uppercase tracking-wide opacity-60">SRV Index</div>
          <div className="text-base font-bold">{srvLine}</div>
          {srvIndex?.labelZh ? (
            <div className="mt-1 text-[10px] opacity-90">{String(srvIndex.labelZh)}</div>
          ) : null}
          {overlapSectors.length ? (
            <div className="mt-1 text-[10px] opacity-80">Overlap: {overlapSectors.join(', ')}</div>
          ) : null}
        </div>

        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs">
          <div className="mb-1 font-medium uppercase tracking-wide text-amber-600">Traffic</div>
          <div className="text-sm font-bold text-amber-700">{summaryLine.title}</div>
          <div className="mt-1 text-[10px] text-amber-800">{summaryLine.detail}</div>
        </div>
      </div>

      {/* Special alerts */}
      {risk === 'capitulation_v_bottom' && (
        <div className="rounded-lg border border-fuchsia-600/40 bg-fuchsia-600/15 px-3 py-2 text-xs text-fuchsia-800 dark:text-fuchsia-200">
          🚨 Capitulation_V_Bottom (恐慌冰点共振) — 国家队入场 + 恐慌极值，左侧绝佳试错点出现
        </div>
      )}
      {risk === 'confirmed_uptrend' && (
        <div className="rounded-lg border border-emerald-600/40 bg-emerald-600/15 px-3 py-2 text-xs text-emerald-800 dark:text-emerald-200">
          🟢 Follow-Through Day 右侧主升浪确立 — 解除宏观死锁，放开攻击权限
        </div>
      )}

      {/* Market Breadth — compact stat grid */}
      <div
        className={`rounded-lg border px-3 py-2 text-xs ${
          breadthPanic ? 'border-red-500/40 bg-red-500/10' : 'border-[var(--k-border)] bg-[var(--k-surface-2)]'
        }`}
      >
        <div className={`mb-2 font-medium ${breadthPanic ? 'text-red-700' : 'text-[var(--k-fg)]'}`}>
          涨跌家数
        </div>
        <div className="grid grid-cols-7 gap-1 text-center">
          <div>
            <div className="text-[10px] opacity-60">涨</div>
            <div className="font-mono text-emerald-600">{up.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">跌</div>
            <div className="font-mono text-red-600">{down.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">平</div>
            <div className="font-mono text-[var(--k-muted)]">{flat.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">比</div>
            <div className="font-mono">{ratio}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">成交额</div>
            <div className="font-mono">{turnover}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">溢价</div>
            <div className="font-mono">{premium}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">炸板</div>
            <div className="font-mono">{failed}</div>
          </div>
        </div>
        {breadthPanic ? (
          <div className="mt-1 text-[10px] text-red-700">
            Down &ge; {BREADTH_PANIC_DOWN_THRESHOLD.toLocaleString()}: force red lights and extreme caution.
          </div>
        ) : null}
      </div>

      {/* Index Signals — compact grid */}
      {indexSignals.length ? (
        <div>
          <div className="mb-1 flex items-center gap-2 text-xs text-[var(--k-muted)]">
            <DashboardHeader helpId="idxRule.title" align="left" width={420} />
          </div>
          <div className="grid gap-2 md:grid-cols-2">
            {indexSignals.map((it: any) => {
              const signal = String(it?.signal ?? 'unknown');
              const source = String(it?.source ?? 'unknown');
              const tradeTime = String(it?.tradeTime ?? '').trim();
              const asOfDate = String(it?.asOfDate ?? '').trim();
              const realtime = it?.realtime === true;
              const freshness = realtime ? 'realtime' : 'EOD';
              const asOfDisplay = tradeTime || asOfDate || '—';
              const quoteError = String(it?.quoteError ?? '').trim();
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
                  <div className="font-medium">{String(it?.name ?? it?.tsCode ?? '')}</div>
                  <div className="mt-1 font-mono">
                    {signal} · pos {String(it?.positionRange ?? '—')}
                  </div>
                  <div className="mt-1 text-[var(--k-muted)]">
                    chg{' '}
                    {Number.isFinite(it?.pctChg)
                      ? `${Number(it.pctChg) >= 0 ? '+' : ''}${Number(it.pctChg).toFixed(2)}%`
                      : '—'}{' '}
                    · close{' '}
                    {Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—'} · MA5{' '}
                    {Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—'} · MA20{' '}
                    {Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—'}
                  </div>
                  <div className="mt-1 font-mono text-[10px] text-[var(--k-muted)]">
                    asOf {asOfDisplay} · {freshness} · {source}
                  </div>
                  {quoteError ? (
                    <div className="mt-1 text-[10px] text-amber-700 dark:text-amber-300">
                      quote fallback: {quoteError}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}

      {/* Collapsible details: 5-day + ETF */}
      <div>
        <button
          type="button"
          className="flex w-full items-center gap-1.5 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs font-medium text-[var(--k-muted)] hover:bg-[var(--k-surface)] transition-colors"
          onClick={() => setDetailsOpen((v) => !v)}
        >
          {detailsOpen ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
          Details — Last 5 days · ETF Fund Flow
        </button>
        {detailsOpen && (
          <div className="mt-2 space-y-3">
            {/* Last 5 days */}
            <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
              <table className="w-full border-collapse text-xs">
                <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                  <tr className="text-left">
                    <th className="px-2 py-2 whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.date" align="left" width={280} />
                    </th>
                    <th className="px-2 py-2 text-right whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.ratio" align="right" width={340} />
                    </th>
                    <th className="px-2 py-2 text-right whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.turnover" align="right" width={340} />
                    </th>
                    <th className="px-2 py-2 text-right whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.premiumPct" align="right" width={360} />
                    </th>
                    <th className="px-2 py-2 text-right whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.failedPct" align="right" width={360} />
                    </th>
                    <th className="px-2 py-2 whitespace-nowrap">
                      <DashboardHeader helpId="sentiment5d.risk" align="left" width={340} />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {(items || []).slice(-5).map((it: any, idx: number) => (
                    <tr key={idx} className="border-t border-[var(--k-border)]">
                      <td className="px-2 py-2 font-mono">{String(it.date ?? '')}</td>
                      <td className="px-2 py-2 text-right font-mono">
                        {Number.isFinite(it.upDownRatio) ? Number(it.upDownRatio).toFixed(2) : '—'}
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
                      <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={6}>
                        No sentiment cached yet. Click &quot;Sync & Copy&quot;.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>

            {/* ETF Fund Flow */}
            {(() => {
              const etfFlow: any = ms?.etfFundFlow ?? {};
              const etfItems: any[] = Array.isArray(etfFlow?.items) ? etfFlow.items : [];
              return (
                <div>
                  <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">
                    ETF Fund Flow (Top Watchlist)
                  </div>
                  {etfFlow?.shareLag ? (
                    <div className="mb-1 text-[10px] text-amber-600 dark:text-amber-400">
                      Realtime East Money flow is incomplete. Missing rows are excluded from intraday signals
                      {etfFlow?.intradaySafe === false ? ' — not safe for intraday decisions' : ''}.
                    </div>
                  ) : null}
                  <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                    <table className="w-full border-collapse text-xs">
                      <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                        <tr className="text-left">
                          <th className="px-2 py-2 whitespace-nowrap">
                            <DashboardHeader helpId="etf.name" align="left" width={300} />
                          </th>
                          <th className="px-2 py-2 font-mono whitespace-nowrap">
                            <DashboardHeader helpId="etf.symbol" align="left" width={300} />
                          </th>
                          <th className="px-2 py-2 text-right whitespace-nowrap">
                            <DashboardHeader helpId="etf.mainFlow" align="right" width={340} />
                          </th>
                          <th className="px-2 py-2 text-right whitespace-nowrap">
                            <DashboardHeader helpId="etf.superLarge" align="right" width={360} />
                          </th>
                          <th className="px-2 py-2 text-right whitespace-nowrap">
                            <DashboardHeader helpId="etf.flow3d" align="right" width={300} />
                          </th>
                          <th className="px-2 py-2 whitespace-nowrap">
                            <DashboardHeader helpId="etf.realtimeAsOf" align="left" width={320} />
                          </th>
                          <th className="px-2 py-2 whitespace-nowrap">
                            <DashboardHeader helpId="etf.source" align="left" width={300} />
                          </th>
                          <th className="px-2 py-2 whitespace-nowrap">
                            <DashboardHeader helpId="etf.status" align="left" width={320} />
                          </th>
                          <th className="px-2 py-2 whitespace-nowrap">
                            <DashboardHeader helpId="etf.signal" align="left" width={340} />
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {etfItems.map((it: any, idx: number) => {
                          const flowStatus = String(it?.flowStatus ?? (it?.live === true ? 'Live' : '—'));
                          const live = it?.live === true || flowStatus === 'Live';
                          const isMarketClosed = flowStatus === 'MarketClosed';
                          const flow1dStale =
                            !live && !isMarketClosed && it?.netFlow1d == null && (it?.flowAsOfDate != null || it?.netFlow1dLagged != null);
                          const flow1dDisplay = flow1dStale ? '— (stale)' : fmtSignedAmountCn(it?.netFlow1d);
                          const superLargeFlow = fmtSignedAmountCn(it?.superLargeNetInflow);
                          const largeFlow = fmtSignedAmountCn(it?.largeNetInflow);
                          const signalText = String(it?.signalDisplay ?? it?.signal ?? '—');
                          const isDataLag = String(it?.signal ?? '') === 'Data Lag';
                          const realtimeAsOf = String(it?.tradeTime ?? it?.flowAsOfDate ?? etfFlow?.asOfDate ?? '—');
                          return (
                            <tr key={idx} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-2">{String(it?.name ?? '')}</td>
                              <td className="px-2 py-2 font-mono">{String(it?.symbol ?? '')}</td>
                              <td className="px-2 py-2 text-right font-mono">{flow1dDisplay}</td>
                              <td className="px-2 py-2 text-right font-mono">{superLargeFlow}/{largeFlow}</td>
                              <td className="px-2 py-2 text-right font-mono">{fmtSignedAmountCn(it?.netFlow3d)}</td>
                              <td className="px-2 py-2 font-mono">{realtimeAsOf}</td>
                              <td className="px-2 py-2 font-mono">{String(it?.source ?? '—')}</td>
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
                              <td className={isDataLag ? 'px-2 py-2 text-[var(--k-muted)]' : 'px-2 py-2'}>
                                {signalText}
                              </td>
                            </tr>
                          );
                        })}
                        {!etfItems.length ? (
                          <tr>
                            <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={9}>
                              No ETF fund flow cached yet. Click &quot;Sync sentiment&quot;.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                </div>
              );
            })()}
          </div>
        )}
      </div>

      {/* Action buttons */}
      <div className="flex items-center gap-2">
        <Button size="sm" variant="secondary" disabled={sentimentBusy} onClick={() => onSyncSentiment()}>
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
                .catch(() => toastSentimentCopy(false, 'Copy failed. Please allow clipboard access.'));
            } catch (e) {
              toastSentimentCopy(false, e instanceof Error ? e.message : String(e));
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
            });
          }}
        >
          Reference
        </Button>
      </div>
      {sentimentCopyStatus ? (
        <div className={`text-xs ${sentimentCopyStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {sentimentCopyStatus.text}
        </div>
      ) : null}
    </div>
  );
}
