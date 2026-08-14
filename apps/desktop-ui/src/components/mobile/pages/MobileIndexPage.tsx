'use client';

import * as React from 'react';

import { useMacroSnapshotQuery } from '@/lib/queries/macro';
import { MobileCard, MobileSection, PctText, StatusPill } from '../primitives';

/** 指数 (mobile) — CN index signals. §5.2 中频. */
function signalTone(signal?: string): 'open' | 'warn' | 'danger' | 'neutral' {
  if (signal === 'deep_green' || signal === 'light_green' || signal === 'green') return 'open';
  if (signal === 'yellow') return 'warn';
  if (signal === 'red') return 'danger';
  return 'neutral';
}

function signalZh(signal?: string): string {
  const zh: Record<string, string> = {
    deep_green: '深绿',
    light_green: '浅绿',
    green: '绿',
    yellow: '黄',
    red: '红',
  };
  return zh[String(signal ?? '')] ?? signal ?? '—';
}

export function MobileIndexPage() {
  const snap = useMacroSnapshotQuery();

  const rows = (snap.data?.cnIndexSignals ?? [])
    .filter((x) => x.name)
    .sort((a, b) => Number(b.featured ?? false) - Number(a.featured ?? false));

  const macroRows = snap.data?.macro ?? [];
  const etf = snap.data?.etfFlowSignal;
  const etfVerdictZh =
    etf?.verdict === 'confirm' ? '确认净流入' : etf?.verdict === 'contradict' ? '背离净流出' : etf?.verdict === 'neutral' ? '中性' : '—';

  return (
    <div className="space-y-4">
      {etf ? (
        <MobileSection title="ETF 资金流信号">
          <MobileCard className="p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="min-w-0">
                <div className="text-[var(--m-text-base)] font-semibold">{etfVerdictZh}</div>
                <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                  宽基 {etf.broadDirection === 'buy' ? '净买' : etf.broadDirection === 'outflow' ? '净流出' : etf.broadDirection === 'mixed' ? '分歧' : '—'}
                  {etf.sectorDirection ? ` · 板块 ${etf.sectorDirection === 'buy' ? '净买' : etf.sectorDirection === 'outflow' ? '净流出' : etf.sectorDirection === 'mixed' ? '分歧' : '—'}` : ''}
                </div>
              </div>
              <div className="shrink-0 text-right text-[var(--m-text-xs)] text-[var(--k-muted)]">
                {etf.asOfDate ? <div>{etf.asOfDate}</div> : null}
                {etf.confirmCount != null || etf.contradictCount != null ? (
                  <div>
                    确认 {etf.confirmCount ?? 0} / 背离 {etf.contradictCount ?? 0}
                  </div>
                ) : null}
              </div>
            </div>
          </MobileCard>
        </MobileSection>
      ) : null}

      <MobileSection
        title={`指数信号（${rows.length}）`}
        action={
          <button type="button" onClick={() => void snap.refetch()} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            刷新
          </button>
        }
      >
        {snap.data?.warning ? (
          <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3 text-[var(--m-text-sm)] text-[var(--k-warn)]">
            {snap.data.warning}
          </MobileCard>
        ) : null}

        {rows.length ? (
          <MobileCard>
            {rows.map((r, idx) => (
              <div
                key={r.tsCode ?? r.name}
                className={
                  idx === 0
                    ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                    : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                }
              >
                <div className="min-w-0 flex-1">
                  <div className="truncate text-[var(--m-text-base)] font-medium">
                    {r.name}
                    {r.featured ? <span className="ml-1.5"><StatusPill tone="open">聚焦</StatusPill></span> : null}
                  </div>
                  <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                    {signalZh(r.signal)}
                    {r.positionRange ? ` · pos ${r.positionRange}` : ''}
                  </div>
                </div>
                <div className="shrink-0 text-right">
                  <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                    {r.close != null ? r.close.toFixed(2) : '—'}
                  </div>
                  {r.pctChg != null ? <PctText value={r.pctChg} /> : null}
                </div>
                <div className="shrink-0">
                  <StatusPill tone={signalTone(r.signal)}>{signalZh(r.signal)}</StatusPill>
                </div>
              </div>
            ))}
          </MobileCard>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {snap.isLoading ? '加载中…' : '暂无指数数据'}
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title={`宏观指标（${macroRows.length}）`}>
        {macroRows.length ? (
          <MobileCard>
            {macroRows.map((r, idx) => (
              <div
                key={r.seriesId ?? r.name}
                className={
                  idx === 0
                    ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                    : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                }
              >
                <div className="min-w-0 flex-1">
                  <div className="truncate text-[var(--m-text-base)] font-medium">
                    {r.name}
                    {r.signalLabel ? (
                      <span className="ml-1.5">
                        <StatusPill tone={r.signal === 'up' || r.signal === 'buy' ? 'open' : r.signal === 'down' || r.signal === 'sell' ? 'danger' : 'neutral'}>
                          {r.signalLabel}
                        </StatusPill>
                      </span>
                    ) : null}
                  </div>
                  {r.why ? (
                    <div className="mt-0.5 truncate text-[var(--m-text-xs)] text-[var(--k-muted)]">{r.why}</div>
                  ) : null}
                </div>
                <div className="shrink-0 text-right">
                  <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                    {r.quotePrice ?? r.close ?? '—'}
                    {r.unit ? ` ${r.unit}` : ''}
                  </div>
                  {r.quotePctChg != null || r.pctChg != null ? (
                    <div className="text-[var(--m-text-xs)]">
                      <PctText value={r.quotePctChg ?? r.pctChg ?? 0} />
                    </div>
                  ) : null}
                </div>
              </div>
            ))}
          </MobileCard>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无宏观数据
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
