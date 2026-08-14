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

export function MobileIndexPage() {
  const snap = useMacroSnapshotQuery();

  const rows = (snap.data?.cnIndexSignals ?? [])
    .filter((x) => x.name)
    .sort((a, b) => Number(b.featured ?? false) - Number(a.featured ?? false));

  return (
    <div className="space-y-4">
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
                    {r.signal ?? '—'}
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
                  <StatusPill tone={signalTone(r.signal)}>{r.signal ?? '—'}</StatusPill>
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
    </div>
  );
}
