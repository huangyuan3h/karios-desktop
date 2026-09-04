'use client';

import * as React from 'react';

import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';
import { MobileCard, MobileSection, StatusPill } from '../primitives';

/** 对账 tab — behavior audit deviations vs the S-3 backtest book. §5.1. */
export function ReconcileTab() {
  const audit = useBehaviorAuditQuery();

  const extraRows = (audit.data ?? []).flatMap((r) =>
    (r.extraList ?? []).map((e) => ({ ...e, market: r.market })),
  );

  // OPT-140 satellite leg (vs twin-star engine book — info, never a warning).
  const satExtraRows = (audit.data ?? []).flatMap((r) =>
    (r.satExtraList ?? []).map((e) => ({ ...e, market: r.market })),
  );
  const satMissingRows = (audit.data ?? []).flatMap((r) =>
    (r.satMissingList ?? []).map((m) => ({ ...m, market: r.market })),
  );
  const satExpected = (audit.data ?? []).reduce((n, r) => n + (r.satExpected ?? 0), 0);
  const satActual = (audit.data ?? []).reduce((n, r) => n + (r.actualSat ?? 0), 0);
  const hasSatData =
    satExpected > 0 || satActual > 0 || satExtraRows.length > 0 || satMissingRows.length > 0;

  const satPanel = hasSatData ? (
    <MobileCard className="border-[var(--k-accent)]/30 bg-[var(--k-accent)]/5 p-3">
      <div className="text-[var(--m-text-sm)] font-semibold">
        🛰 卫星腿（引擎账本对照）：实持 {satActual} / 引擎应持 {satExpected}
        {satExtraRows.length === 0 && satMissingRows.length === 0 ? ' · 一致' : ''}
      </div>
      {satExtraRows.map((e) => (
        <div key={`sat-extra-${e.market}-${e.symbol}`} className="mt-1 truncate text-[var(--m-text-sm)] text-[var(--k-muted)]">
          账外持有 · <span className="font-mono">{e.symbol}</span>
          {e.name ? ` ${e.name}` : ''}
        </div>
      ))}
      {satMissingRows.map((m) => (
        <div key={`sat-miss-${m.market}-${m.symbol}`} className="mt-1 truncate text-[var(--m-text-sm)] text-[var(--k-muted)]">
          引擎应持未持有 · <span className="font-mono">{m.symbol}</span>
        </div>
      ))}
    </MobileCard>
  ) : null;

  if (audit.isLoading && !audit.data) {
    return (
      <div className="space-y-3">
        <div className="m-shimmer h-16" />
        <div className="m-shimmer h-16" />
      </div>
    );
  }

  return (
    <MobileSection title="行为对账">
      {extraRows.length ? (
        <div className="space-y-2">
          {extraRows.map((e) => (
            <MobileCard
              key={`${e.market}-${e.symbol}`}
              className={
                e.kind === 'exited'
                  ? 'border-[var(--k-danger)]/40 bg-[var(--k-danger)]/5 p-3'
                  : 'border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3'
              }
            >
              <div className="flex items-center justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-[var(--m-text-base)] font-semibold">
                    {e.kind === 'exited' ? '该卖没卖' : '买了不该买'} ·{' '}
                    <span className="font-mono">{e.symbol}</span>
                  </div>
                  {e.name ? (
                    <div className="mt-0.5 truncate text-[var(--m-text-sm)] text-[var(--k-muted)]">
                      {e.name} · {e.market}
                    </div>
                  ) : null}
                  {e.costPrice != null ? (
                    <div className="mt-0.5 font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      成本 {e.costPrice}
                    </div>
                  ) : null}
                </div>
                <StatusPill tone={e.kind === 'exited' ? 'danger' : 'warn'}>
                  {e.kind === 'exited' ? '卖' : '错买'}
                </StatusPill>
              </div>
            </MobileCard>
          ))}
        </div>
      ) : (
        <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
          {audit.data?.length ? '✅ 持仓与回测口径一致' : '暂无对账数据'}
        </MobileCard>
      )}
      {satPanel ? <div className="mt-2">{satPanel}</div> : null}
    </MobileSection>
  );
}
