'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useIndustryFundFlowQuery, useIndustryMainlineQuery, runIndustryFlowSync } from '@/lib/queries/industryFlow';
import { fmtAmountCn, fmtSignedAmountCn } from '@/lib/dashboard-format';
import { MobileCard, MobileSection, PriceText, StatusPill } from '../primitives';

/** 行业资金流 (mobile) — mainline + top in/outflow. §5.2 中频. */

export function MobileIndustryFlowPage() {
  const qc = useQueryClient();
  const flow = useIndustryFundFlowQuery(10, 200);
  const mainline = useIndustryMainlineQuery();
  const [syncing, setSyncing] = React.useState(false);

  const top = flow.data?.top ?? [];
  const inflow = [...top].sort((a, b) => b.netInflow - a.netInflow).slice(0, 5);
  const outflow = [...top].sort((a, b) => a.netInflow - b.netInflow).slice(0, 5);
  const mainlineNames = new Set((mainline.data?.currentMainline ?? []).map((m) => m.industryName));

  const sync = async () => {
    setSyncing(true);
    try {
      await runIndustryFlowSync(qc, { force: false });
    } finally {
      setSyncing(false);
    }
  };

  const renderRows = (rows: typeof inflow) => (
    <MobileCard>
      {rows.map((r, idx) => (
        <div
          key={r.industryCode}
          className={
            idx === 0
              ? 'flex items-center justify-between gap-2 px-3 py-2.5'
              : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
          }
        >
          <div className="min-w-0 flex-1">
            <div className="truncate text-[var(--m-text-base)] font-medium">
              {r.industryName}
              {mainlineNames.has(r.industryName) ? (
                <span className="ml-1.5">
                  <StatusPill tone="open">主线</StatusPill>
                </span>
              ) : null}
            </div>
            <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">10 日累计 {fmtAmountCn(r.sum10d)}</div>
          </div>
          <div className="shrink-0">
            <span
              className="text-[var(--m-text-base)] font-semibold"
              style={{ color: r.netInflow > 0 ? 'var(--k-up)' : r.netInflow < 0 ? 'var(--k-down)' : 'inherit' }}
            >
              {fmtSignedAmountCn(r.netInflow)}
            </span>
          </div>
        </div>
      ))}
    </MobileCard>
  );

  return (
    <div className="space-y-4">
      <MobileSection
        title="行业资金流"
        action={
          <div className="flex items-center gap-2">
            <span className="text-[var(--m-text-xs)] text-[var(--k-muted)]">{flow.data?.asOfDate ?? ''}</span>
            <button type="button" onClick={() => void sync()} disabled={syncing} className="text-[var(--m-text-sm)] text-[var(--k-accent)] disabled:opacity-50">
              {syncing ? '同步中…' : '同步'}
            </button>
          </div>
        }
      >
        {mainline.data?.warning ? (
          <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3 text-[var(--m-text-sm)] text-[var(--k-warn)]">
            {mainline.data.warning}
          </MobileCard>
        ) : null}

        <MobileSection title={`主线板块（${mainline.data?.currentMainline?.length ?? 0}）`}>
          {mainline.data?.currentMainline?.length ? (
            <MobileCard>
              {mainline.data.currentMainline.map((m, idx) => (
                <div
                  key={m.industryName}
                  className={
                    idx === 0
                      ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                      : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                  }
                >
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">{m.industryName}</div>
                    <div className="mt-0.5 flex flex-wrap gap-x-2 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {m.flags?.flow?.flowPositive ? <span className="text-[var(--k-up)]">资金+</span> : <span className="text-[var(--k-down)]">资金-</span>}
                      {m.flags?.breadth?.breadthPositive ? <span className="text-[var(--k-up)]">广度+</span> : <span className="text-[var(--k-down)]">广度-</span>}
                      {m.flags?.trend?.trendUp ? <span className="text-[var(--k-up)]">趋势↑</span> : <span className="text-[var(--k-down)]">趋势↓</span>}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] tabular-nums">{m.totalScore.toFixed(1)}</div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {m.flowScore.toFixed(1)}/{m.breadthScore.toFixed(1)}/{m.trendScore.toFixed(1)}
                    </div>
                  </div>
                </div>
              ))}
            </MobileCard>
          ) : (
            <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
              {mainline.isLoading ? '加载中…' : '暂无主线数据'}
            </MobileCard>
          )}
        </MobileSection>

        <MobileSection title="净流入 Top 5">
          {inflow.length ? renderRows(inflow) : <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无数据</MobileCard>}
        </MobileSection>

        <MobileSection title="净流出 Top 5">
          {outflow.length ? renderRows(outflow) : <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无数据</MobileCard>}
        </MobileSection>
      </MobileSection>
    </div>
  );
}
