'use client';

import * as React from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { useIndustryFundFlowQuery, useIndustryMainlineQuery, runIndustryFlowSync, type IndustryFundFlowPoint } from '@/lib/queries/industryFlow';
import { fmtAmountCn, fmtSignedAmountCn } from '@/lib/dashboard-format';
import { MobileCard, MobileSection, StatusPill } from '../primitives';

/** 行业资金流 (mobile) — mainline + top in/outflow. §5.2 中频. */

function TrendSpark({ series }: { series: IndustryFundFlowPoint[] }) {
  const vals = series.map((p) => (Number.isFinite(p.netInflow) ? p.netInflow : 0));
  const maxAbs = Math.max(1, ...vals.map((v) => Math.abs(v)));
  const w = 84;
  const h = 26;
  const mid = Math.round(h / 2);
  const pts = vals.map((v, i) => {
    const x = series.length <= 1 ? w / 2 : (i / (series.length - 1)) * (w - 4) + 2;
    const y = mid - (v / maxAbs) * (mid - 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const last = vals[vals.length - 1] ?? 0;
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="block">
      <line x1="0" y1={mid} x2={w} y2={mid} stroke="rgba(120,120,120,0.25)" strokeWidth="1" />
      {pts.length >= 2 ? (
        <polyline
          fill="none"
          stroke={last >= 0 ? 'var(--k-up)' : 'var(--k-down)'}
          strokeWidth="2"
          points={pts.join(' ')}
        />
      ) : (
        <circle cx={w / 2} cy={mid} r="2" fill={last >= 0 ? 'var(--k-up)' : 'var(--k-down)'} />
      )}
    </svg>
  );
}

function RankLine({ flags }: { flags: any }) {
  const flow = flags?.flow;
  if (!flow) return null;
  const parts: React.ReactNode[] = [
    <span key="f5" className="font-mono">5日累计 {fmtAmountCn(flow.sum5d)}</span>,
    <span key="f20" className="font-mono">20日累计 {fmtAmountCn(flow.sum20d)}</span>,
    <span key="r5" className="font-mono">排名5日#{flow.rank5d}</span>,
    <span key="r20" className="font-mono">排名20日#{flow.rank20d}</span>,
  ];
  return (
    <div className="mt-0.5 flex flex-wrap gap-x-2">
      {parts.map((p, i) => (
        <span key={i} className="text-[var(--m-text-xs)] text-[var(--k-muted)]">{p}</span>
      ))}
    </div>
  );
}

function FlowRow({ row, name, sumLabel }: { row: any; name: string; sumLabel: string }) {
  return (
    <div className="flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5 first:border-t-0">
      <div className="min-w-0 flex-1">
        <div className="truncate text-[var(--m-text-base)] font-medium">{name}</div>
        <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">{sumLabel}</div>
      </div>
      <div className="shrink-0 text-right">
        <div
          className="text-[var(--m-text-base)] font-semibold"
          style={{ color: row.netInflow > 0 ? 'var(--k-up)' : row.netInflow < 0 ? 'var(--k-down)' : 'inherit' }}
        >
          {fmtSignedAmountCn(row.netInflow)}
        </div>
        <div className="mt-0.5 flex justify-end text-[var(--m-text-xs)] text-[var(--k-muted)]">
          <TrendSpark series={row.series10d ?? []} />
        </div>
      </div>
    </div>
  );
}

export function MobileIndustryFlowPage() {
  const qc = useQueryClient();
  const flow = useIndustryFundFlowQuery(10, 200);
  const mainline = useIndustryMainlineQuery();
  const [syncing, setSyncing] = React.useState(false);

  const top = flow.data?.top ?? [];
  const inflow = [...top].sort((a, b) => b.netInflow - a.netInflow).slice(0, 5);
  const outflow = [...top].sort((a, b) => a.netInflow - b.netInflow).slice(0, 5);
  const allScores = [...(mainline.data?.allScores ?? [])].sort((a, b) => b.totalScore - a.totalScore).slice(0, 5);

  const sync = async () => {
    setSyncing(true);
    try {
      await runIndustryFlowSync(qc, { force: false });
    } finally {
      setSyncing(false);
    }
  };

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
              {mainline.data.currentMainline.map((m) => (
                <div key={m.industryName} className="flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5 first:border-t-0">
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">{m.industryName}</div>
                    <div className="mt-0.5 flex flex-wrap gap-x-2 text-[var(--m-text-xs)]">
                      <span className={m.flags?.flow?.sum5d > 0 ? 'text-[var(--k-up)]' : 'text-[var(--k-down)]'}>
                        资金{(m.flags?.flow?.sum5d ?? 0) > 0 ? '+' : '-'}
                      </span>
                      <span className={m.flags?.breadth?.limitUpQualified ? 'text-[var(--k-up)]' : 'text-[var(--k-down)]'}>
                        广度{m.flags?.breadth?.limitUpQualified ? '+' : '-'}
                      </span>
                      <span className={m.flags?.trend?.indexAboveMa20 && m.flags?.trend?.ma20Up ? 'text-[var(--k-up)]' : 'text-[var(--k-down)]'}>
                        趋势{(m.flags?.trend?.indexAboveMa20 && m.flags?.trend?.ma20Up) ? '↑' : '↓'}
                      </span>
                      {m.flags?.flow?.positiveDays10d != null ? (
                        <span className={m.flags.flow.positiveDays10d >= 7 ? 'text-[var(--k-up)]' : ''}>
                          10日中{m.flags.flow.positiveDays10d}天正
                        </span>
                      ) : null}
                    </div>
                    <RankLine flags={m.flags} />
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

        <MobileSection title="主线候选 Top 5（按总评分）">
          {allScores.length ? (
            <MobileCard>
              {allScores.map((s) => (
                <div key={s.industryName} className="flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5 first:border-t-0">
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">{s.industryName}</div>
                    <RankLine flags={s.flags} />
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] tabular-nums">{s.totalScore.toFixed(1)}</div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {s.flowScore.toFixed(1)}/{s.breadthScore.toFixed(1)}/{s.trendScore.toFixed(1)}
                    </div>
                  </div>
                </div>
              ))}
            </MobileCard>
          ) : (
            <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
              {mainline.isLoading ? '加载中…' : '暂无数据'}
            </MobileCard>
          )}
        </MobileSection>

        <MobileSection title="净流入 Top 5">
          {inflow.length ? (
            <MobileCard>
              {inflow.map((r) => (
                <FlowRow key={r.industryCode} row={r} name={r.industryName} sumLabel={`10 日累计 ${fmtAmountCn(r.sum10d)}`} />
              ))}
            </MobileCard>
          ) : (
            <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无数据</MobileCard>
          )}
        </MobileSection>

        <MobileSection title="净流出 Top 5">
          {outflow.length ? (
            <MobileCard>
              {outflow.map((r) => (
                <FlowRow key={r.industryCode} row={r} name={r.industryName} sumLabel={`10 日累计 ${fmtAmountCn(r.sum10d)}`} />
              ))}
            </MobileCard>
          ) : (
            <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无数据</MobileCard>
          )}
        </MobileSection>
      </MobileSection>
    </div>
  );
}
