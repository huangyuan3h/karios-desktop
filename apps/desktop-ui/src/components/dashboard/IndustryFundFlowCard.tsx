/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import {
  HotIndustryWorkflowCard,
  type HotIndustryPick,
} from '@/components/pages/HotIndustryWorkflowCard';
import { Button } from '@/components/ui/button';
import {
  buildTopByDateMap,
  dedupeShownDates,
  fmtAmountCn,
} from '@/lib/dashboard-format';

type CopyStatus = { ok: boolean; text: string } | null;

type IndustryFundFlowCardProps = {
  summary: any;
  hotIndustryPicks: HotIndustryPick[];
  onNavigate?: (pageId: string) => void;
  onAddReference: (ref: any) => void;
  copyStatus: CopyStatus;
  onCopyIndustryMarkdown: () => void | Promise<void>;
};

function FlowTable({
  title,
  block,
  dedupedDates,
}: {
  title: string;
  block: any;
  dedupedDates: string[];
}) {
  const flowDates: string[] = Array.isArray(block?.dates) ? block.dates : [];
  const cols: string[] = flowDates.length ? flowDates.slice(-5) : dedupedDates;
  const topRows: any[] = Array.isArray(block?.top) ? block.top : [];
  if (!topRows.length || !cols.length) return null;
  const colDates = cols;

  return (
    <div className="mt-4">
      <div className="mb-2 text-xs text-[var(--k-muted)]">{title}</div>
      <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full border-collapse text-xs">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-2">Industry</th>
              <th className="px-2 py-2 text-right">Sum(5D)</th>
              {colDates.map((d: string) => (
                <th key={d} className="px-2 py-2 text-right font-mono">
                  {String(d).slice(5)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {topRows.slice(0, 10).map((r: any, idx: number) => {
              const seriesArr: any[] = Array.isArray(r?.series) ? r.series : [];
              const seriesMap: Record<string, number> = {};
              for (const p of seriesArr) {
                const dd = String(p?.date ?? '');
                const nv = Number(p?.netInflow ?? 0);
                if (dd) seriesMap[dd] = Number.isFinite(nv) ? nv : 0;
              }
              return (
                <tr
                  key={`${String(r?.industryCode ?? 'unknown')}-${idx}`}
                  className="border-t border-[var(--k-border)]"
                >
                  <td className="px-2 py-2">{String(r?.industryName ?? '')}</td>
                  <td className="px-2 py-2 text-right font-mono">{fmtAmountCn(r?.sum5d)}</td>
                  {colDates.map((d: string) => (
                    <td key={d} className="px-2 py-2 text-right font-mono">
                      {fmtAmountCn(seriesMap[d] ?? 0)}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function IndustryFundFlowCard({
  summary,
  hotIndustryPicks,
  onNavigate,
  onAddReference,
  copyStatus,
  onCopyIndustryMarkdown,
}: IndustryFundFlowCardProps) {
  const datesAll: string[] = Array.isArray(summary?.industryFundFlow?.dates)
    ? summary.industryFundFlow.dates
    : [];
  const rawShownDates = datesAll.slice(-5);
  const map = buildTopByDateMap(summary);
  const { dedupedDates, collapsed } = dedupeShownDates(rawShownDates, map);
  const asOfDate = String(summary?.industryFundFlow?.asOfDate ?? summary?.asOfDate ?? '');

  return (
    <div>
      <div className="mb-4">
        <HotIndustryWorkflowCard
          picks={hotIndustryPicks}
          asOfDate={asOfDate}
          compact
          onOpenScreener={() => onNavigate?.('screener')}
          onOpenWatchlist={() => onNavigate?.('watchlist')}
        />
      </div>
      <div className="mb-2 text-xs text-[var(--k-muted)]">Top5×Date hotspots (names only)</div>
      {collapsed ? (
        <div className="mb-2 text-xs text-[var(--k-muted)]">
          collapsed {collapsed} duplicate non-trading snapshot
          {collapsed > 1 ? 's' : ''}
        </div>
      ) : null}
      <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full border-collapse text-xs">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-2">#</th>
              {dedupedDates.map((d: string) => (
                <th key={d} className="px-2 py-2 font-mono">
                  {String(d).slice(5)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Array.from({ length: 5 }).map((_, i) => (
              <tr key={i} className="border-t border-[var(--k-border)]">
                <td className="px-2 py-2 font-mono">{i + 1}</td>
                {dedupedDates.map((d: string, j: number) => (
                  <td key={j} className="px-2 py-2">
                    {String((map[d] || [])[i] ?? '')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <FlowTable
        title="5D net inflow (Top by 5D sum)"
        block={(summary?.industryFundFlow as any)?.flow5d ?? null}
        dedupedDates={dedupedDates}
      />
      <FlowTable
        title="5D net outflow (Top by 5D sum)"
        block={(summary?.industryFundFlow as any)?.flow5dOut ?? null}
        dedupedDates={dedupedDates}
      />
      <div className="mt-3 flex items-center gap-2">
        <Button size="sm" variant="secondary" onClick={() => onNavigate?.('industryFlow')}>
          打开行业资金流
        </Button>
        <Button size="sm" variant="secondary" onClick={() => void onCopyIndustryMarkdown()}>
          复制Markdown
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => {
            onAddReference({
              kind: 'industryFundFlow',
              refId: `${asOfDate}:5:10`,
              asOfDate,
              days: 5,
              topN: 10,
              view: 'dailyTopByDate',
              title: 'A股行业资金流（按日期Top）',
              createdAt: new Date().toISOString(),
            } as any);
          }}
        >
          参考
        </Button>
      </div>
      {copyStatus ? (
        <div
          className={`mt-2 text-xs ${
            copyStatus.ok ? 'text-emerald-600' : 'text-red-600'
          }`}
        >
          {copyStatus.text}
        </div>
      ) : null}
    </div>
  );
}
