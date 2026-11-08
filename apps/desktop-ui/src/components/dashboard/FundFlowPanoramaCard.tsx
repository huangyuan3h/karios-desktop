'use client';

import { ExternalLink, Waves } from 'lucide-react';

import { FundFlowPanel, type FlowSectionId } from '@/components/pages/FundFlowPanel';
import { cn } from '@/lib/utils';

/** Public reference page for the three context charts (read-only, no data feed). */
export const FUND_FLOW_REFERENCE_URL = 'https://zhibiao.it-t.xyz/global/national-team/';

/** Charts mirrored from the public page: 国家队宽基 ETF 份额 / 两融余额 / 北向资金. */
const PANORAMA_SECTIONS: FlowSectionId[] = ['etf', 'margin', 'north'];

/**
 * Dashboard · 资金流全景（整幅大图）.
 *
 * Same `flow-series` data and chart code as the Backtest 资金流全景 (`FundFlowPanel`),
 * but rendered as a standalone full-width Dashboard block with the three charts the
 * public reference page shows. Read-only context — no strategy / weight input.
 */
export function FundFlowPanoramaCard({
  className,
  chartHeight = 220,
}: {
  className?: string;
  /** Height per chart (default 220 — readable, vs 120 inline in Backtest). */
  chartHeight?: number;
}) {
  return (
    <section
      id="dashboard-fund-flow"
      data-testid="dashboard-fund-flow-panel"
      aria-labelledby="dashboard-fund-flow-title"
      className={cn(
        'scroll-mt-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4',
        className,
      )}
    >
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <Waves size={14} className="text-purple-600 dark:text-purple-400" />
        <span id="dashboard-fund-flow-title" className="text-sm font-medium">
          资金流全景
        </span>
        <span className="text-[11px] text-[var(--k-muted)]">
          国家队宽基 ETF 份额 · 两融余额 · 北向资金
        </span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 py-0.5 text-[10px] text-[var(--k-muted)]">
          只读上下文 · 不影响交易
        </span>
        <a
          href={FUND_FLOW_REFERENCE_URL}
          target="_blank"
          rel="noreferrer noopener"
          className="ml-auto inline-flex items-center gap-1 text-[10px] text-[var(--k-muted)] hover:text-sky-700 dark:hover:text-sky-300"
          title="公开参考页（国家队宽基 ETF 份额 / 两融余额 / 北向资金）"
        >
          参考页：zhibiao · 国家队 <ExternalLink size={10} />
        </a>
      </div>
      <FundFlowPanel
        embedded
        sections={PANORAMA_SECTIONS}
        chartHeight={chartHeight}
        span="1y"
      />
    </section>
  );
}
