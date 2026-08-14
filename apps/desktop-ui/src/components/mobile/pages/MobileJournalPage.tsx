'use client';

import * as React from 'react';

import { apiGetJson } from '@/lib/api/client';
import { MobileCard, MobileSection, MobileSheet, PctText, StatusPill } from '../primitives';

/** 交易日志 (mobile) — trade review list + read-only detail sheet. §5.2 低频. */

type TradeReview = {
  id: string;
  symbol: string;
  stockName: string | null;
  buyDate: string | null;
  sellDate: string | null;
  holdingDays: number | null;
  pnlPct: number | null;
  pnlAmount: number | null;
  positionPct: number | null;
  buyAvgPrice: number | null;
  sellAvgPrice: number | null;
  sellReason: string | null;
  buyLogicNotes: string | null;
  executionNotes: string | null;
  createdAt: string;
};

export function MobileJournalPage() {
  const [items, setItems] = React.useState<TradeReview[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [selected, setSelected] = React.useState<TradeReview | null>(null);

  const refresh = React.useCallback(async () => {
    setLoading(true);
    try {
      const res = await apiGetJson<{ total: number; items: TradeReview[] }>('/trade-reviews?limit=200&offset=0');
      setItems(res.items ?? []);
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  return (
    <div className="space-y-4">
      <MobileSection
        title={`交易复盘（${items.length}）`}
        action={
          <button type="button" onClick={() => void refresh()} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            刷新
          </button>
        }
      >
        {items.length ? (
          <div className="space-y-2">
            {items.map((t) => (
              <MobileCard key={t.id} className="p-3" onClick={() => setSelected(t)}>
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {t.stockName ?? t.symbol}
                    </div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {t.symbol}
                      {t.buyDate ? ` · 买 ${t.buyDate}` : ''}
                      {t.sellDate ? ` · 卖 ${t.sellDate}` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    {t.pnlPct != null ? <PctText value={t.pnlPct} /> : <StatusPill tone="neutral">未平仓</StatusPill>}
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {loading ? '加载中…' : '暂无复盘记录'}
          </MobileCard>
        )}
      </MobileSection>

      <MobileSheet open={selected != null} onClose={() => setSelected(null)} title={`${selected?.stockName ?? selected?.symbol ?? ''} 复盘`}>
        {selected ? (
          <div className="space-y-2.5">
            <div className="flex flex-wrap gap-1.5">
              {selected.pnlPct != null ? <StatusPill tone={selected.pnlPct >= 0 ? 'up' : 'down'}>{selected.pnlPct.toFixed(2)}%</StatusPill> : null}
              {selected.holdingDays != null ? <StatusPill tone="neutral">持有 {selected.holdingDays} 天</StatusPill> : null}
              {selected.sellReason ? <StatusPill tone="warn">{selected.sellReason}</StatusPill> : null}
            </div>
            <MobileCard className="p-3 text-[var(--m-text-sm)]">
              <div className="grid grid-cols-2 gap-y-1.5 text-[var(--m-text-xs)]">
                <span className="text-[var(--k-muted)]">买入价</span>
                <span className="text-right font-mono">{selected.buyAvgPrice ?? '—'}</span>
                <span className="text-[var(--k-muted)]">卖出价</span>
                <span className="text-right font-mono">{selected.sellAvgPrice ?? '—'}</span>
                <span className="text-[var(--k-muted)]">仓位</span>
                <span className="text-right font-mono">{selected.positionPct != null ? `${selected.positionPct}%` : '—'}</span>
                <span className="text-[var(--k-muted)]">盈亏额</span>
                <span className="text-right font-mono">{selected.pnlAmount ?? '—'}</span>
              </div>
            </MobileCard>
            {selected.buyLogicNotes ? (
              <MobileCard className="p-3">
                <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">买入逻辑</div>
                <div className="mt-1 whitespace-pre-wrap text-[var(--m-text-sm)]">{selected.buyLogicNotes}</div>
              </MobileCard>
            ) : null}
            {selected.executionNotes ? (
              <MobileCard className="p-3">
                <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">执行备注</div>
                <div className="mt-1 whitespace-pre-wrap text-[var(--m-text-sm)]">{selected.executionNotes}</div>
              </MobileCard>
            ) : null}
          </div>
        ) : null}
      </MobileSheet>
    </div>
  );
}
