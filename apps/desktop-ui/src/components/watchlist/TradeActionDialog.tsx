'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import { CircleX } from 'lucide-react';

import { Button } from '@/components/ui/button';
import type { WatchlistItem } from '@/lib/watchlist-storage';

export type TradeDialogKind = 'buy' | 'add' | 'sell';

export type TradeDialogOpenState = {
  kind: TradeDialogKind;
  item: WatchlistItem;
  currentPrice: number | null;
};

type TradeActionDialogProps = {
  state: TradeDialogOpenState;
  suggestPct: number;
  onClose: () => void;
  onConfirm: (values: { price: number; positionPct: number }) => void;
};

const PRICE_RE = /^\d+(\.\d{0,3})?$/;
const PCT_RE = /^\d+(\.\d{0,2})?$/;

function titleForKind(kind: TradeDialogKind): string {
  if (kind === 'sell') return '卖出';
  if (kind === 'add') return '加仓';
  return '买入';
}

function pctLabelForKind(kind: TradeDialogKind): string {
  if (kind === 'sell') return '卖出仓位 %';
  return '仓位 %';
}

export function TradeActionDialog({
  state,
  suggestPct,
  onClose,
  onConfirm,
}: TradeActionDialogProps) {
  const { kind, item, currentPrice } = state;
  const heldPct =
    typeof item.positionPct === 'number' && Number.isFinite(item.positionPct)
      ? item.positionPct
      : 0;
  const defaultPrice = currentPrice != null ? String(currentPrice) : '';
  const [price, setPrice] = React.useState(defaultPrice);
  const [positionPct, setPositionPct] = React.useState(
    kind === 'sell' ? String(heldPct) : String(Math.round(suggestPct * 100) / 100),
  );

  React.useEffect(() => {
    setPrice(defaultPrice);
    setPositionPct(
      kind === 'sell' ? String(heldPct) : String(Math.round(suggestPct * 100) / 100),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state]);

  const parsedPrice = Number(price);
  const parsedPct = Number(positionPct);
  const valid =
    PRICE_RE.test(price.trim()) &&
    parsedPrice > 0 &&
    PCT_RE.test(positionPct.trim()) &&
    parsedPct > 0 &&
    parsedPct <= 100;

  const pnlPreview =
    kind === 'sell' && valid && typeof item.costPrice === 'number'
      ? ((parsedPrice - item.costPrice) / item.costPrice) * 100
      : null;

  const costSummary =
    kind === 'sell'
      ? `成本 ${item.costPrice ?? '—'} · 持仓 ${heldPct}%`
      : kind === 'add'
        ? `当前成本 ${item.costPrice ?? '—'} · 持仓 ${heldPct}%`
        : null;

  return createPortal(
    <div
      className="fixed inset-0 z-[9999] grid place-items-center bg-black/30 p-4"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="w-full max-w-[340px] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4 text-xs text-[var(--k-text)] shadow-lg">
        <div className="mb-1 flex items-center justify-between">
          <div className="text-sm font-medium">
            {titleForKind(kind)} <span className="font-mono text-[var(--k-muted)]">{item.symbol}</span>
          </div>
          <button
            type="button"
            className="grid h-7 w-7 place-items-center rounded hover:bg-[var(--k-surface-2)]"
            onClick={onClose}
            aria-label="Close"
          >
            <CircleX className="h-4 w-4" />
          </button>
        </div>
        <div className="mb-3 text-[11px] text-[var(--k-muted)]">
          {item.name || '—'} {costSummary ? ` · ${costSummary}` : ''}
        </div>
        <div className="space-y-2">
          <div>
            <div className="mb-1 text-[var(--k-muted)]">
              {kind === 'sell' ? '卖出价格' : kind === 'add' ? '加仓价格' : '买入价格'}
            </div>
            <input
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-sm outline-none"
              placeholder="0.000"
              inputMode="decimal"
              value={price}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '' || PRICE_RE.test(raw)) setPrice(raw);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && valid) onConfirm({ price: parsedPrice, positionPct: parsedPct });
              }}
            />
          </div>
          <div>
            <div className="mb-1 text-[var(--k-muted)]">{pctLabelForKind(kind)}</div>
            <input
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-sm outline-none"
              placeholder="0"
              inputMode="decimal"
              value={positionPct}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '' || PCT_RE.test(raw)) setPositionPct(raw);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && valid) onConfirm({ price: parsedPrice, positionPct: parsedPct });
              }}
            />
          </div>
        </div>
        {pnlPreview != null ? (
          <div className="mt-2 text-[11px]">
            预计盈亏{' '}
            <span className={`font-mono font-medium ${pnlPreview >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
              {pnlPreview >= 0 ? '+' : ''}
              {pnlPreview.toFixed(2)}%
            </span>
          </div>
        ) : null}
        <div className="mt-4 flex justify-end gap-2">
          <Button size="sm" variant="secondary" onClick={onClose}>
            取消
          </Button>
          <Button
            size="sm"
            disabled={!valid}
            onClick={() => onConfirm({ price: parsedPrice, positionPct: parsedPct })}
          >
            确认{titleForKind(kind)}
          </Button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
