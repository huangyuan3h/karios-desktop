'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import { CircleX } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { fetchWatchlistMarketSnapshot } from '@/lib/watchlist-market';

export type QuickBuyDialogState = {
  symbol: string;
  name: string | null;
  score?: number | null;
  rs?: number | null;
};

type QuickBuyDialogProps = {
  state: QuickBuyDialogState;
  suggestPct: number;
  side?: 'BUY' | 'SELL';
  /** Live price already in memory (row quote) — prefill instantly, skip fetch. */
  initialPrice?: number | null;
  busy?: boolean;
  error?: string | null;
  onClose: () => void;
  onConfirm: (values: { price: number; positionPct: number }) => void;
};

const PRICE_RE = /^\d+(\.\d{0,3})?$/;
const PCT_RE = /^\d+(\.\d{0,2})?$/;

export function QuickBuyDialog({
  state,
  suggestPct,
  side = 'BUY',
  initialPrice = null,
  busy = false,
  error = null,
  onClose,
  onConfirm,
}: QuickBuyDialogProps) {
  const hasInitial =
    typeof initialPrice === 'number' && Number.isFinite(initialPrice) && initialPrice > 0;
  const [positionPct, setPositionPct] = React.useState(String(suggestPct));
  const [price, setPrice] = React.useState(hasInitial ? String(initialPrice) : '');
  const [priceLoading, setPriceLoading] = React.useState(!hasInitial);

  React.useEffect(() => {
    setPositionPct(String(suggestPct));
    if (hasInitial) {
      setPrice(String(initialPrice));
      setPriceLoading(false);
      return;
    }
    setPrice('');
    setPriceLoading(true);
    let cancelled = false;
    void fetchWatchlistMarketSnapshot([state.symbol], {
      forceMarket: false,
      realtime: false,
    })
      .then((snap) => {
        if (cancelled) return;
        const q = snap.quotes[state.symbol.toUpperCase()];
        const p = q?.price;
        // Only prefill when the user hasn't typed yet — never wipe input.
        if (typeof p === 'number' && Number.isFinite(p) && p > 0) {
          setPrice((prev) => (prev === '' ? String(p) : prev));
        }
      })
      .catch(() => {
        /* keep the field empty; user can type the price */
      })
      .finally(() => {
        if (!cancelled) setPriceLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.symbol, suggestPct]);

  const parsedPrice = Number(price);
  const parsedPct = Number(positionPct);
  const valid =
    PRICE_RE.test(price.trim()) &&
    parsedPrice > 0 &&
    PCT_RE.test(positionPct.trim()) &&
    parsedPct > 0 &&
    parsedPct <= 100;

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
            {side === 'SELL' ? '卖出' : '买入'}{' '}
            {state.name ? <span>{state.name}</span> : null}{' '}
            <span className="font-mono text-[var(--k-muted)]">{state.symbol}</span>
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
          {state.score != null && <span>score={state.score} · </span>}
          {state.rs != null && <span>RS 前{Math.round(state.rs * 100)}% · </span>}
          <span>写入 Watchlist 自选并记入模拟盘</span>
        </div>
        <div className="space-y-2">
          <div>
            <div className="mb-1 text-[var(--k-muted)]">
              {side === 'SELL' ? '卖出价格（已按最近行情预填）' : '买入价格（已按最近行情预填）'}
            </div>
            <input
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-sm outline-none"
              placeholder={priceLoading ? '加载中…（可直接输入）' : '0.000'}
              inputMode="decimal"
              value={price}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '' || PRICE_RE.test(raw)) setPrice(raw);
              }}
            />
          </div>
          <div>
            <div className="mb-1 text-[var(--k-muted)]">仓位 %（建议 {suggestPct}% · 总资产）</div>
            <input
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-sm outline-none"
              placeholder="0"
              inputMode="decimal"
              value={positionPct}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '' || PCT_RE.test(raw)) setPositionPct(raw);
              }}
            />
          </div>
        </div>
        <div className="mt-4 flex justify-end gap-2">
          <Button variant="ghost" size="sm" onClick={onClose} disabled={busy}>
            取消
          </Button>
          <Button
            size="sm"
            disabled={!valid || busy}
            onClick={() => onConfirm({ price: parsedPrice, positionPct: parsedPct })}
          >
            {busy ? '提交中…' : `确认${side === 'SELL' ? '卖出' : '买入'}`}
          </Button>
        </div>
        {error ? <div className="mt-2 text-[11px] text-red-500">记录交易失败：{error}</div> : null}
      </div>
    </div>,
    document.body,
  );
}
