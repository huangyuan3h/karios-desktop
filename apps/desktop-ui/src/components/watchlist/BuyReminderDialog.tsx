'use client';

import * as React from 'react';
import { createPortal } from 'react-dom';
import { CircleX } from 'lucide-react';

import { Button } from '@/components/ui/button';

export type BuyReminderDialogState = {
  symbol: string;
  name: string | null;
};

type BuyReminderDialogProps = {
  state: BuyReminderDialogState;
  suggestPct: number;
  onClose: () => void;
  onConfirm: (values: { targetPrice: number | null; note: string }) => void;
};

const PRICE_RE = /^\d+(\.\d{0,3})?$/;

export function BuyReminderDialog({
  state,
  suggestPct,
  onClose,
  onConfirm,
}: BuyReminderDialogProps) {
  const [targetPrice, setTargetPrice] = React.useState('');
  const [note, setNote] = React.useState('');

  React.useEffect(() => {
    setTargetPrice('');
    setNote('');
  }, [state]);

  const parsedTarget = targetPrice === '' ? null : Number(targetPrice);
  const targetValid = targetPrice === '' || (PRICE_RE.test(targetPrice.trim()) && parsedTarget! > 0);
  const valid = targetValid;

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
            提醒买入 <span className="font-mono text-[var(--k-muted)]">{state.symbol}</span>
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
          {state.name || '—'} · S-3 建议仓位 {suggestPct}%（确认后加入自选，行情/趋势/信号/体检自动盯盘）
        </div>
        <div className="space-y-2">
          <div>
            <div className="mb-1 text-[var(--k-muted)]">目标买入价（可选 · 到价自己留意）</div>
            <input
              className="h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-sm outline-none"
              placeholder="0.000"
              inputMode="decimal"
              value={targetPrice}
              onChange={(e) => {
                const raw = e.target.value;
                if (raw === '' || PRICE_RE.test(raw)) setTargetPrice(raw);
              }}
            />
          </div>
          <div>
            <div className="mb-1 text-[var(--k-muted)]">备注（可选）</div>
            <textarea
              className="h-16 w-full resize-none rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm outline-none"
              placeholder="为什么买 / 买多少 / 什么条件下放弃…"
              value={note}
              maxLength={200}
              onChange={(e) => setNote(e.target.value)}
            />
          </div>
        </div>
        <div className="mt-4 flex justify-end gap-2">
          <Button variant="ghost" size="sm" onClick={onClose}>
            取消
          </Button>
          <Button
            size="sm"
            disabled={!valid}
            onClick={() => onConfirm({ targetPrice: parsedTarget, note: note.trim() })}
          >
            确认加入自选
          </Button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
