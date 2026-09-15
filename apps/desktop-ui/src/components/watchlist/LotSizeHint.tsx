'use client';

import * as React from 'react';

import { setAccountCapital, useAccountSettings } from '@/lib/account-settings';
import { suggestLotSizing } from '@/lib/lot-sizing';

function money(cny: number): string {
  return `¥${Math.round(cny).toLocaleString('zh-CN')}`;
}

/** Converts the suggested position % into the nearest valid integer order
 * (A股 100 股 / 科创板 200+1 / 北交所 100+1 / ETF 100 份 / 港股每手).
 * Suggests once the account capital is set; asks for it inline otherwise. */
export function LotSizeHint({
  symbol,
  price,
  targetPct,
  rank = null,
}: {
  symbol: string;
  price: number;
  targetPct: number;
  rank?: number | null;
}) {
  const { capital, rate } = useAccountSettings();
  const [draft, setDraft] = React.useState('');

  if (capital == null) {
    const parsed = Number(draft);
    const save = () => {
      if (Number.isFinite(parsed) && parsed > 0) setAccountCapital(parsed);
    };
    return (
      <div className="mt-1.5 rounded border border-dashed border-[var(--k-border)] px-2 py-1 text-[10px] text-[var(--k-muted)]">
        <div className="flex flex-wrap items-center gap-1">
          <span>总资金（元）</span>
          <input
            className="h-5 w-24 rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1 font-mono outline-none"
            inputMode="decimal"
            placeholder="500000"
            value={draft}
            onChange={(e) => {
              const raw = e.target.value;
              if (raw === '' || /^\d+(\.\d{0,2})?$/.test(raw)) setDraft(raw);
            }}
            onBlur={save}
            onKeyDown={(e) => {
              if (e.key === 'Enter') save();
            }}
          />
          <button type="button" className="rounded border border-[var(--k-border)] px-1" onClick={save}>
            设好
          </button>
          <span>→ 自动换算整数股数（只影响下单建议）</span>
        </div>
      </div>
    );
  }

  const sizing = suggestLotSizing({ symbol, price, targetPct, capital, rank, fxRate: rate });
  if (!sizing) return null;
  const { rule } = sizing;
  const localPrice =
    sizing.localCurrency === 'HKD'
      ? `HK$${price.toLocaleString('zh-CN')}`
      : `¥${price.toLocaleString('zh-CN')}`;
  const localValue =
    sizing.localCurrency === 'HKD'
      ? `HK$${Math.round(sizing.valueLocal).toLocaleString('zh-CN')}`
      : money(sizing.valueLocal);

  return (
    <div className="mt-1.5 rounded border border-[var(--k-border)] bg-[var(--k-surface-2)]/60 px-2 py-1 text-[10px]">
      {sizing.shares > 0 ? (
        <div className="font-medium text-[var(--k-fg)]">
          ≈ {sizing.shares} {rule.unit}（{sizing.shares} × {localPrice} = {localValue}
          {sizing.localCurrency === 'HKD' ? ` ≈ ${money(sizing.valueCny)}` : ''}）· 实际{' '}
          {sizing.actualPct.toFixed(1)}%
        </div>
      ) : (
        <div className="text-amber-700 dark:text-amber-300">
          {sizing.warning}（目标 {money((capital * sizing.targetPct) / 100)}）→ 加预算或换一只
        </div>
      )}
      <div className="mt-0.5 text-[var(--k-muted)]">
        {rule.note}
        {sizing.boost > 1 ? ` · 排名 #${rank} 加成 ×${sizing.boost}` : ''} · 总资金{' '}
        {money(capital)}
      </div>
    </div>
  );
}
