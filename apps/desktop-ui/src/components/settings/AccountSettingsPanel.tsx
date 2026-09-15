'use client';

import * as React from 'react';

import { setAccountCapital, setHkdCnyRate, useAccountSettings } from '@/lib/account-settings';

/** OPT-204: account inputs for integer-lot order conversion. Local-only —
 * never sent to the backend, never changes strategy sizing. */
export function AccountSettingsPanel() {
  const { capital, rate } = useAccountSettings();
  const [capDraft, setCapDraft] = React.useState(capital != null ? String(capital) : '');
  const [rateDraft, setRateDraft] = React.useState(String(rate));

  React.useEffect(() => {
    setCapDraft(capital != null ? String(capital) : '');
  }, [capital]);
  React.useEffect(() => {
    setRateDraft(String(rate));
  }, [rate]);

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-semibold">账户与下单</h3>
        <p className="mt-1 text-xs text-[var(--k-muted)]">
          买入弹窗里把「仓位 %」换算成整数股数用（A股 100
          股/手、科创板 200 股起+1、北交所 100 股起+1、ETF 100 份、港股每手）。只影响下单建议与记账提示，不改变策略仓位与
          Live 决策。存放在本机浏览器。
        </p>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        <label className="flex flex-col gap-1 text-xs">
          <span className="text-[var(--k-muted)]">总资金（人民币元）</span>
          <input
            className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono outline-none"
            inputMode="decimal"
            placeholder="500000"
            value={capDraft}
            onChange={(e) => {
              const raw = e.target.value;
              if (raw === '' || /^\d+(\.\d{0,2})?$/.test(raw)) setCapDraft(raw);
            }}
            onBlur={() => {
              const n = Number(capDraft);
              setAccountCapital(capDraft !== '' && Number.isFinite(n) && n > 0 ? n : null);
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
            }}
          />
        </label>
        <label className="flex flex-col gap-1 text-xs">
          <span className="text-[var(--k-muted)]">港币兑人民币（1 HKD = ? CNY）</span>
          <input
            className="h-9 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono outline-none"
            inputMode="decimal"
            placeholder="0.92"
            value={rateDraft}
            onChange={(e) => {
              const raw = e.target.value;
              if (raw === '' || /^\d+(\.\d{0,3})?$/.test(raw)) setRateDraft(raw);
            }}
            onBlur={() => {
              const n = Number(rateDraft);
              if (Number.isFinite(n) && n > 0.1 && n < 10) setHkdCnyRate(n);
              else setRateDraft(String(rate));
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
            }}
          />
        </label>
      </div>
      <p className="text-[11px] text-[var(--k-muted)]">
        {capital != null ? `当前总资金 ¥${capital.toLocaleString('zh-CN')}` : '总资金未设置'} ·
        汇率 {(rate * 100).toFixed(1)}%
      </p>
    </div>
  );
}
