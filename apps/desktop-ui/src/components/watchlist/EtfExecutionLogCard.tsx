'use client';

/**
 * ETF / 择强开仓执行卡 — log fill outcomes while sample size is tiny.
 * Complements PickStrongAlignBanner (structure) with execution evidence.
 */

import * as React from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { cn } from '@/lib/utils';

type ExecItem = {
  id: string;
  tradeDate: string;
  pickKey: string;
  symbol?: string | null;
  status: string;
  premiumBps?: number | null;
  signalPrice?: number | null;
  fillPrice?: number | null;
  note?: string | null;
};

const STATUS_OPTS = [
  { id: 'filled', label: '成交' },
  { id: 'partial', label: '部分' },
  { id: 'failed', label: '未成交' },
  { id: 'skipped', label: '跳过' },
] as const;

async function fetchLog(limit = 14): Promise<ExecItem[]> {
  const res = await fetch(
    `${DATA_SYNC_BASE_URL}/commodities/sleeve/execution-log?limit=${limit}`,
    { cache: 'no-store' },
  );
  if (!res.ok) throw new Error(`execution-log ${res.status}`);
  const j = (await res.json()) as { items?: ExecItem[] };
  return j.items ?? [];
}

async function postLog(body: Record<string, unknown>): Promise<void> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}/commodities/sleeve/execution-log`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(t || `post ${res.status}`);
  }
}

export function EtfExecutionLogCard() {
  const qc = useQueryClient();
  const healthQ = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const logQ = useQuery({
    queryKey: ['sleeve-execution-log'],
    queryFn: () => fetchLog(14),
    refetchInterval: 60_000,
  });

  const sleeve = healthQ.data?.multiAssetSleeve;
  const pickKey = sleeve?.pick?.key ?? 'REPO';
  const symbol = sleeve?.pick?.symbol ?? '';
  const signalPx = sleeve?.pick?.close ?? null;

  const [status, setStatus] = React.useState<string>('filled');
  const [premium, setPremium] = React.useState('');
  const [fillPx, setFillPx] = React.useState('');
  const [note, setNote] = React.useState('');
  const [err, setErr] = React.useState<string | null>(null);

  const mut = useMutation({
    mutationFn: postLog,
    onSuccess: async () => {
      setErr(null);
      setNote('');
      setPremium('');
      setFillPx('');
      await qc.invalidateQueries({ queryKey: ['sleeve-execution-log'] });
    },
    onError: (e: Error) => setErr(e.message),
  });

  const onSubmit = () => {
    mut.mutate({
      pickKey,
      symbol: symbol || null,
      status,
      premiumBps: premium.trim() === '' ? null : Number(premium),
      signalPrice: signalPx,
      fillPrice: fillPx.trim() === '' ? null : Number(fillPx),
      note: note.trim() || null,
      meta: { action: sleeve?.action ?? null },
    });
  };

  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3 text-sm">
      <div className="flex flex-wrap items-center gap-2 text-[12px] font-medium">
        <span>ETF / 择强执行卡</span>
        <span className="rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 py-0.5 font-mono text-[10px] font-normal">
          今日 {pickKey}
          {symbol ? ` · ${symbol}` : ''}
        </span>
        <span className="text-[10px] font-normal text-[var(--k-muted)]">
          样本少时记成交/溢价/买不到 — 不作收益结论
        </span>
      </div>

      <div className="mt-2 flex flex-wrap items-end gap-2">
        <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
          结果
          <select
            className="h-8 rounded border border-[var(--k-border)] bg-transparent px-2 text-[12px] text-[var(--k-fg)]"
            value={status}
            onChange={(e) => setStatus(e.target.value)}
          >
            {STATUS_OPTS.map((o) => (
              <option key={o.id} value={o.id}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
          溢价 bp
          <input
            className="h-8 w-20 rounded border border-[var(--k-border)] bg-transparent px-2 font-mono text-[12px]"
            inputMode="decimal"
            placeholder="e.g. 80"
            value={premium}
            onChange={(e) => setPremium(e.target.value)}
          />
        </label>
        <label className="flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
          成交价
          <input
            className="h-8 w-24 rounded border border-[var(--k-border)] bg-transparent px-2 font-mono text-[12px]"
            inputMode="decimal"
            placeholder={signalPx != null ? String(signalPx) : '—'}
            value={fillPx}
            onChange={(e) => setFillPx(e.target.value)}
          />
        </label>
        <label className="min-w-[10rem] flex-1 flex flex-col gap-0.5 text-[10px] text-[var(--k-muted)]">
          备注（额度/折溢价/降级 REPO…）
          <input
            className="h-8 rounded border border-[var(--k-border)] bg-transparent px-2 text-[12px]"
            value={note}
            onChange={(e) => setNote(e.target.value)}
          />
        </label>
        <Button
          type="button"
          size="sm"
          className="h-8"
          disabled={mut.isPending || pickKey === 'REPO' || pickKey === 'STOCK'}
          onClick={onSubmit}
        >
          {mut.isPending ? '写入…' : '记一笔'}
        </Button>
      </div>
      {pickKey === 'REPO' || pickKey === 'STOCK' ? (
        <p className="mt-1 text-[10px] text-[var(--k-muted)]">
          今日 pick 非 ETF 腿时，执行卡主要记历史；STOCK/REPO 用行为对账即可。
        </p>
      ) : null}
      {err ? <p className="mt-1 text-[11px] text-red-600 dark:text-red-400">{err}</p> : null}

      <div className="mt-3 overflow-x-auto">
        <table className="w-full min-w-[28rem] text-left text-[11px]">
          <thead className="text-[10px] text-[var(--k-muted)]">
            <tr>
              <th className="py-1 pr-2 font-normal">日期</th>
              <th className="py-1 pr-2 font-normal">pick</th>
              <th className="py-1 pr-2 font-normal">结果</th>
              <th className="py-1 pr-2 font-normal">溢价bp</th>
              <th className="py-1 pr-2 font-normal">信号/成交</th>
              <th className="py-1 font-normal">备注</th>
            </tr>
          </thead>
          <tbody>
            {(logQ.data ?? []).length === 0 ? (
              <tr>
                <td colSpan={6} className="py-2 text-[var(--k-muted)]">
                  暂无记录 — ETF 开仓/轮动当天随手记一笔即可。
                </td>
              </tr>
            ) : (
              (logQ.data ?? []).map((r) => (
                <tr key={r.id} className="border-t border-[var(--k-border)]/60">
                  <td className="py-1 pr-2 font-mono">{r.tradeDate}</td>
                  <td className="py-1 pr-2">
                    {r.pickKey}
                    {r.symbol ? (
                      <span className="text-[var(--k-muted)]"> · {r.symbol}</span>
                    ) : null}
                  </td>
                  <td
                    className={cn(
                      'py-1 pr-2 font-medium',
                      r.status === 'failed' && 'text-red-600 dark:text-red-400',
                      r.status === 'filled' && 'text-emerald-700 dark:text-emerald-300',
                    )}
                  >
                    {r.status}
                  </td>
                  <td className="py-1 pr-2 font-mono">
                    {r.premiumBps != null ? r.premiumBps : '—'}
                  </td>
                  <td className="py-1 pr-2 font-mono text-[var(--k-muted)]">
                    {r.signalPrice ?? '—'} / {r.fillPrice ?? '—'}
                  </td>
                  <td className="py-1 max-w-[12rem] truncate" title={r.note ?? ''}>
                    {r.note || '—'}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
