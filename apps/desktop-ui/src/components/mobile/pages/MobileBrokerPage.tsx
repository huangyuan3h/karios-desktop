'use client';

import * as React from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';

import { apiGetJson, apiPostJson, apiDeleteJson } from '@/lib/api/client';
import { MobileButton, MobileCard, MobileField, MobileSection, MobileSheet } from '../primitives';

/** Broker (mobile) — pingan account overview + positions. §5.2 低频. */

type BrokerAccount = { id: string; broker: string; title: string; accountMasked: string | null; updatedAt: string };
type BrokerState = {
  accountId: string;
  overview: Record<string, unknown>;
  positions: Array<Record<string, unknown>>;
  counts: Record<string, number>;
  updatedAt: string;
};

const pick = <T,>(row: Record<string, unknown>, keys: string[]): T | null => {
  for (const k of keys) {
    const v = row[k];
    if (v != null && v !== '') return v as T;
  }
  return null;
};

const toNum = (v: unknown): number | null => {
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : null;
};

export function MobileBrokerPage() {
  const qc = useQueryClient();
  const [accountId, setAccountId] = React.useState<string | null>(null);

  const accounts = useQuery({
    queryKey: ['broker', 'accounts', 'pingan'],
    queryFn: () => apiGetJson<BrokerAccount[]>('/broker/accounts?broker=pingan'),
  });
  const state = useQuery({
    queryKey: ['broker', 'state', 'pingan', accountId ?? ''],
    queryFn: () => apiGetJson<BrokerState>(`/broker/pingan/accounts/${encodeURIComponent(accountId as string)}/state`),
    enabled: Boolean(accountId),
  });

  const [creating, setCreating] = React.useState(false);
  const [title, setTitle] = React.useState('');
  const [masked, setMasked] = React.useState('');

  const create = async () => {
    if (!title.trim()) return;
    try {
      await apiPostJson('/broker/accounts', { broker: 'pingan', title: title.trim(), accountMasked: masked.trim() || null });
      await qc.invalidateQueries({ queryKey: ['broker'] });
      setCreating(false);
      setTitle('');
      setMasked('');
    } catch {
      setCreating(false);
    }
  };

  const remove = async (id: string) => {
    await apiDeleteJson(`/broker/accounts/${encodeURIComponent(id)}`);
    await qc.invalidateQueries({ queryKey: ['broker'] });
    if (accountId === id) setAccountId(null);
  };

  const accs = accounts.data ?? [];
  const active = accs.find((a) => a.id === accountId) ?? null;
  const st = state.data;
  const positions = st?.positions ?? [];

  return (
    <div className="space-y-4">
      <MobileSection
        title="券商账户"
        action={
          <button type="button" onClick={() => setCreating(true)} className="text-[var(--m-text-sm)] text-[var(--k-accent)]">
            + 新建
          </button>
        }
      >
        <MobileCard className="p-3">
          {accs.length ? (
            <div className="flex flex-wrap gap-1.5">
              {accs.map((a) => (
                <button
                  key={a.id}
                  type="button"
                  onClick={() => setAccountId(a.id)}
                  className={`rounded-[var(--m-radius-pill)] px-2.5 py-1 text-[var(--m-text-xs)] font-medium ${
                    a.id === accountId
                      ? 'bg-[var(--k-accent)] text-white'
                      : 'border border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]'
                  }`}
                >
                  {a.title}
                  {a.accountMasked ? ` (${a.accountMasked})` : ''}
                </button>
              ))}
            </div>
          ) : (
            <div className="text-[var(--m-text-sm)] text-[var(--k-muted)]">暂无账户</div>
          )}
        </MobileCard>

        {active ? (
          <MobileSection title={`${active.title} · 总览`}>
            <MobileCard className="p-3">
              <div className="grid grid-cols-3 gap-1.5">
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1.5">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">总资产</div>
                  <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                    {toNum(st?.overview?.totalAssets) != null ? `${(toNum(st?.overview?.totalAssets) ?? 0).toFixed(2)} 万` : '—'}
                  </div>
                </div>
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1.5">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">可用资金</div>
                  <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                    {toNum(st?.overview?.cashAvailable) != null ? `${(toNum(st?.overview?.cashAvailable) ?? 0).toFixed(2)} 万` : '—'}
                  </div>
                </div>
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1.5">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">当日盈亏</div>
                  <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                    {toNum(st?.overview?.pnlToday) != null ? `${(toNum(st?.overview?.pnlToday) ?? 0).toFixed(2)} 万` : '—'}
                  </div>
                </div>
              </div>
            </MobileCard>
          </MobileSection>
        ) : null}

        {positions.length ? (
          <MobileSection title={`持仓（${positions.length}）`}>
            <MobileCard>
              {positions.map((p, idx) => {
                const name = pick<string>(p, ['name', 'Name']) ?? '—';
                const sym = pick<string>(p, ['ticker', 'Ticker', 'symbol', 'Symbol']) ?? '—';
                const qty = toNum(pick<number>(p, ['qtyHeld', 'qty', 'quantity']) ?? 0);
                const price = toNum(pick<number>(p, ['price', '现价', 'last']) ?? 0);
                const pnl = toNum(pick<number>(p, ['pnlPct', 'pnl%', '盈亏%', 'PnlPct']) ?? 0) ?? 0;
                return (
                  <div
                    key={`${sym}-${idx}`}
                    className={
                      idx === 0
                        ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                        : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                    }
                  >
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[var(--m-text-base)] font-medium">{name}</div>
                      <div className="truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                        {sym} · {qty != null ? `${qty} 股` : ''}
                      </div>
                    </div>
                    <div className="shrink-0 text-right">
                      <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                        {price != null ? price.toFixed(2) : '—'}
                      </div>
                      <div
                        className="text-[var(--m-text-sm)] font-medium"
                        style={{ color: pnl > 0 ? 'var(--k-up)' : pnl < 0 ? 'var(--k-down)' : 'var(--k-muted)' }}
                      >
                        {pnl > 0 ? '▲' : pnl < 0 ? '▼' : ''}
                        {pnl ? `${Math.abs(pnl).toFixed(2)}%` : '—'}
                      </div>
                    </div>
                  </div>
                );
              })}
            </MobileCard>
          </MobileSection>
        ) : null}

        <MobileSection title="操作">
          <div className="flex gap-2">
            <MobileButton variant="ghost" onClick={() => void qc.invalidateQueries({ queryKey: ['broker'] })}>
              刷新
            </MobileButton>
            {active ? (
              <MobileButton variant="danger" onClick={() => void remove(active.id)}>
                删除账户
              </MobileButton>
            ) : null}
          </div>
        </MobileSection>
      </MobileSection>

      <MobileSheet open={creating} onClose={() => setCreating(false)} title="新建账户">
        <div className="space-y-2.5">
          <MobileField label="账户名称">
            <input
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="h-[var(--m-tap)] w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)]"
            />
          </MobileField>
          <MobileField label="账户号（脱敏，可选）">
            <input
              value={masked}
              onChange={(e) => setMasked(e.target.value)}
              className="h-[var(--m-tap)] w-full rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 font-mono text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)]"
            />
          </MobileField>
          <MobileButton block onClick={() => void create()}>
            创建
          </MobileButton>
        </div>
      </MobileSheet>
    </div>
  );
}
