'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, isMarketGateClosed, type PortfolioHolding } from '@/lib/queries/portfolioHealth';
import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';

/**
 * Mobile-first shell (Family Hub Phase 0 · 2026-08-14).
 *
 * The desktop UI is a wide workspace (sidebar + agent panel + dense tables)
 * that is unusable on a phone. This shell renders the three things a phone
 * user actually needs, one screen each, bottom-tab navigated:
 *   执行 — today's gate state + buy candidates + EXIT flags
 *   持仓 — every holding with its stop/trail lines
 *   对账 — behavior-audit deviations (该卖没卖 / 买了不该买)
 * Data comes from the same APIs the desktop pages use — no new backend.
 */

const fmtPct = (v: number | null | undefined, digits = 2) =>
  v == null || !Number.isFinite(v) ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;

function GateBadge({ label, open }: { label: string; open: boolean }) {
  return (
    <span
      className={`rounded-md px-2 py-1 text-[11px] font-semibold ${
        open
          ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400'
          : 'bg-red-500/15 text-red-500 dark:text-red-400'
      }`}
    >
      {label} · {open ? '可买' : '不可买'}
    </span>
  );
}

function HoldingCard({ h, market }: { h: PortfolioHolding; market: string }) {
  const exit = h.action === 'EXIT';
  return (
    <div className={`rounded-xl border p-3 ${exit ? 'border-red-500/40 bg-red-500/5' : 'border-[var(--k-border)] bg-[var(--k-surface)]'}`}>
      <div className="flex items-center gap-2">
        <span className="min-w-0 flex-1 truncate font-semibold text-[13px]">{h.name ?? h.symbol}</span>
        {exit ? (
          <span className="rounded bg-red-500/15 px-1.5 py-0.5 text-[10px] font-bold text-red-500">🚩退出</span>
        ) : (
          <span className="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[10px] text-emerald-600 dark:text-emerald-400">持有</span>
        )}
        <span className={`font-mono text-[13px] tabular-nums ${(h.pnlPct ?? 0) < 0 ? 'text-red-500' : 'text-emerald-600 dark:text-emerald-400'}`}>
          {fmtPct(h.pnlPct)}
        </span>
      </div>
      <div className="mt-0.5 truncate font-mono text-[10.5px] text-[var(--k-muted)]">
        {h.symbol} · {market} · 已持 {h.holdingDays ?? '—'} 天
      </div>
      <div className="mt-2 grid grid-cols-3 gap-1 text-[10.5px]">
        <div className="rounded bg-[var(--k-surface-2)] px-1.5 py-1">
          <div className="text-[9.5px] text-[var(--k-muted)]">止损线</div>
          <div className="font-mono tabular-nums">{h.stopLossLine ?? '—'}</div>
        </div>
        <div className="rounded bg-[var(--k-surface-2)] px-1.5 py-1">
          <div className="text-[9.5px] text-[var(--k-muted)]">移动线</div>
          <div className="font-mono tabular-nums">{h.trailingLine ?? '—'}</div>
        </div>
        <div className="rounded bg-[var(--k-surface-2)] px-1.5 py-1">
          <div className="text-[9.5px] text-[var(--k-muted)]">到期</div>
          <div className="font-mono tabular-nums">{h.expireDate ?? '—'}</div>
        </div>
      </div>
      {h.realtimeAlert ? (
        <div className="mt-2 text-[11px] text-orange-500">⚠ {h.realtimeAlert}</div>
      ) : null}
      {h.realtimeWarning ? (
        <div className="mt-1 text-[10.5px] text-amber-500">⚠ 盘中预警（待收盘确认）</div>
      ) : null}
    </div>
  );
}

export function MobileShell() {
  const [tab, setTab] = React.useState<'执行' | '持仓' | '对账'>('执行');
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const audit = useBehaviorAuditQuery();

  const cn = health.data;
  const hk = cn?.hkHealth ?? null;
  const cnGate = cn == null ? null : isMarketGateClosed(cn);
  const hkGate = hk == null ? null : isMarketGateClosed(hk);
  const cnHoldings = cn?.holdings ?? [];
  const hkHoldings = hk?.holdings ?? [];
  const candidates = cn?.s3Candidates ?? [];

  const extraRows = (audit.data ?? []).flatMap((r) =>
    (r.extraList ?? []).map((e) => ({ ...e, market: r.market })),
  );

  const loading = health.isLoading || health.isFetching;

  return (
    <div className="flex h-dvh w-full flex-col bg-[var(--k-bg)] text-[var(--k-text)]">
      {/* Header */}
      <header className="flex items-center justify-between border-b border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
        <div className="text-[15px] font-bold">Karios</div>
        <div className="flex items-center gap-1.5 text-[10px] text-[var(--k-muted)]">
          {cn ? <GateBadge label="A股" open={!cnGate} /> : null}
          {hk ? <GateBadge label="港股" open={!hkGate} /> : null}
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 space-y-3 overflow-y-auto px-3 py-3">
        {loading && !health.data ? (
          <div className="pt-16 text-center text-sm text-[var(--k-muted)]">加载中…</div>
        ) : null}

        {tab === '执行' ? (
          <>
            {cn?.sentiment || cn?.panicCooldown?.active ? (
              <div className="rounded-xl border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-[11.5px]">
                {cn?.sentiment ? `市场情绪 ${cn.sentiment}` : ''}
                {cn?.panicCooldown?.active
                  ? ` · 恐慌冷却至 ${cn.panicCooldown.cooldownEndDate ?? '—'}`
                  : ''}
              </div>
            ) : null}

            {/* Buy list */}
            <section>
              <div className="mb-1.5 text-[12px] font-semibold text-[var(--k-muted)]">
                下午 2 点买入清单{candidates.length ? `（${candidates.length}）` : ''}
              </div>
              {candidates.length ? (
                <div className="space-y-2">
                  {candidates.map((c) => (
                    <div key={c.symbol ?? c.ts_code} className="flex items-center gap-2 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-[13px] font-semibold">{c.name ?? c.symbol}</span>
                        <span className="block truncate font-mono text-[10.5px] text-[var(--k-muted)]">{c.symbol}</span>
                      </span>
                      <span className="font-mono text-[12px] tabular-nums">score {c.score ?? '—'}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5 text-[11.5px] text-[var(--k-muted)]">
                  {cnGate ? '闸门关闭 · 今日不买' : '今日无候选'}
                </div>
              )}
            </section>

            {/* Exit flags */}
            {[...cnHoldings, ...hkHoldings].some((h) => h.action === 'EXIT') ? (
              <section>
                <div className="mb-1.5 text-[12px] font-semibold text-red-500">需要卖出</div>
                <div className="space-y-2">
                  {[...cnHoldings, ...hkHoldings]
                    .filter((h) => h.action === 'EXIT')
                    .map((h) => (
                      <HoldingCard key={h.symbol} h={h} market={cnHoldings.includes(h) ? 'A股' : '港股'} />
                    ))}
                </div>
              </section>
            ) : null}
          </>
        ) : null}

        {tab === '持仓' ? (
          <div className="space-y-2.5">
            {cnHoldings.length || hkHoldings.length ? (
              [...cnHoldings.map((h) => ({ h, m: 'A股' as const })), ...hkHoldings.map((h) => ({ h, m: '港股' as const }))].map(
                ({ h, m }) => <HoldingCard key={h.symbol} h={h} market={m} />,
              )
            ) : (
              <div className="pt-10 text-center text-[12.5px] text-[var(--k-muted)]">暂无持仓</div>
            )}
          </div>
        ) : null}

        {tab === '对账' ? (
          <div className="space-y-2">
            {extraRows.length ? (
              extraRows.map((e) => (
                <div
                  key={`${e.market}-${e.symbol}`}
                  className={`rounded-xl border p-3 text-[12px] ${
                    e.kind === 'exited'
                      ? 'border-red-500/40 bg-red-500/5'
                      : 'border-orange-500/40 bg-orange-500/5'
                  }`}
                >
                  <div className="font-semibold">
                    {e.kind === 'exited' ? '🔴 该卖没卖' : '🟠 买了不该买'} ·{' '}
                    <span className="font-mono">{e.symbol}</span>
                  </div>
                  {e.name ? <div className="mt-0.5 text-[var(--k-muted)]">{e.name}</div> : null}
                  {e.costPrice != null ? (
                    <div className="mt-0.5 text-[10.5px] text-[var(--k-muted)]">成本 {e.costPrice}</div>
                  ) : null}
                </div>
              ))
            ) : (
              <div className="pt-10 text-center text-[12.5px] text-[var(--k-muted)]">
                {audit.data?.length ? '✅ 持仓与回测口径一致' : '暂无对账数据'}
              </div>
            )}
          </div>
        ) : null}
      </main>

      {/* Bottom tabs */}
      <nav className="flex border-t border-[var(--k-border)] bg-[var(--k-surface)]">
        {(['执行', '持仓', '对账'] as const).map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => setTab(t)}
            className={`flex-1 py-3 text-[12.5px] font-medium ${
              tab === t ? 'text-emerald-600 dark:text-emerald-400' : 'text-[var(--k-muted)]'
            }`}
          >
            {t}
          </button>
        ))}
      </nav>
    </div>
  );
}
