'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Plus, RefreshCw, Trash2 } from 'lucide-react';

import { useWatchlistItems } from '@/hooks/useWatchlistItems';
import { useWatchlistMarketQuery } from '@/lib/queries/watchlist';
import { fetchPortfolioHealth, type PortfolioHolding } from '@/lib/queries/portfolioHealth';
import { useBehaviorAuditQuery, useRefreshBehaviorAudit } from '@/lib/queries/behaviorAudit';
import { MobileButton, MobileCard, MobileField, MobileSection, PctText, StatusPill } from '../primitives';

/**
 * Watchlist tab — the single place to act: sell flags → 2pm buy list →
 * holdings → watchlist. Execution & holdings are part of this view (IA v2).
 */
const fmtPct = (v: number | null | undefined) =>
  v == null || !Number.isFinite(v) ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

function HoldingRow({ h, market }: { h: PortfolioHolding; market: string }) {
  const exit = h.action === 'EXIT';
  return (
    <MobileCard
      className={
        exit
          ? 'border-[var(--k-danger)]/40 bg-[var(--k-danger)]/5 p-3'
          : 'p-3'
      }
    >
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate text-[var(--m-text-base)] font-semibold">{h.name ?? h.symbol}</div>
          <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
            {h.symbol} · {market} · 已持 {h.holdingDays ?? '—'} 天
          </div>
        </div>
        <div className="shrink-0 text-right">
          <div
            className="font-mono text-[var(--m-text-base)] font-semibold tabular-nums"
            style={{ color: (h.pnlPct ?? 0) > 0 ? 'var(--k-up)' : (h.pnlPct ?? 0) < 0 ? 'var(--k-down)' : 'var(--k-muted)' }}
          >
            {fmtPct(h.pnlPct)}
          </div>
          {exit ? <StatusPill tone="danger">退出</StatusPill> : <StatusPill tone="open">持有</StatusPill>}
        </div>
      </div>
      <div className="mt-2 grid grid-cols-3 gap-1.5 text-[var(--m-text-xs)]">
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">止损线</div>
          <div className="font-mono tabular-nums">{h.stopLossLine ?? '—'}</div>
        </div>
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">移动线</div>
          <div className="font-mono tabular-nums">{h.trailingLine ?? '—'}</div>
        </div>
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">到期</div>
          <div className="font-mono tabular-nums">{h.expireDate ?? '—'}</div>
        </div>
      </div>
      {h.realtimeAlert ? (
        <div className="mt-2 text-[var(--m-text-sm)] text-[var(--k-warn)]">⚠ {h.realtimeAlert}</div>
      ) : null}
      {h.reason ? <div className="mt-1 text-[var(--m-text-xs)] text-[var(--k-muted)]">{h.reason}</div> : null}
    </MobileCard>
  );
}

export function MobileWatchlistPage() {
  const {
    items,
    watchlistHydrating,
    onRemove,
    code,
    setCode,
    error,
    addSymbolToWatchlist,
  } = useWatchlistItems();
  const symbols = items.map((i) => i.symbol);
  const market = useWatchlistMarketQuery(symbols);
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const cn = health.data;
  const holdings = [
    ...(cn?.holdings ?? []).map((h) => ({ h, m: 'A股' as const })),
    ...(cn?.hkHealth?.holdings ?? []).map((h) => ({ h, m: '港股' as const })),
  ];
  const exitHoldings = holdings.filter(({ h }) => h.action === 'EXIT');
  const candidates = cn?.s3Candidates ?? [];
  const trend = market.data?.trend ?? {};
  const quotes = market.data?.quotes ?? {};

  const addByCode = () => {
    const parsed = code.trim().toUpperCase();
    if (!parsed) return;
    addSymbolToWatchlist(parsed);
    setCode('');
  };

  const audit = useBehaviorAuditQuery();
  const refreshAudit = useRefreshBehaviorAudit();
  const [auditing, setAuditing] = React.useState(false);
  const auditRows = audit.data ?? [];
  const auditExtra = auditRows.flatMap((r) => (r.extraList ?? []).map((e) => ({ ...e, market: r.market })));
  const auditMissing = auditRows.flatMap((r) => (r.missingList ?? []).map((m) => ({ ...m, market: r.market })));
  const auditDate = auditRows[0]?.auditDate;
  const auditClean = !auditExtra.length && !auditMissing.length;

  const onRunAudit = () => {
    setAuditing(true);
    void refreshAudit.mutateAsync(undefined, { onSettled: () => setAuditing(false) });
  };

  const refreshAll = () => {
    void market.refetch();
    void health.refetch();
  };

  return (
    <div className="space-y-4">
      {/* Behavior audit: real book vs S-3 backtest (OPT-106) */}
      <MobileSection
        title={`行为对账${auditDate ? `（${auditDate}）` : ''}`}
        action={
          <button
            type="button"
            onClick={onRunAudit}
            disabled={auditing || refreshAudit.isPending}
            className="text-[var(--m-text-sm)] text-[var(--k-accent)] disabled:opacity-50"
          >
            {auditing || refreshAudit.isPending ? '回测中（约3-4分钟）…' : '刷新对账'}
          </button>
        }
      >
        {auditClean ? (
          <MobileCard className="p-3">
            <div className="flex items-center gap-2 text-[var(--m-text-sm)]">
              <span className="text-[var(--k-up)]">✅</span>
              {auditDate ? (
                <span>持仓与 S-3 回测口径一致</span>
              ) : (
                <span className="text-[var(--k-muted)]">暂无数据，点「刷新对账」开始（回测模拟约 3-4 分钟）</span>
              )}
            </div>
          </MobileCard>
        ) : (
          <div className="space-y-2">
            {auditExtra.length ? (
              <MobileCard className="border-[var(--k-danger)]/40 bg-[var(--k-danger)]/5 p-3">
                <div className="text-[var(--m-text-sm)] font-semibold text-[var(--k-danger)]">
                  多持 {auditExtra.length} 只（买了不该买 / 该卖没卖）
                </div>
                <div className="mt-1 flex flex-wrap gap-1.5">
                  {auditExtra.map((e) => (
                    <StatusPill key={`${e.market}-${e.symbol}`} tone="danger">
                      {e.symbol} · {e.market}
                    </StatusPill>
                  ))}
                </div>
              </MobileCard>
            ) : null}
            {auditMissing.length ? (
              <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3">
                <div className="text-[var(--m-text-sm)] font-semibold text-[var(--k-warn)]">
                  缺持 {auditMissing.length} 只（回测建议但未持有）
                </div>
                <div className="mt-1 flex flex-wrap gap-1.5">
                  {auditMissing.map((m) => (
                    <StatusPill key={`${m.market}-${m.symbol}`} tone="warn">
                      {m.symbol} · {m.market}
                    </StatusPill>
                  ))}
                </div>
              </MobileCard>
            ) : null}
          </div>
        )}
      </MobileSection>

      {/* Act: sell flags */}
      {exitHoldings.length ? (
        <MobileSection title={`需要卖出（${exitHoldings.length}）`}>
          <div className="space-y-2">
            {exitHoldings.map(({ h, m }) => (
              <HoldingRow key={h.symbol} h={h} market={m} />
            ))}
          </div>
        </MobileSection>
      ) : null}

      {/* Act: 2pm buy list */}
      <MobileSection title={`下午 2 点买入清单${candidates.length ? `（${candidates.length}）` : ''}`}>
        {candidates.length ? (
          <div className="space-y-2">
            {candidates.map((c) => (
              <MobileCard key={c.symbol ?? c.ts_code} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {c.name ?? c.symbol}
                    </div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {c.symbol}
                      {c.industry ? ` · ${c.industry}` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-sm)] tabular-nums">score {c.score ?? '—'}</div>
                    {c.alphaEvents?.[0]?.grade ? (
                      <StatusPill tone="open">{c.alphaEvents[0].grade}</StatusPill>
                    ) : null}
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-4 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            今日无候选
          </MobileCard>
        )}
      </MobileSection>

      {/* Holdings */}
      <MobileSection title={`持仓（${holdings.length}）`}>
        {holdings.length ? (
          <div className="space-y-2">
            {holdings.map(({ h, m }) => (
              <HoldingRow key={`${m}-${h.symbol}`} h={h} market={m} />
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-4 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无持仓
          </MobileCard>
        )}
      </MobileSection>

      {/* Watchlist */}
      <MobileSection
        title={`自选（${items.length}）`}
        action={
          <button
            type="button"
            onClick={refreshAll}
            className="flex items-center gap-1 text-[var(--m-text-sm)] text-[var(--k-accent)]"
          >
            <RefreshCw size={13} /> 刷新
          </button>
        }
      >
        {watchlistHydrating ? (
          <div className="space-y-2">
            <div className="m-shimmer h-11" />
            <div className="m-shimmer h-11" />
          </div>
        ) : items.length ? (
          <MobileCard>
            {items.map((it, idx) => {
              const q = quotes[it.symbol];
              const t = trend[it.symbol];
              return (
                <div
                  key={it.symbol}
                  className={
                    idx === 0
                      ? 'flex items-center justify-between gap-2 px-3 py-2.5'
                      : 'flex items-center justify-between gap-2 border-t border-[var(--k-border)] px-3 py-2.5'
                  }
                >
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[var(--m-text-base)] font-medium">{it.name ?? it.symbol}</div>
                    <div className="truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {it.symbol}
                      {t?.trendStatus ? ` · ${t.trendStatus}` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] tabular-nums">
                      {q?.price?.toFixed(2) ?? '—'}
                    </div>
                    {q?.pctChg != null ? <PctText value={q.pctChg} /> : null}
                  </div>
                  <div className="shrink-0 text-right">
                    {t?.score != null ? (
                      <div className="font-mono text-[var(--m-text-sm)] tabular-nums">score {t.score}</div>
                    ) : null}
                    {t?.buyAction ? (
                      <div className="text-[var(--m-text-xs)] text-[var(--k-accent)]">{t.buyAction}</div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    onClick={() => onRemove(it.symbol)}
                    className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-[var(--k-muted)] active:bg-[var(--k-surface-2)]"
                    aria-label={`删除 ${it.symbol}`}
                  >
                    <Trash2 size={15} />
                  </button>
                </div>
              );
            })}
          </MobileCard>
        ) : (
          <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无自选股，输入代码添加
          </MobileCard>
        )}
      </MobileSection>

      {/* Add */}
      <MobileSection title="添加自选">
        <MobileCard className="space-y-2 p-3">
          <MobileField label="股票代码（6 位 A 股 / 4-5 位港股 / ETF）">
            <div className="flex gap-2">
              <input
                value={code}
                onChange={(e) => setCode(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && addByCode()}
                placeholder="如 600519 / 00700"
                className="h-[var(--m-tap)] min-w-0 flex-1 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)]"
              />
              <MobileButton onClick={addByCode} className="!h-[var(--m-tap)] px-4">
                <Plus size={16} /> 添加
              </MobileButton>
            </div>
          </MobileField>
          {error ? <div className="text-[var(--m-text-sm)] text-[var(--k-danger)]">{error}</div> : null}
        </MobileCard>
      </MobileSection>
    </div>
  );
}
