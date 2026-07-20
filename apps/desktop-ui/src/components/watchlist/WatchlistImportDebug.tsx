'use client';

import * as React from 'react';
import { ArrowDown, ArrowUp } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import type { TrendOkResult } from '@/lib/api/types';
import {
  collectWatchlistRiskAlerts,
  formatGapUp,
  formatIntradayChgPct,
  formatRiskAlerts,
  isIntradaySurge,
} from '@/lib/watchlist-metrics';
import { fmtBuyCell, fmtPrice, fmtScore } from '@/lib/watchlist-table-cells';

function VisibilitySection({
  visible,
  className,
  children,
}: {
  visible: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={className}
      style={{ display: visible ? 'block' : 'none' }}
      aria-hidden={!visible}
    >
      {children}
    </div>
  );
}

export type ScreenerImportDebugState = {
  updatedAt: string | null;
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
};

export type WatchlistImportDebugProps = {
  importDebug: ScreenerImportDebugState;
  importDebugOpen: boolean;
  setImportDebugOpen: (open: boolean) => void;
  importDebugFilter: string;
  setImportDebugFilter: (value: string) => void;
  importDebugScoreSortDir: 'desc' | 'asc';
  setImportDebugScoreSortDir: React.Dispatch<React.SetStateAction<'desc' | 'asc'>>;
  watchlistSet: Set<string>;
  addSymbolToWatchlist: (sym: string) => void;
  setCode: (code: string) => void;
  setError: (error: string | null) => void;
};

export function WatchlistImportDebug({
  importDebug,
  importDebugOpen,
  setImportDebugOpen,
  importDebugFilter,
  setImportDebugFilter,
  importDebugScoreSortDir,
  setImportDebugScoreSortDir,
  watchlistSet,
  addSymbolToWatchlist,
  setCode,
  setError,
}: WatchlistImportDebugProps) {
  const importDebugRows = React.useMemo(() => {
    const q = importDebugFilter.trim().toUpperCase();
    const base = (importDebug.rows || []).filter((r) => {
      if (!q) return true;
      const sym = String(r?.symbol || '').toUpperCase();
      const name = String(r?.name || '').toUpperCase();
      return sym.includes(q) || name.includes(q);
    });
    const arr = [...base];
    arr.sort((a, b) => {
      const sa = a?.score;
      const sb = b?.score;
      const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
      const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const d = va - vb;
      return importDebugScoreSortDir === 'asc' ? d : -d;
    });
    return arr;
  }, [importDebug.rows, importDebugFilter, importDebugScoreSortDir]);

  return (
    <div className="mb-4 min-w-0 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <div className="font-medium">Import debug table</div>
          <Switch
            checked={importDebugOpen}
            onCheckedChange={setImportDebugOpen}
            aria-label="Toggle import debug table"
          />
        </div>
        <div className="text-[var(--k-muted)]">
          {importDebug.updatedAt
            ? new Date(importDebug.updatedAt).toLocaleString()
            : 'No import yet'}
        </div>
      </div>
      <div className="mt-1 flex flex-wrap items-center justify-between gap-2">
        <div className="text-[var(--k-muted)]">
          Scanned {importDebug.scanned} • TrendOK ✅ {importDebug.trendOkCount} • Showing{' '}
          {importDebugRows.length}
        </div>
        <div className="flex items-center gap-2">
          <input
            className="h-8 w-[220px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-xs outline-none"
            placeholder="Filter (symbol/name)"
            value={importDebugFilter}
            onChange={(e) => setImportDebugFilter(e.target.value)}
          />
          <Button
            size="sm"
            variant="secondary"
            onClick={() => setImportDebugFilter('')}
            disabled={!importDebugFilter.trim()}
          >
            Clear
          </Button>
        </div>
      </div>

      <VisibilitySection
        visible={importDebugOpen}
        className="mt-2 max-h-[520px] min-w-0 overflow-auto rounded border border-[var(--k-border)]"
      >
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 bg-[var(--k-surface)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-3 py-2 w-[150px]">Symbol</th>
              <th className="px-3 py-2 w-[140px]">Name</th>
              <th className="px-3 py-2 w-[80px]">TrendOK</th>
              <th className="px-3 py-2 w-[90px]">
                <button
                  type="button"
                  className="inline-flex items-center gap-1 hover:text-[var(--k-text)]"
                  onClick={() =>
                    setImportDebugScoreSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
                  }
                  aria-label="Sort by score"
                  title="Sort by score"
                >
                  <span>Score</span>
                  {importDebugScoreSortDir === 'desc' ? (
                    <ArrowDown className="h-3.5 w-3.5" />
                  ) : (
                    <ArrowUp className="h-3.5 w-3.5" />
                  )}
                </button>
              </th>
              <th className="px-3 py-2 w-[180px]">Buy</th>
              <th className="px-3 py-2 w-[80px]">Intraday%</th>
              <th className="px-3 py-2 w-[52px]">Gap</th>
              <th className="px-3 py-2 w-[180px]">Alerts</th>
              <th className="px-3 py-2 w-[110px]">StopLoss</th>
              <th className="px-3 py-2 w-[120px]">Action</th>
              <th className="px-3 py-2 min-w-[320px]">Notes</th>
            </tr>
          </thead>
          <tbody>
            {importDebugRows.length ? (
              importDebugRows.map((r) => {
                const sym = String(r?.symbol || '');
                const ok = r?.trendOk ?? null;
                const icon = ok == null ? '—' : ok ? '✅' : '❌';
                const buy = fmtBuyCell(r);
                const importAlerts = collectWatchlistRiskAlerts({
                  intradayChgPct: r?.intradayChgPct,
                  gapUp: r?.gapUp,
                  marketRegime: r?.marketRegime,
                  serverAlerts: r?.riskAlerts,
                });
                const notes =
                  (typeof r?.buyWhy === 'string' && r.buyWhy) ||
                  (Array.isArray(r?.missingData) && r.missingData.length
                    ? r.missingData.join(', ')
                    : '');
                const inWl = sym ? watchlistSet.has(sym) : false;
                return (
                  <tr key={sym} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">
                      <button
                        type="button"
                        className="hover:underline"
                        onClick={() => {
                          setCode(sym);
                          setError(null);
                        }}
                        title="Fill the Add input with this symbol"
                      >
                        {sym || '—'}
                      </button>
                    </td>
                    <td className="px-3 py-2">
                      <div className="truncate" title={String(r?.name || '')}>
                        {r?.name || '—'}
                      </div>
                    </td>
                    <td className="px-3 py-2 font-mono">{icon}</td>
                    <td className="px-3 py-2 font-mono">{fmtScore(r?.score ?? null)}</td>
                    <td
                      className={
                        buy.tone === 'buy'
                          ? 'px-3 py-2 font-mono text-emerald-700'
                          : buy.tone === 'avoid'
                            ? 'px-3 py-2 font-mono text-red-600'
                            : buy.tone === 'wait'
                              ? 'px-3 py-2 font-mono text-[var(--k-muted)]'
                              : 'px-3 py-2 font-mono'
                      }
                    >
                      {buy.text}
                    </td>
                    <td
                      className={`px-3 py-2 font-mono ${
                        isIntradaySurge(r?.intradayChgPct) ? 'text-red-600 font-semibold' : ''
                      }`}
                    >
                      {formatIntradayChgPct(r?.intradayChgPct ?? null)}
                    </td>
                    <td
                      className={`px-3 py-2 font-mono ${
                        r?.gapUp === true ? 'text-red-600 font-semibold' : ''
                      }`}
                    >
                      {formatGapUp(r?.gapUp ?? null)}
                    </td>
                    <td className="px-3 py-2 text-xs">
                      {importAlerts.length ? (
                        <div className="truncate" title={formatRiskAlerts(importAlerts)}>
                          {importAlerts.map((alert) => (
                            <div
                              key={alert.code}
                              className={
                                alert.severity === 'block' ? 'text-red-600' : 'text-amber-700'
                              }
                            >
                              {alert.message}
                            </div>
                          ))}
                        </div>
                      ) : (
                        '—'
                      )}
                    </td>
                    <td className="px-3 py-2 font-mono">{fmtPrice(r?.stopLossPrice ?? null)}</td>
                    <td className="px-3 py-2">
                      {inWl ? (
                        <span className="text-[var(--k-muted)]">In watchlist</span>
                      ) : (
                        <Button
                          size="sm"
                          variant="secondary"
                          onClick={() => sym && addSymbolToWatchlist(sym)}
                          disabled={!sym}
                        >
                          Add
                        </Button>
                      )}
                    </td>
                    <td className="px-3 py-2 text-[var(--k-muted)]">
                      <div className="truncate" title={notes}>
                        {notes || '—'}
                      </div>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td className="px-3 py-3 text-[var(--k-muted)]" colSpan={11}>
                  No import results yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </VisibilitySection>
    </div>
  );
}
