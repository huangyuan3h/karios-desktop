'use client';

import * as React from 'react';
import { Plus, Save, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

type TradeReview = {
  id: string;
  symbol: string;
  stockName: string | null;
  buyDate: string | null;
  sellDate: string | null;
  holdingDays: number | null;
  pnlAmount: number | null;
  pnlPct: number | null;
  totalCapitalImpactPct: number | null;
  maxLossGuardrailPct: number;
  marketLightEntry: string | null;
  marketLightExit: string | null;
  buyLogicFundResonance: boolean;
  buyLogicPatternBreakout: boolean;
  buyLogicMacroSentiment: boolean;
  buyLogicNotes: string | null;
  positionPct: number | null;
  buyAvgPrice: number | null;
  initialDefensePrice: number | null;
  sellAvgPrice: number | null;
  sellReason: string | null;
  executionNotes: string | null;
  goodActions: string | null;
  improvementAreas: string | null;
  customPayload: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
};

type ListTradeReviewsResponse = {
  total: number;
  items: TradeReview[];
};

type TradeReviewForm = {
  symbol: string;
  stockName: string;
  buyDate: string;
  sellDate: string;
  holdingDays: string;
  pnlAmount: string;
  pnlPct: string;
  totalCapitalImpactPct: string;
  maxLossGuardrailPct: string;
  marketLightEntry: string;
  marketLightExit: string;
  buyLogicFundResonance: boolean;
  buyLogicPatternBreakout: boolean;
  buyLogicMacroSentiment: boolean;
  buyLogicNotes: string;
  positionPct: string;
  buyAvgPrice: string;
  initialDefensePrice: string;
  sellAvgPrice: string;
  sellReason: string;
  executionNotes: string;
  goodActions: string;
  improvementAreas: string;
};

const EMPTY_FORM: TradeReviewForm = {
  symbol: '',
  stockName: '',
  buyDate: '',
  sellDate: '',
  holdingDays: '',
  pnlAmount: '',
  pnlPct: '',
  totalCapitalImpactPct: '',
  maxLossGuardrailPct: '2',
  marketLightEntry: '',
  marketLightExit: '',
  buyLogicFundResonance: false,
  buyLogicPatternBreakout: false,
  buyLogicMacroSentiment: false,
  buyLogicNotes: '',
  positionPct: '',
  buyAvgPrice: '',
  initialDefensePrice: '',
  sellAvgPrice: '',
  sellReason: '',
  executionNotes: '',
  goodActions: '',
  improvementAreas: '',
};

const EXAMPLE_FORM: TradeReviewForm = {
  symbol: '000001',
  stockName: 'Ping An Bank',
  buyDate: '2026-03-01',
  sellDate: '2026-03-10',
  holdingDays: '9',
  pnlAmount: '1500',
  pnlPct: '4.8',
  totalCapitalImpactPct: '0.9',
  maxLossGuardrailPct: '2',
  marketLightEntry: 'green',
  marketLightExit: 'yellow',
  buyLogicFundResonance: true,
  buyLogicPatternBreakout: true,
  buyLogicMacroSentiment: false,
  buyLogicNotes: 'Sector net inflow ranked #1; breakout above EMA20; global risk neutral.',
  positionPct: '20',
  buyAvgPrice: '12.50',
  initialDefensePrice: '11.90',
  sellAvgPrice: '13.10',
  sellReason: 'B',
  executionNotes: 'Placed stop-loss order immediately after entry and kept it unchanged.',
  goodActions: 'Executed stop-loss discipline without manual intervention.',
  improvementAreas: 'Need to add macro hedge checks when US futures drop sharply.',
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { method: 'DELETE' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
}

function asText(v: string | null | undefined): string {
  return String(v ?? '').trim();
}

function asNumber(v: string): number | undefined {
  const s = String(v).trim();
  if (!s) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
}

function toForm(x: TradeReview): TradeReviewForm {
  return {
    symbol: x.symbol || '',
    stockName: asText(x.stockName),
    buyDate: asText(x.buyDate),
    sellDate: asText(x.sellDate),
    holdingDays: x.holdingDays == null ? '' : String(x.holdingDays),
    pnlAmount: x.pnlAmount == null ? '' : String(x.pnlAmount),
    pnlPct: x.pnlPct == null ? '' : String(x.pnlPct),
    totalCapitalImpactPct: x.totalCapitalImpactPct == null ? '' : String(x.totalCapitalImpactPct),
    maxLossGuardrailPct: String(x.maxLossGuardrailPct ?? 2),
    marketLightEntry: asText(x.marketLightEntry),
    marketLightExit: asText(x.marketLightExit),
    buyLogicFundResonance: Boolean(x.buyLogicFundResonance),
    buyLogicPatternBreakout: Boolean(x.buyLogicPatternBreakout),
    buyLogicMacroSentiment: Boolean(x.buyLogicMacroSentiment),
    buyLogicNotes: asText(x.buyLogicNotes),
    positionPct: x.positionPct == null ? '' : String(x.positionPct),
    buyAvgPrice: x.buyAvgPrice == null ? '' : String(x.buyAvgPrice),
    initialDefensePrice: x.initialDefensePrice == null ? '' : String(x.initialDefensePrice),
    sellAvgPrice: x.sellAvgPrice == null ? '' : String(x.sellAvgPrice),
    sellReason: asText(x.sellReason),
    executionNotes: asText(x.executionNotes),
    goodActions: asText(x.goodActions),
    improvementAreas: asText(x.improvementAreas),
  };
}

function buildPayload(form: TradeReviewForm): Record<string, unknown> {
  return {
    symbol: form.symbol.trim(),
    stockName: form.stockName.trim() || undefined,
    buyDate: form.buyDate.trim() || undefined,
    sellDate: form.sellDate.trim() || undefined,
    holdingDays: asNumber(form.holdingDays),
    pnlAmount: asNumber(form.pnlAmount),
    pnlPct: asNumber(form.pnlPct),
    totalCapitalImpactPct: asNumber(form.totalCapitalImpactPct),
    maxLossGuardrailPct: asNumber(form.maxLossGuardrailPct) ?? 2,
    marketLightEntry: form.marketLightEntry.trim() || undefined,
    marketLightExit: form.marketLightExit.trim() || undefined,
    buyLogicFundResonance: form.buyLogicFundResonance,
    buyLogicPatternBreakout: form.buyLogicPatternBreakout,
    buyLogicMacroSentiment: form.buyLogicMacroSentiment,
    buyLogicNotes: form.buyLogicNotes.trim() || undefined,
    positionPct: asNumber(form.positionPct),
    buyAvgPrice: asNumber(form.buyAvgPrice),
    initialDefensePrice: asNumber(form.initialDefensePrice),
    sellAvgPrice: asNumber(form.sellAvgPrice),
    sellReason: form.sellReason.trim() || undefined,
    executionNotes: form.executionNotes.trim() || undefined,
    goodActions: form.goodActions.trim() || undefined,
    improvementAreas: form.improvementAreas.trim() || undefined,
  };
}

export function JournalTradeReviewPage({ onBack }: { onBack: () => void }) {
  const [items, setItems] = React.useState<TradeReview[]>([]);
  const [selectedId, setSelectedId] = React.useState<string | null>(null);
  const [form, setForm] = React.useState<TradeReviewForm>(EMPTY_FORM);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const refreshList = React.useCallback(async () => {
    const r = await apiGetJson<ListTradeReviewsResponse>('/trade-reviews?limit=200&offset=0');
    const xs = Array.isArray(r.items) ? r.items : [];
    setItems(xs);
    return xs;
  }, []);

  const loadOne = React.useCallback(async (id: string) => {
    const row = await apiGetJson<TradeReview>(`/trade-reviews/${encodeURIComponent(id)}`);
    setSelectedId(row.id);
    setForm(toForm(row));
  }, []);

  React.useEffect(() => {
    void (async () => {
      setBusy(true);
      setError(null);
      try {
        const xs = await refreshList();
        if (xs.length > 0) await loadOne(xs[0].id);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setBusy(false);
      }
    })();
  }, [loadOne, refreshList]);

  async function onNew() {
    setSelectedId(null);
    setForm(EMPTY_FORM);
  }

  function onUseExample() {
    setSelectedId(null);
    setForm(EXAMPLE_FORM);
  }

  async function onSave() {
    const symbol = form.symbol.trim();
    if (!symbol) {
      setError('Symbol is required.');
      return;
    }
    setBusy(true);
    setError(null);
    try {
      if (!selectedId) {
        const created = await apiPostJson<TradeReview>('/trade-reviews', buildPayload(form));
        await refreshList();
        await loadOne(created.id);
      } else {
        await apiPutJson<TradeReview>(`/trade-reviews/${encodeURIComponent(selectedId)}`, buildPayload(form));
        await refreshList();
        await loadOne(selectedId);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onDelete() {
    if (!selectedId) return;
    setBusy(true);
    setError(null);
    try {
      await apiDelete(`/trade-reviews/${encodeURIComponent(selectedId)}`);
      const xs = await refreshList();
      if (xs.length > 0) {
        await loadOne(xs[0].id);
      } else {
        setSelectedId(null);
        setForm(EMPTY_FORM);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Journal / Trade Review</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Manage trade postmortems in the same Journal module.
          </div>
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button size="sm" variant="secondary" onClick={onBack}>
            Back to Journal
          </Button>
          <Button size="sm" variant="secondary" onClick={() => void onNew()} disabled={busy} className="gap-2">
            <Plus className="h-4 w-4" />
            New
          </Button>
          <Button size="sm" variant="secondary" onClick={onUseExample} disabled={busy}>
            Use Example
          </Button>
          <Button size="sm" onClick={() => void onSave()} disabled={busy} className="gap-2">
            <Save className="h-4 w-4" />
            Save
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onDelete()}
            disabled={busy || !selectedId}
            className="gap-2"
          >
            <Trash2 className="h-4 w-4" />
            Delete
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-12">
        <section className="md:col-span-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 flex items-center justify-between">
            <div className="text-sm font-medium">Trade Reviews</div>
            <div className="text-xs text-[var(--k-muted)]">{items.length} items</div>
          </div>
          <div className="overflow-auto rounded border border-[var(--k-border)]">
            <div className="divide-y divide-[var(--k-border)]">
              {items.map((it) => {
                const active = it.id === selectedId;
                return (
                  <button
                    key={it.id}
                    type="button"
                    className={[
                      'w-full px-3 py-2 text-left text-sm',
                      'hover:bg-[var(--k-surface-2)]',
                      active ? 'bg-[var(--k-surface-2)]' : 'bg-[var(--k-surface)]',
                    ].join(' ')}
                    onClick={() => void loadOne(it.id)}
                  >
                    <div className="truncate font-medium">{it.symbol}</div>
                    <div className="mt-0.5 truncate text-xs text-[var(--k-muted)]">
                      {it.stockName || 'Unnamed'}
                    </div>
                  </button>
                );
              })}
              {!items.length ? (
                <div className="px-3 py-3 text-sm text-[var(--k-muted)]">No trade reviews yet.</div>
              ) : null}
            </div>
          </div>
        </section>

        <section className="md:col-span-8 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <Tabs defaultValue="setup">
            <TabsList>
              <TabsTrigger value="setup">Setup & Execution</TabsTrigger>
              <TabsTrigger value="lessons">Lessons & Notes</TabsTrigger>
            </TabsList>
            <TabsContent value="setup" className="mt-3">
              <div className="grid gap-3 md:grid-cols-12">
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Symbol
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.symbol}
                    onChange={(e) => setForm((x) => ({ ...x, symbol: e.target.value }))}
                    placeholder="Example: 000001"
                  />
                </label>
                <label className="md:col-span-8 text-xs text-[var(--k-muted)]">
                  Stock Name
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.stockName}
                    onChange={(e) => setForm((x) => ({ ...x, stockName: e.target.value }))}
                    placeholder="Example: Ping An Bank"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Buy Date
                  <input
                    type="date"
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.buyDate}
                    onChange={(e) => setForm((x) => ({ ...x, buyDate: e.target.value }))}
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Sell Date
                  <input
                    type="date"
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.sellDate}
                    onChange={(e) => setForm((x) => ({ ...x, sellDate: e.target.value }))}
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Holding Days
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.holdingDays}
                    onChange={(e) => setForm((x) => ({ ...x, holdingDays: e.target.value }))}
                    placeholder="Example: 9"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  PnL Amount
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.pnlAmount}
                    onChange={(e) => setForm((x) => ({ ...x, pnlAmount: e.target.value }))}
                    placeholder="Example: 1500"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  PnL %
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.pnlPct}
                    onChange={(e) => setForm((x) => ({ ...x, pnlPct: e.target.value }))}
                    placeholder="Example: 4.8"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Total Capital Impact %
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.totalCapitalImpactPct}
                    onChange={(e) => setForm((x) => ({ ...x, totalCapitalImpactPct: e.target.value }))}
                    placeholder="Example: 0.9 (must be >= -2)"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Entry Light
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.marketLightEntry}
                    onChange={(e) => setForm((x) => ({ ...x, marketLightEntry: e.target.value }))}
                    placeholder="Example: green"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Exit Light
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.marketLightExit}
                    onChange={(e) => setForm((x) => ({ ...x, marketLightExit: e.target.value }))}
                    placeholder="Example: yellow"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Position %
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.positionPct}
                    onChange={(e) => setForm((x) => ({ ...x, positionPct: e.target.value }))}
                    placeholder="Example: 20"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Buy Avg Price
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.buyAvgPrice}
                    onChange={(e) => setForm((x) => ({ ...x, buyAvgPrice: e.target.value }))}
                    placeholder="Example: 12.50"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Initial Defense
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.initialDefensePrice}
                    onChange={(e) => setForm((x) => ({ ...x, initialDefensePrice: e.target.value }))}
                    placeholder="Example: 11.90"
                  />
                </label>
                <label className="md:col-span-4 text-xs text-[var(--k-muted)]">
                  Sell Avg Price
                  <input
                    className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                    value={form.sellAvgPrice}
                    onChange={(e) => setForm((x) => ({ ...x, sellAvgPrice: e.target.value }))}
                    placeholder="Example: 13.10"
                  />
                </label>
              </div>
              <div className="mt-3 grid gap-2 md:grid-cols-3">
                <label className="inline-flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={form.buyLogicFundResonance}
                    onChange={(e) => setForm((x) => ({ ...x, buyLogicFundResonance: e.target.checked }))}
                  />
                  Fund Resonance
                </label>
                <label className="inline-flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={form.buyLogicPatternBreakout}
                    onChange={(e) => setForm((x) => ({ ...x, buyLogicPatternBreakout: e.target.checked }))}
                  />
                  Pattern Breakout
                </label>
                <label className="inline-flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={form.buyLogicMacroSentiment}
                    onChange={(e) => setForm((x) => ({ ...x, buyLogicMacroSentiment: e.target.checked }))}
                  />
                  Macro / Sentiment
                </label>
              </div>
              <label className="mt-3 block text-xs text-[var(--k-muted)]">
                Sell Reason
                <input
                  className="mt-1 h-9 w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
                  value={form.sellReason}
                  onChange={(e) => setForm((x) => ({ ...x, sellReason: e.target.value }))}
                  placeholder="Example: B (trailing take-profit) / C (market turns red)"
                />
              </label>
            </TabsContent>
            <TabsContent value="lessons" className="mt-3">
              <div className="grid gap-3">
                <label className="text-xs text-[var(--k-muted)]">
                  Buy Logic Notes
                  <textarea
                    className="mt-1 min-h-[88px] w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm outline-none"
                    value={form.buyLogicNotes}
                    onChange={(e) => setForm((x) => ({ ...x, buyLogicNotes: e.target.value }))}
                    placeholder="Example: Sector cash inflow ranked #1; TV signal triggered; broke above EMA20."
                  />
                </label>
                <label className="text-xs text-[var(--k-muted)]">
                  Execution Notes
                  <textarea
                    className="mt-1 min-h-[88px] w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm outline-none"
                    value={form.executionNotes}
                    onChange={(e) => setForm((x) => ({ ...x, executionNotes: e.target.value }))}
                    placeholder="Example: Position was 20%, stop-loss order placed immediately after entry."
                  />
                </label>
                <label className="text-xs text-[var(--k-muted)]">
                  Good Actions
                  <textarea
                    className="mt-1 min-h-[88px] w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm outline-none"
                    value={form.goodActions}
                    onChange={(e) => setForm((x) => ({ ...x, goodActions: e.target.value }))}
                    placeholder="Example: Strictly executed stop-loss order and did not cancel manually."
                  />
                </label>
                <label className="text-xs text-[var(--k-muted)]">
                  Improvement Areas
                  <textarea
                    className="mt-1 min-h-[88px] w-full rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-sm outline-none"
                    value={form.improvementAreas}
                    onChange={(e) => setForm((x) => ({ ...x, improvementAreas: e.target.value }))}
                    placeholder="Example: Add macro hedge checklist when overseas markets show risk-off signals."
                  />
                </label>
              </div>
            </TabsContent>
          </Tabs>
        </section>
      </div>
    </div>
  );
}

