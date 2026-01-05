'use client';

import * as React from 'react';
import { RefreshCw, Sparkles } from 'lucide-react';

import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

function DebugBlock({
  title,
  description,
  defaultOpen,
  children,
}: {
  title: string;
  description?: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  return (
    <details
      open={defaultOpen}
      className="mb-3 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)]"
    >
      <summary className="cursor-pointer select-none px-3 py-2 text-xs font-medium">
        <div className="flex flex-col gap-1">
          <div className="text-[var(--k-text)]">{title}</div>
          {description ? <div className="text-[var(--k-muted)]">{description}</div> : null}
        </div>
      </summary>
      <div className="border-t border-[var(--k-border)] px-3 py-2">{children}</div>
    </details>
  );
}

type BrokerAccount = {
  id: string;
  broker: string;
  title: string;
  accountMasked: string | null;
  updatedAt: string;
};

type StrategyOrder = {
  kind: string;
  side: string;
  trigger: string;
  qty: string;
  timeInForce?: string | null;
  notes?: string | null;
};

type StrategyRecommendation = {
  symbol: string;
  ticker: string;
  name: string;
  thesis: string;
  levels: {
    support: string[];
    resistance: string[];
    invalidations: string[];
  };
  orders: StrategyOrder[];
  positionSizing: string;
  riskNotes: string[];
};

type StrategyCandidate = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  score: number;
  rank: number;
  why: string;
};

type StrategyReport = {
  id: string;
  date: string;
  accountId: string;
  accountTitle: string;
  createdAt: string;
  model: string;
  markdown?: string | null;
  candidates: StrategyCandidate[];
  leader: { symbol: string; reason: string };
  recommendations: StrategyRecommendation[];
  riskNotes: string[];
  inputSnapshot?: unknown;
  raw?: unknown;
};

type StrategyPrompt = {
  accountId: string;
  prompt: string;
  updatedAt: string | null;
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPutJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${QUANT_BASE_URL}${path}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

function mdEscapeCell(v: unknown): string {
  return String(v ?? '')
    .replaceAll('|', '\\|')
    .replaceAll('\n', ' ')
    .trim();
}

function buildStrategyMarkdown(report: StrategyReport): string {
  const lines: string[] = [];
  lines.push(`# Daily Strategy Report`);
  lines.push(``);
  lines.push(`- Date: ${report.date}`);
  lines.push(`- Account: ${report.accountTitle} (${report.accountId})`);
  lines.push(`- Model: ${report.model}`);
  lines.push(`- CreatedAt: ${new Date(report.createdAt).toLocaleString()}`);
  lines.push(``);

  lines.push(`## Top candidates (≤ 5)`);
  if (report.candidates?.length) {
    lines.push(`| Rank | Ticker | Name | Score | Why |`);
    lines.push(`| --- | --- | --- | --- | --- |`);
    for (const c of report.candidates.slice(0, 5)) {
      lines.push(
        `| ${mdEscapeCell(c.rank)} | ${mdEscapeCell(c.ticker)} | ${mdEscapeCell(c.name)} | ${mdEscapeCell(
          Math.round(Number(c.score ?? 0)),
        )} | ${mdEscapeCell(c.why)} |`,
      );
    }
  } else {
    lines.push(`_No candidates returned._`);
  }
  lines.push(``);

  lines.push(`## Leader`);
  lines.push(`- Symbol: ${report.leader?.symbol || 'N/A'}`);
  lines.push(`- Reason: ${report.leader?.reason || '—'}`);
  lines.push(``);

  lines.push(`## Recommendations (≤ 3)`);
  if (report.recommendations?.length) {
    for (const [idx, r] of report.recommendations.slice(0, 3).entries()) {
      lines.push(`### ${idx + 1}. ${mdEscapeCell(r.ticker)} ${mdEscapeCell(r.name)}`);
      lines.push(`- Symbol: ${mdEscapeCell(r.symbol)}`);
      lines.push(`- Thesis: ${mdEscapeCell(r.thesis)}`);
      lines.push(`- Position sizing: ${mdEscapeCell(r.positionSizing || '—')}`);
      lines.push(``);
      lines.push(`**Levels**`);
      lines.push(`- Support: ${(r.levels?.support ?? []).join(' / ') || '—'}`);
      lines.push(`- Resistance: ${(r.levels?.resistance ?? []).join(' / ') || '—'}`);
      lines.push(`- Invalidations: ${(r.levels?.invalidations ?? []).join(' / ') || '—'}`);
      lines.push(``);
      lines.push(`**Orders**`);
      if (r.orders?.length) {
        lines.push(`| Kind | Side | Trigger | Qty | TIF | Notes |`);
        lines.push(`| --- | --- | --- | --- | --- | --- |`);
        for (const o of r.orders.slice(0, 12)) {
          lines.push(
            `| ${mdEscapeCell(o.kind)} | ${mdEscapeCell(o.side)} | ${mdEscapeCell(o.trigger)} | ${mdEscapeCell(
              o.qty,
            )} | ${mdEscapeCell(o.timeInForce || '')} | ${mdEscapeCell(o.notes || '')} |`,
          );
        }
      } else {
        lines.push(`- (none)`);
      }
      lines.push(``);
      if (r.riskNotes?.length) {
        lines.push(`**Risk notes**`);
        for (const x of r.riskNotes) lines.push(`- ${mdEscapeCell(x)}`);
        lines.push(``);
      }
    }
  } else {
    lines.push(`_No recommendations returned._`);
    lines.push(``);
  }

  lines.push(`## Account-level risk notes`);
  if (report.riskNotes?.length) {
    for (const x of report.riskNotes) lines.push(`- ${mdEscapeCell(x)}`);
  } else {
    lines.push(`- None`);
  }
  lines.push(``);

  return lines.join('\n');
}

export function StrategyPage() {
  const { addReference } = useChatStore();
  const [accounts, setAccounts] = React.useState<BrokerAccount[]>([]);
  const [accountId, setAccountId] = React.useState<string>('');
  const [prompt, setPrompt] = React.useState<string>('');
  const [report, setReport] = React.useState<StrategyReport | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [ctxAccount, setCtxAccount] = React.useState(true);
  const [ctxScreener, setCtxScreener] = React.useState(true);
  const [ctxIndustryFlow, setCtxIndustryFlow] = React.useState(true);
  const [ctxSentiment, setCtxSentiment] = React.useState(true);
  const [ctxLeaders, setCtxLeaders] = React.useState(true);
  const [ctxStocks, setCtxStocks] = React.useState(true);

  const reportMd = React.useMemo(() => {
    if (!report) return '';
    const md = (report.markdown ?? '').trim();
    return md || buildStrategyMarkdown(report);
  }, [report]);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const acc = await apiGetJson<BrokerAccount[]>('/broker/accounts?broker=pingan');
      setAccounts(acc);
      const effectiveAccountId = accountId || acc[0]?.id || '';
      if (!accountId && effectiveAccountId) setAccountId(effectiveAccountId);
      if (effectiveAccountId) {
        const p = await apiGetJson<StrategyPrompt>(
          `/strategy/accounts/${encodeURIComponent(effectiveAccountId)}/prompt`,
        );
        setPrompt(p.prompt || '');
        try {
          const r = await apiGetJson<StrategyReport>(
            `/strategy/accounts/${encodeURIComponent(effectiveAccountId)}/daily`,
          );
          setReport(r);
        } catch {
          setReport(null);
        }
      } else {
        setPrompt('');
        setReport(null);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [accountId]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function onSavePrompt() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      await apiPutJson<StrategyPrompt>(
        `/strategy/accounts/${encodeURIComponent(accountId)}/prompt`,
        {
          prompt,
        },
      );
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onGenerateToday() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<StrategyReport>(
        `/strategy/accounts/${encodeURIComponent(accountId)}/daily`,
        {
          force: true,
          includeAccountState: ctxAccount,
          includeTradingView: ctxScreener,
          includeIndustryFundFlow: ctxIndustryFlow,
          includeMarketSentiment: ctxSentiment,
          includeLeaders: ctxLeaders,
          includeStocks: ctxStocks,
        },
      );
      setReport(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onLoadCached() {
    if (!accountId) return;
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<StrategyReport>(
        `/strategy/accounts/${encodeURIComponent(accountId)}/daily`,
        {
          force: false,
          includeAccountState: ctxAccount,
          includeTradingView: ctxScreener,
          includeIndustryFundFlow: ctxIndustryFlow,
          includeMarketSentiment: ctxSentiment,
          includeLeaders: ctxLeaders,
          includeStocks: ctxStocks,
        },
      );
      setReport(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-5xl p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Strategy (Swing 1-10D)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Generate a daily action guide from TradingView + account state + stock context.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Select value={accountId} onValueChange={(v) => setAccountId(v)}>
            <SelectTrigger className="h-9 w-[240px]">
              <SelectValue placeholder="Select account" />
            </SelectTrigger>
            <SelectContent>
              {accounts.map((a) => (
                <SelectItem key={a.id} value={a.id}>
                  {a.title}
                  {a.accountMasked ? ` (${a.accountMasked})` : ''}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="secondary" size="sm" onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Account strategy prompt</div>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="secondary"
              disabled={!accountId || busy}
              onClick={() => void onSavePrompt()}
            >
              Save
            </Button>
            <Button
              size="sm"
              variant="secondary"
              disabled={!accountId || busy}
              onClick={() => void onLoadCached()}
            >
              Use cached
            </Button>
            <Button
              size="sm"
              disabled={!accountId || busy}
              onClick={() => void onGenerateToday()}
              className="gap-2"
            >
              {busy ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {busy ? 'Generating…' : 'Generate today'}
            </Button>
          </div>
        </div>
        {busy ? (
          <div className="mb-3 flex items-center gap-2 text-xs text-[var(--k-muted)]">
            <div className="h-2 w-2 rounded-full bg-[var(--k-muted)] animate-pulse" />
            <div className="animate-pulse">Calling LLM (stage1 → stage2)…</div>
          </div>
        ) : null}
        <Textarea
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="Describe account constraints, preferences, forbidden assets, position sizing rules..."
          className="min-h-[120px]"
        />
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Tip: include constraints like max positions, max per-position %, no margin, CN/HK only,
          etc.
        </div>
      </section>

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Context toggles (sent to Strategy LLM)</div>
        <div className="grid gap-2 text-sm md:grid-cols-2">
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxAccount}
              onChange={(e) => setCtxAccount(e.target.checked)}
            />
            <span>Account state (overview/positions/orders/trades)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxScreener}
              onChange={(e) => setCtxScreener(e.target.checked)}
            />
            <span>TradingView screeners (latest)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxIndustryFlow}
              onChange={(e) => setCtxIndustryFlow(e.target.checked)}
            />
            <span>Industry fund flow (Top10 + 10D)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxSentiment}
              onChange={(e) => setCtxSentiment(e.target.checked)}
            />
            <span>Market sentiment (breadth &amp; limit-up)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxLeaders}
              onChange={(e) => setCtxLeaders(e.target.checked)}
            />
            <span>Leaders (龙头股, last 10 trading days)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={ctxStocks}
              onChange={(e) => setCtxStocks(e.target.checked)}
            />
            <span>Per-stock deep context (bars/chips/fund-flow)</span>
          </label>
        </div>
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Note: “Generate today” uses these toggles immediately. “Use cached” may return a report
          generated with different toggles.
        </div>
      </section>

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">How it works (2-stage LLM)</div>
        <div className="text-sm text-[var(--k-muted)]">
          <div className="mb-2">
            Stage 1: <span className="text-[var(--k-text)]">Pick Top5 candidates</span> using
            account state + TradingView + industry flow (no per-stock deep context). Returns JSON
            with scores.
          </div>
          <div>
            Stage 2: Fetch deep context only for Stage-1 candidates (chips + fund flow + bars tails)
            and generate the final Markdown report with tables and action plans.
          </div>
        </div>
      </section>

      {report ? (
        <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="font-medium">Daily report</div>
              <div className="mt-1 text-xs text-[var(--k-muted)]">
                Date: {report.date} • model: {report.model} • created:{' '}
                {new Date(report.createdAt).toLocaleString()}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant="secondary"
                onClick={() => {
                  void navigator.clipboard?.writeText(reportMd);
                }}
              >
                Copy markdown
              </Button>
              <Button
                size="sm"
                disabled={!accountId}
                onClick={() => {
                  addReference({
                    kind: 'strategyReport',
                    refId: report.id,
                    reportId: report.id,
                    accountId: report.accountId,
                    accountTitle: report.accountTitle,
                    date: report.date,
                    createdAt: report.createdAt,
                  });
                }}
              >
                Reference report to chat
              </Button>
            </div>
          </div>

          <div className="mt-4">
            <MarkdownMessage content={reportMd} className="text-sm" />
          </div>

          <div className="mt-4">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setDebugOpen((v) => !v)}
              className="h-8 px-3 text-xs"
            >
              {debugOpen ? 'Hide debug' : 'Show debug'}
            </Button>
            {debugOpen ? (
              <div className="mt-2 overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                <div className="mb-2 text-xs font-medium">LLM interaction debug</div>
                <div className="mb-2 text-xs text-[var(--k-muted)]">
                  Request = payload sent to ai-service. Response = JSON returned by ai-service.
                </div>
                <DebugBlock
                  title="Markdown (normalized)"
                  description="This is the exact markdown text we render above."
                  defaultOpen={false}
                >
                  <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                    {reportMd}
                  </pre>
                </DebugBlock>
                {(() => {
                  const rawObj = (report.raw ?? {}) as Record<string, unknown>;
                  const dbg = rawObj['debug'];
                  const dbgObj =
                    dbg && typeof dbg === 'object' ? (dbg as Record<string, unknown>) : null;
                  const stage1 = dbgObj?.['stage1'];
                  const stage2 = dbgObj?.['stage2'];
                  const stage1Obj =
                    stage1 && typeof stage1 === 'object'
                      ? (stage1 as Record<string, unknown>)
                      : null;
                  const stage2Obj =
                    stage2 && typeof stage2 === 'object'
                      ? (stage2 as Record<string, unknown>)
                      : null;
                  if (stage1Obj && stage2Obj) {
                    return (
                      <>
                        <DebugBlock
                          title="Stage 1 — Request (candidates JSON)"
                          description="No per-stock deep context. Uses account + TradingView latest + industry matrix."
                          defaultOpen={false}
                        >
                          <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                            {JSON.stringify(stage1Obj['request'] ?? null, null, 2)}
                          </pre>
                        </DebugBlock>

                        <DebugBlock
                          title="Stage 1 — Response (Top5 + scores)"
                          description="Top5 candidates + score; leader selection."
                          defaultOpen
                        >
                          <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                            {JSON.stringify(stage1Obj['response'] ?? null, null, 2)}
                          </pre>
                        </DebugBlock>

                        <DebugBlock
                          title="Stage 2 — Request (markdown report)"
                          description="Includes deep context only for Stage-1 selected symbols."
                          defaultOpen={false}
                        >
                          <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                            {JSON.stringify(stage2Obj['request'] ?? null, null, 2)}
                          </pre>
                        </DebugBlock>

                        <DebugBlock
                          title="Stage 2 — Response"
                          description="Raw ai-service JSON response (contains markdown)."
                          defaultOpen={false}
                        >
                          <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                            {JSON.stringify(stage2Obj['response'] ?? null, null, 2)}
                          </pre>
                        </DebugBlock>
                      </>
                    );
                  }
                  return (
                    <>
                      <DebugBlock title="Request (to ai-service)" defaultOpen={false}>
                        <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                          {JSON.stringify(
                            {
                              date: report.date,
                              accountId: report.accountId,
                              accountTitle: report.accountTitle,
                              context: report.inputSnapshot ?? null,
                            },
                            null,
                            2,
                          )}
                        </pre>
                      </DebugBlock>

                      <DebugBlock title="Response (from ai-service)" defaultOpen={false}>
                        <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
                          {JSON.stringify(report.raw ?? report, null, 2)}
                        </pre>
                      </DebugBlock>
                    </>
                  );
                })()}
              </div>
            ) : null}
          </div>
        </section>
      ) : (
        <div className="text-sm text-[var(--k-muted)]">No report yet. Click “Generate today”.</div>
      )}
    </div>
  );
}
