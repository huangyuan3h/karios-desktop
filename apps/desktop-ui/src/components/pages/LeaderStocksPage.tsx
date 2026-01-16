'use client';

import * as React from 'react';

import { RefreshCw, Sparkles } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';

type LeaderSeriesPoint = { date: string; close: number };

type LeaderPick = {
  id: string;
  date: string;
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  entryPrice?: number | null;
  score?: number | null;
  liveScore?: number | null;
  liveScoreUpdatedAt?: string | null;
  reason: string;
  whyBullets?: string[];
  expectedDurationDays?: number | null;
  buyZone?: Record<string, unknown>;
  triggers?: Array<Record<string, unknown>>;
  invalidation?: string | null;
  targetPrice?: Record<string, unknown>;
  probability?: number | null;
  risks?: string[];
  sourceSignals?: Record<string, unknown>;
  riskPoints?: string[];
  createdAt: string;
  nowClose?: number | null;
  pctSinceEntry?: number | null;
  series?: LeaderSeriesPoint[];
  todayChangePct?: number | null; // percent
  trendSeries?: LeaderSeriesPoint[];
};

type LeaderListResponse = {
  days: number;
  dates: string[];
  leaders: LeaderPick[];
};

type LeaderDailyResponse = {
  date: string;
  leaders: LeaderPick[];
  debug?: unknown;
};

type MainlineTheme = {
  kind: string;
  name: string;
  compositeScore: number;
  structureScore: number;
  logicScore: number;
  logicGrade?: string | null;
  logicSummary?: string | null;
  leaderCandidate?: Record<string, unknown> | null;
  topTickers?: Array<{
    symbol: string;
    ticker: string;
    name: string;
    chgPct?: number;
    volRatio?: number;
    turnover?: number;
  }>;
  followersCount?: number;
  limitupCount?: number;
  volSurge?: number;
  todayStrength?: number;
  ret3d?: number;
  decaySignals?: string[];
};

type MainlineSnapshot = {
  id: string;
  tradeDate: string;
  asOfTs: string;
  accountId: string;
  createdAt: string;
  universeVersion: string;
  riskMode?: string | null;
  selected?: MainlineTheme | null;
  themesTopK?: MainlineTheme[];
  debug?: unknown;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
}

function asStringArray(v: unknown): string[] {
  return Array.isArray(v) ? v.map((x) => String(x)).filter(Boolean) : [];
}

function fmtTodayPct(r: LeaderPick): string {
  const p = Number(r.todayChangePct);
  if (!Number.isFinite(p)) return '—';
  const sign = p > 0 ? '+' : '';
  return `${sign}${p.toFixed(2)}%`;
}

function fmtSinceEntryPct(r: LeaderPick): string {
  const p = Number(r.pctSinceEntry);
  if (!Number.isFinite(p)) return '—';
  const pct = p * 100.0;
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(2)}%`;
}

function fmtLeaderScore(r: LeaderPick): string {
  const live = Number(r.liveScore);
  if (Number.isFinite(live)) return String(Math.round(live));
  const s = Number(r.score);
  if (Number.isFinite(s)) return String(Math.round(s));
  return '—';
}

function fmtLocalDateTime(x: string): string {
  const d = new Date(x);
  if (Number.isNaN(d.getTime())) return x;
  return d.toLocaleString();
}

function riskModeExplain(riskMode: string | null | undefined): string {
  const v = String(riskMode ?? '').trim();
  if (v === 'no_new_positions') return '风险高：不建议开新仓（只处理持仓）';
  if (v === 'caution') return '谨慎：建议小仓位、等确认（回封/回踩）';
  if (v === 'normal') return '正常：可以按信号参与（仍需风控）';
  if (v === 'hot') return '偏热：趋势强、可积极一些（仍建议分批）';
  if (v === 'euphoric') return '亢奋：极强但波动大，追高需更严格止损';
  return '—';
}

function scoreLabel(score: number | null | undefined): string {
  const n = Number(score);
  if (!Number.isFinite(n)) return '—';
  if (n >= 85) return '强';
  if (n >= 70) return '中等偏强';
  return '偏弱';
}

function fmtBuyZoneText(r: LeaderPick) {
  const bz = r.buyZone ?? {};
  const low = isRecord(bz) ? bz.low : null;
  const high = isRecord(bz) ? bz.high : null;
  const note = isRecord(bz) ? bz.note : null;
  if (low == null || high == null) return '—';
  return `${String(low)} - ${String(high)}${note ? ` (${String(note)})` : ''}`;
}

function fmtTargetText(r: LeaderPick) {
  const tp = r.targetPrice ?? {};
  const primary = isRecord(tp) ? tp.primary : null;
  const stretch = isRecord(tp) ? tp.stretch : null;
  const note = isRecord(tp) ? tp.note : null;
  if (primary == null && stretch == null) return '—';
  const s = stretch != null ? `${String(primary)} / ${String(stretch)}` : String(primary);
  return `${s}${note ? ` (${String(note)})` : ''}`;
}

function fmtProbability(p: number | null | undefined) {
  const n = Number(p);
  if (!Number.isFinite(n) || n <= 0) return '—';
  const clamped = Math.max(1, Math.min(5, Math.round(n)));
  const pct = clamped * 20;
  const label =
    clamped === 1
      ? '低'
      : clamped === 2
        ? '偏低'
        : clamped === 3
          ? '中等'
          : clamped === 4
            ? '偏高'
            : '很高';
  return `${pct}%（${label}）`;
}

function fmtTriggerText(t: Record<string, unknown>) {
  const kind = String(t.kind ?? '');
  const label =
    kind === 'breakout' ? 'Breakout' : kind === 'pullback' ? 'Pullback' : kind || 'Trigger';
  const cond = String(t.condition ?? '').trim();
  const val = t.value;
  const tail = val != null && String(val).trim() ? ` @ ${String(val)}` : '';
  return `${label}: ${cond}${tail}`.trim();
}

function fmtPlanLine(r: LeaderPick) {
  const bz = r.buyZone ?? {};
  const tp = r.targetPrice ?? {};
  const bzLow = isRecord(bz) ? bz.low : null;
  const bzHigh = isRecord(bz) ? bz.high : null;
  const tpPrimary = isRecord(tp) ? tp.primary : null;
  const parts: string[] = [];
  if (bzLow != null && bzHigh != null) parts.push(`Buy:${String(bzLow)}-${String(bzHigh)}`);
  if (tpPrimary != null) parts.push(`Target:${String(tpPrimary)}`);
  if (Number.isFinite(r.expectedDurationDays as number))
    parts.push(`Dur:${String(r.expectedDurationDays)}d`);
  if (Number.isFinite(r.probability as number)) parts.push(`P:${fmtProbability(r.probability)}`);
  return parts.length ? parts.join(' • ') : '';
}

function CloseSparkline({ series }: { series: LeaderSeriesPoint[] }) {
  const vals = series.map((p) => p.close).filter((x) => Number.isFinite(x));
  if (!vals.length) return null;
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const w = 120;
  const h = 24;
  const pad = 2;
  const span = Math.max(1e-6, max - min);
  const pts = vals
    .map((v, i) => {
      const x = pad + (i / Math.max(1, vals.length - 1)) * (w - pad * 2);
      const y = pad + (1 - (v - min) / span) * (h - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="block">
      <polyline fill="none" stroke="currentColor" strokeWidth="1.5" points={pts} opacity="0.85" />
    </svg>
  );
}

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

export function LeaderStocksPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const [data, setData] = React.useState<LeaderListResponse | null>(null);
  const [mainline, setMainline] = React.useState<MainlineSnapshot | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [mainlineBusy, setMainlineBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [lastDebug, setLastDebug] = React.useState<unknown>(null);

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      // NOTE: Do NOT force refresh on page enter/refresh.
      // Live score refresh should only happen on "Generate today" or Dashboard "Sync all".
      const r = await apiGetJson<LeaderListResponse>('/leader?days=10&force=false');
      setData(r);
      const ml = await apiGetJson<MainlineSnapshot>('/leader/mainline');
      setMainline(ml);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  async function onGenerateToday() {
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<LeaderDailyResponse>('/leader/daily', { force: true });
      setLastDebug(r.debug ?? null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onDetectMainline() {
    setMainlineBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<MainlineSnapshot>('/leader/mainline/generate', {
        force: true,
        topK: 3,
        universeVersion: 'v0',
      });
      setMainline(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setMainlineBusy(false);
    }
  }

  const leaders = React.useMemo(() => data?.leaders ?? [], [data]);
  // Consolidated view: dedupe by symbol, keep the latest record (largest date).
  const consolidated = React.useMemo(() => {
    const m = new Map<string, LeaderPick>();
    for (const it of leaders) {
      const key = String(it.symbol || it.ticker || '').trim();
      if (!key) continue;
      const prev = m.get(key);
      if (!prev || String(it.date || '').localeCompare(String(prev.date || '')) > 0) {
        m.set(key, it);
      }
    }
    return Array.from(m.values()).sort((a, b) => {
      const sa = Number.isFinite(a.liveScore as number)
        ? (a.liveScore as number)
        : Number.isFinite(a.score as number)
          ? (a.score as number)
          : -1;
      const sb = Number.isFinite(b.liveScore as number)
        ? (b.liveScore as number)
        : Number.isFinite(b.score as number)
          ? (b.score as number)
          : -1;
      if (sb !== sa) return sb - sa;
      return String(b.date || '').localeCompare(String(a.date || ''));
    });
  }, [leaders]);

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Leader Stocks (龙头股)</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Maintain up to 2 leaders per trading day, keep last 10 trading days.
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            disabled={busy}
            onClick={() => void refresh()}
            className="gap-2"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            variant="secondary"
            size="sm"
            disabled={mainlineBusy}
            onClick={() => void onDetectMainline()}
            className="gap-2"
          >
            {mainlineBusy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Sparkles className="h-4 w-4" />
            )}
            {mainlineBusy ? 'Detecting…' : 'Detect mainline'}
          </Button>
          <Button
            size="sm"
            disabled={busy}
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
          <Button
            size="sm"
            variant="secondary"
            disabled={!leaders.length}
            onClick={() => {
              addReference({
                kind: 'leaderStocks',
                refId: `leaderStocks:10:${Date.now()}`,
                days: 10,
                createdAt: new Date().toISOString(),
              });
            }}
          >
            Reference
          </Button>
        </div>
      </div>

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-sm font-medium">主线（你最需要看的结论）</div>
            <div className="mt-1 text-xs text-[var(--k-muted)]">
              tradeDate: {mainline?.tradeDate ?? '—'} • updated:{' '}
              {mainline?.createdAt ? fmtLocalDateTime(mainline.createdAt) : '—'} • riskMode:{' '}
              {mainline?.riskMode ?? '—'}
            </div>
            <div className="mt-1 text-xs text-[var(--k-muted)]">
              {riskModeExplain(mainline?.riskMode ?? null)}
            </div>
          </div>
          <div className="text-xs text-[var(--k-muted)]">
            {mainline?.id ? `snapshot: ${mainline.id.slice(0, 8)}…` : 'no snapshot'}
          </div>
        </div>

        <div className="mt-3">
          {mainline?.selected ? (
            <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-sm font-semibold">
                  结论：今天主线是「{mainline.selected.kind} · {mainline.selected.name}」（
                  {scoreLabel(mainline.selected.compositeScore)}）
                </div>
                <div className="text-xs text-[var(--k-muted)]">
                  composite {Math.round(Number(mainline.selected.compositeScore ?? 0))} • structure{' '}
                  {Math.round(Number(mainline.selected.structureScore ?? 0))} • logic{' '}
                  {Math.round(Number(mainline.selected.logicScore ?? 0))}
                  {mainline.selected.logicGrade ? ` (${mainline.selected.logicGrade})` : ''}
                </div>
              </div>
              <div className="mt-2 grid gap-2 md:grid-cols-2">
                <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                  <div className="text-xs font-medium">为什么这么判断（证据）</div>
                  <div className="mt-2 space-y-1 text-xs text-[var(--k-muted)]">
                    <div>
                      涨停家数（LU）:{' '}
                      <span className="font-mono">
                        {Number(mainline.selected.limitupCount ?? 0)}
                      </span>
                      {' · '}
                      跟涨梯队（Followers）:{' '}
                      <span className="font-mono">
                        {Number(mainline.selected.followersCount ?? 0)}
                      </span>
                    </div>
                    <div>
                      结构分（Struct）:{' '}
                      <span className="font-mono">
                        {Math.round(Number(mainline.selected.structureScore ?? 0))}
                      </span>
                      {' · '}
                      逻辑分（Logic）:{' '}
                      <span className="font-mono">
                        {Math.round(Number(mainline.selected.logicScore ?? 0))}
                      </span>
                    </div>
                    <div className="text-[11px]">
                      提示：Struct=板块内是否“有龙头+有梯队+联动强”；Logic=AI基于证据给的持续性判断（不抓新闻）。
                    </div>
                  </div>
                </div>
                <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-3">
                  <div className="text-xs font-medium">相关股票（点进去你就能验证）</div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {(mainline.selected.topTickers ?? []).slice(0, 12).map((x) => (
                      <button
                        key={x.symbol}
                        type="button"
                        className="rounded-full border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 py-1 text-xs hover:bg-[var(--k-surface)]"
                        onClick={() => onOpenStock?.(x.symbol)}
                        title={`${x.symbol}${x.chgPct != null ? ` · ${x.chgPct}%` : ''}`}
                      >
                        <span className="font-mono">{x.ticker}</span> {x.name}
                        {typeof x.chgPct === 'number' ? (
                          <span className="ml-1 font-mono text-[var(--k-muted)]">{`${x.chgPct.toFixed(1)}%`}</span>
                        ) : null}
                      </button>
                    ))}
                    {!(mainline.selected.topTickers ?? []).length ? (
                      <div className="text-xs text-[var(--k-muted)]">
                        暂无成分股列表（数据源可能缺失）。
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>
              {mainline.selected.logicSummary ? (
                <div className="mt-2 text-xs text-[var(--k-muted)]">
                  AI 逻辑摘要：{mainline.selected.logicSummary}
                </div>
              ) : null}
              {(mainline.selected.decaySignals ?? []).length ? (
                <div className="mt-2 text-xs text-amber-600">
                  Decay signals: {(mainline.selected.decaySignals ?? []).join(' · ')}
                </div>
              ) : null}
              {mainline.selected.leaderCandidate && isRecord(mainline.selected.leaderCandidate) ? (
                <div className="mt-2 text-xs text-[var(--k-muted)]">
                  结构龙头候选（不一定最终入选）:{' '}
                  <button
                    type="button"
                    className="font-mono text-[var(--k-accent)] hover:underline"
                    onClick={() => {
                      const sym = String(
                        (mainline.selected?.leaderCandidate as Record<string, unknown>)?.symbol ??
                          '',
                      );
                      if (sym) onOpenStock?.(sym);
                    }}
                  >
                    {String(
                      (mainline.selected.leaderCandidate as Record<string, unknown>)?.ticker ?? '',
                    )}
                  </button>{' '}
                  {String(
                    (mainline.selected.leaderCandidate as Record<string, unknown>)?.name ?? '',
                  )}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-[var(--k-muted)]">
              结论：暂无明确主线（可能是轮动/多线混战）。你可以先观望，或者用“Detect
              mainline”再跑一次看看。
            </div>
          )}
        </div>

        {(mainline?.themesTopK ?? []).length ? (
          <div className="mt-3 overflow-auto rounded-md border border-[var(--k-border)]">
            <table className="w-full border-collapse text-xs">
              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-2 py-2">备选主线（TopK）</th>
                  <th className="px-2 py-2 text-right">综合</th>
                  <th className="px-2 py-2 text-right">结构</th>
                  <th className="px-2 py-2 text-right">逻辑</th>
                  <th className="px-2 py-2 text-right">涨停</th>
                  <th className="px-2 py-2 text-right">跟涨</th>
                </tr>
              </thead>
              <tbody>
                {(mainline?.themesTopK ?? []).map((t) => (
                  <tr key={`${t.kind}:${t.name}`} className="border-t border-[var(--k-border)]">
                    <td className="px-2 py-2">
                      <details>
                        <summary className="cursor-pointer">
                          <span className="font-mono text-[var(--k-muted)]">{t.kind}</span> {t.name}
                        </summary>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {(t.topTickers ?? []).slice(0, 12).map((x) => (
                            <button
                              key={x.symbol}
                              type="button"
                              className="rounded-full border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 py-1 text-xs hover:bg-[var(--k-surface)]"
                              onClick={() => onOpenStock?.(x.symbol)}
                            >
                              <span className="font-mono">{x.ticker}</span> {x.name}
                            </button>
                          ))}
                          {!(t.topTickers ?? []).length ? (
                            <div className="text-xs text-[var(--k-muted)]">暂无成分股列表。</div>
                          ) : null}
                        </div>
                      </details>
                    </td>
                    <td className="px-2 py-2 text-right font-mono">
                      {Math.round(Number(t.compositeScore ?? 0))}
                    </td>
                    <td className="px-2 py-2 text-right font-mono">
                      {Math.round(Number(t.structureScore ?? 0))}
                    </td>
                    <td className="px-2 py-2 text-right font-mono">
                      {Math.round(Number(t.logicScore ?? 0))}
                      {t.logicGrade ? ` ${t.logicGrade}` : ''}
                    </td>
                    <td className="px-2 py-2 text-right font-mono">
                      {Number(t.limitupCount ?? 0)}
                    </td>
                    <td className="px-2 py-2 text-right font-mono">
                      {Number(t.followersCount ?? 0)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">
            Leaders (deduped, keep latest per symbol • last {data?.days ?? 10} trading days)
          </div>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => setDebugOpen((v) => !v)}
            className="h-8 px-3 text-xs"
          >
            {debugOpen ? 'Hide debug' : 'Show debug'}
          </Button>
        </div>

        {debugOpen && lastDebug ? (
          <div className="mb-4 overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
            <div className="mb-2 text-xs font-medium">Last generate debug</div>
            <pre className="whitespace-pre-wrap break-words text-xs text-[var(--k-muted)]">
              {JSON.stringify(lastDebug, null, 2)}
            </pre>
          </div>
        ) : null}

        {consolidated.length ? (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead className="bg-[var(--k-surface)]">
                <tr>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                    Symbol
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                    Name
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                    Live score
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                    Upside
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                    Last date
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-right">
                    Today
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                    Why
                  </th>
                  <th className="whitespace-nowrap border-b border-[var(--k-border)] px-2 py-2 text-left">
                    Trend
                  </th>
                </tr>
              </thead>
              <tbody>
                {consolidated.map((r) => (
                  <React.Fragment key={r.id}>
                    <tr className="bg-[var(--k-surface)]">
                      <td className="border-b border-[var(--k-border)] px-2 py-2 font-mono">
                        <button
                          type="button"
                          className="text-[var(--k-accent)] hover:underline"
                          onClick={() => onOpenStock?.(r.symbol)}
                          title="Open stock detail"
                        >
                          {r.ticker || r.symbol}
                        </button>
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2">{r.name}</td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right">
                        <div className="flex flex-col items-end">
                          <div className="font-mono">{fmtLeaderScore(r)}</div>
                          {r.liveScoreUpdatedAt ? (
                            <div className="mt-0.5 text-[10px] text-[var(--k-muted)]">
                              updated: {fmtLocalDateTime(String(r.liveScoreUpdatedAt))}
                            </div>
                          ) : null}
                        </div>
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right font-mono">
                        {Number.isFinite(Number(r.score)) ? String(Math.round(Number(r.score))) : '—'}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right font-mono">
                        {String(r.date || '—')}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-right font-mono">
                        {fmtTodayPct(r)}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2 text-[var(--k-muted)]">
                        {r.whyBullets?.length ? (
                          <div className="space-y-1">
                            <ul className="list-disc pl-4">
                              {r.whyBullets.slice(0, 3).map((x, idx) => (
                                <li key={idx}>{x}</li>
                              ))}
                            </ul>
                            {fmtPlanLine(r) ? (
                              <div className="text-[11px] opacity-80">{fmtPlanLine(r)}</div>
                            ) : null}
                          </div>
                        ) : (
                          r.reason
                        )}
                      </td>
                      <td className="border-b border-[var(--k-border)] px-2 py-2">
                        {(r.trendSeries?.length || r.series?.length) ? (
                          <CloseSparkline series={(r.trendSeries ?? r.series ?? []) as LeaderSeriesPoint[]} />
                        ) : (
                          <div className="text-[11px] text-[var(--k-muted)]">—</div>
                        )}
                      </td>
                    </tr>
                    <tr>
                      <td colSpan={8} className="border-b border-[var(--k-border)] px-2 py-2">
                        <details>
                          <summary className="cursor-pointer text-xs text-[var(--k-muted)]">
                            Details
                          </summary>
                          <div className="mt-2 grid gap-3 text-xs md:grid-cols-3">
                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Source</div>
                              <div className="mt-2 space-y-2 text-[var(--k-muted)]">
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">
                                    Industries
                                  </div>
                                  <div className="mt-1 flex flex-wrap gap-1">
                                    {isRecord(r.sourceSignals) &&
                                    asStringArray(r.sourceSignals.industries).length ? (
                                      asStringArray(r.sourceSignals.industries)
                                        .slice(0, 3)
                                        .map((x, idx) => (
                                          <span
                                            key={idx}
                                            className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-0.5"
                                          >
                                            {String(x)}
                                          </span>
                                        ))
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">
                                    Screeners
                                  </div>
                                  <div className="mt-1">
                                    {isRecord(r.sourceSignals) &&
                                    asStringArray(r.sourceSignals.screeners).length ? (
                                      <ul className="list-disc pl-4">
                                        {asStringArray(r.sourceSignals.screeners)
                                          .slice(0, 3)
                                          .map((x, idx) => (
                                            <li key={idx}>{String(x)}</li>
                                          ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">
                                    Notes
                                  </div>
                                  <div className="mt-1">
                                    {isRecord(r.sourceSignals) &&
                                    asStringArray(r.sourceSignals.notes).length ? (
                                      <ul className="list-disc pl-4">
                                        {asStringArray(r.sourceSignals.notes)
                                          .slice(0, 3)
                                          .map((x, idx) => (
                                            <li key={idx}>{String(x)}</li>
                                          ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </div>

                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Plan</div>
                              <div className="mt-2 space-y-2 text-[var(--k-muted)]">
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Performance</div>
                                  <div className="font-mono">
                                    Today: {fmtTodayPct(r)} • Since: {fmtSinceEntryPct(r)}
                                    {fmtTodayPct(r) === '—' ? (
                                      <span className="ml-2 text-[11px] opacity-70">
                                        (no bars yet; open Stock page to sync)
                                      </span>
                                    ) : null}
                                  </div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Duration</div>
                                  <div>
                                    {Number.isFinite(r.expectedDurationDays as number)
                                      ? `${r.expectedDurationDays} days`
                                      : '—'}
                                  </div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Buy zone</div>
                                  <div className="font-mono">{fmtBuyZoneText(r)}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Targets</div>
                                  <div className="font-mono">{fmtTargetText(r)}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Invalidation</div>
                                  <div>{String(r.invalidation ?? '—')}</div>
                                </div>
                                <div className="grid grid-cols-[96px_1fr] gap-2">
                                  <div className="opacity-80">Probability</div>
                                  <div>{fmtProbability(r.probability ?? null)}</div>
                                </div>
                                <div>
                                  <div className="text-[11px] uppercase tracking-wide opacity-80">
                                    Triggers
                                  </div>
                                  <div className="mt-1">
                                    {Array.isArray(r.triggers) && r.triggers.length ? (
                                      <ul className="list-disc pl-4">
                                        {r.triggers.slice(0, 4).map((t, idx) => (
                                          <li key={idx}>{fmtTriggerText(t)}</li>
                                        ))}
                                      </ul>
                                    ) : (
                                      <span>—</span>
                                    )}
                                  </div>
                                </div>
                              </div>
                            </div>

                            <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                              <div className="text-xs font-medium text-[var(--k-text)]">Risks</div>
                              <div className="mt-2 text-[var(--k-muted)]">
                                {((r.risks?.length ? r.risks : r.riskPoints) ?? []).length ? (
                                  <ul className="list-disc pl-4">
                                    {((r.risks?.length ? r.risks : r.riskPoints) ?? [])
                                      .slice(0, 6)
                                      .map((x, idx) => (
                                        <li key={idx}>{x}</li>
                                      ))}
                                  </ul>
                                ) : (
                                  <span>—</span>
                                )}
                              </div>
                            </div>
                          </div>
                        </details>
                      </td>
                    </tr>
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">
            No leaders yet. Click “Generate today” after syncing screener + industry flow.
          </div>
        )}
      </section>
    </div>
  );
}
