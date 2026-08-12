/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

import { DashboardHeader } from '@/components/dashboard/DashboardHeader';
import { Button } from '@/components/ui/button';
import {
  BREADTH_PANIC_DOWN_THRESHOLD,
  buildIndexTrafficSummary,
  executionGateBadgeClass,
  fmtAmountCn,
  formatSrvIndexLine,
  srvIndexBadgeClass,
  translateRisk,
  translateGateMode,
  translateRegime,
  translateIndexLight,
  translateSignal,
  translateReason,
} from '@/lib/dashboard-format';
import { parseExecutionGate } from '@/lib/execution-action';
import { buildSentimentMarkdown } from '@/lib/dashboard-export';

type Props = {
  dash: any;
  summary: any;
  sentimentBusy: boolean;
  onSyncSentiment: () => void;
  toastSentimentCopy: (ok: boolean, text: string) => void;
  sentimentCopyStatus: { ok: boolean; text: string } | null;
  addReference: (ref: any) => void;
};

function GateBadge({ label, gate }: { label: string; gate: any }) {
  return (
    <div className={`rounded-lg border px-3 py-2 text-sm ${executionGateBadgeClass(gate.mode)}`}>
      <div className="flex flex-wrap items-center gap-2 font-medium">
        <span>
          {label}: {translateGateMode(gate.mode)}
        </span>
        {label === 'A股闸门' && gate.indexLight === 'red' ? (
          <span className="rounded bg-red-500/15 px-1.5 py-0.5 text-[11px] font-bold text-red-600 dark:text-red-400">
            红灯日 · 禁开新仓
          </span>
        ) : null}
        <span className="text-xs opacity-90">允许开仓={String(gate.allowNewEntries)}</span>
        <span className="text-xs opacity-90">
          {translateRegime(gate.marketRegime)} · {translateIndexLight(gate.indexLight)}
          {gate.positionRangeHint
            ? ` · 仓位 ${gate.positionRangeHint}`
            : null}
        </span>
      </div>
      {gate.satelliteNote ? <div className="mt-1 text-xs opacity-90">{gate.satelliteNote}</div> : null}
      {gate.reasons?.length ? (
        <div className="mt-1 text-xs opacity-80">原因: {gate.reasons.map(translateReason).join(' · ')}</div>
      ) : null}
    </div>
  );
}

export function MarketSentimentCard({
  dash,
  summary,
  sentimentBusy,
  onSyncSentiment,
  toastSentimentCopy,
  sentimentCopyStatus,
  addReference,
}: Props) {
  const ms = dash?.marketSentiment ?? {};
  const items: any[] = Array.isArray(ms.items) ? ms.items : [];
  const latest = items.length ? items[items.length - 1] : null;
  const indexSignals: any[] = Array.isArray(ms.indexSignals) ? ms.indexSignals : [];
  const summaryLine = buildIndexTrafficSummary(indexSignals);  const risk = String(latest?.riskMode ?? '—');
  const premium = Number.isFinite(latest?.yesterdayLimitUpPremium)
    ? `${Number(latest.yesterdayLimitUpPremium).toFixed(2)}%`
    : '—';
  const failed = Number.isFinite(latest?.failedLimitUpRate)
    ? `${Number(latest.failedLimitUpRate).toFixed(1)}%`
    : '—';
  const turnover = fmtAmountCn(latest?.marketTurnoverCny);
  const ratio = Number.isFinite(latest?.upDownRatio)
    ? Number(latest.upDownRatio).toFixed(2)
    : '—';
  const up = Number(latest?.upCount ?? 0);
  const down = Number(latest?.downCount ?? 0);
  const flat = Number(latest?.flatCount ?? 0);
  const breadthPanic = down >= BREADTH_PANIC_DOWN_THRESHOLD;
  const srvIndex: any = ms?.srvIndex ?? null;
  const srvLine = formatSrvIndexLine(srvIndex);
  const srvBadge = srvIndexBadgeClass(srvIndex?.level);
  const overlapSectors: string[] = Array.isArray(srvIndex?.overlapSectors)
    ? srvIndex.overlapSectors.map((x: any) => String(x)).filter(Boolean)
    : [];
  const badge =
    risk === 'confirmed_uptrend'
      ? 'border-emerald-600/40 bg-emerald-600/15 text-emerald-700'
      : risk === 'capitulation_v_bottom'
        ? 'border-fuchsia-600/40 bg-fuchsia-600/15 text-fuchsia-700'
        : risk === 'extreme_caution' || breadthPanic
          ? 'border-red-600/40 bg-red-600/15 text-red-700'
          : risk === 'no_new_positions'
            ? 'border-red-500/30 bg-red-500/10 text-red-600'
            : risk === 'caution'
              ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
              : risk === 'hot'
                ? 'border-green-500/30 bg-green-500/10 text-green-700'
                : risk === 'euphoric'
                  ? 'border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-700'
                  : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';

  const gate = parseExecutionGate(ms?.executionGate);

  return (
    <div className="flex flex-col gap-3">
      {/* Execution Gate — A股 + 港股 独立仓位闸门 */}
      {gate ? (
        <div className="grid gap-2 md:grid-cols-2">
          <GateBadge label="A股闸门" gate={gate} />
          {gate.hkGate ? <GateBadge label="港股闸门" gate={gate.hkGate} /> : null}
        </div>
      ) : null}

      {/* Risk + SRV + Traffic — 3-column compact grid */}
      <div className="grid grid-cols-3 gap-2">
        <div className={`rounded-lg border px-3 py-2 text-xs ${badge}`}>
          <div className="mb-1 font-medium uppercase tracking-wide opacity-60">风险</div>
          <div className="text-base font-bold">{translateRisk(risk)}</div>
          {Array.isArray(latest?.rules) && latest.rules.length ? (
            <div className="mt-1 text-[10px] opacity-80">
              {latest.rules.slice(0, 2).map((x: any) => String(x)).join(' · ')}
            </div>
          ) : null}
        </div>

        <div className={`rounded-lg border px-3 py-2 text-xs ${srvBadge}`}>
          <div className="mb-1 font-medium uppercase tracking-wide opacity-60">SRV 指数</div>
          <div className="text-base font-bold">{srvLine}</div>
          {srvIndex?.labelZh ? (
            <div className="mt-1 text-[10px] opacity-90">{String(srvIndex.labelZh)}</div>
          ) : null}
          {overlapSectors.length ? (
            <div className="mt-1 text-[10px] opacity-80">重叠: {overlapSectors.join(', ')}</div>
          ) : null}
        </div>

        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs">
          <div className="mb-1 font-medium uppercase tracking-wide text-amber-600">行情</div>
          <div className="text-sm font-bold text-amber-700">{summaryLine.title}</div>
          <div className="mt-1 text-[10px] text-amber-800">{summaryLine.detail}</div>
        </div>
      </div>

      {/* ETF flow confirmation signal (secondary factor) */}
      {(() => {
        const ef: any = ms?.etfFlowSignal;
        if (!ef) return null;
        const verdict = String(ef?.verdict ?? 'neutral');
        const badgeCls =
          verdict === 'confirm'
            ? 'border-emerald-500/30 bg-emerald-500/10'
            : verdict === 'contradict'
              ? 'border-red-500/30 bg-red-500/10'
              : 'border-[var(--k-border)] bg-[var(--k-surface-2)]';
        const textCls =
          verdict === 'confirm'
            ? 'text-emerald-700 dark:text-emerald-300'
            : verdict === 'contradict'
              ? 'text-red-700 dark:text-red-300'
              : 'text-[var(--k-muted)]';
        const verdictZh =
          verdict === 'confirm' ? '确认净流入' : verdict === 'contradict' ? '背离净流出' : '中性';
        const broadZh =
          ef?.broadDirection === 'buy'
            ? '国家队净买'
            : ef?.broadDirection === 'outflow'
              ? '国家队流出'
              : '中性';
        const sectorZh =
          ef?.sectorDirection === 'buy'
            ? '板块动量'
            : ef?.sectorDirection === 'outflow'
              ? '机构流出'
              : '中性';
        return (
          <div className={`rounded-lg border px-3 py-2 text-xs ${badgeCls}`}>
            <div className="mb-1 font-medium uppercase tracking-wide opacity-60">资金确认 (ETF)</div>
            <div className={`text-sm font-bold ${textCls}`}>{verdictZh}</div>
            <div className="mt-1 text-[10px] opacity-80">
              国家队 {broadZh} · 板块 {sectorZh}
              {ef?.incomplete ? ' · 数据不完整' : ''}
              {ef?.asOfDate ? ` · ${String(ef.asOfDate)}` : ''}
            </div>
          </div>
        );
      })()}

      {/* Special alerts */}
      {risk === 'capitulation_v_bottom' && (
        <div className="rounded-lg border border-fuchsia-600/40 bg-fuchsia-600/15 px-3 py-2 text-xs text-fuchsia-800 dark:text-fuchsia-200">
          🚨 Capitulation_V_Bottom (恐慌冰点共振) — 国家队入场 + 恐慌极值，左侧绝佳试错点出现
        </div>
      )}
      {risk === 'confirmed_uptrend' && (
        <div className="rounded-lg border border-emerald-600/40 bg-emerald-600/15 px-3 py-2 text-xs text-emerald-800 dark:text-emerald-200">
          🟢 Follow-Through Day 右侧主升浪确立 — 解除宏观死锁，放开攻击权限
        </div>
      )}

      {/* Market Breadth — compact stat grid */}
      <div
        className={`rounded-lg border px-3 py-2 text-xs ${
          breadthPanic ? 'border-red-500/40 bg-red-500/10' : 'border-[var(--k-border)] bg-[var(--k-surface-2)]'
        }`}
      >
        <div className={`mb-2 font-medium ${breadthPanic ? 'text-red-700' : 'text-[var(--k-fg)]'}`}>
          涨跌家数
        </div>
        <div className="grid grid-cols-4 gap-1 text-center sm:grid-cols-7">
          <div>
            <div className="text-[10px] opacity-60">涨</div>
            <div className="whitespace-nowrap font-mono text-emerald-600">{up.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">跌</div>
            <div className="whitespace-nowrap font-mono text-red-600">{down.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">平</div>
            <div className="whitespace-nowrap font-mono text-[var(--k-muted)]">{flat.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">比</div>
            <div className="whitespace-nowrap font-mono">{ratio}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">成交额</div>
            <div className="whitespace-nowrap font-mono">{turnover}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">溢价</div>
            <div className="whitespace-nowrap font-mono">{premium}</div>
          </div>
          <div>
            <div className="text-[10px] opacity-60">炸板</div>
            <div className="whitespace-nowrap font-mono">{failed}</div>
          </div>
        </div>
        {breadthPanic ? (
          <div className="mt-1 text-[10px] text-red-700">
            下跌 &ge; {BREADTH_PANIC_DOWN_THRESHOLD.toLocaleString()}：触发红色预警，极度谨慎。
          </div>
        ) : null}
      </div>

      {/* Index Signals — compact grid */}
      {indexSignals.length ? (
        <div>
          <div className="mb-1 flex items-center gap-2 text-xs text-[var(--k-muted)]">
            <DashboardHeader helpId="idxRule.title" align="left" width={420} />
          </div>
          <div className="grid gap-2 md:grid-cols-2">
            {indexSignals.map((it: any) => {
              const signal = String(it?.signal ?? 'unknown');
              const source = String(it?.source ?? 'unknown');
              const tradeTime = String(it?.tradeTime ?? '').trim();
              const asOfDate = String(it?.asOfDate ?? '').trim();
              const realtime = it?.realtime === true;
              const freshness = realtime ? '实时' : '收盘';
              const asOfDisplay = tradeTime || asOfDate || '—';
              const quoteError = String(it?.quoteError ?? '').trim();
              const featured = it?.featured === true;
              const signalBadge =
                signal === 'deep_green'
                  ? 'border-emerald-600/40 bg-emerald-600/15 text-emerald-800'
                  : signal === 'light_green' || signal === 'green'
                    ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700'
                    : signal === 'red'
                      ? 'border-red-500/30 bg-red-500/10 text-red-600'
                      : signal === 'yellow'
                        ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                        : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
              return (
                <div
                  key={String(it?.tsCode ?? it?.name)}
                  className={`rounded-lg border px-3 py-2 text-xs ${signalBadge} ${
                    featured ? 'md:col-span-2' : ''
                  }`}                >
                  <div className="font-medium">
                    {featured ? '★ ' : ''}
                    {String(it?.name ?? it?.tsCode ?? '')}
                  </div>
                  <div className="mt-1 font-mono">
                    {translateSignal(signal)} · 仓位 {String(it?.positionRange ?? '—')}
                  </div>
                  <div className="mt-1 text-[var(--k-muted)]">
                    涨幅{' '}
                    {Number.isFinite(it?.pctChg)
                      ? `${Number(it.pctChg) >= 0 ? '+' : ''}${Number(it.pctChg).toFixed(2)}%`
                      : '—'}{' '}
                    · 收盘{' '}
                    {Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—'} · MA5{' '}
                    {Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—'} · MA20{' '}
                    {Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—'}
                  </div>
                  <div className="mt-1 font-mono text-[10px] text-[var(--k-muted)]">
                    截至 {asOfDisplay} · {freshness} · {source}
                  </div>
                  {featured && Array.isArray(it?.rules) && it.rules.length ? (
                    <div className="mt-1 text-[10px] opacity-80">
                      规则: {it.rules.map((x: any) => String(x)).join(' · ')}
                    </div>
                  ) : null}
                  {quoteError ? (
                    <div className="mt-1 text-[10px] text-amber-700 dark:text-amber-300">
                      行情回退: {quoteError}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}

      {/* Last 5 days — always open */}
      <div>
        <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">近5日</div>
        <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
          <table className="w-full border-collapse text-xs">
            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
              <tr className="text-left">
                <th className="px-2 py-2 whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.date" align="left" width={280} />
                </th>
                <th className="px-2 py-2 text-right whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.ratio" align="right" width={340} />
                </th>
                <th className="px-2 py-2 text-right whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.turnover" align="right" width={340} />
                </th>
                <th className="px-2 py-2 text-right whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.premiumPct" align="right" width={360} />
                </th>
                <th className="px-2 py-2 text-right whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.failedPct" align="right" width={360} />
                </th>
                <th className="px-2 py-2 whitespace-nowrap">
                  <DashboardHeader helpId="sentiment5d.risk" align="left" width={340} />
                </th>
              </tr>
            </thead>
            <tbody>
              {(items || []).slice(-5).map((it: any, idx: number) => (
                <tr key={idx} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-2 font-mono">{String(it.date ?? '')}</td>
                  <td className="px-2 py-2 text-right font-mono">
                    {Number.isFinite(it.upDownRatio) ? Number(it.upDownRatio).toFixed(2) : '—'}
                  </td>
                  <td className="px-2 py-2 text-right font-mono">
                    {fmtAmountCn(it.marketTurnoverCny)}
                  </td>
                  <td className="px-2 py-2 text-right font-mono">
                    {Number.isFinite(it.yesterdayLimitUpPremium)
                      ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
                      : '—'}
                  </td>
                  <td className="px-2 py-2 text-right font-mono">
                    {Number.isFinite(it.failedLimitUpRate)
                      ? `${Number(it.failedLimitUpRate).toFixed(1)}%`
                      : '—'}
                  </td>
                  <td className="px-2 py-2">{String(it.riskMode ?? '')}</td>
                </tr>
              ))}
              {!items.length ? (
                <tr>
                  <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={6}>
                    暂无情绪数据。请点击“同步并复制”。
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      {/* Action buttons */}
      <div className="flex items-center gap-2">
        <Button size="sm" variant="secondary" disabled={sentimentBusy} onClick={() => onSyncSentiment()}>
          {sentimentBusy ? (
            <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
          ) : (
            <RefreshCw className="mr-2 h-4 w-4" />
          )}
          同步情绪
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => {
            try {
              const md = buildSentimentMarkdown(summary, '#');
              void navigator.clipboard
                .writeText(md)
                .then(() => toastSentimentCopy(true, '已复制Markdown。'))
                .catch(() => toastSentimentCopy(false, '复制失败，请允许剪贴板访问。'));
            } catch (e) {
              toastSentimentCopy(false, e instanceof Error ? e.message : String(e));
            }
          }}
        >
          复制Markdown
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => {
            const asOfDate = String(ms.asOfDate ?? dash?.asOfDate ?? '');
            addReference({
              kind: 'marketSentiment',
              refId: `${asOfDate}:5`,
              asOfDate,
              days: 5,
              title: 'A股市场情绪（涨跌与涨停）',
              createdAt: new Date().toISOString(),
            });
          }}
        >
          参考
        </Button>
      </div>
      {sentimentCopyStatus ? (
        <div className={`text-xs ${sentimentCopyStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {sentimentCopyStatus.text}
        </div>
      ) : null}
    </div>
  );
}
