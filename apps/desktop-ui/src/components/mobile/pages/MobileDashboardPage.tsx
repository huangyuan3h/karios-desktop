'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';
import { useNewsItemsQuery } from '@/lib/queries/news';
import { useIndustryFundFlowQuery } from '@/lib/queries/industryFlow';
import { useDashboardSentimentQuery } from '@/lib/queries/sentiment';
import { fmtAmountCn, fmtSignedAmountCn } from '@/lib/dashboard-format';
import { GateBadge, MobileCard, MobileSection, PriceText, StatusPill } from '../primitives';

const RISK_ZH: Record<string, string> = {
  normal: '正常',
  confirmed_uptrend: '确认上升',
  capitulation_v_bottom: '恐慌筑底',
  extreme_caution: '极度谨慎',
  no_new_positions: '禁止新仓',
  caution: '谨慎',
  hot: '过热',
  euphoric: '亢奋',
};

const RISK_HINT_ZH: Record<string, string> = {
  normal: '可正常开仓',
  confirmed_uptrend: '趋势确认，可正常开仓',
  capitulation_v_bottom: '恐慌性抛售后筑底，观察企稳',
  extreme_caution: '极度谨慎，禁止新开仓',
  no_new_positions: '禁止新开仓，仅处理现有持仓',
  caution: '谨慎对待，控制仓位',
  hot: '市场过热，注意追高风险',
  euphoric: '情绪亢奋，警惕顶部风险',
};

const GATE_MODE_ZH: Record<string, string> = {
  ATTACK: '进攻',
  WEAK_ATTACK: '弱进攻',
  HOLD_ONLY: '防守',
  DEFEND: '防守',
};

const GATE_MODE_TONE: Record<string, 'open' | 'warn' | 'danger' | 'neutral'> = {
  ATTACK: 'open',
  WEAK_ATTACK: 'open',
  HOLD_ONLY: 'warn',
  DEFEND: 'danger',
};

const GATE_REASON_ZH: Record<string, string> = {
  BREADTH_PANIC: '广度恐慌',
  ETF_FLOW_CONFIRM: 'ETF 净流入确认',
  ETF_FLOW_CONTRADICT: 'ETF 资金流背离',
  INTRADAY_OVERFLOW_OVERRIDE: '盘中溢出覆盖',
  REGIME_DIVERGING: '弱势震荡',
  REGIME_STRONG: '强势',
  REGIME_WEAK: '弱势',
  RISK_EXTREME_CAUTION: '情绪极度谨慎',
  RISK_NO_NEW: '禁止新仓',
  SRV_ELEVATED: 'SRV 偏高',
  SRV_EXTREME_HIGH: 'SRV 极高',
  SRV_STABLE: 'SRV 平稳',
  SRV_UNKNOWN: 'SRV 未知',
};

function gateReasonZh(code: string): string {
  return GATE_REASON_ZH[code] ?? code;
}

function sentimentTone(risk: string): 'open' | 'warn' | 'danger' | 'neutral' {
  if (risk === 'confirmed_uptrend' || risk === 'hot') return 'open';
  if (risk === 'caution') return 'warn';
  if (risk === 'extreme_caution' || risk === 'no_new_positions' || risk === 'capitulation_v_bottom') return 'danger';
  return 'neutral';
}

/** Gate card — A股/港股 闸门（数据来自 marketSentiment.executionGate） */
function GateCard({ label, gate }: { label: string; gate: any }) {
  if (!gate || typeof gate !== 'object') {
    return (
      <MobileCard className="p-3">
        <div className="text-[var(--m-text-base)] font-semibold">{label}</div>
        <div className="mt-1 text-[var(--m-text-xs)] text-[var(--k-muted)]">暂无闸门数据</div>
      </MobileCard>
    );
  }
  const mode = String(gate.mode || '—');
  const allow = Boolean(gate.allowNewEntries);
  const regime = String(gate.marketRegime || '—');
  const light = String(gate.indexLight || '—');
  const reasons: string[] = Array.isArray(gate.reasons) ? gate.reasons.map((x: unknown) => String(x)) : [];
  const posHint = gate.positionRangeHint ? String(gate.positionRangeHint) : null;
  return (
    <MobileCard
      className={
        allow
          ? 'p-3'
          : 'border-[var(--k-danger)]/50 bg-[var(--k-danger)]/10 p-3'
      }
    >
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-[var(--m-text-base)] font-semibold">{label}</span>
            {allow ? <StatusPill tone="open">允许开仓</StatusPill> : null}
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-1.5">
            <StatusPill tone={GATE_MODE_TONE[mode] ?? 'neutral'}>
              {GATE_MODE_ZH[mode] ?? mode}
            </StatusPill>
          </div>
        </div>
        <div className="shrink-0 text-right">
          <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">
            {regime}
            {light && light !== '—' ? ` · ${light}灯` : ''}
          </div>
          {posHint ? (
            <div className="mt-1 text-[var(--m-text-sm)] font-semibold text-[var(--k-warn)]">{posHint}</div>
          ) : null}
        </div>
      </div>
      {reasons.length ? (
        <div className="mt-2 text-[var(--m-text-xs)] text-[var(--k-muted)]">
          原因: {reasons.map(gateReasonZh).join(' · ')}
        </div>
      ) : null}
    </MobileCard>
  );
}

/** Dashboard (mobile) — gates + sentiment + news pulse + top inflow. §5.2 高频. */
export function MobileDashboardPage() {
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const news = useNewsItemsQuery(24, 5);
  const flow = useIndustryFundFlowQuery(10, 200);
  const senti = useDashboardSentimentQuery();

  const cn = health.data;
  const cnGate = cn == null ? null : isMarketGateClosed(cn);
  const hk = cn?.hkHealth ?? null;
  const hkGate = hk == null ? null : isMarketGateClosed(hk);

  const topIn = [...(flow.data?.top ?? [])].sort((a, b) => b.netInflow - a.netInflow).slice(0, 5);

  const ms: any = (senti.data as any)?.marketSentiment ?? {};
  const gate: any = ms?.executionGate ?? {};
  const cnExecutionGate = gate?.cnGate ?? null;
  const hkExecutionGate = gate?.hkGate ?? null;
  const sItems: any[] = Array.isArray(ms.items) ? ms.items : [];
  const sLatest = sItems.length ? sItems[sItems.length - 1] : null;
  const sRisk = String(sLatest?.riskMode ?? '—');
  const upCount = Number(sLatest?.upCount ?? 0);
  const downCount = Number(sLatest?.downCount ?? 0);

  return (
    <div className="space-y-4">
      <MobileSection title="今日状态">
        <MobileCard className="p-3">
          <div className="flex items-center justify-between">
            <div className="flex gap-1.5">
              {cn ? <GateBadge market="A股" open={!cnGate} /> : null}
              {hk ? <GateBadge market="港股" open={!hkGate} /> : null}
            </div>
            <div className="flex gap-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">
              {cn ? <StatusPill tone={cnGate ? 'closed' : 'open'}>{cn.regime ?? '—'}</StatusPill> : null}
              <StatusPill tone="neutral">强度 {cn?.strength ?? '—'}</StatusPill>
            </div>
          </div>
          <div className="mt-2 flex justify-between text-[var(--m-text-xs)] text-[var(--k-muted)]">
            <span>买入候选 {cn?.s3Candidates?.length ?? 0} 个</span>
            <span>持仓 {cn?.holdings?.length ?? 0} + {(hk?.holdings?.length ?? 0) ? `${hk?.holdings?.length ?? 0} 港股` : ''}</span>
            <span>数据 {cn?.tradeDate ?? '—'}</span>
          </div>
        </MobileCard>
        {cn?.sentiment || cn?.panicCooldown?.active ? (
          <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3 text-[var(--m-text-sm)] text-[var(--k-warn)]">
            {cn?.sentiment ? `市场情绪 ${cn.sentiment}` : ''}
            {cn?.panicCooldown?.active ? ` · 恐慌冷却至 ${cn.panicCooldown.cooldownEndDate ?? '—'}` : ''}
          </MobileCard>
        ) : null}
      </MobileSection>

      <MobileSection title="市场情绪">
        <div className="space-y-2">
          <MobileCard className="p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-[var(--m-text-base)] font-semibold">风险模式</span>
                  <StatusPill tone={sentimentTone(sRisk)}>{RISK_ZH[sRisk] ?? sRisk}</StatusPill>
                </div>
                <div className="mt-1 text-[var(--m-text-sm)] text-[var(--k-muted)]">
                  {RISK_HINT_ZH[sRisk] ?? ''}
                </div>
              </div>
              <div className="shrink-0 text-right font-mono text-[var(--m-text-xs)] text-[var(--k-muted)] tabular-nums">
                <div>
                  <span style={{ color: 'var(--k-up)' }}>↑{upCount}</span>
                  <span> / </span>
                  <span style={{ color: 'var(--k-down)' }}>↓{downCount}</span>
                </div>
                <div className="mt-0.5">
                  溢价 {Number.isFinite(sLatest?.yesterdayLimitUpPremium) ? `${Number(sLatest.yesterdayLimitUpPremium).toFixed(2)}%` : '—'}
                </div>
                <div className="mt-0.5">成交 {fmtAmountCn(sLatest?.marketTurnoverCny)}</div>
              </div>
            </div>
          </MobileCard>
          {cnExecutionGate ? <GateCard label="A股闸门" gate={cnExecutionGate} /> : null}
          {hkExecutionGate ? <GateCard label="港股闸门" gate={hkExecutionGate} /> : null}
          {!cnExecutionGate && !hkExecutionGate ? (
            <MobileCard className="p-3">
              <div className="grid grid-cols-3 gap-2">
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2.5 py-2">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">涨 / 跌</div>
                  <div className="mt-1 font-mono text-[var(--m-text-base)] tabular-nums">
                    <span style={{ color: 'var(--k-up)' }}>{upCount}</span>
                    <span className="text-[var(--k-muted)]"> / </span>
                    <span style={{ color: 'var(--k-down)' }}>{downCount}</span>
                  </div>
                </div>
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2.5 py-2">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">涨停溢价</div>
                  <div className="mt-1 font-mono text-[var(--m-text-base)] tabular-nums">
                    {Number.isFinite(sLatest?.yesterdayLimitUpPremium) ? `${Number(sLatest.yesterdayLimitUpPremium).toFixed(2)}%` : '—'}
                  </div>
                </div>
                <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2.5 py-2">
                  <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">两市成交</div>
                  <div className="mt-1 font-mono text-[var(--m-text-sm)] tabular-nums">
                    {fmtAmountCn(sLatest?.marketTurnoverCny)}
                  </div>
                </div>
              </div>
            </MobileCard>
          ) : null}
          {Array.isArray(sLatest?.rules) && sLatest.rules.length ? (
            <MobileCard className="p-3">
              <div className="text-[var(--m-text-xs)] text-[var(--k-muted)]">
                {sLatest.rules.slice(0, 3).map((x: unknown) => String(x)).join(' · ')}
              </div>
            </MobileCard>
          ) : null}
        </div>
      </MobileSection>

      <MobileSection title={`买入候选${cn?.s3Candidates?.length ? `（${cn.s3Candidates.length}）` : ''}`}>
        {cn?.s3Candidates?.length ? (
          <div className="space-y-2">
            {cn.s3Candidates.slice(0, 8).map((c) => (
              <MobileCard key={c.symbol ?? c.ts_code} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">{c.name ?? c.symbol}</div>
                    <div className="mt-0.5 truncate text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {c.symbol}
                      {c.industry ? ` · ${c.industry}` : ''}
                      {c.alphaEvents?.length ? ` · ${c.alphaEvents.length} 条催化` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-base)] font-semibold tabular-nums">
                      score {c.score != null ? c.score.toFixed(0) : '—'}
                    </div>
                    {c.rs != null ? (
                      <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">RS 前 {Math.round(c.rs * 100)}%</div>
                    ) : null}
                  </div>
                </div>
                {c.industryFlow?.netInflow5d != null ? (
                  <div className="mt-1.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                    行业 5 日净流入 {fmtAmountCn(c.industryFlow.netInflow5d)}
                    {c.industryFlow.rank5d != null ? ` · 排名 ${c.industryFlow.rank5d}` : ''}
                  </div>
                ) : null}
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {cn?.circuitBlocked ? '回撤熔断中，暂停新买入' : '今日暂无买入候选'}
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title={`持仓${(cn?.holdings?.length ?? 0) + (hk?.holdings?.length ?? 0) ? `（${(cn?.holdings?.length ?? 0) + (hk?.holdings?.length ?? 0)}）` : ''}`}>
        {(() => {
          const holdings = [
            ...(cn?.holdings ?? []).map((h) => ({ ...h, market: 'A股' })),
            ...(hk?.holdings ?? []).map((h) => ({ ...h, market: '港股' })),
          ];
          if (!holdings.length) {
            return (
              <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
                暂无持仓
              </MobileCard>
            );
          }
          return (
            <div className="space-y-2">
              {holdings.map((h) => {
                const pnl = h.pnlPct ?? 0;
                return (
                  <MobileCard key={h.symbol} className="p-3">
                    <div className="flex items-center justify-between gap-2">
                      <div className="min-w-0">
                        <div className="truncate text-[var(--m-text-base)] font-semibold">{h.name ?? h.symbol}</div>
                        <div className="mt-0.5 truncate text-[var(--m-text-xs)] text-[var(--k-muted)]">
                          {h.symbol} · {h.market}
                          {h.holdingDays != null ? ` · 已持 ${h.holdingDays} 天` : ''}
                        </div>
                      </div>
                      <div className="shrink-0 text-right">
                        <div
                          className="font-mono text-[var(--m-text-base)] font-semibold tabular-nums"
                          style={{ color: pnl > 0 ? 'var(--k-up)' : pnl < 0 ? 'var(--k-down)' : 'var(--k-muted)' }}
                        >
                          {pnl > 0 ? '+' : ''}{pnl.toFixed(2)}%
                        </div>
                        {h.action === 'EXIT' ? <StatusPill tone="danger">退出</StatusPill> : null}
                      </div>
                    </div>
                    {h.stopLossLine != null || h.trailingLine != null ? (
                      <div className="mt-2 grid grid-cols-2 gap-1.5 text-[var(--m-text-xs)]">
                        {h.stopLossLine != null ? (
                          <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
                            <span className="text-[var(--k-muted)]">止损 </span>
                            <span className="font-mono tabular-nums">{h.stopLossLine}</span>
                          </div>
                        ) : null}
                        {h.trailingLine != null ? (
                          <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
                            <span className="text-[var(--k-muted)]">移动 </span>
                            <span className="font-mono tabular-nums">{h.trailingLine}</span>
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                  </MobileCard>
                );
              })}
            </div>
          );
        })()}
      </MobileSection>

      <MobileSection title="行业资金流 Top 5">
        {topIn.length ? (
          <div className="space-y-2">
            {topIn.map((r, i) => (
              <MobileCard key={r.industryCode} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {i + 1}. {r.industryName}
                    </div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      10 日累计 {fmtAmountCn(r.sum10d)}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <span
                      className="text-[var(--m-text-base)] font-semibold"
                      style={{ color: r.netInflow > 0 ? 'var(--k-up)' : r.netInflow < 0 ? 'var(--k-down)' : 'inherit' }}
                    >
                      {fmtSignedAmountCn(r.netInflow)}
                    </span>
                    <div className="mt-0.5 text-right text-[var(--m-text-xs)] text-[var(--k-muted)]">净流入</div>
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无行业资金流数据
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title="最新要闻">
        {news.data?.items.length ? (
          <div className="space-y-2">
            {news.data.items.slice(0, 5).map((n) => (
              <MobileCard key={n.id} className="p-3">
                <div className="truncate text-[var(--m-text-base)] font-medium">{n.title}</div>
                {n.aiSummary ? (
                  <div className="mt-1 line-clamp-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">
                    {n.aiSummary}
                  </div>
                ) : null}
                {n.eventType ? <StatusPill tone="neutral">{n.eventType}</StatusPill> : null}
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无新闻
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
