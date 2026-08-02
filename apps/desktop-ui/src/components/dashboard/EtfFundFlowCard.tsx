/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import { DashboardHeader } from '@/components/dashboard/DashboardHeader';
import { fmtSignedAmountCn } from '@/lib/dashboard-format';

type Props = {
  dash: any;
};

export function EtfFundFlowCard({ dash }: Props) {
  const ms = dash?.marketSentiment ?? {};
  const etfFlow: any = ms?.etfFundFlow ?? {};
  const etfItems: any[] = Array.isArray(etfFlow?.items) ? etfFlow.items : [];

  return (
    <div className="flex flex-col gap-2">
      <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">ETF资金流 (持仓关注)</div>
      {etfFlow?.shareLag ? (
        <div className="mb-1 text-[10px] text-amber-600 dark:text-amber-400">
          东方财富实时资金流不完整，缺失行已从盘中信号中排除
          {etfFlow?.intradaySafe === false ? ' — 盘中决策不可用' : ''}。
        </div>
      ) : null}
      <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full border-collapse text-xs">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-2 whitespace-nowrap">
                <DashboardHeader helpId="etf.name" align="left" width={300} />
              </th>
              <th className="px-2 py-2 font-mono whitespace-nowrap">
                <DashboardHeader helpId="etf.symbol" align="left" width={300} />
              </th>
              <th className="px-2 py-2 text-right whitespace-nowrap">
                <DashboardHeader helpId="etf.mainFlow" align="right" width={340} />
              </th>
              <th className="px-2 py-2 text-right whitespace-nowrap">
                <DashboardHeader helpId="etf.superLarge" align="right" width={360} />
              </th>
              <th className="px-2 py-2 text-right whitespace-nowrap">
                <DashboardHeader helpId="etf.flow3d" align="right" width={300} />
              </th>
              <th className="px-2 py-2 whitespace-nowrap">
                <DashboardHeader helpId="etf.realtimeAsOf" align="left" width={320} />
              </th>
              <th className="px-2 py-2 whitespace-nowrap">
                <DashboardHeader helpId="etf.source" align="left" width={300} />
              </th>
              <th className="px-2 py-2 whitespace-nowrap">
                <DashboardHeader helpId="etf.status" align="left" width={320} />
              </th>
              <th className="px-2 py-2 whitespace-nowrap">
                <DashboardHeader helpId="etf.signal" align="left" width={340} />
              </th>
            </tr>
          </thead>
          <tbody>
            {etfItems.map((it: any, idx: number) => {
              const flowStatus = String(it?.flowStatus ?? (it?.live === true ? 'Live' : '—'));
              const live = it?.live === true || flowStatus === 'Live';
              const isMarketClosed = flowStatus === 'MarketClosed';
              const flow1dStale =
                !live && !isMarketClosed && it?.netFlow1d == null && (it?.flowAsOfDate != null || it?.netFlow1dLagged != null);
              const flow1dDisplay = flow1dStale ? '— (stale)' : fmtSignedAmountCn(it?.netFlow1d);
              const superLargeFlow = fmtSignedAmountCn(it?.superLargeNetInflow);
              const largeFlow = fmtSignedAmountCn(it?.largeNetInflow);
              const signalText = String(it?.signalDisplay ?? it?.signal ?? '—');
              const isDataLag = String(it?.signal ?? '') === 'Data Lag';
              const realtimeAsOf = String(it?.tradeTime ?? it?.flowAsOfDate ?? etfFlow?.asOfDate ?? '—');
              return (
                <tr key={idx} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-2">{String(it?.name ?? '')}</td>
                  <td className="px-2 py-2 font-mono">{String(it?.symbol ?? '')}</td>
                  <td className="px-2 py-2 text-right font-mono">{flow1dDisplay}</td>
                  <td className="px-2 py-2 text-right font-mono">{superLargeFlow}/{largeFlow}</td>
                  <td className="px-2 py-2 text-right font-mono">{fmtSignedAmountCn(it?.netFlow3d)}</td>
                  <td className="px-2 py-2 font-mono">{realtimeAsOf}</td>
                  <td className="px-2 py-2 font-mono">{String(it?.source ?? '—')}</td>
                  <td
                    className={`px-2 py-2 font-mono ${
                      flowStatus === 'Live'
                        ? 'font-semibold text-emerald-600'
                        : isMarketClosed
                          ? 'text-[var(--k-muted)]'
                          : flowStatus === 'Stale' || flowStatus === 'Missing'
                            ? 'text-amber-600'
                            : 'text-[var(--k-muted)]'
                    }`}
                  >
                    {isMarketClosed ? '已收盘' : flowStatus}
                  </td>
                  <td className={isDataLag ? 'px-2 py-2 text-[var(--k-muted)]' : 'px-2 py-2'}>
                    {signalText}
                  </td>
                </tr>
              );
            })}
            {!etfItems.length ? (
              <tr>
                <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={9}>
                  暂无ETF资金流数据。请点击“同步情绪”。
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}
