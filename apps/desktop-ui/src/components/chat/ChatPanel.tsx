'use client';

import * as React from 'react';

import { ChatComposer } from '@/components/chat/ChatComposer';
import { ChatMessageList } from '@/components/chat/ChatMessageList';
import { AI_BASE_URL, QUANT_BASE_URL } from '@/lib/endpoints';
import { newId } from '@/lib/id';
import { useChatStore } from '@/lib/chat/store';
import type { ChatAttachment, ChatMessage, ChatReference } from '@/lib/chat/types';

type TvSnapshotDetail = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  filters: string[];
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

type BrokerSnapshotDetail = {
  id: string;
  broker: string;
  accountId: string | null;
  capturedAt: string;
  kind: string;
  createdAt: string;
  imagePath: string;
  extracted: Record<string, unknown>;
};

type BrokerAccountState = {
  accountId: string;
  broker: string;
  updatedAt: string;
  overview: Record<string, unknown>;
  positions: Array<Record<string, unknown>>;
  conditionalOrders: Array<Record<string, unknown>>;
  trades: Array<Record<string, unknown>>;
  counts: Record<string, number>;
};

type StockBarsDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  bars: Array<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    amount: string;
  }>;
};

type StockChipsDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    profitRatio: string;
    avgCost: string;
    cost90Low: string;
    cost90High: string;
    cost90Conc: string;
    cost70Low: string;
    cost70High: string;
    cost70Conc: string;
  }>;
};

type StockFundFlowDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    close: string;
    changePct: string;
    mainNetAmount: string;
    mainNetRatio: string;
    superNetAmount: string;
    superNetRatio: string;
    largeNetAmount: string;
    largeNetRatio: string;
    mediumNetAmount: string;
    mediumNetRatio: string;
    smallNetAmount: string;
    smallNetRatio: string;
  }>;
};

type LeaderStocksList = {
  days: number;
  dates: string[];
  leaders: Array<{
    id: string;
    date: string;
    symbol: string;
    market: string;
    ticker: string;
    name: string;
    entryPrice?: number | null;
    nowClose?: number | null;
    pctSinceEntry?: number | null;
    score?: number | null;
    liveScore?: number | null;
    liveScoreUpdatedAt?: string | null;
    reason?: string | null;
    whyBullets?: string[];
    expectedDurationDays?: number | null;
    buyZone?: Record<string, unknown>;
    targetPrice?: Record<string, unknown>;
    probability?: number | null;
    sourceSignals?: Record<string, unknown>;
    riskPoints?: string[];
  }>;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return isRecord(v) ? v : null;
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function asStringArray(v: unknown): string[] {
  return asArray(v)
    .map((x) => String(x))
    .filter(Boolean);
}

function getStr(obj: Record<string, unknown>, key: string, fallback = ''): string {
  const v = obj[key];
  if (typeof v === 'string') return v;
  if (v == null) return fallback;
  return String(v);
}

function getNum(obj: Record<string, unknown>, key: string, fallback = 0): number {
  const v = obj[key];
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function pickColumns(headers: string[]) {
  const preferred = [
    'Ticker',
    'Name',
    'Symbol',
    'Price',
    'Change %',
    'Rel Volume',
    'Rel Volume 1W',
    'Market cap',
    'Sector',
    'Analyst Rating',
    'RSI (14)',
  ];
  const set = new Set(headers);
  const picked = preferred.filter((h) => set.has(h));
  const rest = headers.filter((h) => !picked.includes(h));
  return [...picked, ...rest].slice(0, 8);
}

async function buildReferenceBlock(refs: ChatReference[]): Promise<string> {
  let out = '# Reference Context\n\n';
  for (const ref of refs) {
    if (ref.kind === 'tv') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/integrations/tradingview/snapshots/${encodeURIComponent(ref.snapshotId)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load snapshot');
        const snap = (await resp.json()) as TvSnapshotDetail;
        out += `## TradingView: ${ref.screenerName}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- capturedAt: ${ref.capturedAt}\n`;
        out += `- url: ${snap.url}\n`;
        if (snap.screenTitle) out += `- screenTitle: ${snap.screenTitle}\n`;
        if (Array.isArray(snap.filters) && snap.filters.length) {
          out += `- filters: ${snap.filters.join(' | ')}\n`;
        }
        const cols = pickColumns(snap.headers);
        out += `- columns: ${cols.join(', ')}\n`;
        const rows = snap.rows.slice(0, 20);
        out += `\nRows (first ${rows.length}):\n`;
        for (const r of rows) {
          const line = cols.map((c) => `${c}=${(r[c] ?? '').replaceAll('\n', ' ')}`).join(' ; ');
          out += `- ${line}\n`;
        }
        out += `\n`;
      } catch {
        out += `## TradingView: ${ref.screenerName}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- capturedAt: ${ref.capturedAt}\n`;
        out += `- status: failed to load snapshot\n\n`;
      }
      continue;
    }

    if (ref.kind === 'watchlistStock') {
      const name = (ref.name ?? '').trim();
      out += `## Watchlist: ${ref.symbol}${name ? ` (${name})` : ''}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      if (ref.asOfDate) out += `- asOfDate: ${ref.asOfDate}\n`;
      if (typeof ref.close === 'number') out += `- close: ${ref.close}\n`;
      if (typeof ref.trendOk === 'boolean') out += `- trendOk: ${ref.trendOk ? 'true' : 'false'}\n`;
      if (typeof ref.score === 'number') out += `- score: ${ref.score}\n`;
      if (typeof ref.stopLossPrice === 'number') out += `- stopLossPrice: ${ref.stopLossPrice}\n`;
      if (ref.buyMode) out += `- buyMode: ${ref.buyMode}\n`;
      if (ref.buyAction) out += `- buyAction: ${ref.buyAction}\n`;
      if (typeof ref.buyZoneLow === 'number' || typeof ref.buyZoneHigh === 'number') {
        out += `- buyZone: ${ref.buyZoneLow ?? '—'} .. ${ref.buyZoneHigh ?? '—'}\n`;
      }
      if (ref.buyWhy) out += `\nWhy:\n- ${ref.buyWhy.replaceAll('\n', ' ')}\n`;
      out += `\n`;
      continue;
    }

    if (ref.kind === 'watchlistTable') {
      out += `## Watchlist table\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      out += `- total: ${ref.total}\n\n`;

      const rows = Array.isArray(ref.items) ? ref.items.slice(0, 50) : [];
      out += `Rows (first ${rows.length}):\n`;
      for (const r of rows) {
        const parts: string[] = [];
        parts.push(`symbol=${r.symbol}`);
        if (r.name) parts.push(`name=${String(r.name).replaceAll('\n', ' ')}`);
        if (r.asOfDate) parts.push(`asOf=${r.asOfDate}`);
        if (typeof r.close === 'number') parts.push(`close=${r.close}`);
        if (typeof r.score === 'number') parts.push(`score=${r.score}`);
        if (typeof r.trendOk === 'boolean') parts.push(`trendOk=${r.trendOk ? 'true' : 'false'}`);
        if (typeof r.stopLossPrice === 'number') parts.push(`stopLoss=${r.stopLossPrice}`);
        if (r.buyMode) parts.push(`buyMode=${r.buyMode}`);
        if (r.buyAction) parts.push(`buyAction=${r.buyAction}`);
        if (typeof r.buyZoneLow === 'number' || typeof r.buyZoneHigh === 'number') {
          parts.push(`buyZone=${r.buyZoneLow ?? '—'}..${r.buyZoneHigh ?? '—'}`);
        }
        out += `- ${parts.join(' ; ')}\n`;
      }
      out += `\n`;
      continue;
    }

    if (ref.kind === 'broker') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/broker/${encodeURIComponent(ref.broker)}/snapshots/${encodeURIComponent(ref.snapshotId)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load broker snapshot');
        const snap = (await resp.json()) as BrokerSnapshotDetail;

        out += `## Broker: ${ref.accountTitle}\n`;
        out += `- broker: ${snap.broker}\n`;
        if (snap.accountId) out += `- accountId: ${snap.accountId}\n`;
        out += `- kind: ${snap.kind}\n`;
        out += `- capturedAt: ${snap.capturedAt}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        const extracted = isRecord(snap.extracted) ? snap.extracted : {};
        const kindVal = extracted['kind'];
        const kind = typeof kindVal === 'string' ? kindVal : String(snap.kind || 'unknown');
        const data = asRecord(extracted['data']) ?? {};

        if (kind === 'account_overview') {
          out += `\nAccount overview:\n`;
          for (const k of [
            'currency',
            'totalAssets',
            'securitiesValue',
            'cashAvailable',
            'withdrawable',
            'pnlTotal',
            'pnlToday',
            'accountIdMasked',
          ]) {
            if (data[k] != null) out += `- ${k}: ${String(data[k])}\n`;
          }
          out += `\n`;
        } else if (kind === 'positions' && Array.isArray(data['positions'])) {
          const rows = asArray(data['positions']).slice(0, 30);
          out += `\nPositions (first ${rows.length}):\n`;
          for (const it of rows) {
            const p = asRecord(it) ?? {};
            const ticker = getStr(p, 'ticker');
            const name = getStr(p, 'name');
            const qty = getStr(p, 'qtyHeld') || getStr(p, 'qty');
            const price = getStr(p, 'price');
            const cost = getStr(p, 'cost');
            const pnl = getStr(p, 'pnl');
            const pnlPct = getStr(p, 'pnlPct');
            out += `- ${ticker} ${name} qty=${qty} price=${price} cost=${cost} pnl=${pnl} pnlPct=${pnlPct}\n`;
          }
          out += `\n`;
        } else if (kind === 'conditional_orders' && Array.isArray(data['orders'])) {
          const rows = asArray(data['orders']).slice(0, 30);
          out += `\nConditional orders (first ${rows.length}):\n`;
          for (const it of rows) {
            const o = asRecord(it) ?? {};
            out +=
              `- ${getStr(o, 'ticker')} ${getStr(o, 'name')} side=${getStr(o, 'side')} ` +
              `trigger=${getStr(o, 'triggerCondition')} ${getStr(o, 'triggerValue')} ` +
              `qty=${getStr(o, 'qty')} status=${getStr(o, 'status')} validUntil=${getStr(o, 'validUntil')}\n`;
          }
          out += `\n`;
        } else {
          out += `\nExtracted JSON:\n`;
          out += `${JSON.stringify(extracted, null, 2)}\n\n`;
        }
      } catch {
        out += `## Broker: ${ref.accountTitle}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- status: failed to load snapshot\n\n`;
      }
      continue;
    }

    if (ref.kind === 'brokerState') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/broker/${encodeURIComponent(ref.broker)}/accounts/${encodeURIComponent(ref.accountId)}/state`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load broker account state');
        const st = (await resp.json()) as BrokerAccountState;
        out += `## Broker account state: ${ref.accountTitle}\n`;
        out += `- broker: ${st.broker}\n`;
        out += `- accountId: ${st.accountId}\n`;
        out += `- updatedAt: ${st.updatedAt}\n`;
        const counts = st.counts || {};
        out += `- counts: positions=${counts.positions ?? st.positions?.length ?? 0}, conditionalOrders=${counts.conditionalOrders ?? st.conditionalOrders?.length ?? 0}, trades=${counts.trades ?? st.trades?.length ?? 0}\n`;

        const ov = st.overview || {};
        if (Object.keys(ov).length) {
          out += `\nAccount overview:\n`;
          for (const k of Object.keys(ov)) {
            out += `- ${k}: ${String(ov[k])}\n`;
          }
        }

        const ps = (st.positions || []).slice(0, 40);
        if (ps.length) {
          out += `\nPositions (first ${ps.length}):\n`;
          for (const p of ps) {
            out +=
              `- ${getStr(p, 'ticker')} ${getStr(p, 'name')} qty=${getStr(p, 'qtyHeld') || getStr(p, 'qty')} ` +
              `price=${getStr(p, 'price')} cost=${getStr(p, 'cost')} pnl=${getStr(p, 'pnl')} pnlPct=${getStr(p, 'pnlPct')}\n`;
          }
        }

        const os = (st.conditionalOrders || []).slice(0, 40);
        if (os.length) {
          out += `\nConditional orders (first ${os.length}):\n`;
          for (const o of os) {
            out +=
              `- ${getStr(o, 'ticker')} ${getStr(o, 'name')} side=${getStr(o, 'side')} ` +
              `trigger=${getStr(o, 'triggerCondition')} ${getStr(o, 'triggerValue')} ` +
              `qty=${getStr(o, 'qty')} status=${getStr(o, 'status')} validUntil=${getStr(o, 'validUntil')}\n`;
          }
        }

        const ts = (st.trades || []).slice(0, 60);
        if (ts.length) {
          out += `\nTrades (first ${ts.length}):\n`;
          for (const t of ts) {
            const when = getStr(t, 'time') || getStr(t, 'date');
            out +=
              `- ${when} ${getStr(t, 'side')} ${getStr(t, 'ticker')} ${getStr(t, 'name')} ` +
              `qty=${getStr(t, 'qty')} price=${getStr(t, 'price')}\n`;
          }
        }

        out += `\n`;
      } catch {
        out += `## Broker account state: ${ref.accountTitle}\n`;
        out += `- status: failed to load state\n\n`;
      }
      continue;
    }

    if (ref.kind === 'strategyReport') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/strategy/accounts/${encodeURIComponent(ref.accountId)}/daily?date=${encodeURIComponent(ref.date)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load strategy report');
        const repRaw = (await resp.json()) as unknown;
        const rep = asRecord(repRaw) ?? {};
        out += `## Strategy report: ${ref.accountTitle}\n`;
        out += `- date: ${String(rep['date'] ?? ref.date)}\n`;
        out += `- model: ${String(rep['model'] ?? '')}\n`;
        out += `- createdAt: ${String(rep['createdAt'] ?? ref.createdAt)}\n`;
        if (typeof rep['markdown'] === 'string' && rep['markdown'].trim()) {
          out += `\nMarkdown report:\n`;
          out += `${String(rep['markdown']).trim()}\n\n`;
          continue;
        }
        const leader = asRecord(rep['leader']);
        if (leader) {
          out += `\nLeader:\n`;
          out += `- symbol: ${String(leader['symbol'] ?? '')}\n`;
          out += `- reason: ${String(leader['reason'] ?? '')}\n`;
        }
        const cands = asArray(rep['candidates']).slice(0, 5);
        if (cands.length) {
          out += `\nCandidates (first ${cands.length}):\n`;
          for (const it of cands) {
            const c = asRecord(it) ?? {};
            out +=
              `- #${getStr(c, 'rank')} ${getStr(c, 'ticker')} ${getStr(c, 'name')} ` +
              `score=${getStr(c, 'score')} why=${getStr(c, 'why')}\n`;
          }
        }
        const recs = asArray(rep['recommendations']).slice(0, 3);
        if (recs.length) {
          out += `\nRecommendations (first ${recs.length}):\n`;
          for (const it of recs) {
            const r = asRecord(it) ?? {};
            out += `- ${getStr(r, 'ticker')} ${getStr(r, 'name')} thesis=${getStr(r, 'thesis')}\n`;
            const orders = asArray(r['orders']).slice(0, 8);
            if (orders.length) {
              out += `  Orders:\n`;
              for (const it2 of orders) {
                const o = asRecord(it2) ?? {};
                out +=
                  `  - ${getStr(o, 'kind')} ${getStr(o, 'side')} trigger=${getStr(o, 'trigger')} ` +
                  `qty=${getStr(o, 'qty')} tif=${getStr(o, 'timeInForce')}\n`;
              }
            }
          }
        }
        out += `\n`;
      } catch {
        out += `## Strategy report: ${ref.accountTitle}\n`;
        out += `- date: ${ref.date}\n`;
        out += `- status: failed to load report\n\n`;
      }
      continue;
    }

    if (ref.kind === 'industryFundFlow') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/market/cn/industry-fund-flow?days=${encodeURIComponent(String(ref.days))}&topN=${encodeURIComponent(String(ref.topN))}&asOfDate=${encodeURIComponent(ref.asOfDate)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load industry fund flow');
        const ffRaw = (await resp.json()) as unknown;
        const ff = asRecord(ffRaw) ?? {};
        const title = String(ref.title ?? 'CN industry fund flow');
        out += `## ${title}\n`;
        out += `- asOfDate: ${String(ff['asOfDate'] ?? ref.asOfDate)}\n`;
        out += `- days: ${String(ff['days'] ?? ref.days)}\n`;
        out += `- topN: ${String(ref.topN)}\n`;
        if (ref.metric) out += `- metric: ${ref.metric}\n`;
        if (ref.windowDays) out += `- windowDays: ${ref.windowDays}\n`;
        if (ref.direction) out += `- direction: ${ref.direction}\n`;
        if (ref.view) out += `- view: ${ref.view}\n`;

        const items = asArray(ff['top']);
        const view = ref.view ?? 'rankedList';
        const windowDays = Math.max(1, Math.min(Number(ref.windowDays ?? ref.days ?? 10), 30));
        const metric = ref.metric ?? 'netInflow';
        function sumLastN(series: unknown[], n: number): number {
          const xs = Array.isArray(series) ? series : [];
          const tail = xs.slice(-n);
          let s = 0;
          for (const it of tail) {
            const p = asRecord(it) ?? {};
            s += getNum(p, 'netInflow', 0);
          }
          return s;
        }

        if (view === 'dailyTopByDate') {
          const dates = asArray(ff['dates']).map((d) => String(d));
          const rawShown = dates.slice(-Math.max(1, Math.min(Number(ref.days ?? 10), 30)));
          const topK = Math.max(1, Math.min(Number(ref.topN ?? 5), 20));
          const shown: string[] = [];
          let collapsed = 0;
          let prevSig = '';
          for (const d of rawShown) {
            const scored = items
              .map((it) => {
                const r = asRecord(it) ?? {};
                const series = asArray(r['series10d']);
                const p0 = series.find((x) => getStr(asRecord(x) ?? {}, 'date') === String(d));
                const p = asRecord(p0) ?? {};
                const v = getNum(p, 'netInflow', 0);
                return { name: getStr(r, 'industryName'), v };
              })
              .sort((a, b) => b.v - a.v)
              .slice(0, topK)
              .map((x) => x.name)
              .filter(Boolean);
            const sig = scored.join('|');
            if (sig && sig === prevSig) {
              collapsed += 1;
              continue;
            }
            shown.push(d);
            prevSig = sig;
          }
          out += `\nDaily top inflow by date:\n`;
          if (collapsed) {
            out += `- note: collapsed ${collapsed} duplicate non-trading snapshot${collapsed > 1 ? 's' : ''}\n`;
          }
          for (const d of shown) {
            const scored = items
              .map((it) => {
                const r = asRecord(it) ?? {};
                const series = asArray(r['series10d']);
                const p0 = series.find((x) => getStr(asRecord(x) ?? {}, 'date') === String(d));
                const p = asRecord(p0) ?? {};
                const v = getNum(p, 'netInflow', 0);
                return { name: getStr(r, 'industryName'), v };
              })
              .sort((a, b) => b.v - a.v)
              .slice(0, topK)
              .map((x) => x.name)
              .filter(Boolean);
            out += `- ${String(d)}: ${scored.join(' / ')}\n`;
          }
        } else {
          type ScoredIndustry = { r: Record<string, unknown>; score: number };
          const scored: ScoredIndustry[] = items.map((it) => {
            const r = asRecord(it) ?? {};
            const net = getNum(r, 'netInflow', 0);
            const sum = metric === 'sum' ? sumLastN(asArray(r['series10d']), windowDays) : net;
            return { r, score: sum };
          });
          const dir = ref.direction ?? 'in';
          scored.sort((a: ScoredIndustry, b: ScoredIndustry) =>
            dir === 'out' ? a.score - b.score : b.score - a.score,
          );
          const top = scored.slice(0, ref.topN).map((x: ScoredIndustry) => x.r);
          if (top.length) {
            out += `\nTop industries:\n`;
            for (const r of top) {
              out += `- ${getStr(r, 'industryName')} netInflow=${getStr(r, 'netInflow')} sum10d=${getStr(r, 'sum10d')}\n`;
              const series = asArray(r['series10d']).slice(0, 10);
              if (series.length) {
                out += `  series10d:\n`;
                for (const it of series) {
                  const p = asRecord(it) ?? {};
                  out += `  - ${getStr(p, 'date')}: ${getStr(p, 'netInflow')}\n`;
                }
              }
            }
          }
        }
        out += `\n`;
      } catch {
        out += `## ${ref.title || 'CN industry fund flow'}\n`;
        out += `- asOfDate: ${ref.asOfDate}\n`;
        out += `- status: failed to load\n\n`;
      }
      continue;
    }

    if (ref.kind === 'leaderStocks') {
      try {
        // Do NOT force refresh here (avoid triggering live score updates from chat reference).
        const resp = await fetch(
          `${QUANT_BASE_URL}/leader?days=${encodeURIComponent(String(ref.days))}&force=false`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load leader stocks');
        const ls = (await resp.json()) as LeaderStocksList;
        out += `## Leader stocks (last ${String(ls.days ?? ref.days)} trading days)\n`;
        out += `- days: ${String(ls.days ?? ref.days)}\n`;
        out += `- dates: ${(Array.isArray(ls.dates) ? ls.dates : []).join(', ')}\n\n`;

        const leaders = Array.isArray(ls.leaders) ? ls.leaders : [];
        if (leaders.length) {
          out += `| Date | Ticker | Name | LiveScore | Dur(d) | BuyZone | Target | P | Why |\n`;
          out += `|---|---|---|---:|---:|---|---|---:|---|\n`;
          for (const r of leaders) {
            const date = String(r.date ?? '');
            const ticker = String(r.ticker ?? r.symbol ?? '');
            const name = String(r.name ?? '');
            const liveScore = Number.isFinite(r.liveScore as number)
              ? String(Math.round(r.liveScore as number))
              : Number.isFinite(r.score as number)
                ? String(Math.round(r.score as number))
                : '—';
            const dur = Number.isFinite(r.expectedDurationDays as number)
              ? String(r.expectedDurationDays)
              : '—';
            const bz = r.buyZone ?? {};
            const bzLow = isRecord(bz) ? bz['low'] : null;
            const bzHigh = isRecord(bz) ? bz['high'] : null;
            const buyZone =
              bzLow != null && bzHigh != null ? `${String(bzLow)}-${String(bzHigh)}` : '—';
            const tp = r.targetPrice ?? {};
            const target = isRecord(tp) && tp['primary'] != null ? String(tp['primary']) : '—';
            const pNum = Number.isFinite(r.probability as number)
              ? Math.max(1, Math.min(5, Math.round(r.probability as number)))
              : null;
            const prob = pNum != null ? `${pNum * 20}%` : '—';
            const why =
              Array.isArray(r.whyBullets) && r.whyBullets.length
                ? r.whyBullets
                    .slice(0, 2)
                    .map((x) => String(x))
                    .join(' / ')
                : String(r.reason ?? '').replaceAll('\n', ' ');
            out += `| ${date} | ${ticker} | ${name} | ${liveScore} | ${dur} | ${buyZone} | ${target} | ${prob} | ${why} |\n`;
          }
          out += `\n`;

          // NOTE: Do not force refresh deep context from chat reference.
        }
      } catch {
        out += `## Leader stocks\n`;
        out += `- status: failed to load\n\n`;
      }
      continue;
    }

    if (ref.kind === 'marketSentiment') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/market/cn/sentiment?days=${encodeURIComponent(String(ref.days))}&asOfDate=${encodeURIComponent(String(ref.asOfDate))}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load market sentiment');
        const msRaw = (await resp.json()) as unknown;
        const ms = asRecord(msRaw) ?? {};
        const items = asArray(ms['items']);
        const latest = items.length ? items[items.length - 1] : null;
        const latestRec = asRecord(latest) ?? {};
        out += `## Market sentiment (CN A-share)\n`;
        out += `- asOfDate: ${String(ms['asOfDate'] ?? ref.asOfDate)}\n`;
        out += `- riskMode: ${String(latestRec['riskMode'] ?? '—')}\n`;
        const rules = asStringArray(latestRec['rules']);
        if (rules.length) {
          out += `- rules: ${rules.join(' | ')}\n`;
        }
        out += `\n`;

        out += `| Date | Up | Down | Flat | Ratio | YdayLimitUpPremium | FailedLimitUpRate | Risk |\n`;
        out += `|---|---:|---:|---:|---:|---:|---:|---|\n`;
        for (const it of items.slice(-ref.days)) {
          const row = asRecord(it) ?? {};
          const date = getStr(row, 'date');
          const up = getNum(row, 'upCount', 0);
          const down = getNum(row, 'downCount', 0);
          const flat = getNum(row, 'flatCount', 0);
          const ratio = Number.isFinite(Number(row['upDownRatio']))
            ? Number(row['upDownRatio']).toFixed(2)
            : '—';
          const prem = Number.isFinite(Number(row['yesterdayLimitUpPremium']))
            ? `${Number(row['yesterdayLimitUpPremium']).toFixed(2)}%`
            : '—';
          const failed = Number.isFinite(Number(row['failedLimitUpRate']))
            ? `${Number(row['failedLimitUpRate']).toFixed(1)}%`
            : '—';
          const risk = getStr(row, 'riskMode');
          out += `| ${date} | ${up} | ${down} | ${flat} | ${ratio} | ${prem} | ${failed} | ${risk} |\n`;
        }
        out += `\n`;
      } catch {
        out += `## Market sentiment (CN A-share)\n`;
        out += `- status: failed to load\n\n`;
      }
      continue;
    }

    // Rank/Quant endpoints removed - page cleanup
    if (ref.kind === 'rankList' || ref.kind === 'intradayRankList') {
      out += `## Rank/Quant feature removed\n`;
      out += `- This feature has been removed.\n\n`;
      continue;
    }

    if (ref.kind === 'journal') {
      out += `## Trading Journal: ${ref.title}\n`;
      out += `- journalId: ${ref.journalId}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      out += `\nContent:\n`;
      out += `${ref.content}\n\n`;
      continue;
    }

    if (ref.kind !== 'stock') {
      out += `## Unknown reference\n`;
      out += `- kind: ${String((ref as unknown as { kind?: unknown })?.kind ?? '')}\n\n`;
      continue;
    }

    // Stock reference
    try {
      const [barsResp, chipsResp, ffResp] = await Promise.all([
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/bars?days=${ref.barsDays}`,
          { cache: 'no-store' },
        ),
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/chips?days=${ref.chipsDays}`,
          { cache: 'no-store' },
        ).catch(() => null),
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/fund-flow?days=${ref.fundFlowDays}`,
          { cache: 'no-store' },
        ).catch(() => null),
      ]);

      if (!barsResp.ok) throw new Error('failed to load stock bars');
      const snap = (await barsResp.json()) as StockBarsDetail;
      out += `## Stock: ${snap.ticker} ${snap.name}\n`;
      out += `- symbol: ${snap.symbol}\n`;
      out += `- market: ${snap.market}\n`;
      out += `- currency: ${snap.currency}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;

      const bars = snap.bars.slice(-ref.barsDays);
      out += `\nBars (last ${bars.length}):\n`;
      for (const b of bars) {
        out += `- ${b.date} O=${b.open} H=${b.high} L=${b.low} C=${b.close} V=${b.volume} A=${b.amount}\n`;
      }

      if (chipsResp && chipsResp.ok) {
        const chips = (await chipsResp.json()) as StockChipsDetail;
        const items = (chips.items || []).slice(-ref.chipsDays);
        if (items.length) {
          out += `\nChips (last ${items.length}):\n`;
          for (const it of items) {
            out +=
              `- ${it.date} profitRatio=${it.profitRatio} avgCost=${it.avgCost} ` +
              `70%=[${it.cost70Low},${it.cost70High}] conc70=${it.cost70Conc} ` +
              `90%=[${it.cost90Low},${it.cost90High}] conc90=${it.cost90Conc}\n`;
          }
        }
      }

      if (ffResp && ffResp.ok) {
        const ff = (await ffResp.json()) as StockFundFlowDetail;
        const items = (ff.items || []).slice(-ref.fundFlowDays);
        if (items.length) {
          out += `\nFund flow (last ${items.length}):\n`;
          for (const it of items) {
            out +=
              `- ${it.date} close=${it.close} chg=${it.changePct}% ` +
              `main=${it.mainNetAmount}(${it.mainNetRatio}%) ` +
              `super=${it.superNetAmount}(${it.superNetRatio}%) ` +
              `large=${it.largeNetAmount}(${it.largeNetRatio}%) ` +
              `medium=${it.mediumNetAmount}(${it.mediumNetRatio}%) ` +
              `small=${it.smallNetAmount}(${it.smallNetRatio}%)\n`;
          }
        }
      }

      out += `\n`;
    } catch {
      out += `## Stock: ${ref.ticker} ${ref.name}\n`;
      out += `- symbol: ${ref.symbol}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      out += `- status: failed to load bars\n\n`;
    }
  }
  return out;
}

export function ChatPanel() {
  const {
    activeSession,
    createSession,
    appendMessages,
    updateMessageContent,
    state,
    renameSession,
    removeReference,
    clearReferences,
  } = useChatStore();
  const scrollerRef = React.useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = React.useRef(true);

  React.useEffect(() => {
    if (!activeSession) {
      createSession();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const messages: ChatMessage[] = activeSession?.messages ?? [];
  const lastMessageId = messages[messages.length - 1]?.id ?? '';
  const lastMessageLen = messages[messages.length - 1]?.content?.length ?? 0;

  React.useEffect(() => {
    const el = scrollerRef.current;
    if (!el) return;
    if (!stickToBottomRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [messages.length, lastMessageId, lastMessageLen]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div
        ref={scrollerRef}
        className="min-h-0 flex-1 overflow-auto"
        onScroll={() => {
          const el = scrollerRef.current;
          if (!el) return;
          const distanceToBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
          stickToBottomRef.current = distanceToBottom < 80;
        }}
      >
        <ChatMessageList messages={messages} />
      </div>

      <ChatComposer
        references={state.references}
        onRemoveReference={removeReference}
        onClearReferences={clearReferences}
        onSend={(text: string, attachments: ChatAttachment[]) => {
          if (!activeSession) return;
          const shouldInferTitle =
            activeSession.title === 'New chat' && messages.every((m) => m.role !== 'user');
          const now = new Date().toISOString();
          const userMessage: ChatMessage = {
            id: newId(),
            role: 'user',
            content: text,
            createdAt: now,
            attachments,
          };
          const assistantId = newId();
          const assistantMessage: ChatMessage = {
            id: assistantId,
            role: 'assistant',
            content: '',
            createdAt: now,
          };

          appendMessages(activeSession.id, [userMessage, assistantMessage]);

          const sp = state.settings.systemPrompt.trim();
          const baseMessages = [...messages, userMessage]
            .filter((m) => m.role !== 'system')
            .map((m) => ({
              role: m.role,
              content: m.content,
              attachments: m.attachments,
            }));
          const refs = state.references;

          (async () => {
            try {
              if (shouldInferTitle) {
                try {
                  const t = await fetch(`${AI_BASE_URL}/title`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({
                      text,
                      systemPrompt: state.settings.systemPrompt,
                    }),
                  });
                  if (t.ok) {
                    const data = (await t.json()) as { title?: string };
                    const title = typeof data.title === 'string' ? data.title.trim() : '';
                    if (title) {
                      renameSession(activeSession.id, title);
                    }
                  }
                } catch {
                  // ignore
                }
              }

              const referenceText = refs.length > 0 ? await buildReferenceBlock(refs) : '';
              const payload = {
                messages: [
                  ...(sp ? [{ role: 'system' as const, content: sp }] : []),
                  ...(referenceText ? [{ role: 'system' as const, content: referenceText }] : []),
                  ...baseMessages,
                ],
              };

              const resp = await fetch(`${AI_BASE_URL}/chat`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify(payload),
              });

              if (!resp.ok || !resp.body) {
                const msg = await resp.text().catch(() => '');
                updateMessageContent(
                  activeSession.id,
                  assistantId,
                  `**Error**: AI service failed (${resp.status}).\n\n${msg}`,
                );
                return;
              }

              const reader = resp.body.getReader();
              const decoder = new TextDecoder();
              let acc = '';
              while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                acc += decoder.decode(value, { stream: true });
                updateMessageContent(activeSession.id, assistantId, acc);
              }
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              updateMessageContent(activeSession.id, assistantId, `**Error**: ${message}`);
            }
          })();
        }}
      />
    </div>
  );
}
