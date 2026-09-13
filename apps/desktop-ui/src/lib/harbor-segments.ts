import type { TimelineRow } from '@/lib/queries/backtest';

/**
 * Auto-segmented Harbor holding blocks for the Timeline bar.
 * Consecutive days with the same pick (STOCK basket phase or one ETF) merge into
 * a single block whose width = days, so the bar reads as phases instead of
 * hundreds of hairline cells.
 */
export type HarborSegment = {
  ident: string;
  /** Effective portfolio state: stock core, stock+parking, or parking only. */
  mode: 'STOCK' | 'MIXED' | 'PARK';
  pick: string;
  start: string;
  end: string;
  days: number;
  positions: number;
  /** Idle share at the segment end (parking applies to this part). */
  idlePct: number;
  /** ETF code without exchange suffix (e.g. 513100); null for STOCK/REPO. */
  code: string | null;
  /** Symbols held on the last day of the segment (STOCK phases). */
  stockSymbols: string[];
  /** Sells that happened during the segment. */
  exits: string[];
  /** Cumulative Harbor NAV% at the segment end. */
  navPct: number;
  /** Segment return vs the previous segment end; null for the first segment. */
  retPct: number | null;
  labels: { full: string; short: string; medium: string; tiny: string };
  title: string;
};

const PICK_META: Record<string, { name: string; tiny: string; code: string | null }> = {
  STOCK: { name: '股票', tiny: '股', code: null },
  GOLD: { name: '黄金', tiny: '金', code: '518880' },
  OIL: { name: '原油', tiny: '油', code: '513350' },
  NASDAQ: { name: '纳指', tiny: '纳', code: '513100' },
  BOND10: { name: '国债', tiny: '债', code: '511260' },
  REPO: { name: '逆回购', tiny: '回', code: 'GC001' },
};

function shortCode(ts: string | null | undefined): string | null {
  if (!ts) return null;
  return ts.split('.')[0] || null;
}

/** One-line "what the portfolio held that day" for tooltips. */
export function harborHoldLine(r: TimelineRow): string {
  const pick = r.pick ?? 'REPO';
  const meta = PICK_META[pick] ?? { name: pick, tiny: pick.slice(0, 1), code: null };
  const code = shortCode(r.pickTs) ?? meta.code;
  const pos = r.positions ?? 0;
  if (pos > 0) {
    const syms = (r.stockSymbols ?? []).slice(0, 4).join(' ');
    const park =
      (r.idlePct ?? 0) > 0 && pick !== 'REPO' ? `+停车 ${meta.name}${code ? ` ${code}` : ''}` : '';
    return `股票 ${pos}票${syms ? ` ${syms}` : ''}${park ? ` ${park}` : ''}`;
  }
  return `${meta.name}${code ? ` ${code}` : ''}`;
}

function pickNav(r: TimelineRow): number {
  return (
    r.navSimReturnPct ?? r.navSingleReturnPct ?? r.navMultiReturnPct ?? r.navBaseReturnPct ?? 0
  );
}

function fmtSignedPct(v: number): string {
  return `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
}

function stockHoldLine(counts: Map<string, number>): string {
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([sym, days]) => (counts.size > 1 ? `${sym}×${days}` : sym))
    .join(' ');
}

function buildLabels(
  mode: HarborSegment['mode'],
  pick: string,
  days: number,
  positions: number,
  code: string | null,
  retPct: number | null,
): HarborSegment['labels'] {
  const meta = PICK_META[pick] ?? { name: pick, tiny: pick.slice(0, 1), code: null };
  const sym = code ?? meta.code;
  const ret = retPct != null ? ` ${fmtSignedPct(retPct)}` : '';
  if (mode === 'PARK') {
    return {
      full: `${meta.name}${sym ? ` ${sym}` : ''} · ${days}天${ret}`,
      short: `${meta.name}${sym ? ` ${sym}` : ''}`,
      medium: sym ?? meta.name,
      tiny: meta.tiny,
    };
  }
  const park = `${meta.name}${sym ? ` ${sym}` : ''}`;
  if (mode === 'MIXED') {
    return {
      full: `股票 ${positions}票+${park} · ${days}天${ret}`,
      short: `股票 ${positions}票+${meta.name}`,
      medium: `股${positions}+${meta.tiny}`,
      tiny: '股',
    };
  }
  return {
    full: `股票 ${positions}票 · ${days}天${ret}`,
    short: `股票 ${positions}票`,
    medium: `股${positions}`,
    tiny: '股',
  };
}

function segmentMode(r: TimelineRow): HarborSegment['mode'] {
  if ((r.positions ?? 0) <= 0) return 'PARK';
  return (r.idlePct ?? 0) >= 50 ? 'MIXED' : 'STOCK';
}

export function buildHarborSegments(rows: TimelineRow[]): HarborSegment[] {
  const out: HarborSegment[] = [];
  let prevNavEnd: number | null = null;
  let current: HarborSegment | null = null;
  let counts = new Map<string, number>();

  const flush = () => {
    if (!current) return;
    current.title = [
      `${current.start}~${current.end} · ${current.days}天`,
      current.mode === 'PARK'
        ? `${PICK_META[current.pick]?.name ?? current.pick}${current.code ? ` ${current.code}` : ''}`
        : `股票 ${current.positions}票${counts.size ? ` · 持有(${stockHoldLine(counts)})` : ''}${
            current.mode === 'MIXED'
              ? ` · 闲置${current.idlePct}%停 ${PICK_META[current.pick]?.name ?? current.pick}${
                  current.code ? ` ${current.code}` : ''
                }`
              : ''
          }`,
      current.retPct != null ? `段收益 ${fmtSignedPct(current.retPct)}` : null,
      `港湾 ${current.navPct.toFixed(2)}%`,
      current.exits.length ? `卖出 ${current.exits.join(' ')}` : null,
    ]
      .filter(Boolean)
      .join(' · ');
    prevNavEnd = current.navPct;
    out.push(current);
    current = null;
  };

  for (const r of rows) {
    const pick = r.pick ?? 'REPO';
    const mode = segmentMode(r);
    const code = mode === 'STOCK' ? null : (shortCode(r.pickTs) ?? PICK_META[pick]?.code ?? null);
    const ident = mode === 'STOCK' ? 'STOCK' : `${mode}:${pick}:${code ?? ''}`;
    const nav = pickNav(r);
    if (!current || current.ident !== ident) {
      flush();
      const retPct = prevNavEnd == null ? null : nav - prevNavEnd;
      current = {
        ident,
        mode,
        pick,
        start: r.date,
        end: r.date,
        days: 1,
        positions: r.positions ?? 0,
        idlePct: r.idlePct ?? 0,
        code,
        stockSymbols: [...(r.stockSymbols ?? [])],
        exits: [...(r.exits ?? [])],
        navPct: nav,
        retPct,
        labels: buildLabels(mode, pick, 1, r.positions ?? 0, code, retPct),
        title: '',
      };
      counts = new Map();
    } else {
      current.end = r.date;
      current.days += 1;
      current.positions = r.positions ?? current.positions;
      current.idlePct = r.idlePct ?? current.idlePct;
      current.navPct = nav;
      current.stockSymbols = [...(r.stockSymbols ?? [])];
      if (r.exits?.length) current.exits.push(...r.exits);
      current.labels = buildLabels(
        mode,
        pick,
        current.days,
        current.positions,
        code,
        current.retPct,
      );
    }
    if (mode !== 'PARK') {
      for (const s of r.stockSymbols ?? []) counts.set(s, (counts.get(s) ?? 0) + 1);
    }
  }
  flush();
  return out;
}

/** Rough rendered width of a 9px label (CJK ~9px, latin ~5px) plus padding. */
export function estimateLabelPx(text: string): number {
  let w = 0;
  for (const ch of text) {
    w += ch.codePointAt(0)! > 0x2e80 ? 9 : 5.2;
  }
  return w + 6;
}

/** Pick the longest label variant that fits the block width; '' when too narrow. */
export function fitSegmentLabel(labels: HarborSegment['labels'], widthPx: number): string {
  for (const variant of [labels.full, labels.short, labels.medium, labels.tiny]) {
    if (variant && estimateLabelPx(variant) <= widthPx) return variant;
  }
  return '';
}
