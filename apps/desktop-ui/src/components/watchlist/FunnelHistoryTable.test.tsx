import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderToString } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { FunnelHistoryTable, toFunnelRow } from './FunnelHistoryTable';

const runWithFunnel = (overrides: Record<string, unknown> = {}) => ({
  runId: 'run-1',
  tradeDate: '2026-08-05',
  trigger: 'scheduled',
  skipped: false,
  meta: {
    funnel: {
      tvHit: 10,
      passPullback: 5,
      passTrendOk: 4,
      addedNew: 2,
      fallbackUsed: false,
      fallbackHit: 0,
      fallbackTrendOk: 0,
      fallbackAdded: 0,
    },
  },
  ...overrides,
});

function renderTable() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return renderToString(
    <QueryClientProvider client={client}>
      <FunnelHistoryTable limit={10} />
    </QueryClientProvider>,
  );
}

describe('toFunnelRow', () => {
  it('computes conversion pct from tvHit when present', () => {
    const row = toFunnelRow(runWithFunnel());
    expect(row).not.toBeNull();
    expect(row!.conversionPct).toBe(40); // 4/10
    expect(row!.fallbackUsed).toBe(false);
  });

  it('uses fallback ratio when primary pool was empty', () => {
    const row = toFunnelRow(
      runWithFunnel({
        meta: {
          funnel: {
            tvHit: 0,
            passPullback: 0,
            passTrendOk: 0,
            addedNew: 1,
            fallbackUsed: true,
            fallbackHit: 40,
            fallbackTrendOk: 6,
            fallbackAdded: 1,
          },
        },
      }),
    );
    expect(row).not.toBeNull();
    expect(row!.fallbackUsed).toBe(true);
    expect(row!.conversionPct).toBe(15); // 6/40
  });

  it('returns null when meta has no funnel', () => {
    expect(toFunnelRow({ runId: 'x', meta: {} })).toBeNull();
  });

  it('returns null conversion when no hits at all', () => {
    const row = toFunnelRow(
      runWithFunnel({
        meta: { funnel: { tvHit: 0, passPullback: 0, passTrendOk: 0, addedNew: 0 } },
      }),
    );
    expect(row).not.toBeNull();
    expect(row!.conversionPct).toBeNull();
  });
});

describe('FunnelHistoryTable', () => {
  it('renders the section title and loading state on first frame', () => {
    const html = renderTable();
    expect(html).toContain('Funnel History');
    expect(html).toContain('Loading');
  });
});
