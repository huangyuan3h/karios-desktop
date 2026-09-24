import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BacktestPage } from './BacktestPage';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <BacktestPage />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  // Timeline default follows the 默认策略 setting; these tests exercise the harbor UI.
  window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('harbor'));
  apiGetJson.mockReset();
  apiGetJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/api/backtest/overview')) {
      return {
        ok: true,
        cnBaseline: {
          generatedAt: '2026-08-12T02:25:19Z',
          windows: {
            OOS2: {
              totalNetPnlPct: 112.654,
              winRate: 0.48,
              sharpe: 5.22,
              trades: null,
              maxDrawdownPct: 23.346,
            },
            train: {
              totalNetPnlPct: 76.734,
              winRate: 0.435,
              sharpe: 3.31,
              trades: null,
              maxDrawdownPct: 16.637,
            },
            valid: {
              totalNetPnlPct: 88.212,
              winRate: 0.613,
              sharpe: 8.8,
              trades: null,
              maxDrawdownPct: 11.778,
            },
          },
        },
        hkBaseline: {
          generatedAt: '2026-08-10T13:35:04Z',
          windows: {
            OOS2: {
              totalNetPnlPct: 267.987,
              winRate: 0.39,
              sharpe: 2.21,
              trades: null,
              maxDrawdownPct: 29.719,
            },
            train: {
              totalNetPnlPct: 26.855,
              winRate: 0.414,
              sharpe: 1.91,
              trades: null,
              maxDrawdownPct: 18.863,
            },
            valid: {
              totalNetPnlPct: 60.647,
              winRate: 0.417,
              sharpe: 6.32,
              trades: null,
              maxDrawdownPct: 8.329,
            },
          },
        },
        rollingOos: {
          windowStart: '2026-05-13',
          windowEnd: '2026-08-11',
          warning: true,
          warnings: ['HK: -8.5% dd=19.5% sharpe=-3.2 trades=55'],
          markets: {
            CN: {
              closed: 1,
              winRate: 0.0,
              totalNetPnlPct: -0.633,
              maxDrawdownPct: 0.633,
              sharpe: null,
            },
            HK: {
              closed: 55,
              winRate: 0.255,
              totalNetPnlPct: -8.451,
              maxDrawdownPct: 19.497,
              sharpe: -3.2,
            },
          },
        },
        longWindowCN: {
          window: '2021-08-01 ~ 2026-08-11',
          totalNetPnlPct: 250.8,
          maxDrawdownPct: 40.9,
          sharpe: 2.65,
          trades: 1401,
          byYear: { 2021: 341, 2022: 93, 2023: -263, 2024: 606, 2025: 956, 2026: 1325 },
        },
      };
    }
    if (String(path).includes('/api/backtest/recon/latest')) {
      return {
        ok: true,
        items: [
          {
            reconDate: '2026-08-07',
            market: 'HK',
            window: 'valid',
            expected: 19,
            actual: 0,
            aligned: 0,
            missing: 19,
            extra: 0,
          },
          {
            reconDate: '2026-08-07',
            market: 'CN',
            window: 'valid',
            expected: 0,
            actual: 0,
            aligned: 0,
            missing: 0,
            extra: 0,
          },
        ],
      };
    }
    if (String(path).includes('/api/backtest/core-audit')) {
      return {
        ok: true,
        day: '2026-09-01',
        gate: { regime: '—', panicActive: false, gateOpen: false },
        holdings: [],
        counts: { ok: 0, warn: 0, violation: 0 },
      };
    }
    if (String(path).includes('/api/backtest/paper-vs-backtest')) {
      return {
        ok: true,
        report: {
          generatedAt: '2026-08-12',
          sampleCount: 2,
          verdict: '样本 <20 笔：结论待积累（C4 未定案）',
          rows: [
            {
              symbol: 'HK:00622',
              market: 'HK',
              entryDate: '2026-08-10',
              paper: { pnlPct: 2.12, closeReason: 'trailing_stop' },
              backtest: { pnlPct: 2.02, closeReason: 'end_of_window' },
              diff: { entryPriceDiffPct: 0 },
              note: '存在差异',
            },
          ],
          summary: {
            paper: { closed: 2, winRate: 0.5, avgPnlPct: -1.0 },
            backtestMatched: { closed: 1, winRate: 1.0, avgPnlPct: 2.02 },
          },
        },
      };
    }
    if (String(path).includes('/api/backtest/strategy-catalog')) {
      const windows = {
        OOS2: { total: 55.2, cagr: 58, mdd: -14.3, sharpe: 1.73 },
        train: { total: 52.2, cagr: 138.2, mdd: -8, sharpe: 3.01 },
        valid: { total: 50.3, cagr: 156.6, mdd: -21.8, sharpe: 2.14 },
        long: { total: 201.5, cagr: 25.7, mdd: -22.8, sharpe: 1 },
      };
      const entry = (
        key: string,
        name: string,
        status: string,
        statusLabel: string,
        role: string,
        roleLabel: string,
        timelineStrategy?: string,
      ) => ({
        key,
        name,
        structure: `${name} structure`,
        status,
        statusLabel,
        role,
        roleLabel,
        timelineStrategy: timelineStrategy ?? key,
        doc: `docs/${key}.md`,
        tag: key,
        updated: '2026-09-14',
        windows,
        pros: [`${name} pro`],
        cons: [`${name} con`],
      });
      return {
        ok: true,
        strategies: [
          entry('starship', '星舰', 'aggressive_pending', '激进 · 前置未满', 'offense', '进攻'),
          entry('starport', '星港', 'product_candidate_increment', '产品候选增量', 'balanced', '均衡'),
          entry(
            'homeport',
            '母港',
            'product_candidate',
            '产品候选',
            'defense',
            '防守',
            'homeport_m30',
          ),
          entry('harbor', '港湾', 'live', 'Live · 日落', 'live', 'Live 底座'),
        ],
      };
    }
    if (String(path).includes('/api/backtest/timeline')) {
      const satelliteOnly = String(path).includes('strategy=starship');
      // NOTE: 'strategy=homeport_m30' contains 'strategy=homeport' — the M30
      // defensive tier intentionally shares the homeport risk-leg mock shape.
      const isHomeport = String(path).includes('strategy=homeport');
      const isStarport = String(path).includes('strategy=starport');
      const satLeg = satelliteOnly || isStarport;
      const rows = [
          {
            date: '2026-08-01',
            deployedPct: 100,
            idlePct: 0,
            positions: 10,
            cnPositions: 10,
            hkPositions: 0,
            stockMarket: 'A股',
            stockSymbols: ['600519', '000858'],
            stockMom: null,
            pick: 'NASDAQ',
            pickTs: '513100.SH',
            navBase: 1.01,
            navSleeve: null,
            navSingle: 1.05,
            navMulti: 1.05,
            navBaseReturnPct: 1,
            navSingleReturnPct: 5,
            navMultiReturnPct: 5,
            navSimReturnPct: 6,
            exits: [],
          },
          {
            date: '2026-08-04',
            deployedPct: 100,
            idlePct: 0,
            positions: 10,
            cnPositions: 10,
            hkPositions: 0,
            stockMarket: 'A股',
            stockSymbols: ['600519', '300750'],
            stockMom: null,
            pick: 'NASDAQ',
            pickTs: '513100.SH',
            navBase: 1.02,
            navSleeve: null,
            navSingle: 1.08,
            navMulti: 1.08,
            navBaseReturnPct: 2,
            navSingleReturnPct: 8,
            navMultiReturnPct: 8,
            navSimReturnPct: 9,
            exits: ['000858'],
          },
          {
            date: '2026-08-05',
            deployedPct: 0,
            idlePct: 100,
            positions: 0,
            cnPositions: 0,
            hkPositions: 0,
            stockMarket: '空仓',
            stockSymbols: [],
            stockMom: null,
            pick: 'OIL',
            pickTs: '513350.SH',
            navBase: 1.02,
            navSleeve: null,
            navSingle: 1.08,
            navMulti: 1.08,
            navBaseReturnPct: 2,
            navSingleReturnPct: 8,
            navMultiReturnPct: 8,
            navSimReturnPct: 9,
            exits: [],
          },
          {
            date: '2026-08-06',
            deployedPct: 0,
            idlePct: 100,
            positions: 0,
            cnPositions: 0,
            hkPositions: 0,
            stockMarket: '空仓',
            stockSymbols: [],
            stockMom: null,
            pick: 'REPO',
            pickTs: null,
            navBase: 1.02,
            navSleeve: null,
            navSingle: 1.08,
            navMulti: 1.08,
            navBaseReturnPct: 2,
            navSingleReturnPct: 8,
            navMultiReturnPct: 8,
            navSimReturnPct: 9,
            exits: [],
          },
        ];
      const satRows = rows.map((r, i) => ({
        ...r,
        // Standalone satellite rows (starship) carry no base-leg columns —
        // mirrors sgap_to_timeline_rows. Overlay rows (星港/双子星) keep the
        // base leg (positions/symbols/pick/baseline) like the real blend.
        ...(satelliteOnly
          ? {
              positions: 0,
              cnPositions: 0,
              hkPositions: 0,
              stockMarket: '空仓',
              stockSymbols: [],
              exits: [],
              navBaseReturnPct: undefined,
              pick: 'S-GAP',
              pickTs: '',
            }
          : {}),
        satNav: r.navSingle,
        satNavReturnPct: r.navSingleReturnPct,
        satActive: true,
        satPositions: i % 2 === 0 ? 4 : 2,
        satSlots: 4,
        filledToday: i % 2 === 0 ? 2 : 0,
        idleSlots: i % 2 === 0 ? 0 : 2,
        gateOpen: true,
        parkedNav: r.navSingle,
        parkedReturnPct: r.navSingleReturnPct,
        parkedWeight: i % 2 === 0 ? 1 : 0.5,
        // 08-01 建仓 GOLD → 08-04 换仓 OIL → 08-05 回撤转现金 → 08-06 再建 GOLD
        parkedPick: i === 0 ? 'GOLD' : i === 1 ? 'OIL' : i === 2 ? 'REPO' : 'GOLD',
        parkedTs: i === 0 || i === 3 ? '518880.SH' : i === 1 ? '513350.SH' : 'GC001',
        parkedSides: i === 1 ? 2 : 1,
        parkedRetPct: i === 2 ? -0.9 : 0.8,
        parkedTrail: i === 2,
        cashShare: i % 2 === 0 ? 1 : 0.5,
      }));
      const day = (i: number) =>
        new Date(Date.UTC(2026, 4, 1) + i * 86_400_000).toISOString().slice(0, 10);
      // 32 older fills (2026-05-01 .. 2026-06-01) + the recent ones below →
      // 34 flow rows; the "近 30" list must keep the NEWEST 30 (oldest 4 hidden).
      const olderFills = Array.from({ length: 32 }, (_, i) => ({
        kind: 'fill',
        date: day(i),
        ts: `T${String(i).padStart(4, '0')}.SZ`,
        amp: 2 + (i % 5),
        ampRank: 10 + i,
        entryDate: day(i),
        exitDate: day(i),
        pnlPct: 1,
        contribPct: 0.1,
        closeReason: 'body_exit',
      }));
      // 15 older parked buy/sell pairs (2025-12-01 ..) — older than the 5 recent
      // events; the "近 30" parked list must drop the OLDEST 5, not the newest.
      const olderParked = Array.from({ length: 15 }, (_, i) => {
        const d = new Date(Date.UTC(2025, 11, 1) + i * 86_400_000).toISOString().slice(0, 10);
        return [
          {
            date: d,
            kind: 'buy' as const,
            key: 'GOLD',
            name: '华安黄金ETF',
            ts: `P${String(i).padStart(4, '0')}.SH`,
            price: 4,
            reason: 'entry' as const,
          },
          {
            date: d,
            kind: 'sell' as const,
            key: 'GOLD',
            name: '华安黄金ETF',
            ts: `P${String(i).padStart(4, '0')}.SH`,
            price: 4.1,
            reason: 'rotate' as const,
          },
        ];
      }).flat();
      // Harbor-family rows carry the parking trade marker (08-05 OIL idle day builds).
      const baseRows = rows.map((r) =>
        r.date === '2026-08-05' ? { ...r, parkedSides: 1, parkedRetPct: 0.8 } : r,
      );
      const withRisk = isHomeport || isStarport;
      const riskUniverse = [
        { ts: '510300.SH', name: '沪深300' },
        { ts: '510500.SH', name: '中证500' },
        { ts: '518880.SH', name: '黄金' },
        { ts: '513100.SH', name: '纳指100' },
        { ts: '511260.SH', name: '10年国债' },
      ];
      const riskWeights = {
        '510300.SH': 0.15,
        '510500.SH': 0.15,
        '518880.SH': 0.3,
        '513100.SH': 0.2,
        '511260.SH': 0.2,
      };
      const addRiskTop = (rs: typeof rows) =>
        rs.map((r) => ({ ...r, riskTop: '518880.SH', riskTopW: 0.3 }));
      const outRows = withRisk ? addRiskTop(satLeg ? satRows : baseRows) : satLeg ? satRows : baseRows;
      // Harbor-family parking audit trail (harbor/homeport + overlays carry it).
      const harborParkedBlotter = [
        {
          date: '2026-08-01',
          kind: 'buy' as const,
          key: 'GOLD',
          name: '华安黄金ETF',
          ts: '518880.SH',
          price: 5.12,
          reason: 'entry' as const,
        },
        {
          date: '2026-08-04',
          kind: 'sell' as const,
          key: 'GOLD',
          name: '华安黄金ETF',
          ts: '518880.SH',
          price: 5.24,
          reason: 'trail' as const,
        },
      ];
      const harborParkedHeld = {
        key: 'GOLD',
        name: '华安黄金ETF',
        ts: '518880.SH',
        since: '2026-08-01',
        price: 5.31,
        weight: 0.5,
      };
      return {
        ok: true,
        strategy: satelliteOnly ? 'starship' : 'harbor',
        mode: 'mom_compare',
        satWeight: satelliteOnly ? 1 : String(path).includes('strategy=starport') ? 0.2 : 0.5,
        summary: {
          fusedPct: 12.5,
          basePct: 3.2,
          maxDdFusedPct: 9.4,
          ...(satelliteOnly
            ? {
                parkedPct: 18.4,
                parkedMaxDdPct: 28.5,
                parkedAvgWeight: 0.75,
                parkedTrades: 5,
                parkedTrailExits: 1,
              }
            : {}),
          ...(withRisk ? { riskPct: 8.8, riskMaxDdPct: 5.5 } : {}),
        },
        rows: outRows,
        ...(satLeg
          ? {
              openPositions: [
                {
                  ts: '300906.SZ',
                  entryDate: '2026-08-01',
                  entryPrice: 28.12,
                  close: 28.51,
                  heldDays: 2,
                  daysLeft: 1,
                  exitDue: '2026-08-04',
                  pnlPct: 1.39,
                },
              ],
              blotter: [
                {
                  kind: 'open',
                  date: '2026-08-01',
                  ts: '300906.SZ',
                  amp: 13.8,
                  ampRank: 5,
                  entryDate: '2026-08-01',
                  pnlPct: 1.39,
                  contribPct: 0.35,
                  closeReason: 'open',
                },
                {
                  kind: 'fill',
                  date: '2026-08-04',
                  ts: '688155.SH',
                  amp: 2.8,
                  ampRank: 6,
                  entryDate: '2026-08-01',
                  exitDate: '2026-08-04',
                  pnlPct: 3.44,
                  contribPct: 0.86,
                  closeReason: 'body_exit',
                },
                {
                  kind: 'skip_t1',
                  date: '2026-08-04',
                  ts: '000593.SZ',
                  amp: 0,
                  ampRank: 1,
                  closeReason: 'skip_t1_limit',
                },
                ...olderFills,
              ],
            }
          : {}),
        ...(satelliteOnly
          ? {
              parkedBlotter: [
                {
                  date: '2026-08-01',
                  kind: 'buy',
                  key: 'GOLD',
                  name: '华安黄金ETF',
                  ts: '518880.SH',
                  price: 5.12,
                  reason: 'entry',
                },
                {
                  date: '2026-08-04',
                  kind: 'sell',
                  key: 'GOLD',
                  name: '华安黄金ETF',
                  ts: '518880.SH',
                  price: 5.24,
                  reason: 'rotate',
                },
                {
                  date: '2026-08-04',
                  kind: 'buy',
                  key: 'OIL',
                  name: '富国油气QDII',
                  ts: '513350.SH',
                  price: 1.38,
                  reason: 'rotate',
                },
                {
                  date: '2026-08-05',
                  kind: 'sell',
                  key: 'OIL',
                  name: '富国油气QDII',
                  ts: '513350.SH',
                  price: 1.41,
                  reason: 'trail',
                },
                {
                  date: '2026-08-06',
                  kind: 'buy',
                  key: 'GOLD',
                  name: '华安黄金ETF',
                  ts: '518880.SH',
                  price: 5.3,
                  reason: 'entry',
                },
                ...olderParked,
              ],
              parkedHeld: {
                key: 'GOLD',
                name: '华安黄金ETF',
                ts: '518880.SH',
                since: '2026-08-06',
                price: 5.31,
                weight: 0.75,
              },
            }
          : {}),
        ...(!satelliteOnly
          ? { parkedBlotter: harborParkedBlotter, parkedHeld: harborParkedHeld }
          : {}),
        ...(withRisk
          ? {
              riskUniverse,
              riskBlotter: [
                {
                  date: '2026-08-01',
                  weights: {
                    '510300.SH': 0.2,
                    '510500.SH': 0.2,
                    '518880.SH': 0.2,
                    '513100.SH': 0.2,
                    '511260.SH': 0.2,
                  },
                  turnover: 0,
                },
                { date: '2026-09-01', weights: riskWeights, turnover: 0.1 },
              ],
              riskHeld: { date: '2026-09-30', weights: riskWeights },
            }
          : {}),
      };
    }
    if (String(path).includes('/api/backtest/return-attribution')) {
      return { ok: true, rows: [], summary: {} };
    }
    if (String(path).includes('/api/backtest/exit-attribution')) {
      return {
        ok: true,
        days: 5,
        closedCount: 0,
        withForwardCount: 0,
        excluded: 0,
        insufficient: true,
        hint: null,
        overall: {
          count: 0,
          avgFwdPct: null,
          earlyCount: 0,
          wellCount: 0,
          neutralCount: 0,
          earlyRate: null,
          wellRate: null,
        },
        byReason: {},
        exposure: { maxSimultaneous: 0, singleStockWeightFloorPct: null, note: '' },
      };
    }
    if (String(path).includes('/api/backtest/correlation-status')) {
      return {
        ok: true,
        capPct: 30,
        clusters: {},
        overLimit: [],
        blockedSymbols: [],
        topPairs: [],
        empiricalNote: null,
      };
    }
    // Keep other compare-tab endpoints from throwing and unmounting the page.
    if (String(path).includes('/api/backtest/')) return { ok: true };
    return { ok: true };
  });
});

describe('BacktestPage', () => {
  it('shows the selected strategy timeline under the catalog panel', async () => {
    renderPage();
    expect(await screen.findByText(/Timeline（港湾/)).toBeDefined();
    expect(screen.queryByText(/资金流全景/)).toBeNull();
    fireEvent.click(await screen.findByRole('button', { name: /均衡/ }));
    expect(await screen.findByText(/Timeline（星港/)).toBeDefined();
    fireEvent.click(await screen.findByRole('button', { name: /进攻/ }));
    expect(await screen.findByText(/Timeline（星舰/)).toBeDefined();
    fireEvent.click(await screen.findByRole('button', { name: /防守/ }));
    expect(await screen.findByText(/Timeline（母港/)).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: '双子星' }));
    expect(await screen.findByText(/Timeline（双子星/)).toBeDefined();
  });

  it('hovers satellite-only rows without crashing (no navBaseReturnPct)', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    const bar = await screen.findByTestId('harbor-hold-bar');
    fireEvent.mouseMove(bar, { clientX: 0 });
    const tip = await screen.findByTestId('harbor-day-tip');
    expect(tip.textContent).toContain('基线 —');
    expect(tip.textContent).toContain('卫星 4/4仓 · 空0槽');
    expect(tip.textContent).toContain('停车 黄金 100%');
  });

  it('labels the starship v2 parked-cash overlay', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    expect((await screen.findAllByText(/停车 黄金 100%/)).length).toBeGreaterThanOrEqual(1);
    expect((await screen.findAllByText(/停车 原油 50%/)).length).toBeGreaterThanOrEqual(1);
    expect(await screen.findByText(/停车均值 75%/)).toBeDefined();
     expect(await screen.findByText(/激进对照：卫星 \+ 闲钱 100% 停 H2 ETF/)).toBeDefined();

  });

  it('records the parking ETF per day (建仓/换仓/回撤转现金) in the starship day table', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    // 08-04 rotate: sell GOLD → buy OIL
    expect(await screen.findByText(/停车 原油 50%（换仓）/)).toBeDefined();
    // 08-05 trail exit: the sleeve sits in cash (REPO)
    expect(await screen.findByText(/停车 现金 100%（回撤8%→现金）/)).toBeDefined();
    // 08-06 re-entry, and the tooltip carries the sleeve trades + day return
    const cell = await screen.findByText(/停车 黄金 50%（建仓）/);
    const title = cell.getAttribute('title') ?? '';
    expect(title).toContain('停车 黄金');
    expect(title).toContain('代码 518880.SH');
    expect(title).toContain('买黄金 518880.SH @5.3');
  });

  it('paints satellite timeline blocks by slot occupancy, not as gray parking', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    const held = await screen.findByTestId('harbor-seg-2026-08-01');
    expect(held.getAttribute('style') ?? '').toContain('linear-gradient');
    expect(held.className).not.toContain('bg-gray-300');
    const half = screen.getByTestId('harbor-seg-2026-08-04');
    expect(half.getAttribute('style') ?? '').toContain('50%');
    expect(await screen.findByText(/卫星 4\/4仓 2天/)).toBeDefined();
    expect(await screen.findByText(/卫星 2\/4仓 2天/)).toBeDefined();
  });

  it('shows satellite leg details (positions + blotter) for 星舰', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    expect(await screen.findByText('卫星腿明细')).toBeDefined();
    expect((await screen.findAllByText('300906.SZ')).length).toBeGreaterThan(0);
    expect((await screen.findAllByText('3 日到期')).length).toBeGreaterThan(0);
    expect((await screen.findAllByText('688155.SH')).length).toBeGreaterThan(0);
    expect(await screen.findByText('当前持仓 1 · 买 1 / 卖 33 · 跳过 1')).toBeDefined();
  });

  it('keeps the NEWEST 30 satellite flow rows (not the oldest)', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    await screen.findByText('卫星腿明细');
    // 34 flow rows → "近 30" must show 2026-08-04 .. 2026-05-05, dropping the 4 oldest.
    expect(await screen.findByText('T0004.SZ')).toBeDefined();
    expect(screen.queryByText('T0000.SZ')).toBeNull();
    expect(screen.queryByText('T0003.SZ')).toBeNull();
  });

  it('labels the satellite overlay weight and caliber per strategy', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /产品候选增量/ }));
    expect(
      await screen.findByText(/组合权重 0\.2（卫星 v1 · 冻结口径）/),
    ).toBeDefined();
    expect(await screen.findByText(/贡献%（腿内）/)).toBeDefined();
  });

  it('records the parked-sleeve ETF trades in the starship audit', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /激进 · 前置未满/ }));
    expect(await screen.findByText('停车套筒明细（闲钱买 ETF）')).toBeDefined();
    expect(await screen.findByText('买 18 / 卖 17 · 回撤出场 1')).toBeDefined();
    expect(await screen.findByText(/当前持有 黄金 518880.SH/)).toBeDefined();
    expect(await screen.findByText(/停车权重 75%/)).toBeDefined();
    expect((await screen.findAllByText('513350.SH')).length).toBeGreaterThan(0);
    expect((await screen.findAllByText('回撤 8%')).length).toBeGreaterThan(0);
    // 35 parked events → "近 30" must drop the OLDEST ones (P0000/P0001 hidden).
    expect((await screen.findAllByText(/P0005\.SH/)).length).toBeGreaterThan(0);
    expect(screen.queryAllByText(/P0000\.SH/).length).toBe(0);
    expect(screen.queryAllByText(/P0001\.SH/).length).toBe(0);
  });

  it('shows the harbor timeline on compare tab', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    expect(await screen.findByText(/港湾 · 选强股票 \+ 闲钱停 ETF 停车场/)).toBeDefined();
    expect(await screen.findByText(/资金流全景/)).toBeDefined();
    expect((await screen.findAllByText('港湾NAV%')).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/滚动过去一年/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/产品过去一年/)).toBeDefined();
    expect(screen.getByText(/三窗 · OOS2/)).toBeDefined();
    expect(screen.getByText(/NAV 叠加/)).toBeDefined();
    expect(screen.getByTestId('harbor-nav-chart')).toBeDefined();
  });

  it('switches the timeline strategy label to 母港', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    fireEvent.click(await screen.findByRole('button', { name: '母港' }));
    expect(await screen.findByText(/Timeline（母港/)).toBeDefined();
  });

  it('defaults the timeline to the configured 星舰 B strategy', async () => {
    window.localStorage.removeItem('karios.strategyMode.v2');
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    expect(await screen.findByText(/Timeline（星舰 B/)).toBeDefined();
  });

  it('renders auto-segmented holding blocks and per-day hover details', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    await screen.findByText(/港湾 · 选强股票 \+ 闲钱停 ETF 停车场/);
    expect(await screen.findByTestId('harbor-seg-2026-08-01')).toBeDefined();
    expect(screen.getByTestId('harbor-seg-2026-08-05')).toBeDefined();
    expect(screen.getAllByText(/股票 2天/).length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByText(/港股 \d+天/)).toBeNull();
    expect(screen.queryByText(/A\+H \d+天/)).toBeNull();

    const bar = screen.getByTestId('harbor-hold-bar');
    fireEvent.mouseMove(bar, { clientX: 0 });
    const tip = await screen.findByTestId('harbor-day-tip');
    expect(tip.textContent).toContain('2026-08-01');
    expect(tip.textContent).toContain('股票 10票');
    expect(tip.textContent).toContain('600519');
    expect(tip.textContent).toContain('港湾 5.00%');

    fireEvent.mouseMove(bar, { clientX: 0.3 });
    expect((await screen.findByTestId('harbor-day-tip')).textContent).toContain('2026-08-04');
    fireEvent.mouseLeave(bar);
    expect(screen.queryByTestId('harbor-day-tip')).toBeNull();
  });

  it('switches timeline query to the OOS2 gate window', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    expect(await screen.findByText(/港湾 · 选强股票 \+ 闲钱停 ETF 停车场/)).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: /三窗 · OOS2/ }));
    expect(
      apiGetJson.mock.calls.some((c: unknown[]) => String(c[0]).includes('start=2024-08-01')),
    ).toBe(true);
  });
  it('shows the S-3 conclusion board with baselines, long window and params', async () => {
    renderPage();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/S-3 股票腿三窗/)).toBeDefined();
    expect(await screen.findByText('112.7%')).toBeDefined();
    expect(screen.getByText('88.2%')).toBeDefined();
    expect(screen.getByText('+250.8%')).toBeDefined();
    expect(screen.getByText('1401 笔')).toBeDefined();
    expect(screen.getByText('2023')).toBeDefined();
    expect(screen.getByText('-263')).toBeDefined();
    expect(screen.getAllByText('score 65').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('RS 前 50%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('RS 前 40%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('移动 -12%')).toBeDefined();
  });

  it('shows rolling OOS warning and recon strip', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    expect(await screen.findByText(/回测 vs Paper 对账/)).toBeDefined();
    expect(screen.getByText('缺 19 · 多 0')).toBeDefined();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/滚动 OOS（最近 90 天/)).toBeDefined();
    expect(screen.getByText(/HK: -8.5% dd=19.5% sharpe=-3.2 trades=55/)).toBeDefined();
    expect(screen.getAllByText('港股').length).toBeGreaterThanOrEqual(1);
  });

  it('shows the C4 paper-vs-backtest comparison with verdict banner', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    expect(await screen.findByText(/C4 · paper vs 回测逐笔对照/)).toBeDefined();
    expect(await screen.findByText(/样本 <20 笔：结论待积累（C4 未定案）/)).toBeDefined();
    expect(screen.getByText('50.0%')).toBeDefined();
    expect(screen.getByText('-1.0%')).toBeDefined();
    expect(screen.getByText('100.0%')).toBeDefined();
    expect(screen.getByText('HK:00622')).toBeDefined();
    expect(screen.getByText('trailing_stop')).toBeDefined();
    expect(screen.getByText('存在差异')).toBeDefined();
  });

  it('collapses the advanced parameter tools behind a toggle', async () => {
    renderPage();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/高级：参数敏感度工具/)).toBeDefined();
    expect(screen.queryByText('运行回测')).toBeNull();
    fireEvent.click(screen.getByText(/高级：参数敏感度工具/));
    expect(screen.getByText('运行回测')).toBeDefined();
    expect(screen.getByText('敏感度网格 (36)')).toBeDefined();
  });

  it('shows the harbor parking leg in the day table and audit panel', async () => {
    renderPage();
    // 08-05 OIL idle day builds the parking leg (harbor rows carry pick/markers).
    expect(await screen.findByText(/停车 原油 100%（建仓）/)).toBeDefined();
    expect(await screen.findByText('停车套筒明细（闲钱买 ETF）')).toBeDefined();
    expect(await screen.findByText('买 1 / 卖 1 · 回撤出场 1')).toBeDefined();
  });

  it('shows the B3 leg for 母港 (day column + rebalance panel)', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('对比'));
    fireEvent.click(await screen.findByRole('button', { name: '母港' }));
    expect(await screen.findByText(/Timeline（母港/)).toBeDefined();
    expect(await screen.findByText('B3篮子明细（风险预算）')).toBeDefined();
    expect(
      await screen.findByText(/当前权重 沪深300 15% · 中证500 15% · 黄金 30%/),
    ).toBeDefined();
    expect((await screen.findAllByText(/黄金 30%/)).length).toBeGreaterThanOrEqual(1);
    expect(await screen.findByText(/B3 8.8%/)).toBeDefined();
  });

  it('shows both base and satellite legs for 星港', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /产品候选增量/ }));
    expect(await screen.findByText(/Timeline（星港/)).toBeDefined();
    // Base leg: stock tickets survive next to the satellite leg.
    expect((await screen.findAllByText(/600519/)).length).toBeGreaterThanOrEqual(1);
    expect((await screen.findAllByText(/卫星 4仓/)).length).toBeGreaterThanOrEqual(1);
    expect(await screen.findByText('卫星腿明细')).toBeDefined();
    expect(await screen.findByText('停车套筒明细（闲钱买 ETF）')).toBeDefined();
    expect(await screen.findByText('B3篮子明细（风险预算）')).toBeDefined();
  });

  it('shows base + parking + B3 but no satellite for 母港M30', async () => {
    renderPage();
    fireEvent.click(await screen.findByRole('button', { name: /防守/ }));
    expect(await screen.findByText(/Timeline（母港/)).toBeDefined();
    expect((await screen.findAllByText(/600519/)).length).toBeGreaterThanOrEqual(1);
    expect(await screen.findByText('停车套筒明细（闲钱买 ETF）')).toBeDefined();
    expect(await screen.findByText('B3篮子明细（风险预算）')).toBeDefined();
    expect(screen.queryByText('卫星腿明细')).toBeNull();
  });

});
