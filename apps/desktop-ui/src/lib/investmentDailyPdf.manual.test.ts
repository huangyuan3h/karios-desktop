/**
 * Manual PDF smoke test (not run in default CI).
 *
 * Run from repo root:
 *   WRITE_SAMPLE_PDF=1 pnpm --filter desktop-ui exec vitest run src/lib/investmentDailyPdf.manual.test.ts
 *
 * Output: OS temp dir file `karios-investment-daily-manual.pdf` (path printed to stdout).
 * First run may need network to load Noto Sans SC from CDN (can take tens of seconds).
 *
 * Keep sample text moderate: very long CJK + many page breaks can make @react-pdf/renderer slow.
 */
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { describe, expect, it } from 'vitest';

import type { HotIndustryPick } from '@/components/pages/HotIndustryWorkflowCard';

import { parseInvestmentDailyReportResponse, renderInvestmentDailyPdfToBlob } from './investmentDailyPdf';

const longCn =
  '这是一段用于测试中文换行与版面是否越界的长文本，包含沪深指数收报与北向资金等常见表述，重复以占满一行并触发自动折行。';

/** ~2× longCn — enough to stress-wrap without exploding layout time */
const mediumBlock = longCn.repeat(2);

function buildSampleReport() {
  return parseInvestmentDailyReportResponse({
    trafficLightPositionAndSentiment: `【红绿灯测试】${mediumBlock}\n\n第二段：情绪偏热时控制仓位，避免与主线段落重复表述。`,
    marketEnvironmentHighlights: `- 要点A：${longCn.slice(0, 100)}\n- 要点B：汇率与商品对风险偏好影响\n- 要点C：${longCn.slice(0, 60)}`,
    hotIndustriesFormalAnalysis: `【热点书面分析测试】${mediumBlock}动量突破与五日强势排名的含义说明。`,
    capitalFlowAndMainline: `【主线资金测试】${mediumBlock}\n\n与上段不同的侧重点：行业五日净流入矩阵、支线轮动。`,
    topStocks: [
      {
        symbol: '600000.SH',
        name: '浦发银行',
        rationale: `${longCn} 理由第二句含分数与趋势引用。理由第三句。`,
      },
      {
        symbol: '000001.SZ',
        name: '平安银行',
        rationale: `${longCn.slice(0, 120)} 第二只标的测试。`,
      },
      {
        symbol: '601318.SH',
        name: '中国平安',
        rationale: `${longCn.slice(0, 120)} 第三只标的测试。`,
      },
    ],
    topNews: Array.from({ length: 5 }, (_, i) => ({
      title: `测试新闻标题 ${i + 1}：${longCn.slice(0, 30)}`,
      summary: `${longCn.slice(0, 100)} 摘要句二。`,
    })),
  });
}

/** Minimal dashboard-like summary so tables and env box render. */
function buildSampleSummary(): unknown {
  return {
    asOfDate: '2026-05-06',
    marketEnvironmentZh: `${mediumBlock}收报与成交额等数据用于灰框折行测试。`,
    marketSentiment: {
      asOfDate: '2026-05-06',
      indexSignals: [
        {
          name: '上证综指',
          tsCode: '000001.SH',
          signal: 'green',
          positionRange: '60%-80%',
          pctChg: 0.42,
          close: 3345.67,
          ma5: 3320.1,
          ma20: 3288.0,
          asOfDate: '2026-05-06',
        },
        {
          name: '深证成指',
          tsCode: '399001.SZ',
          signal: 'yellow',
          positionRange: '40%-60%',
          pctChg: -0.15,
          close: 10123.45,
          ma5: 10150.0,
          ma20: 10080.0,
          asOfDate: '2026-05-06',
        },
      ],
      items: [
        {
          date: '2026-05-02',
          upDownRatio: 1.2,
          marketTurnoverCny: 980000000000,
          yesterdayLimitUpPremium: 2.1,
          failedLimitUpRate: 12.5,
          riskMode: 'normal',
          rules: ['成交额阈值', '广度检查'],
        },
        {
          date: '2026-05-06',
          upDownRatio: 0.95,
          marketTurnoverCny: 1050000000000,
          yesterdayLimitUpPremium: 1.8,
          failedLimitUpRate: 15.0,
          riskMode: 'hot',
          rules: ['涨停溢价回落'],
        },
      ],
    },
    industryFundFlow: {
      dates: ['2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05', '2026-05-06'],
      topByDate: [
        { date: '2026-05-06', top: ['半导体', '证券', '电力设备', '有色金属', '医药生物'] },
      ],
      flow5d: {
        dates: ['2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05', '2026-05-06'],
        top: [
          {
            industryName: '半导体',
            sum5d: 1200000000,
            series: [
              { date: '2026-05-06', netInflow: 350000000 },
              { date: '2026-05-05', netInflow: 280000000 },
            ],
          },
        ],
      },
      flow5dOut: {
        dates: ['2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05', '2026-05-06'],
        top: [
          {
            industryName: '房地产',
            sum5d: -900000000,
            series: [{ date: '2026-05-06', netInflow: -120000000 }],
          },
        ],
      },
    },
    macroSnapshot: {
      macro: [
        {
          name: '标普500',
          seriesId: 'SPX',
          close: 5200,
          pctChg: 0.33,
          ma5: 5180,
          ma20: 5100,
          asOfDate: '2026-05-05',
          source: 'test',
        },
      ],
    },
  };
}

function buildSampleHotPicks(): HotIndustryPick[] {
  return [
    {
      industryName: '半导体',
      dailyRank: 3,
      fiveDayRank: 5,
      netInflow: 350000000,
      sum5d: 1200000000,
      rankChange: 8,
      momentumSignal: true,
    },
    {
      industryName: '证券',
      dailyRank: 8,
      fiveDayRank: 4,
      netInflow: 120000000,
      sum5d: 800000000,
      rankChange: -2,
      momentumSignal: false,
    },
  ];
}

/** Font CDN + layout can exceed 2m on slow networks; keep generous cap. */
const MANUAL_PDF_TEST_TIMEOUT_MS = 300_000;

describe('investmentDailyPdf manual sample', () => {
  it.skipIf(process.env.WRITE_SAMPLE_PDF !== '1')(
    'WRITE_SAMPLE_PDF=1: writes karios-investment-daily-manual.pdf under OS tmpdir',
    async () => {
      const blob = await renderInvestmentDailyPdfToBlob({
        report: buildSampleReport(),
        subtitleTimeZh: '2026-05-06 20:00:00',
        filename: 'karios-investment-daily-manual.pdf',
        summary: buildSampleSummary(),
        hotIndustryPicks: buildSampleHotPicks(),
      });
      expect(blob.size).toBeGreaterThan(2000);

      const buf = Buffer.from(await blob.arrayBuffer());
      const outPath = path.join(os.tmpdir(), 'karios-investment-daily-manual.pdf');
      await fs.writeFile(outPath, buf);
      // eslint-disable-next-line no-console -- manual test artifact path
      console.info(`[investmentDailyPdf.manual] wrote ${outPath} (${buf.length} bytes)`);
    },
    MANUAL_PDF_TEST_TIMEOUT_MS,
  );
});
