import { describe, expect, it } from 'vitest';

import {
  SCHEDULER_GROUP_META,
  SCHEDULER_GROUP_ORDER,
  SCHEDULER_JOB_CATALOG,
  SchedulerJobsResponseSchema,
  groupSchedulerJobs,
} from './scheduler';

describe('SCHEDULER_JOB_CATALOG', () => {
  it('contains all expected job types', () => {
    const jobTypes = SCHEDULER_JOB_CATALOG.map((m) => m.jobType);
    expect(jobTypes).toEqual(
      expect.arrayContaining([
        'stock_close_sync',
        'stock_basic_sync',
        'hk_basic_sync',
        'hk_daily_full',
        'hk_industry_sync',
        'etf_fund_basic_sync',
        'etf_daily_full',
        'stock_daily_full',
        'stock_adj_factor_full',
        'stock_close_catchup',
        'index_daily_full',
        'index_basic_sync',
        'macro_daily_full',
        'eastmoney_industry_sync',
        'watchlist_automation',
        'cn_industry_post_close_sync',
        'alpha_radar_pipeline',
        'alpha_radar_ingest',
        'alpha_radar_process',
        'news_fetch_job',
        'news_enrich_job',
        'morning_brief_am',
        'morning_brief_pm',
        'paper_twin_star',
        'twin_star_reminder',
        'twin_star_intraday',
        'bar_5min_close',
      ]),
    );
  });

  it('lists paper_twin_star in watchlist automation after the live jobs', () => {
    const job = SCHEDULER_JOB_CATALOG.find((m) => m.jobType === 'paper_twin_star');
    expect(job).toMatchObject({
      group: 'watchlistAutomation',
      sortOrder: 27,
      tracked: true,
    });
  });

  it('has unique job types', () => {
    const seen = new Set<string>();
    for (const m of SCHEDULER_JOB_CATALOG) {
      expect(seen.has(m.jobType)).toBe(false);
      seen.add(m.jobType);
    }
  });

  it('every job is tracked (writes to sync_job_record)', () => {
    // All jobs must write to sync_job_record so the UI can show OK/FAIL.
    for (const m of SCHEDULER_JOB_CATALOG) {
      expect(m.tracked).toBe(true);
    }
  });

  it('uses Chinese title and schedule for the HK jobs', () => {
    const hk = SCHEDULER_JOB_CATALOG.filter((m) => m.group === 'hk');
    expect(hk.length).toBe(3);
    for (const m of hk) {
      expect(m.titleCn).toMatch(/港股/);
      expect(m.scheduleCn.length).toBeGreaterThan(0);
    }
  });

  it('has no TradingView capture jobs (retired 2026-08-12)', () => {
    const tv = SCHEDULER_JOB_CATALOG.filter((m) => m.group === 'tvScreener' as never);
    expect(tv).toHaveLength(0);
  });

  it('every job has a group entry in SCHEDULER_GROUP_META', () => {
    for (const m of SCHEDULER_JOB_CATALOG) {
      expect(SCHEDULER_GROUP_META[m.group]).toBeDefined();
    }
  });

  it('every group has an entry in SCHEDULER_GROUP_ORDER', () => {
    const groups = new Set(SCHEDULER_GROUP_ORDER);
    for (const m of SCHEDULER_JOB_CATALOG) {
      expect(groups.has(m.group)).toBe(true);
    }
  });
});

describe('groupSchedulerJobs', () => {
  it('returns groups in the canonical order', () => {
    const grouped = groupSchedulerJobs();
    expect(grouped.map((g) => g.group)).toEqual([...SCHEDULER_GROUP_ORDER]);
  });

  it('sorts jobs within each group by sortOrder', () => {
    const grouped = groupSchedulerJobs();
    for (const g of grouped) {
      const orders = g.jobs.map((j) => j.sortOrder);
      const sorted = [...orders].sort((a, b) => a - b);
      expect(orders).toEqual(sorted);
    }
  });
});

describe('SchedulerJobsResponseSchema', () => {
  it('parses a minimal valid payload', () => {
    const parsed = SchedulerJobsResponseSchema.parse({
      ok: true,
      jobs: {
        stock_basic_sync: {
          todayRun: {
            id: 1,
            job_type: 'stock_basic_sync',
            sync_at: '2026-07-29T10:00:00Z',
            success: true,
            last_ts_code: null,
            error_message: null,
          },
          lastSuccess: null,
        },
      },
      hkIndustryCoverage: null,
      alphaRadar: null,
      watchlistAutomation: null,
    });
    expect(parsed.ok).toBe(true);
    expect(parsed.jobs.stock_basic_sync.todayRun?.success).toBe(true);
  });

  it('parses HK coverage fields', () => {
    const parsed = SchedulerJobsResponseSchema.parse({
      ok: true,
      jobs: {},
      hkIndustryCoverage: {
        ok: true,
        totalHk: 100,
        mappedHk: 30,
        missingHk: 70,
        coveragePct: 30.0,
        jobType: 'hk_industry_sync',
      },
      alphaRadar: null,
      watchlistAutomation: null,
    });
    expect(parsed.hkIndustryCoverage?.coveragePct).toBe(30.0);
    expect(parsed.hkIndustryCoverage?.missingHk).toBe(70);
  });
});
