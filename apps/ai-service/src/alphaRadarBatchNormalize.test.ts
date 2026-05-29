import { describe, expect, it } from 'vitest';

import {
  normalizeAlphaRadarExtract,
  parseAlphaRadarBatchExtract,
} from './alphaRadarBatchNormalize';

describe('parseAlphaRadarBatchExtract', () => {
  it('accepts camelCase and coerces source_index', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          trendName: 'GPU datacenter capex',
          catalyst: 'Hyperscaler spending rises',
          globalTarget: 'NVDA',
          urgencyLevel: 'A',
          keywordsForMapping: ['算力', '数据中心'],
          sourceIndex: '2',
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends[0]?.source_index).toBe(2);
      expect(result.data.trends[0]?.trend_name).toContain('GPU');
      expect(result.data.trends[0]?.macro_theme).toContain('GPU');
      expect(result.data.trends[0]?.catalyst_grade).toBe('A');
      expect(result.data.trends[0]?.urgency_level).toBe('A');
    }
  });

  it('parses macro_theme and catalyst_grade explicitly', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: 'Next-Gen Energy',
          catalyst_grade: 'S',
          catalyst: 'Grid-scale storage orders accelerate',
          global_target: 'N/A',
          keywords_for_mapping: ['液冷散热', '储能'],
          source_index: 0,
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends[0]?.macro_theme).toBe('Next-Gen Energy');
      expect(result.data.trends[0]?.catalyst_grade).toBe('S');
      expect(result.data.trends[0]?.trend_name).toBe('Next-Gen Energy');
      expect(result.data.trends[0]?.urgency_level).toBe('S');
    }
  });

  it('fills defaults for sparse rows', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [{ trend_name: 'HBM shortage' }],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends[0]?.global_target).toBe('N/A');
      expect(result.data.trends[0]?.keywords_for_mapping.length).toBeGreaterThan(0);
      expect(result.data.trends[0]?.macro_theme).toBe('HBM shortage');
      expect(result.data.trends[0]?.catalyst_grade).toBe('B');
    }
  });
});

describe('normalizeAlphaRadarExtract', () => {
  it('omits source_index from single-document trends', () => {
    const normalized = normalizeAlphaRadarExtract({
      trends: [
        {
          macroTheme: 'HBM Supply Chain',
          catalystGrade: 'A',
          catalyst: 'Capacity tightens',
        },
      ],
    }) as { trends: Array<Record<string, unknown>> };
    expect(normalized.trends[0]?.macro_theme).toBe('HBM Supply Chain');
    expect(normalized.trends[0]?.catalyst_grade).toBe('A');
    expect(normalized.trends[0]?.source_index).toBeUndefined();
  });
});
