import { describe, expect, it } from 'vitest';

import { parseAlphaRadarBatchExtract } from './alphaRadarBatchNormalize';

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
    }
  });
});
