import { describe, expect, it } from 'vitest';

import {
  normalizeAlphaRadarExtract,
  parseAlphaRadarBatchExtract,
} from './alphaRadarBatchNormalize.js';

describe('parseAlphaRadarBatchExtract V4', () => {
  it('accepts V4 fields and coerces source_index', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: 'GPU datacenter capex',
          driver_type: 'Global_Tech',
          catalyst_grade: 'A',
          event_focus: 'Hyperscaler spending rises',
          a_share_mapping: ['中际旭创', '600000'],
          logic_summary: '算力基建拉动光模块',
          sourceIndex: '2',
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends[0]?.source_index).toBe(2);
      expect(result.data.trends[0]?.macro_theme).toContain('GPU');
      expect(result.data.trends[0]?.driver_type).toBe('Global_Tech');
      expect(result.data.trends[0]?.catalyst_grade).toBe('A');
      expect(result.data.trends[0]?.a_share_mapping).toEqual(['中际旭创', '600000']);
    }
  });

  it('parses macro_theme and catalyst_grade explicitly', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: '国家级设备更新',
          driver_type: 'Domestic_Policy',
          catalyst_grade: 'S',
          event_focus: '发改委下达1万亿超长期特别国债用于设备更新',
          a_share_mapping: ['三一重工'],
          logic_summary: '万亿国债驱动设备更新',
          source_index: 0,
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends[0]?.macro_theme).toBe('国家级设备更新');
      expect(result.data.trends[0]?.catalyst_grade).toBe('S');
      expect(result.data.trends[0]?.driver_type).toBe('Domestic_Policy');
    }
  });

  it('drops B-grade trends', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: 'HBM shortage',
          driver_type: 'Global_Tech',
          catalyst_grade: 'B',
          event_focus: 'minor update',
          a_share_mapping: ['产业趋势'],
          logic_summary: '边缘噪音',
          source_index: 0,
        },
        {
          macro_theme: '铜供给挤压',
          driver_type: 'Cycle_Reversal',
          catalyst_grade: 'S',
          event_focus: '铜价突破历史新高',
          a_share_mapping: ['江西铜业'],
          logic_summary: '产能出清后价格暴涨',
          source_index: 1,
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends).toHaveLength(1);
      expect(result.data.trends[0]?.macro_theme).toBe('铜供给挤压');
    }
  });

  it('truncates logic_summary to 30 chars', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: '锂价反转',
          driver_type: 'Cycle_Reversal',
          catalyst_grade: 'A',
          event_focus: '锂现货连续上涨',
          a_share_mapping: ['赣锋锂业'],
          logic_summary: '这是一段超过三十个汉字的逻辑推演说明应该被截断处理',
          source_index: 0,
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect((result.data.trends[0]?.logic_summary ?? '').length).toBeLessThanOrEqual(30);
    }
  });

  it('accepts hk_mapping as optional and caps at 3 (OPT-052)', () => {
    const result = parseAlphaRadarBatchExtract({
      trends: [
        {
          macro_theme: '腾讯 AI 资本开支回升',
          driver_type: 'Global_Tech',
          catalyst_grade: 'S',
          event_focus: '腾讯 Q2 capex 同比 +80%',
          a_share_mapping: ['光环新网'],
          hk_mapping: ['00700', '腾讯', 'Tencent', 'extra'],
          logic_summary: 'HK AI 算力 capex 重启',
          source_index: 0,
        },
        {
          macro_theme: '纯 A 股政策',
          driver_type: 'Domestic_Policy',
          catalyst_grade: 'S',
          event_focus: '设备更新',
          a_share_mapping: ['三一重工'],
          logic_summary: '国债驱动设备更新',
          source_index: 1,
        },
      ],
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.trends).toHaveLength(2);
      expect(result.data.trends[0]?.hk_mapping).toEqual(['00700', '腾讯', 'Tencent']);
      // Trends without hk_mapping must default to [] (not undefined) so downstream Python can iterate safely.
      expect(result.data.trends[1]?.hk_mapping).toEqual([]);
    }
  });
});

describe('normalizeAlphaRadarExtract', () => {
  it('omits source_index from single-document trends', () => {
    const normalized = normalizeAlphaRadarExtract({
      trends: [
        {
          macroTheme: 'HBM Supply Chain',
          driverType: 'Global_Tech',
          catalystGrade: 'A',
          eventFocus: 'Capacity tightens',
          aShareMapping: ['中际旭创'],
          logicSummary: 'HBM紧缺拉动封测',
        },
      ],
    }) as { trends: Array<Record<string, unknown>> };
    expect(normalized.trends[0]?.macro_theme).toBe('HBM Supply Chain');
    expect(normalized.trends[0]?.catalyst_grade).toBe('A');
    expect(normalized.trends[0]?.source_index).toBeUndefined();
  });
});
