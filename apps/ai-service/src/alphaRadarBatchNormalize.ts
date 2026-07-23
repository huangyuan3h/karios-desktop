import { z } from 'zod';

import { AlphaRadarExtractBatchResponseSchema, AlphaRadarDriverTypeSchema } from './schemas';
import { stripModelThinking } from './model_thinking';

const GRADES = new Set(['S', 'A', 'B', 'C']);

const CATEGORY_DRIVER_DEFAULT: Record<string, z.infer<typeof AlphaRadarDriverTypeSchema>> = {
  academic: 'Global_Tech',
  earnings: 'Global_Tech',
  research: 'Global_Tech',
  policy: 'Domestic_Policy',
  cycle: 'Cycle_Reversal',
  consensus: 'Cycle_Reversal',
};

function asString(value: unknown, fallback = ''): string {
  if (value == null) return fallback;
  return stripModelThinking(String(value));
}

function normalizeGrade(value: unknown): 'S' | 'A' | 'B' | null {
  const raw = asString(value, 'B').toUpperCase();
  if (raw === 'S' || raw === 'A') return raw;
  if (GRADES.has(raw)) return 'B';
  return 'B';
}

function normalizeDriverType(
  value: unknown,
  categoryHint?: string,
): z.infer<typeof AlphaRadarDriverTypeSchema> {
  const raw = asString(value);
  if (raw === 'Global_Tech' || raw === 'Domestic_Policy' || raw === 'Cycle_Reversal') {
    return raw;
  }
  const camel = raw.replace(/\s+/g, '_');
  if (camel === 'Global_Tech' || camel === 'Domestic_Policy' || camel === 'Cycle_Reversal') {
    return camel;
  }
  const hint = asString(categoryHint).toLowerCase();
  return CATEGORY_DRIVER_DEFAULT[hint] ?? 'Global_Tech';
}

function normalizeShareMapping(value: unknown, macroTheme: string): string[] {
  const raw = value ?? [];
  const items: string[] = [];
  if (Array.isArray(raw)) {
    for (const entry of raw) {
      const text = asString(entry);
      if (text) items.push(text);
    }
  } else {
    const text = asString(raw);
    if (text) items.push(text);
  }
  const deduped = [...new Set(items)].slice(0, 3);
  if (deduped.length) return deduped;
  return macroTheme ? [macroTheme.slice(0, 40)] : ['产业趋势'];
}

function normalizeLogicSummary(value: unknown, fallback: string): string {
  const text = asString(value, fallback) || fallback;
  return text.slice(0, 30);
}

function normalizeSourceIndex(value: unknown, fallback: number): number {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 0) return Math.min(49, Math.floor(n));
  return Math.min(49, Math.max(0, fallback));
}

export function normalizeAlphaRadarTrendRow(
  item: unknown,
  idx = 0,
  categoryHint?: string,
): Record<string, unknown> | null {
  const row = item && typeof item === 'object' ? (item as Record<string, unknown>) : {};
  const catalystGrade = normalizeGrade(
    row.catalyst_grade ?? row.catalystGrade ?? row.urgency_level ?? row.urgencyLevel,
  );
  if (catalystGrade === 'B' || catalystGrade === null) {
    return null;
  }

  const macroTheme = asString(
    row.macro_theme ?? row.macroTheme ?? row.trend_name ?? row.trendName,
    'Unknown trend',
  );
  const eventFocus = asString(
    row.event_focus ?? row.eventFocus ?? row.catalyst,
    macroTheme,
  );
  const logicSummary = normalizeLogicSummary(
    row.logic_summary ?? row.logicSummary,
    eventFocus.slice(0, 30),
  );

  return {
    macro_theme: macroTheme.slice(0, 120),
    driver_type: normalizeDriverType(row.driver_type ?? row.driverType, categoryHint),
    catalyst_grade: catalystGrade,
    event_focus: eventFocus.slice(0, 2000),
    a_share_mapping: normalizeShareMapping(
      row.a_share_mapping ?? row.aShareMapping ?? row.keywords_for_mapping ?? row.keywordsForMapping,
      macroTheme,
    ),
    logic_summary: logicSummary,
    source_index: normalizeSourceIndex(row.source_index ?? row.sourceIndex, idx),
  };
}

export function normalizeAlphaRadarBatchExtract(raw: unknown, categoryHint?: string): unknown {
  const root = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const trendsRaw = Array.isArray(root.trends) ? root.trends : [];
  const trends = trendsRaw
    .slice(0, 8)
    .map((item, idx) => normalizeAlphaRadarTrendRow(item, idx, categoryHint))
    .filter((row): row is Record<string, unknown> => row !== null);
  return {
    trends,
    model: asString(root.model) || undefined,
  };
}

export function normalizeAlphaRadarExtract(raw: unknown, categoryHint?: string): unknown {
  const root = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const trendsRaw = Array.isArray(root.trends) ? root.trends : [];
  const trends = trendsRaw
    .slice(0, 3)
    .map((item, idx) => normalizeAlphaRadarTrendRow(item, idx, categoryHint))
    .filter((row): row is Record<string, unknown> => row !== null)
    .map((row) => {
      const rest = { ...row };
      delete rest.source_index;
      return rest;
    });
  return {
    trends,
    model: asString(root.model) || undefined,
  };
}

export function parseAlphaRadarBatchExtract(raw: unknown, categoryHint?: string) {
  const normalized = normalizeAlphaRadarBatchExtract(raw, categoryHint);
  return AlphaRadarExtractBatchResponseSchema.safeParse(normalized);
}
