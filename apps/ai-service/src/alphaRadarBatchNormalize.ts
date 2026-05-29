import { z } from 'zod';

import { AlphaRadarExtractBatchResponseSchema } from './schemas';

const URGENCY = new Set(['S', 'A', 'B', 'C']);

function asString(value: unknown, fallback = ''): string {
  if (value == null) return fallback;
  return String(value).trim();
}

function normalizeUrgency(value: unknown): 'S' | 'A' | 'B' | 'C' {
  const raw = asString(value, 'B').toUpperCase();
  if (URGENCY.has(raw)) return raw as 'S' | 'A' | 'B' | 'C';
  return 'B';
}

function normalizeKeywords(value: unknown): string[] {
  if (Array.isArray(value)) {
    const out = value.map((x) => asString(x)).filter(Boolean).slice(0, 8);
    if (out.length) return out;
  }
  return ['产业趋势'];
}

function normalizeSourceIndex(value: unknown, fallback: number): number {
  const n = Number(value);
  if (Number.isFinite(n) && n >= 0) return Math.min(49, Math.floor(n));
  return Math.min(49, Math.max(0, fallback));
}

export function normalizeAlphaRadarTrendRow(
  item: unknown,
  idx = 0,
): Record<string, unknown> {
  const row = item && typeof item === 'object' ? (item as Record<string, unknown>) : {};
  const macroTheme = asString(
    row.macro_theme ?? row.macroTheme ?? row.trend_name ?? row.trendName,
    'Unknown trend',
  );
  const trendName = asString(row.trend_name ?? row.trendName, macroTheme) || macroTheme;
  const catalystGrade = normalizeUrgency(
    row.catalyst_grade ?? row.catalystGrade ?? row.urgency_level ?? row.urgencyLevel,
  );
  const catalyst = asString(row.catalyst, trendName);
  return {
    macro_theme: macroTheme.slice(0, 120),
    catalyst_grade: catalystGrade,
    trend_name: trendName.slice(0, 200),
    catalyst: catalyst.slice(0, 2000),
    global_target: asString(row.global_target ?? row.globalTarget, 'N/A').slice(0, 120),
    urgency_level: catalystGrade,
    keywords_for_mapping: normalizeKeywords(row.keywords_for_mapping ?? row.keywordsForMapping),
    source_index: normalizeSourceIndex(row.source_index ?? row.sourceIndex, idx),
  };
}

export function normalizeAlphaRadarBatchExtract(raw: unknown): unknown {
  const root = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const trendsRaw = Array.isArray(root.trends) ? root.trends : [];
  const trends = trendsRaw.slice(0, 8).map((item, idx) => normalizeAlphaRadarTrendRow(item, idx));
  return {
    trends,
    model: asString(root.model) || undefined,
  };
}

export function normalizeAlphaRadarExtract(raw: unknown): unknown {
  const root = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const trendsRaw = Array.isArray(root.trends) ? root.trends : [];
  const trends = trendsRaw.slice(0, 5).map((item, idx) => {
    const row = normalizeAlphaRadarTrendRow(item, idx);
    const { source_index: _sourceIndex, ...rest } = row;
    return rest;
  });
  return {
    trends,
    model: asString(root.model) || undefined,
  };
}

export function parseAlphaRadarBatchExtract(raw: unknown) {
  const normalized = normalizeAlphaRadarBatchExtract(raw);
  return AlphaRadarExtractBatchResponseSchema.safeParse(normalized);
}
