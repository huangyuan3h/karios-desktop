/** Map machine warning codes from /macro/snapshot to user-facing copy. */

const MACRO_WARNING_LABELS: Record<string, string> = {
  put_iv_live_fetch_failed_using_db: '510300 Put IV: live fetch failed — showing last DB value',
  put_iv_fetch_failed: '510300 Put IV unavailable',
  no_510300_put_iv_candidate: '510300 Put IV: no ATM put candidate from live sources',
};

export function formatMacroWarningPart(raw: string): string {
  const part = String(raw || '').trim();
  if (!part) return '';
  if (MACRO_WARNING_LABELS[part]) return MACRO_WARNING_LABELS[part];
  if (part.startsWith('macro_data_stale:')) {
    const ids = part.slice('macro_data_stale:'.length).trim();
    return ids ? `Offshore indices may be stale (${ids})` : 'Offshore indices may be stale';
  }
  return part;
}

/** Format a single code or a `; `-joined warning string from the API. */
export function formatMacroWarning(raw: string | null | undefined): string {
  const text = String(raw || '').trim();
  if (!text) return '';
  return text
    .split(';')
    .map((p) => formatMacroWarningPart(p))
    .filter(Boolean)
    .join(' · ');
}
