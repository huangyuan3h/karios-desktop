export function asTrimmedString(v: unknown): string {
  return typeof v === 'string' ? v.trim() : '';
}

export function jsonStringifyPretty(v: unknown): string {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

export function jsonStringifyCompact(v: unknown): string {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

export function toIndentedText(v: unknown, indent = 0): string {
  const pad = ' '.repeat(Math.max(0, indent));
  if (v == null) return `${pad}—`;
  if (typeof v === 'string') return `${pad}${v}`;
  if (typeof v === 'number' || typeof v === 'boolean') return `${pad}${String(v)}`;
  if (Array.isArray(v)) {
    if (!v.length) return `${pad}[]`;
    const lines: string[] = [];
    for (const it of v) {
      const isScalar =
        it == null || typeof it === 'string' || typeof it === 'number' || typeof it === 'boolean';
      if (isScalar) {
        lines.push(`${pad}- ${it == null ? '—' : String(it)}`);
        continue;
      }
      lines.push(`${pad}-`);
      lines.push(toIndentedText(it, indent + 2));
    }
    return lines.join('\n');
  }
  if (typeof v === 'object') {
    const obj = v as Record<string, unknown>;
    const keys = Object.keys(obj);
    if (!keys.length) return `${pad}{}`;
    const lines: string[] = [];
    for (const k of keys.sort()) {
      const val = obj[k];
      const isScalar =
        val == null ||
        typeof val === 'string' ||
        typeof val === 'number' ||
        typeof val === 'boolean';
      if (isScalar) {
        lines.push(`${pad}${k}: ${val == null ? '—' : String(val)}`);
        continue;
      }
      lines.push(`${pad}${k}:`);
      lines.push(toIndentedText(val, indent + 2));
    }
    return lines.join('\n');
  }
  return `${pad}${String(v)}`;
}

export function buildContextMarkdown(context: unknown): string {
  if (!context || typeof context !== 'object' || Array.isArray(context)) {
    return '---\n\n### context\n\n```text\n' + toIndentedText(context) + '\n```\n';
  }
  const obj = context as Record<string, unknown>;
  const preferredOrder = [
    'date',
    'account',
    'accountPrompt',
    'accountState',
    'watchlist',
    'tradingView',
    'industryFundFlow',
    'marketSentiment',
    'leaderStocks',
    'mainline',
    'quant2d',
    'candidateUniverse',
    'stage1',
    'selectedSymbols',
    'stocks',
  ];
  const keys = Object.keys(obj);
  const ordered: string[] = [];
  for (const k of preferredOrder) {
    if (Object.prototype.hasOwnProperty.call(obj, k)) ordered.push(k);
  }
  const rest = keys.filter((k) => !ordered.includes(k)).sort();
  ordered.push(...rest);

  let out = '';
  for (const k of ordered) {
    out += `---\n\n### ${k}\n\n`;
    out += '```text\n' + toIndentedText(obj[k]) + '\n```\n';
  }
  return out || '---\n\n### context\n\n```text\n' + toIndentedText(context) + '\n```\n';
}

export function buildPromptDebug({
  system,
  promptText,
  context,
}: {
  system: string;
  promptText: string;
  context: unknown;
}): {
  system: string;
  promptText: string;
  contextJsonCompact: string;
  contextJsonPretty: string;
  contextMarkdown: string;
  promptMarkdown: string;
} {
  const contextJsonCompact = jsonStringifyCompact(context);
  const contextJsonPretty = jsonStringifyPretty(context);
  const contextMarkdown = buildContextMarkdown(context);
  const promptMarkdown =
    '## System\n\n```text\n' +
    system +
    '\n```\n\n' +
    '## Prompt (as sent to model)\n\n```text\n' +
    promptText +
    '\n```\n\n' +
    '## Context (segmented)\n\n' +
    contextMarkdown;

  return {
    system,
    promptText,
    contextJsonCompact,
    contextJsonPretty,
    contextMarkdown,
    promptMarkdown,
  };
}

export function normalizeOptionalString(v: unknown): string | undefined {
  const s = asTrimmedString(v);
  return s ? s : undefined;
}
