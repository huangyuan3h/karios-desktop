export function tryParseJsonObject(text: string): unknown {
  const t0 = text.trim();
  // Strip common markdown code fences if present.
  const t =
    t0.startsWith('```') && t0.endsWith('```')
      ? t0.replace(/^```[a-zA-Z0-9_-]*\s*/m, '').replace(/```$/m, '').trim()
      : t0;
  try {
    return JSON.parse(t);
  } catch {
    // Try to extract the first JSON object block.
    const start = t.indexOf('{');
    const end = t.lastIndexOf('}');
    if (start >= 0 && end > start) {
      const slice = t.slice(start, end + 1).trim();
      return JSON.parse(slice);
    }
    throw new Error('Failed to parse JSON');
  }
}


