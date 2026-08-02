const BIG_NUMBER_UNITS: Array<[number, string]> = [
  [1e12, 'T'],
  [1e9, 'B'],
  [1e6, 'M'],
  [1e3, 'K'],
];

const TEXT_HEADERS =
  /^(symbol|ticker|name|sector|industry|country|flags|analyst|screen\s*title|currency)/i;

const PERCENT_HEADERS =
  /(change|chg|perf|yield|growth|eps|dividend|div\s*yield)/i;

const BIG_NUMBER_HEADERS =
  /^(avg\s*volume|volume|market\s*cap|cap|avg\s*vol)/i;

const PRICE_LIKE_HEADERS =
  /^(price|sma\d*|ema\d*|macd|close|open|low|high|high\s*52\s*w)/i;

const RATIO_HEADERS = /^(rsi|p\/e|pe)\b/i;

/**
 * True if the value already looks like a finished, human-readable string.
 * Examples: "+3.82%", "11.06 M", "165.78 B USD", "44.00 CNY", "−0.55%".
 * Raw numbers like "47510154" or "-1.5177065767285085" return false.
 */
export function isAlreadyFormattedScreenerValue(value: string): boolean {
  if (!value) return false;
  if (value.includes('%')) return true;
  if (/\s[BMKTS]$/i.test(value)) return true;
  if (/\s[BMKTS]\s/i.test(value)) return true;
  if (/\b(?:CNY|USD|HKD|JPY|EUR|GBP|AUD|HKD)\b/i.test(value)) return true;
  if (/^[+−]/.test(value)) return true;
  return false;
}

function parseScreenerNumber(value: string): number | null {
  const cleaned = value.replace(/,/g, '').trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function formatBigNumber(n: number): string {
  const sign = n < 0 ? '-' : '';
  const abs = Math.abs(n);
  for (const [threshold, unit] of BIG_NUMBER_UNITS) {
    if (abs >= threshold) {
      return `${sign}${(abs / threshold).toFixed(2)}${unit}`;
    }
  }
  return `${sign}${abs.toFixed(0)}`;
}

export function isScreenerTextColumn(header: string): boolean {
  return TEXT_HEADERS.test(String(header).trim());
}

/**
 * Format a TradingView screener cell value for human-readable display.
 *
 * TradingView scrapes sometimes return raw numbers ("47510154",
 * "-1.5177065767285085", "97126646894.81198") instead of the
 * "+1.95%" / "11.06 M" / "165.78 B USD" form they show in the UI.
 * This helper normalizes both forms into something readable.
 *
 *  - Identifiers / categorical columns (Symbol, Name, Sector, ...) pass through.
 *  - Already-formatted strings (+1.95%, 11.06 M, 165.78 B USD) pass through.
 *  - Raw numeric values get 2-decimal rounding and B/M/K units where appropriate.
 */
export function formatScreenerCell(
  header: string,
  value: string | number | null | undefined,
): string {
  if (value == null) return '';
  const s = String(value).trim();
  if (!s) return '';

  const h = String(header).trim();

  if (isScreenerTextColumn(h)) {
    return s;
  }

  if (isAlreadyFormattedScreenerValue(s)) {
    return s;
  }

  const n = parseScreenerNumber(s);
  if (n == null) return s;

  if (BIG_NUMBER_HEADERS.test(h)) {
    return formatBigNumber(n);
  }

  if (PERCENT_HEADERS.test(h) || /%/.test(h)) {
    return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`;
  }

  if (RATIO_HEADERS.test(h)) {
    return n.toFixed(2);
  }

  if (PRICE_LIKE_HEADERS.test(h)) {
    return n.toFixed(2);
  }

  // Rel Volume / similar ratios that already look clean — leave with 2 decimals.
  if (/^rel\s*vol/i.test(h)) {
    return n.toFixed(2);
  }

  // Unknown numeric column: still trim floating-point noise but keep readable.
  return n.toFixed(2);
}

/**
 * Format every value in a screener row according to its header.
 * Empty / missing values stay empty so the table doesn't render `NaN`.
 */
export function formatScreenerRow(
  headers: string[],
  row: Record<string, string>,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const h of headers) {
    out[h] = formatScreenerCell(h, row[h]);
  }
  return out;
}
