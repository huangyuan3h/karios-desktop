export const ALPHA_RADAR_V4_SYSTEM_PROMPT = `# Role: Alpha Catalyst Evaluation Engine (V4 Dual-Core)
You process global tech news, domestic macro policy, and commodity cycle RSS feeds.
Your goal: identify catalysts that can drive multi-billion-yuan A-share main waves. Reject marginal noise.

# Mandatory Actions
For each item, assign exactly one Driver_Type and grade Catalyst_Grade using the rules below.
Do NOT output Catalyst_Grade B. If an item only qualifies as B, omit it entirely.

## Driver 1: Global_Tech (global tech resonance)
- S: Global megacap strategic inflection (e.g. $10B+ capex shift), disruptive breakthrough, critical infra shortage.
- A: Major product launch, mid-size order catalyst.
- B: Concept hype, minor tech update without fundamentals. (DROP — do not output)

## Driver 2: Domestic_Policy (domestic macro & industrial policy)
- S: National-level, systemic, real money — PBoC surprise easing, trillion-scale special bonds, Politburo-level industry mandate.
- A: Ministry-level policy — MIIT/NDRC concrete standards, subsidy rules with funding.
- B: Local subsidies, leader visits, slogan documents without funding. (DROP — do not output)

## Driver 3: Cycle_Reversal (industry cycle & commodities)
- S: Post-crisis rebirth — BOTH (1) prolonged losses → capacity exit/bankruptcy AND (2) spot/futures prices rising sharply.
- A: Inventory at historic lows, price stabilizes; OR major supply shock (strike, blockade).
- B: Routine 1–2% commodity moves, routine maintenance shutdowns. (DROP — do not output)

# Output Constraints
1. Be cold and objective. Empty trends array is valid when nothing meets S or A.
2. a_share_mapping: 1–3 pure-play A-share leaders (name or 6-digit code). No fringe stocks.
3. logic_summary: max 30 Chinese characters — causal chain only.
4. event_focus: factual statement only; no speculation.
5. Return valid JSON matching the schema. No markdown fences.
6. Do NOT output chain-of-thought, <think>, <thinking>, or any reasoning tags — JSON only.`;

export const ALPHA_RADAR_V4_JSON_FIELDS = `- macro_theme: standardized theme bucket (e.g. "光通信超级周期", "全球铜供给挤压")
- driver_type: Global_Tech | Domestic_Policy | Cycle_Reversal
- catalyst_grade: S | A only (never B)
- event_focus: factual evidence (1-2 sentences)
- a_share_mapping: 1-3 A-share leader names or 6-digit codes
- logic_summary: causal logic, max 30 Chinese characters`;

export type AlphaRadarDocumentInput = {
  documentId?: string;
  title: string;
  url: string;
  category: string;
  summary?: string | null;
};

export function buildExtractInstruction(params: {
  category: string;
  title: string;
  sourceUrl: string;
  text: string;
}): string {
  const { category, title, sourceUrl, text } = params;
  return (
    `Source category: ${category}\n` +
    `Title: ${title}\n` +
    `URL: ${sourceUrl}\n\n` +
    'Task: Extract up to 3 highest-signal S/A catalysts from this document.\n' +
    'Return JSON: {"trends":[...]} with fields:\n' +
    `${ALPHA_RADAR_V4_JSON_FIELDS}\n\n` +
    'Document text:\n' +
    text
  );
}

export function buildExtractBatchInstruction(docs: AlphaRadarDocumentInput[]): string {
  const digest = docs
    .map(
      (d, idx) =>
        `### Item ${idx}\n` +
        `DocumentId: ${d.documentId ?? ''}\n` +
        `Category: ${d.category}\n` +
        `Title: ${d.title}\n` +
        `URL: ${d.url}\n` +
        `Summary: ${(d.summary || '').trim() || '(none)'}\n`,
    )
    .join('\n');

  return (
    `Batch size: ${docs.length} items\n\n` +
    'Task: Extract up to 8 distinct S/A catalysts across ALL items (deduplicate similar themes).\n' +
    'Omit any item that only qualifies as B-grade.\n' +
    'Return JSON: {"trends":[{"macro_theme","driver_type","catalyst_grade","event_focus","a_share_mapping","logic_summary","source_index"}]}\n' +
    `${ALPHA_RADAR_V4_JSON_FIELDS}\n` +
    'source_index = 0-based item number.\n\n' +
    digest
  );
}

export const ALPHA_RADAR_V4_JSON_SUFFIX =
  '\n\nOutput ONLY one JSON object with key "trends" (array). No markdown fences. No <think> tags. Empty array is valid.';

export function buildMapCnSystemPrompt(params: {
  candidateCount: number;
  seedSymbols?: string[];
}): string {
  const { candidateCount, seedSymbols = [] } = params;
  const seedHint =
    seedSymbols.length > 0
      ? `LLM-preferred symbols/names to prioritize: ${seedSymbols.join(', ')}. `
      : '';
  return (
    'You validate and map global/cycle/policy catalysts to pure-play A-share leaders. ' +
    seedHint +
    (candidateCount > 0
      ? 'Pick at most 2 symbols ONLY from the candidate list unless external context strongly supports a listed name. '
      : 'No local candidates were found. You MAY suggest up to 2 well-known A-share leaders using your knowledge, but confidence must be <= 0.45 and rationale must say manual review required. ') +
    'Symbol format must be CN:xxxxxx (6-digit ticker). ' +
    'Prefer industry purity over size. Mark low confidence when evidence is weak. ' +
    'AVOID system integrators / OEM assemblers. Prefer upstream component/material leaders. ' +
    'Return valid JSON only. No markdown fences. No <think> or chain-of-thought tags.'
  );
}

export function buildMapCnInstruction(params: {
  trend: Record<string, unknown>;
  candidates: unknown[];
  externalContext?: string | null;
  knowledgeFallback: boolean;
  seedSymbols?: string[];
}): string {
  const { trend, candidates, externalContext, knowledgeFallback, seedSymbols = [] } = params;
  return (
    'Trend JSON:\n' +
    JSON.stringify(trend) +
    (seedSymbols.length ? `\n\nPreferred A-share mapping from extract step:\n${JSON.stringify(seedSymbols)}` : '') +
    '\n\nCandidate A-shares:\n' +
    (candidates.length ? JSON.stringify(candidates) : '(empty — use cautious knowledge fallback if allowed)') +
    (externalContext ? `\n\nExternal search context:\n${externalContext}` : '') +
    (knowledgeFallback
      ? '\n\nIf candidates are empty, return best-effort CN: symbols with confidence <= 0.45.'
      : '') +
    '\n\nReturn cnSymbols (max 2) with symbol (CN:xxxxxx), name, confidence (0-1), rationale (Chinese).'
  );
}
