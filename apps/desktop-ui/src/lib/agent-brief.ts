/**
 * Agent judgment brief — the agent-first pack behind Sync & Copy.
 *
 * `GET /v1/agent/judgment-brief` is the single JSON the decision agent (乐乐)
 * reads: decision contract + gate + book + market facts + the frozen backtest
 * comparison. Research reports (研报), Alpha Radar trends/catalyst stocks and
 * the Auto-QA penalties are intentionally NOT part of it.
 *
 * The clipboard button copies `judgment-brief.md` — the same payload rendered
 * as compact Markdown — so the copied text can never drift from the API the
 * agent reads.
 */
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

export type JudgmentBriefMode = 'full' | 'compact';

export const JUDGMENT_BRIEF_PATH = '/v1/agent/judgment-brief';

/** Raised when the backend does not serve the brief yet (old :4330 process). */
export class JudgmentBriefUnavailableError extends Error {
  constructor(status: number, detail: string) {
    super(
      `决策简报接口不可用（HTTP ${status}）：data-sync 服务需要重启才能提供 ` +
        `GET /v1/agent/judgment-brief。旧进程没有该路由，不会回退到旧的 Alpha/研报包。` +
        (detail ? ` [${detail.slice(0, 200)}]` : ''),
    );
    this.name = 'JudgmentBriefUnavailableError';
  }
}

export function buildJudgmentBriefUrl(opts?: {
  mode?: JudgmentBriefMode;
  tradeDate?: string;
}): string {
  const params = new URLSearchParams();
  params.set('mode', opts?.mode ?? 'full');
  if (opts?.tradeDate) params.set('tradeDate', opts.tradeDate);
  return `${DATA_SYNC_BASE_URL}${JUDGMENT_BRIEF_PATH}.md?${params.toString()}`;
}

/**
 * Fetch the brief as Markdown (the exact text the Sync & Copy button copies).
 * A 404 means the running data-sync service predates the route — surface that
 * clearly instead of silently rebuilding the old alpha/研报 pack.
 */
export async function fetchJudgmentBriefMarkdown(opts?: {
  mode?: JudgmentBriefMode;
  tradeDate?: string;
  timeoutMs?: number;
}): Promise<string> {
  const url = buildJudgmentBriefUrl(opts);
  const res = await fetch(url, {
    cache: 'no-store',
    signal: AbortSignal.timeout(opts?.timeoutMs ?? 120_000),
  });
  if (res.status === 404) {
    const detail = await res.text().catch(() => '');
    throw new JudgmentBriefUnavailableError(404, detail);
  }
  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(
      `决策简报生成失败：HTTP ${res.status} ${res.statusText}${detail ? `: ${detail}` : ''}`,
    );
  }
  const text = await res.text();
  if (!text.trim()) {
    throw new Error('决策简报返回为空（data-sync 服务可能刚重启，请稍后重试）');
  }
  return text;
}
