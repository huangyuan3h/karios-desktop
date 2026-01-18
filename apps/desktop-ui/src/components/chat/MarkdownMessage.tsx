import type { ReactNode } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { cn } from '@/lib/utils';

function normalizeMarkdownForRender(content: string): string {
  const s = (content ?? '').replaceAll('\r\n', '\n').replaceAll('\r', '\n');
  if (!s.trim()) return '';

  // Avoid modifying fenced code blocks.
  const parts = s.split('```');
  for (let i = 0; i < parts.length; i += 2) {
    // If model puts analysis text on the same line as known headings, split it.
    // Example: "## 0 结果摘要 主线偏向..." -> "## 0 结果摘要\n\n主线偏向..."
    // Keep this conservative: only apply to our fixed headings.
    parts[i] = parts[i].replace(
      /^(##\s*(?:0|1|2|3|4|5)\s*(?:结果摘要|资金板块|候选Top3|持仓计划|执行要点|条件单总表))\s+([^\n#].*)$/gm,
      '$1\n\n$2',
    );

    // Insert blank lines before headings that are not at line start (e.g. "# title ## 1) ...").
    parts[i] = parts[i].replace(/([^\n])(?=#{2,6}\s)/g, '$1\n\n');

    // Fix "one-line tables" produced by LLMs so remark-gfm can parse them.
    // Typical bad patterns:
    // - "... | A | B | C ||---|---|---|| 1 | ..."  (rows concatenated with "||" or "| |")
    // Step 1: split concatenated rows (only outside code fences).
    let seg = parts[i];
    seg = seg.replace(/\|\|\s*(?=[-:]{3,})/g, '|\n|'); // header -> separator
    seg = seg.replace(/\|\|\s*(?=\d+\s*\|)/g, '|\n|'); // separator -> first data row (rank starts with number)
    seg = seg.replace(/\|\s+\|/g, '|\n|'); // general row boundary written as "| |"
    // Step 2: ensure the table starts at line beginning (header row + next separator row).
    seg = seg.replace(/([^\n])\s*(\|[^\n]*\n\|\s*[-:]{3,}[^\n]*)/g, '$1\n\n$2');
    parts[i] = seg;
  }
  return parts.join('```');
}

export function MarkdownMessage({ content, className }: { content: string; className?: string }) {
  const normalized = normalizeMarkdownForRender(content);
  return (
    <div
      className={cn(
        // Keep headings compact so long reports remain readable.
        'prose prose-zinc dark:prose-invert max-w-none',
        'prose-h1:text-2xl prose-h1:leading-tight prose-h1:my-3',
        'prose-h2:text-xl prose-h2:leading-tight prose-h2:my-3',
        'prose-h3:text-lg prose-h3:leading-tight prose-h3:my-2',
        'prose-p:leading-relaxed prose-p:my-2',
        'prose-li:my-1',
        'prose-hr:my-4',
        className,
      )}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          pre: ({ children }) => (
            <pre className="not-prose my-3 overflow-x-auto rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3 text-xs leading-relaxed text-[var(--k-text)]">
              {children}
            </pre>
          ),
          table: ({ children }) => (
            <div className="not-prose my-3 overflow-x-auto rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] shadow-sm">
              <table className="m-0 w-full border-collapse text-sm">{children}</table>
            </div>
          ),
          thead: ({ children }) => (
            <thead className="bg-[var(--k-surface-2)] align-middle">{children}</thead>
          ),
          th: ({ children }) => (
            <th className="whitespace-nowrap border-b border-[var(--k-border)] px-3 py-2 text-left font-medium">
              {children}
            </th>
          ),
          tbody: ({ children }) => <tbody className="divide-y divide-[var(--k-border)]">{children}</tbody>,
          tr: ({ children }) => <tr className="odd:bg-[var(--k-surface)] even:bg-[var(--k-surface-2)]">{children}</tr>,
          td: ({ children }) => (
            <td className="align-top border-b border-[var(--k-border)] px-3 py-2 whitespace-normal break-words">
              {children}
            </td>
          ),
          code: (props) => {
            // react-markdown's typings don't expose `inline` on this component prop, but it exists at runtime.
            const p = props as unknown as { inline?: boolean; className?: string; children?: ReactNode };
            const inline = Boolean(p.inline);
            if (inline) {
              return (
                <code className="rounded bg-[var(--k-surface-2)] px-1 py-0.5 text-[0.9em] text-[var(--k-text)]">
                  {p.children}
                </code>
              );
            }
            return <code className={cn('text-xs text-[var(--k-text)]', p.className)}>{p.children}</code>;
          },
          blockquote: ({ children }) => (
            <blockquote className="border-l-4 border-[var(--k-border)] pl-3 text-[var(--k-muted)]">
              {children}
            </blockquote>
          ),
        }}
      >
        {normalized}
      </ReactMarkdown>
    </div>
  );
}


