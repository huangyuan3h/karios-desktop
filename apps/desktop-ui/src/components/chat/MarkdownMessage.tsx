import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { cn } from '@/lib/utils';

function normalizeMarkdownForRender(content: string): string {
  const s = (content ?? '').replaceAll('\r\n', '\n').replaceAll('\r', '\n');
  if (!s.trim()) return '';

  // Avoid modifying fenced code blocks.
  const parts = s.split('```');
  for (let i = 0; i < parts.length; i += 2) {
    // Insert blank lines before headings that are not at line start (e.g. "# title ## 1) ...").
    parts[i] = parts[i].replace(/([^\n])(?=#{2,6}\s)/g, '$1\n\n');
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
          table: ({ children }) => (
            <div className="my-3 overflow-x-auto rounded-md border border-[var(--k-border)] bg-[var(--k-surface)]">
              <table className="w-full border-collapse text-sm">{children}</table>
            </div>
          ),
          thead: ({ children }) => <thead className="bg-[var(--k-surface-2)]">{children}</thead>,
          th: ({ children }) => (
            <th className="whitespace-nowrap border-b border-[var(--k-border)] px-3 py-2 text-left font-medium">
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td className="align-top border-b border-[var(--k-border)] px-3 py-2">{children}</td>
          ),
          code: ({ children }) => (
            <code className="rounded bg-[var(--k-surface-2)] px-1 py-0.5 text-[0.9em]">{children}</code>
          ),
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


