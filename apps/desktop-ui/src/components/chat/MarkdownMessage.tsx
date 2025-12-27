import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { cn } from '@/lib/utils';

export function MarkdownMessage({ content, className }: { content: string; className?: string }) {
  return (
    <div
      className={cn(
        // Keep headings compact so long reports remain readable.
        'prose prose-zinc dark:prose-invert max-w-none',
        'prose-h1:text-2xl prose-h1:leading-tight prose-h1:my-3',
        'prose-h2:text-xl prose-h2:leading-tight prose-h2:my-3',
        'prose-h3:text-lg prose-h3:leading-tight prose-h3:my-2',
        className,
      )}
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
    </div>
  );
}


