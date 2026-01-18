'use client';

import * as React from 'react';
import {
  Bold,
  Code,
  Italic,
  Link2,
  List,
  ListOrdered,
  Quote,
  Redo2,
  Strikethrough,
  Underline,
  Undo2,
} from 'lucide-react';
import { createPlateEditor, Plate, PlateContent, useEditorRef } from 'platejs/react';
import type { SlateEditor, Value } from 'platejs';

import {
  BaseBasicBlocksPlugin,
  BaseBasicMarksPlugin,
  BaseBlockquotePlugin,
  BaseHeadingPlugin,
  BaseUnderlinePlugin,
} from '@platejs/basic-nodes';
import { BaseCodeBlockPlugin } from '@platejs/code-block';
import { BaseLinkPlugin, upsertLink } from '@platejs/link';
import { BaseListPlugin, toggleList } from '@platejs/list';
import { MarkdownPlugin } from '@platejs/markdown';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { cn } from '@/lib/utils';

function ToolbarButton({
  title,
  onClick,
  active,
  disabled,
  children,
}: {
  title: string;
  onClick: () => void;
  active?: boolean;
  disabled?: boolean;
  children: React.ReactNode;
}) {
  return (
    <Button
      type="button"
      size="sm"
      variant={active ? 'secondary' : 'ghost'}
      className={cn(
        'h-8 w-8 p-0',
        active
          ? 'bg-[var(--k-surface)] text-[var(--k-text)] hover:bg-[var(--k-surface)]'
          : 'text-[var(--k-muted)] hover:bg-[var(--k-surface)] hover:text-[var(--k-text)]',
      )}
      title={title}
      aria-label={title}
      disabled={disabled}
      onClick={() => onClick()}
    >
      {children}
    </Button>
  );
}

function PlateToolbar() {
  // Plate's `useEditorRef` is intentionally generic; cast to access runtime editor APIs for toolbar.
  const editor = useEditorRef() as unknown as {
    undo: () => void;
    redo: () => void;
    history?: { undos?: unknown[]; redos?: unknown[] };
    tf: { toggleMark: (k: string) => void; toggleBlock: (k: string) => void; focus: () => void };
    getMarks?: () => Record<string, unknown> | null | undefined;
  };
  const marks = editor.getMarks?.() ?? {};
  const hasMark = (k: string) => Boolean(marks?.[k]);

  const [block, setBlock] = React.useState<'p' | 'h1' | 'h2' | 'h3'>('p');

  React.useEffect(() => {
    // Best-effort: keep dropdown stable; we don't attempt deep selection analysis here.
    // User can still change it to apply the block type.
  }, []);

  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2">
      <ToolbarButton title="Undo" onClick={() => editor.undo()} disabled={!editor.history?.undos?.length}>
        <Undo2 className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton title="Redo" onClick={() => editor.redo()} disabled={!editor.history?.redos?.length}>
        <Redo2 className="h-4 w-4" />
      </ToolbarButton>

      <div className="mx-1 h-6 w-px bg-[var(--k-border)]" />

      <Select
        value={block}
        onValueChange={(v) => {
          const next = v as 'p' | 'h1' | 'h2' | 'h3';
          setBlock(next);
          editor.tf.toggleBlock(next);
          editor.tf.focus();
        }}
      >
        <SelectTrigger className="h-8 w-[140px] bg-[var(--k-surface)]">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="p">Paragraph</SelectItem>
          <SelectItem value="h1">Heading 1</SelectItem>
          <SelectItem value="h2">Heading 2</SelectItem>
          <SelectItem value="h3">Heading 3</SelectItem>
        </SelectContent>
      </Select>

      <div className="mx-1 h-6 w-px bg-[var(--k-border)]" />

      <ToolbarButton
        title="Bold"
        onClick={() => {
          editor.tf.toggleMark('bold');
          editor.tf.focus();
        }}
        active={hasMark('bold')}
      >
        <Bold className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Italic"
        onClick={() => {
          editor.tf.toggleMark('italic');
          editor.tf.focus();
        }}
        active={hasMark('italic')}
      >
        <Italic className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Underline"
        onClick={() => {
          editor.tf.toggleMark('underline');
          editor.tf.focus();
        }}
        active={hasMark('underline')}
      >
        <Underline className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Strikethrough"
        onClick={() => {
          editor.tf.toggleMark('strikethrough');
          editor.tf.focus();
        }}
        active={hasMark('strikethrough')}
      >
        <Strikethrough className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Inline code"
        onClick={() => {
          editor.tf.toggleMark('code');
          editor.tf.focus();
        }}
        active={hasMark('code')}
      >
        <Code className="h-4 w-4" />
      </ToolbarButton>

      <div className="mx-1 h-6 w-px bg-[var(--k-border)]" />

      <ToolbarButton
        title="Blockquote"
        onClick={() => {
          editor.tf.toggleBlock('blockquote');
          editor.tf.focus();
        }}
      >
        <Quote className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Code block"
        onClick={() => {
          editor.tf.toggleBlock('code_block');
          editor.tf.focus();
        }}
      >
        <Code className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Bullet list"
        onClick={() => {
          toggleList(editor as unknown as SlateEditor, { listStyleType: 'disc' }, {});
          editor.tf.focus();
        }}
      >
        <List className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Ordered list"
        onClick={() => {
          toggleList(editor as unknown as SlateEditor, { listStyleType: 'decimal' }, {});
          editor.tf.focus();
        }}
      >
        <ListOrdered className="h-4 w-4" />
      </ToolbarButton>
      <ToolbarButton
        title="Insert link"
        onClick={() => {
          const url = window.prompt('Link URL');
          const u = (url ?? '').trim();
          if (!u) return;
          upsertLink(editor as unknown as SlateEditor, { url: u });
          editor.tf.focus();
        }}
      >
        <Link2 className="h-4 w-4" />
      </ToolbarButton>
    </div>
  );
}

export function PlateJournalEditor({
  initialMarkdown,
  onMarkdownChange,
  className,
}: {
  initialMarkdown: string;
  onMarkdownChange: (markdown: string) => void;
  className?: string;
}) {
  const editor = React.useMemo(() => {
    return createPlateEditor({
      plugins: [
        BaseBasicBlocksPlugin,
        BaseBasicMarksPlugin,
        BaseHeadingPlugin,
        BaseUnderlinePlugin,
        BaseBlockquotePlugin,
        BaseListPlugin,
        BaseLinkPlugin,
        BaseCodeBlockPlugin,
        MarkdownPlugin,
      ],
      value: (e) =>
        // MarkdownPlugin augments `editor.api` at runtime; TS types don't include it by default.
        (e as unknown as { api: { markdown: { deserialize: (md: string) => Value } } }).api.markdown.deserialize(
          initialMarkdown || '',
        ),
    });
    // initialMarkdown is intentionally captured once; reloading content is handled by keying parent.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className={cn('rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)]', className)}>
      <Plate
        editor={editor}
        onChange={() => {
          // MarkdownPlugin augments `editor.api` at runtime; TS types don't include it by default.
          const md = (editor as unknown as { api: { markdown: { serialize: () => string } } }).api.markdown.serialize();
          onMarkdownChange(md);
        }}
      >
        <PlateToolbar />
        <div className="p-3">
          <PlateContent className="min-h-[420px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2 text-sm outline-none" />
        </div>
      </Plate>
    </div>
  );
}

