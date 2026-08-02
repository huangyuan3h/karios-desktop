/**
 * Strip chain-of-thought wrappers that some OpenAI-compatible models
 * (e.g. MiniMax-M3) embed inside `message.content`.
 *
 * Supported tags: <think>, <thinking>, <reason>, <reasoning>
 * (case-insensitive; tolerates whitespace inside tags).
 */

const THINK_TAG_NAMES = 'think|thinking|reason|reasoning';

const THINK_BLOCK_RE = new RegExp(
  `<\\s*(?:${THINK_TAG_NAMES})\\s*>[\\s\\S]*?<\\s*/\\s*(?:${THINK_TAG_NAMES})\\s*>`,
  'gi',
);

const UNCLOSED_THINK_RE = new RegExp(`<\\s*(?:${THINK_TAG_NAMES})\\s*>[\\s\\S]*$`, 'i');

const ORPHAN_CLOSE_RE = new RegExp(`<\\s*/\\s*(?:${THINK_TAG_NAMES})\\s*>`, 'gi');

const OPEN_THINK_RE = new RegExp(`<\\s*(?:${THINK_TAG_NAMES})\\s*>`, 'i');
const CLOSE_THINK_RE = new RegExp(`<\\s*/\\s*(?:${THINK_TAG_NAMES})\\s*>`, 'i');

/** Longest plausible incomplete open/close tag we may need to hold across stream chunks. */
const MAX_PARTIAL_TAG_LEN = 24;

export function stripModelThinking(text: string): string {
  if (!text) return '';
  let out = text.replace(THINK_BLOCK_RE, '');
  // Truncated responses often leave an unclosed … block with no final answer.
  out = out.replace(UNCLOSED_THINK_RE, '');
  out = out.replace(ORPHAN_CLOSE_RE, '');
  return out.trim();
}

/**
 * Strip a leading/trailing markdown code fence from a JSON response.
 *
 * Some chat models (e.g. MiniMax-M3) wrap JSON in ```json ... ``` even
 * when no JSON-schema response_format was requested. Downstream
 * `json.loads` chokes on the leading ``` with
 * `Expecting value: line 1 column 1 (char 0)`.
 *
 * Only meant for routes whose callers will parse the result as JSON;
 * chat-style routes should leave code fences intact.
 */
export function stripJsonCodeFence(text: string): string {
  if (!text) return '';
  const fenced = /^```(?:json|JSON)?\s*\n?([\s\S]*?)\n?```\s*$/.exec(text);
  if (fenced) return fenced[1].trim();
  return text
    .replace(/^```(?:json|JSON)?\s*\n?/, '')
    .replace(/\n?```\s*$/, '')
    .trim();
}

/**
 * Stateful stripper for streaming model text. Emits only content outside think blocks.
 * Call {@link flush} when the upstream stream ends.
 */
export class ThinkingStreamStripper {
  private buf = '';
  private inThink = false;

  get isInThink(): boolean {
    return this.inThink;
  }

  push(chunk: string): string {
    if (!chunk) return '';
    this.buf += chunk;
    let out = '';

    while (this.buf.length > 0) {
      if (this.inThink) {
        const closeMatch = this.buf.match(CLOSE_THINK_RE);
        if (!closeMatch || closeMatch.index === undefined) {
          // Hold a short suffix in case the close tag is split across chunks.
          if (this.buf.length > MAX_PARTIAL_TAG_LEN) {
            this.buf = this.buf.slice(-MAX_PARTIAL_TAG_LEN);
          }
          break;
        }
        this.buf = this.buf.slice(closeMatch.index + closeMatch[0].length);
        this.inThink = false;
        continue;
      }

      const openMatch = this.buf.match(OPEN_THINK_RE);
      if (!openMatch || openMatch.index === undefined) {
        // Hold a trailing '<'… fragment that might become an open tag.
        const holdFrom = findPartialTagHoldIndex(this.buf);
        out += this.buf.slice(0, holdFrom);
        this.buf = this.buf.slice(holdFrom);
        break;
      }

      out += this.buf.slice(0, openMatch.index);
      this.buf = this.buf.slice(openMatch.index + openMatch[0].length);
      this.inThink = true;
    }

    return out;
  }

  flush(): string {
    if (this.inThink) {
      this.buf = '';
      this.inThink = false;
      return '';
    }
    const rest = this.buf;
    this.buf = '';
    return rest;
  }
}

function findPartialTagHoldIndex(s: string): number {
  const lt = s.lastIndexOf('<');
  if (lt < 0) return s.length;
  const tail = s.slice(lt);
  // Hold an incomplete tag fragment so open tags split across chunks are not leaked.
  if (!tail.includes('>') && /^<\/?[a-zA-Z\s]*$/.test(tail)) return lt;
  return s.length;
}

/** Wrap a text stream, stripping think blocks as chunks arrive. */
export function stripThinkingFromTextStream(
  source: ReadableStream<string>,
): ReadableStream<string> {
  const stripper = new ThinkingStreamStripper();
  const reader = source.getReader();

  return new ReadableStream<string>({
    async pull(controller) {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          const rest = stripper.flush();
          if (rest) controller.enqueue(rest);
          controller.close();
          return;
        }
        const cleaned = stripper.push(value ?? '');
        if (cleaned) {
          controller.enqueue(cleaned);
          return;
        }
        // Still inside a think block — keep reading until we have visible text or EOF.
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}
