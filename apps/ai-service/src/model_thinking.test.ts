import { describe, expect, it } from 'vitest';

import {
  ThinkingStreamStripper,
  stripModelThinking,
  stripThinkingFromTextStream,
} from './model_thinking';

describe('stripModelThinking', () => {
  it('returns empty for empty input', () => {
    expect(stripModelThinking('')).toBe('');
  });

  it('passes through text without think tags', () => {
    expect(stripModelThinking('1. Oil prices rose\n2. Fed holds rates')).toBe(
      '1. Oil prices rose\n2. Fed holds rates',
    );
  });

  it('strips a complete <think> block and keeps the answer', () => {
    const text = `<think>
scanning headlines for finance relevance...
</think>

1. 油价上涨
2. 美联储按兵不动`;
    expect(stripModelThinking(text)).toBe('1. 油价上涨\n2. 美联储按兵不动');
  });

  it('strips <thinking> and <reasoning> variants', () => {
    expect(stripModelThinking('<thinking>x</thinking>\nhello')).toBe('hello');
    expect(stripModelThinking('<reasoning>y</reasoning>\nworld')).toBe('world');
    expect(stripModelThinking('<reason>z</reason>\nok')).toBe('ok');
  });

  it('tolerates whitespace inside tags and is case-insensitive', () => {
    expect(stripModelThinking('< Think >hidden</ Think >\nvisible')).toBe('visible');
  });

  it('drops unclosed think blocks (truncated MiniMax-style output)', () => {
    const text = `<think>
1. Houthis Claim Strikes... MAJOR
2. Debate Over Jewelry... not financial`;
    expect(stripModelThinking(text)).toBe('');
  });

  it('keeps prefix before an unclosed think block', () => {
    expect(stripModelThinking('prefix\n<think>\nincomplete')).toBe('prefix');
  });

  it('removes multiple think blocks', () => {
    expect(stripModelThinking('<think>a</think>mid<think>b</think>end')).toBe('midend');
  });

  it('removes orphan closing tags', () => {
    expect(stripModelThinking('answer</think>')).toBe('answer');
  });
});

describe('ThinkingStreamStripper', () => {
  it('strips think blocks split across chunks', () => {
    const s = new ThinkingStreamStripper();
    expect(s.push('<thi')).toBe('');
    expect(s.push('nk>secret')).toBe('');
    expect(s.push('</thin')).toBe('');
    expect(s.push('k>\n1. final')).toBe('\n1. final');
    expect(s.flush()).toBe('');
  });

  it('emits plain text immediately', () => {
    const s = new ThinkingStreamStripper();
    expect(s.push('hello ')).toBe('hello ');
    expect(s.push('world')).toBe('world');
    expect(s.flush()).toBe('');
  });

  it('drops trailing unclosed think on flush', () => {
    const s = new ThinkingStreamStripper();
    expect(s.push('ok\n<think>partial')).toBe('ok\n');
    expect(s.flush()).toBe('');
  });
});

describe('stripThinkingFromTextStream', () => {
  it('strips think content from a ReadableStream', async () => {
    const source = new ReadableStream<string>({
      start(controller) {
        controller.enqueue('<think>hide</think>');
        controller.enqueue('visible');
        controller.close();
      },
    });
    const cleaned = stripThinkingFromTextStream(source);
    const reader = cleaned.getReader();
    let out = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      out += value;
    }
    expect(out).toBe('visible');
  });
});
