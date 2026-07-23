import { describe, expect, it } from 'vitest';

import { ThinkingStreamStripper, stripModelThinking } from './strip-model-thinking';

describe('stripModelThinking', () => {
  it('strips MiniMax-style think blocks', () => {
    const text = `<think>
1. Houthis Claim Strikes... MAJOR
</think>

1. 油价受也门冲突影响上涨`;
    expect(stripModelThinking(text)).toBe('1. 油价受也门冲突影响上涨');
  });

  it('drops unclosed think content', () => {
    expect(stripModelThinking('<think>\nonly reasoning')).toBe('');
  });

  it('passes through clean text', () => {
    expect(stripModelThinking('1. Fed holds rates')).toBe('1. Fed holds rates');
  });
});

describe('ThinkingStreamStripper', () => {
  it('hides think chunks until close tag', () => {
    const s = new ThinkingStreamStripper();
    expect(s.push('<think>secret')).toBe('');
    expect(s.isInThink).toBe(true);
    expect(s.push('</think>\n答案')).toBe('\n答案');
    expect(s.isInThink).toBe(false);
    expect(s.flush()).toBe('');
  });
});
