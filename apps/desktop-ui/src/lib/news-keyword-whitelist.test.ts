import { describe, expect, it } from 'vitest';

import { filterNewsByKeyword, isNewsTitleWhitelisted } from './news-keyword-whitelist';

describe('isNewsTitleWhitelisted', () => {
  it('matches Chinese core keywords', () => {
    expect(isNewsTitleWhitelisted('英伟达发布新一代AI芯片')).toBe(true);
    expect(isNewsTitleWhitelisted('美联储宣布降息')).toBe(true);
    expect(isNewsTitleWhitelisted('今日原油价格上涨')).toBe(true);
    expect(isNewsTitleWhitelisted('台积电上调资本支出')).toBe(true);
  });

  it('matches English core keywords', () => {
    expect(isNewsTitleWhitelisted('Fed signals rate cut in September')).toBe(true);
    expect(isNewsTitleWhitelisted('Nvidia unveils new AI GPU')).toBe(true);
    expect(isNewsTitleWhitelisted('OPEC agrees to cut oil output')).toBe(true);
  });

  it('rejects irrelevant news', () => {
    expect(isNewsTitleWhitelisted('Chipotle launches new menu item')).toBe(false);
    expect(isNewsTitleWhitelisted('Former Peru president withdraws charges')).toBe(false);
    expect(isNewsTitleWhitelisted('Celebrity wedding announcement')).toBe(false);
  });

  it('handles empty input', () => {
    expect(isNewsTitleWhitelisted('')).toBe(false);
    expect(isNewsTitleWhitelisted(null as unknown as string)).toBe(false);
  });

  it('is case-insensitive', () => {
    expect(isNewsTitleWhitelisted('AI breakthrough')).toBe(true);
    expect(isNewsTitleWhitelisted('ai breakthrough')).toBe(true);
  });
});

describe('filterNewsByKeyword', () => {
  it('filters array of news items', () => {
    const items = [
      { title: '英伟达 AI 芯片大卖' },
      { title: 'Chipotle launches new menu' },
      { title: '美联储宣布降息 25bp' },
      { title: 'Former Peru president withdraws charges' },
      { title: 'WTI 原油突破 90 美元' },
    ];
    const out = filterNewsByKeyword(items);
    expect(out.map((x) => x.title)).toEqual([
      '英伟达 AI 芯片大卖',
      '美联储宣布降息 25bp',
      'WTI 原油突破 90 美元',
    ]);
  });

  it('handles empty / non-array input', () => {
    expect(filterNewsByKeyword([])).toEqual([]);
    expect(filterNewsByKeyword(null as unknown as Array<{ title?: string }>)).toEqual([]);
    expect(filterNewsByKeyword(undefined as unknown as Array<{ title?: string }>)).toEqual([]);
  });
});
