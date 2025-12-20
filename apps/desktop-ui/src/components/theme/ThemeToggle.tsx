'use client';

import * as React from 'react';

import { Switch } from '@/components/ui/switch';

const STORAGE_KEY = 'karios.theme';

type Theme = 'light' | 'dark';

function getTheme(): Theme {
  const el = document.documentElement;
  const t = el.dataset.theme;
  return t === 'dark' ? 'dark' : 'light';
}

function setTheme(theme: Theme) {
  document.documentElement.dataset.theme = theme;
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // ignore
  }
}

export function ThemeToggle() {
  const [theme, setThemeState] = React.useState<Theme>('light');

  React.useEffect(() => {
    setThemeState(getTheme());
  }, []);

  return (
    <Switch
      checked={theme === 'dark'}
      onCheckedChange={(checked) => {
        const next: Theme = checked ? 'dark' : 'light';
        setTheme(next);
        setThemeState(next);
      }}
      title={theme === 'dark' ? 'Switch to light' : 'Switch to dark'}
      aria-label="Toggle theme"
    />
  );
}


