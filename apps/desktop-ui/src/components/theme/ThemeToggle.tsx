'use client';

import * as React from 'react';
import { Moon, Sun } from 'lucide-react';

import { Button } from '@/components/ui/button';

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
    <Button
      variant="secondary"
      size="sm"
      className="h-9 w-9 rounded-full p-0"
      onClick={() => {
        const next: Theme = theme === 'dark' ? 'light' : 'dark';
        setTheme(next);
        setThemeState(next);
      }}
      title={theme === 'dark' ? 'Switch to light' : 'Switch to dark'}
      aria-label="Toggle theme"
    >
      {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
    </Button>
  );
}


