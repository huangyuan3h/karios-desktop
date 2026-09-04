/**
 * Timeline window catalog (OPT-130).
 *
 * Gate windows (OOS2/train/valid) are the walk-forward rejector.
 * product_year and trailing are display-only — never a parameter gate.
 * holdout is read-only until the window is full.
 */

export type TimelineWindowRole = 'gate' | 'display' | 'readonly';

export type TimelineWindowId =
  | 'trailing'
  | 'product_year'
  | 'OOS2'
  | 'train'
  | 'valid'
  | 'holdout';

export type TimelineWindow = {
  id: TimelineWindowId;
  label: string;
  role: TimelineWindowRole;
  start: string;
  end: string;
  note: string;
};

/** Frozen product past-year used in clip4 vs core compare (not a rejector). */
export const PRODUCT_YEAR = { start: '2025-08-28', end: '2026-08-28' } as const;

/** Walk-forward rejector — AGENTS.md / run_walk_forward.py. */
export const WALK_FORWARD = {
  OOS2: { start: '2024-08-01', end: '2025-08-01' },
  train: { start: '2025-08-01', end: '2026-02-01' },
  valid: { start: '2026-03-01', end: '2026-08-07' },
} as const;

export const HOLDOUT_START = '2026-08-08';
export const HOLDOUT_PLANNED_END = '2027-02-08';

const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;

export function addCalendarYears(iso: string, years: number): string {
  if (!ISO_DAY.test(iso)) return iso;
  const [y, m, d] = iso.split('-').map(Number);
  const dt = new Date(Date.UTC(y, (m ?? 1) - 1, d ?? 1));
  dt.setUTCFullYear(dt.getUTCFullYear() + years);
  return dt.toISOString().slice(0, 10);
}

export function resolveTimelineWindows(todayIso: string): TimelineWindow[] {
  const holdoutEnd = todayIso < HOLDOUT_PLANNED_END ? todayIso : HOLDOUT_PLANNED_END;
  return [
    {
      id: 'trailing',
      label: '滚动过去一年',
      role: 'display',
      start: addCalendarYears(todayIso, -1),
      end: todayIso,
      note: 'trailing · 展示用 · 不当拒收闸',
    },
    {
      id: 'product_year',
      label: '产品过去一年',
      role: 'display',
      start: PRODUCT_YEAR.start,
      end: PRODUCT_YEAR.end,
      note: 'frozen 2025-08-28~2026-08-28 · 不当拒收闸',
    },
    {
      id: 'OOS2',
      label: '三窗 · OOS2',
      role: 'gate',
      start: WALK_FORWARD.OOS2.start,
      end: WALK_FORWARD.OOS2.end,
      note: '拒收闸 · 弱市年',
    },
    {
      id: 'train',
      label: '三窗 · train',
      role: 'gate',
      start: WALK_FORWARD.train.start,
      end: WALK_FORWARD.train.end,
      note: '拒收闸',
    },
    {
      id: 'valid',
      label: '三窗 · valid',
      role: 'gate',
      start: WALK_FORWARD.valid.start,
      end: WALK_FORWARD.valid.end,
      note: '拒收闸',
    },
    {
      id: 'holdout',
      label: 'holdout',
      role: 'readonly',
      start: HOLDOUT_START,
      end: holdoutEnd,
      note: '未满窗不调参',
    },
  ];
}

export function roleBadge(role: TimelineWindowRole): string {
  if (role === 'gate') return '拒收闸';
  if (role === 'readonly') return '只读';
  return '展示';
}
