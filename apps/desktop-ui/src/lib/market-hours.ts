export function getShanghaiTimeParts(now: Date = new Date()): {
  weekday: string;
  hour: number;
  minute: number;
} {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);
  const map = new Map(parts.map((p) => [p.type, p.value]));
  return {
    weekday: map.get('weekday') ?? '',
    hour: Number(map.get('hour') ?? 0),
    minute: Number(map.get('minute') ?? 0),
  };
}

export function getShanghaiTodayIso(now: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const map = new Map(parts.map((p) => [p.type, p.value]));
  const y = map.get('year') ?? '1970';
  const m = map.get('month') ?? '01';
  const d = map.get('day') ?? '01';
  return `${y}-${m}-${d}`;
}

export function getShanghaiMinutes(now: Date = new Date()): number {
  const { hour, minute } = getShanghaiTimeParts(now);
  return hour * 60 + minute;
}

export function isWeekdayShanghai(now: Date = new Date()): boolean {
  const { weekday } = getShanghaiTimeParts(now);
  return ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday);
}

export function isShanghaiTradingTime(now: Date = new Date()): boolean {
  if (!isWeekdayShanghai(now)) return false;
  const { hour, minute } = getShanghaiTimeParts(now);
  const minutes = hour * 60 + minute;
  const inMorning = minutes >= 9 * 60 + 30 && minutes <= 11 * 60 + 30;
  const inAfternoon = minutes >= 13 * 60 && minutes <= 15 * 60;
  const inLunch = minutes > 11 * 60 + 30 && minutes < 13 * 60;
  return inMorning || inAfternoon || inLunch;
}

/** Trading hours + after-hours until 20:00 (matches data-sync-service quote window). */
export function isShanghaiQuoteWindow(now: Date = new Date()): boolean {
  if (!isWeekdayShanghai(now)) return false;
  const { hour, minute } = getShanghaiTimeParts(now);
  const minutes = hour * 60 + minute;
  if (isShanghaiTradingTime(now)) return true;
  return minutes > 15 * 60 && minutes <= 20 * 60;
}

/** Alias used by Dashboard sync/refresh logic. */
export const isShanghaiSyncWindow = isShanghaiQuoteWindow;

/** Weekday 17:30–20:00 automation poll window. */
export function isAutomationPollWindow(now: Date = new Date()): boolean {
  if (!isWeekdayShanghai(now)) return false;
  const minutes = getShanghaiMinutes(now);
  return minutes >= 17 * 60 + 30 && minutes <= 20 * 60;
}
