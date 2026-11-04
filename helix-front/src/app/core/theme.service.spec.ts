import { afterEach, beforeEach, describe, expect, it } from '@jest/globals';

import {
  DARK_THEME_CLASS,
  THEME_STORAGE_KEY,
  ThemeService,
} from './theme.service';

describe('ThemeService', () => {
  let systemDark: boolean;
  let reducedMotion: boolean;
  let listeners: Array<() => void>;
  const hasDarkClass = () => document.body.classList.contains(DARK_THEME_CLASS);

  beforeEach(() => {
    systemDark = false;
    reducedMotion = false;
    listeners = [];
    localStorage.clear();
    document.body.classList.remove(DARK_THEME_CLASS);
    (window as any).matchMedia = (media: string) => ({
      media,
      get matches() {
        return media.includes('reduced-motion') ? reducedMotion : systemDark;
      },
      addEventListener: (_: string, cb: () => void) => listeners.push(cb),
      removeEventListener: () => {},
    });
  });

  afterEach(() => {
    delete (window as any).matchMedia;
    delete (document as any).startViewTransition;
  });

  it('cross-fades the switch unless reduced motion is on', () => {
    const service = new ThemeService(document);
    const updates: Array<() => void> = [];
    (document as any).startViewTransition = (cb: () => void) =>
      updates.push(cb);

    service.setMode('dark');
    expect(hasDarkClass()).toBe(false);
    updates[0]();
    expect(hasDarkClass()).toBe(true);

    reducedMotion = true;
    service.setMode('light');
    expect(updates.length).toBe(1);
    expect(hasDarkClass()).toBe(false);
  });

  it('defaults to light, applies and persists the chosen mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'bogus');
    const service = new ThemeService(document);
    expect(service.mode).toBe('light');
    expect(hasDarkClass()).toBe(false);

    service.setMode('dark');
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
    expect(hasDarkClass()).toBe(true);
    expect(new ThemeService(document).mode).toBe('dark');

    service.setMode('light');
    expect(hasDarkClass()).toBe(false);
  });

  it('follows the system preference live in system mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'system');
    const service = new ThemeService(document);
    const seen: boolean[] = [];
    service.isDark$.subscribe((dark) => seen.push(dark));
    expect(hasDarkClass()).toBe(false);

    systemDark = true;
    listeners.forEach((cb) => cb());
    expect(hasDarkClass()).toBe(true);
    expect(seen).toEqual([false, true]);

    service.setMode('light');
    expect(hasDarkClass()).toBe(false);
  });
});
