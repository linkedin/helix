import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, NgZone, OnDestroy, Optional } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';

export type ThemeMode = 'light' | 'dark' | 'system';

// Also read by the inline script in index.html
export const THEME_STORAGE_KEY = 'helix-theme';
export const DARK_THEME_CLASS = 'dark-theme';

const MODES: ThemeMode[] = ['light', 'dark', 'system'];

@Injectable({ providedIn: 'root' })
export class ThemeService implements OnDestroy {
  private currentMode: ThemeMode;
  private readonly dark = new BehaviorSubject<boolean>(false);
  private readonly systemDark?: MediaQueryList;
  private readonly onSystemChange = () => this.apply();

  readonly isDark$ = this.dark.pipe(distinctUntilChanged());

  constructor(
    @Inject(DOCUMENT) private document: Document,
    @Optional() private zone?: NgZone,
  ) {
    this.systemDark = document.defaultView?.matchMedia?.(
      '(prefers-color-scheme: dark)',
    );
    this.systemDark?.addEventListener?.('change', this.onSystemChange);
    this.currentMode = this.readStoredMode();
    this.apply();
  }

  get mode(): ThemeMode {
    return this.currentMode;
  }

  setMode(mode: ThemeMode) {
    try {
      this.document.defaultView?.localStorage.setItem(THEME_STORAGE_KEY, mode);
    } catch (e) {
      // storage can be unavailable (private mode); the choice still applies
    }
    this.currentMode = mode;
    // Cross-fade where supported; the callback runs outside the Angular zone
    const doc = this.document as Document & {
      startViewTransition?: (update: () => void) => unknown;
    };
    const reduceMotion = this.document.defaultView?.matchMedia?.(
      '(prefers-reduced-motion: reduce)',
    )?.matches;
    if (doc.startViewTransition && !reduceMotion) {
      doc.startViewTransition(() =>
        this.zone ? this.zone.run(() => this.apply()) : this.apply(),
      );
    } else {
      this.apply();
    }
  }

  ngOnDestroy() {
    this.systemDark?.removeEventListener?.('change', this.onSystemChange);
  }

  private readStoredMode(): ThemeMode {
    try {
      const stored = this.document.defaultView?.localStorage.getItem(
        THEME_STORAGE_KEY,
      ) as ThemeMode;
      return MODES.includes(stored) ? stored : 'light';
    } catch (e) {
      return 'light';
    }
  }

  private apply() {
    const dark =
      this.currentMode === 'dark' ||
      (this.currentMode === 'system' && !!this.systemDark?.matches);
    this.document.body.classList.toggle(DARK_THEME_CLASS, dark);
    this.dark.next(dark);
  }
}
