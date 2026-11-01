import { Component } from '@angular/core';

import { ThemeMode, ThemeService } from '../../core/theme.service';

@Component({
  selector: 'hi-theme-toggle',
  templateUrl: './theme-toggle.component.html',
})
export class ThemeToggleComponent {
  readonly options: { mode: ThemeMode; label: string; icon: string }[] = [
    { mode: 'light', label: 'Light', icon: 'light_mode' },
    { mode: 'dark', label: 'Dark', icon: 'dark_mode' },
    { mode: 'system', label: 'System', icon: 'brightness_auto' },
  ];

  constructor(public theme: ThemeService) {}

  get current() {
    return this.options.find((o) => o.mode === this.theme.mode);
  }
}
