import 'zone.js';
import 'zone.js/dist/zone-testing';
import { TestBed } from '@angular/core/testing';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatMenuModule } from '@angular/material/menu';
import { MatTooltipModule } from '@angular/material/tooltip';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { afterEach, describe, expect, it } from '@jest/globals';

import { DARK_THEME_CLASS, ThemeService } from '../../core/theme.service';
import { ThemeToggleComponent } from './theme-toggle.component';

describe('ThemeToggleComponent', () => {
  afterEach(() => {
    localStorage.clear();
    document.body.classList.remove(DARK_THEME_CLASS);
  });

  it('offers light, dark and system and applies the picked one', () => {
    TestBed.configureTestingModule({
      imports: [
        NoopAnimationsModule,
        MatButtonModule,
        MatIconModule,
        MatMenuModule,
        MatTooltipModule,
      ],
      declarations: [ThemeToggleComponent],
    });
    const fixture = TestBed.createComponent(ThemeToggleComponent);
    fixture.detectChanges();

    const trigger: HTMLElement = fixture.nativeElement.querySelector('button');
    expect(trigger.getAttribute('aria-label')).toBe('Color theme: Light');
    expect(trigger.querySelector('mat-icon').textContent.trim()).toBe(
      'dark_mode',
    );

    trigger.click();
    fixture.detectChanges();
    const items = Array.from(
      document.querySelectorAll('[role="menuitemradio"]'),
    ) as HTMLElement[];
    expect(items.map((i) => i.querySelector('span').textContent)).toEqual([
      'Light',
      'Dark',
      'System',
    ]);
    expect(items.map((i) => i.getAttribute('aria-checked'))).toEqual([
      'true',
      'false',
      'false',
    ]);

    items[1].click();
    fixture.detectChanges();
    expect(TestBed.inject(ThemeService).mode).toBe('dark');
    expect(document.body.classList.contains(DARK_THEME_CLASS)).toBe(true);
    expect(trigger.getAttribute('aria-label')).toBe('Color theme: Dark');
    expect(trigger.querySelector('mat-icon').textContent.trim()).toBe(
      'light_mode',
    );
  });
});
