import 'zone.js';
import 'zone.js/dist/zone-testing';

import { Component, NO_ERRORS_SCHEMA } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { describe, expect, it } from '@jest/globals';

import { snapshotFixture, withResources } from '../testing/snapshot.fixture';
import { CapacityHeatmapComponent } from './capacity-heatmap.component';

@Component({
  template: `<hi-capacity-heatmap
    [snapshot]="snapshot"
    [key]="key"
    [topStateOnly]="top"
    [mode]="mode"
    [byZone]="byZone"
  ></hi-capacity-heatmap>`,
})
class HostComponent {
  snapshot = snapshotFixture();
  key = 'CU';
  top = false;
  mode: 'load' | 'median' = 'load';
  byZone = true;
}

describe('CapacityHeatmapComponent', () => {
  // jsdom does not scroll
  Element.prototype.scrollIntoView = () => {};

  function create(snapshot = snapshotFixture()) {
    TestBed.configureTestingModule({
      declarations: [HostComponent, CapacityHeatmapComponent],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(HostComponent);
    fixture.componentInstance.snapshot = snapshot;
    fixture.detectChanges();
    return fixture;
  }

  function tile(html: HTMLElement, name: string): HTMLElement {
    return html.querySelector(`.tile[data-instance="${name}"]`);
  }

  function legend(html: HTMLElement) {
    return Array.from(html.querySelectorAll('.legend-item')).map((item) => [
      item.getAttribute('data-band'),
      item.querySelector('b').textContent,
    ]);
  }

  it('sizes each zone to its own instances, and only a full zone grows', () => {
    const html: HTMLElement = create().nativeElement;
    const zone = (name: string) =>
      html.querySelector(`[data-zone="${name}"]`) as HTMLElement;

    // z1 and z2 hold one instance, z3 two, and the shared column count is 4
    expect(zone('z1').style.getPropertyValue('--hi-zone-columns')).toBe('1');
    expect(zone('z3').style.getPropertyValue('--hi-zone-columns')).toBe('2');
    expect(html.querySelectorAll('.zone.full').length).toBe(0);
  });

  it('renders zones in name order, shaded cells and legend counts', () => {
    const html: HTMLElement = create().nativeElement;

    expect(
      Array.from(html.querySelectorAll('.zone')).map((z) =>
        z.getAttribute('data-zone'),
      ),
    ).toEqual(['z1', 'z2', 'z3']);
    expect(html.querySelectorAll('.tile').length).toBe(4);
    expect(
      html.querySelector('[data-zone="z3"] .zone-head').textContent,
    ).toContain('avg 120%');
    const n4 = tile(html, 'n4');
    expect(n4.className).toContain('shade-unknown');
    expect(n4.textContent.trim()).toBe('?');
    expect(n4.classList.contains('offline')).toBe(true);
    expect(n4.classList.contains('op')).toBe(true);
    expect(n4.getAttribute('aria-label')).toBe(
      'n4, z3, CU unknown, not live, operation DISABLE',
    );
    const n3 = tile(html, 'n3');
    expect(n3.className).toContain('shade-over');
    expect(n3.classList.contains('over')).toBe(true);
    expect(n3.textContent).toContain('120%');
    expect(n3.querySelector('.err-mark')).not.toBeNull();
    expect(tile(html, 'n1').querySelector('.err-mark')).toBeNull();
    expect(legend(html)).toEqual([
      ['unknown', '1'],
      ['low', '1'],
      ['mid', '0'],
      ['high', '0'],
      ['hot', '1'],
      ['over', '1'],
    ]);
  });

  it('follows the capacity key and the top state toggle', () => {
    const fixture = create();
    fixture.componentInstance.key = 'DISK';
    fixture.componentInstance.top = true;
    fixture.detectChanges();
    const html: HTMLElement = fixture.nativeElement;

    expect(tile(html, 'n3').className).toContain('shade-l12');
    expect(tile(html, 'n3').getAttribute('aria-label')).toContain(
      'DISK top state 1200 / 2000 (60%)',
    );
    expect(legend(html)[2]).toEqual(['mid', '1']);
  });

  it('switches the scale to distance from the median', () => {
    const fixture = create();
    fixture.componentInstance.mode = 'median';
    fixture.detectChanges();
    const html: HTMLElement = fixture.nativeElement;

    expect(tile(html, 'n1').className).toContain('shade-m5');
    expect(tile(html, 'n3').className).toContain('shade-p3');
    expect(html.querySelector('.legend').textContent).toContain('median 90%');
  });

  it('pins the detail of a clicked tile and keeps it in step with the toggles', () => {
    const fixture = create(withResources());
    const html: HTMLElement = fixture.nativeElement;
    const detail = html.querySelector('.detail');
    const rows = (selector: string) =>
      Array.from(detail.querySelectorAll(selector)).map((row) =>
        Array.from(row.querySelectorAll('span, .muted'))
          .map((cell) => cell.textContent.trim())
          .filter(Boolean)
          .join(' '),
      );
    expect(detail.textContent).toContain('Zones, hottest first');

    const n3 = tile(html, 'n3');
    expect(n3.tagName).toBe('BUTTON');
    n3.click();
    fixture.detectChanges();
    expect(n3.classList.contains('selected')).toBe(true);
    expect(detail.querySelector('h3').textContent).toBe('n3');
    expect((detail.querySelector('a.open') as any).routerLink).toEqual([
      '../instances',
      'n3',
    ]);
    expect(rows('.key')).toEqual([
      'CU 1200 / 1000 (120%)',
      'DISK 2400 / 2000 (120%)',
    ]);
    expect(
      Array.from(detail.querySelectorAll('dd')).map((d) =>
        d.textContent.trim(),
      ),
    ).toEqual(['10 (4 top state)', '2 (1 top state)', '2']);
    expect(rows('.resource')).toEqual([
      'db 800 CU (80%) 4 replicas, 1 top state',
      'idx 400 CU (40%) 2 replicas, 1 top state, 2 ERROR',
      'legacy not WAGED 6 replicas, 1 top state',
    ]);

    fixture.componentInstance.top = true;
    fixture.detectChanges();
    expect(rows('.key')[0]).toBe('CU 600 / 1000 (60%)');
    (detail.querySelector('.close') as HTMLElement).click();
    fixture.detectChanges();
    expect(detail.textContent).toContain('Zones, hottest first');
    expect(n3.classList.contains('selected')).toBe(false);
  });

  it('ranks zones and the hottest instances, and selects from the list', () => {
    const fixture = create();
    const html: HTMLElement = fixture.nativeElement;
    const rows = Array.from(html.querySelectorAll('.detail .zone-row')).map(
      (row) =>
        Array.from(row.querySelectorAll('span'))
          .map((cell) => cell.textContent.trim())
          .filter(Boolean)
          .join(' '),
    );

    expect(rows).toEqual([
      'z3 120% max 120%',
      'z2 90% max 90%',
      'z1 30% max 30%',
      'n3 z3 120%',
      'n2 z2 90%',
      'n1 z1 30%',
    ]);
    (html.querySelectorAll('.detail .zone-row')[4] as HTMLElement).click();
    fixture.detectChanges();
    expect(tile(html, 'n2').classList.contains('selected')).toBe(true);
    expect(html.querySelector('.detail h3').textContent).toBe('n2');
  });

  it('drops the zones and their ranking when not grouped', () => {
    const fixture = create();
    fixture.componentInstance.byZone = false;
    fixture.detectChanges();
    const html: HTMLElement = fixture.nativeElement;

    expect(html.querySelector('.map').classList.contains('flat')).toBe(true);
    expect(
      Array.from(html.querySelectorAll('.zone')).map((z) =>
        z.getAttribute('data-zone'),
      ),
    ).toEqual(['All instances']);
    expect(html.querySelectorAll('.tile').length).toBe(4);
    expect(html.querySelector('.detail').textContent).not.toContain(
      'Zones, hottest first',
    );
    expect(html.querySelectorAll('.detail .zone-row').length).toBe(3);
  });

  it('finds instances by name and selects the first match on enter', () => {
    const fixture = create();
    const html: HTMLElement = fixture.nativeElement;
    const input = html.querySelector(
      'input[type="search"]',
    ) as HTMLInputElement;
    input.value = 'N3';
    input.dispatchEvent(new Event('input'));
    fixture.detectChanges();

    expect(html.querySelector('.map').classList.contains('finding')).toBe(true);
    expect(
      Array.from(html.querySelectorAll('.tile.match')).map((t) =>
        t.getAttribute('data-instance'),
      ),
    ).toEqual(['n3']);
    expect(html.querySelector('.find').textContent).toContain('1 found');
    expect(input.value).toBe('N3');
    input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter' }));
    fixture.detectChanges();
    expect(tile(html, 'n3').classList.contains('selected')).toBe(true);

    input.value = ' ';
    input.dispatchEvent(new Event('input'));
    fixture.detectChanges();
    expect(html.querySelector('.map').classList.contains('finding')).toBe(
      false,
    );
  });

  it('shows one hover card for the cell under the pointer or focus', () => {
    const fixture = create();
    const html: HTMLElement = fixture.nativeElement;
    const map = html.querySelector('.map');

    tile(html, 'n3').dispatchEvent(new Event('mouseover', { bubbles: true }));
    fixture.detectChanges();
    expect(html.querySelectorAll('.hover-card').length).toBe(1);
    expect(html.querySelector('.hover-card').textContent).toContain(
      'n3, z3, CU 1200 / 1000 (120%), 2 ERROR replicas',
    );
    tile(html, 'n1').dispatchEvent(new Event('focusin', { bubbles: true }));
    fixture.detectChanges();
    expect(html.querySelector('.hover-card b').textContent).toBe('n1');
    map.dispatchEvent(new Event('mouseleave'));
    fixture.detectChanges();
    expect(html.querySelector('.hover-card')).toBeNull();
  });

  it('builds 1,000 instances in 50 zones quickly, in dense cells', () => {
    const base = snapshotFixture().instances[0];
    const instances = Array.from({ length: 1000 }, (_, i) => ({
      ...base,
      name: `host-${i}`,
      zone: `zone-${i % 50}`,
      used: { CU: (i * 37) % 1400, DISK: 0 },
    }));
    const fixture = create({ ...snapshotFixture(), instances });
    const heatmap: CapacityHeatmapComponent = fixture.debugElement.query(
      By.directive(CapacityHeatmapComponent),
    ).componentInstance;

    const start = performance.now();
    heatmap.ngOnChanges();
    expect(performance.now() - start).toBeLessThan(200);
    expect(heatmap.heatmap.zones.length).toBe(50);
    expect(heatmap.heatmap.zones[1].label).toBe('zone-1');
    expect(heatmap.heatmap.columns).toBe(5);
    const html: HTMLElement = fixture.nativeElement;
    expect(html.querySelector('.map').classList.contains('dense')).toBe(true);
    expect(html.querySelectorAll('.tile').length).toBe(1000);
    expect(html.querySelector('.tile .value')).toBeNull();
  });
});
