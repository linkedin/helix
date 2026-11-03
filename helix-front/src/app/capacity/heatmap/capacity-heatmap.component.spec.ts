import 'zone.js';
import 'zone.js/dist/zone-testing';

import { Component, NO_ERRORS_SCHEMA } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { describe, expect, it, jest } from '@jest/globals';

import { snapshotFixture, withResources } from '../testing/snapshot.fixture';
import { CapacityHeatmapComponent } from './capacity-heatmap.component';

@Component({
  template: `<hi-capacity-heatmap
    [snapshot]="snapshot"
    [key]="key"
    [topStateOnly]="top"
  ></hi-capacity-heatmap>`,
})
class HostComponent {
  snapshot = snapshotFixture();
  key = 'CU';
  top = false;
}

describe('CapacityHeatmapComponent', () => {
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

  it('renders zones, banded tiles and legend counts', () => {
    const html: HTMLElement = create().nativeElement;

    expect(
      Array.from(html.querySelectorAll('.zone')).map((z) =>
        z.getAttribute('data-zone'),
      ),
    ).toEqual(['z3', 'z2', 'z1']);
    expect(html.querySelectorAll('.tile').length).toBe(4);
    const n4 = tile(html, 'n4');
    expect(n4.className).toContain('band-unknown');
    expect(n4.classList.contains('offline')).toBe(true);
    expect(n4.classList.contains('op')).toBe(true);
    expect(n4.getAttribute('aria-label')).toBe(
      'n4, z3, CU unknown, not live, operation DISABLE',
    );
    expect(tile(html, 'n3').className).toContain('band-over');
    expect(tile(html, 'n3').querySelector('.err-mark')).not.toBeNull();
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

    expect(tile(html, 'n3').className).toContain('band-mid');
    expect(tile(html, 'n3').title).toContain(
      'DISK top state 1200 / 2000 (60%)',
    );
    expect(legend(html)[2]).toEqual(['mid', '1']);
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
    expect(detail.textContent).toContain('Click a tile');

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
    expect(detail.textContent).toContain('Click a tile');
    expect(n3.classList.contains('selected')).toBe(false);
  });

  it('lays out again when the map is resized and stops watching on destroy', () => {
    let resized: () => void;
    const disconnect = jest.fn();
    (window as any).ResizeObserver = class {
      constructor(callback: () => void) {
        resized = callback;
      }
      observe() {}
      disconnect = disconnect;
    };
    const fixture = create();
    const map = fixture.nativeElement.querySelector('.map');
    Object.defineProperty(map, 'clientWidth', { value: 500 });
    resized();
    const zones = fixture.nativeElement.querySelectorAll('.zone');
    const right = Array.from(zones).map(
      (z: HTMLElement) => parseFloat(z.style.left) + parseFloat(z.style.width),
    );

    expect(Math.max(...right)).toBeLessThanOrEqual(500);
    fixture.destroy();
    expect(disconnect).toHaveBeenCalled();
    delete (window as any).ResizeObserver;
  });

  it('lays out 1,000 instances in 50 zones quickly, inside the map', () => {
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
    expect(heatmap.boxes.length).toBe(50);
    const outside = heatmap.boxes.filter(
      (box) =>
        box.x + box.w > 960 ||
        box.y + box.h > heatmap.height ||
        box.tiles.some(
          (t) => t.x + t.w > box.w + 1e-6 || t.y + t.h > box.h + 1e-6,
        ),
    );
    expect(outside).toEqual([]);
    expect(fixture.nativeElement.querySelectorAll('.tile').length).toBe(1000);
  });
});
