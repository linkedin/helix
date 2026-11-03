import 'zone.js';
import 'zone.js/dist/zone-testing';

import { Component } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { ActivatedRoute, Router } from '@angular/router';
import { describe, expect, it, jest } from '@jest/globals';

import { snapshotFixture } from '../testing/snapshot.fixture';
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
  const navigate = jest.fn();
  const route = {};

  function create(snapshot = snapshotFixture()) {
    TestBed.configureTestingModule({
      declarations: [HostComponent, CapacityHeatmapComponent],
      providers: [
        { provide: Router, useValue: { navigate } },
        { provide: ActivatedRoute, useValue: route },
      ],
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

  it('opens the instance page from a tile', () => {
    tile(create().nativeElement, 'n3').click();
    expect(navigate).toHaveBeenCalledWith(['../instances', 'n3'], {
      relativeTo: route,
    });
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
