import 'zone.js';
import 'zone.js/dist/zone-testing';

import { TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { describe, expect, it, jest } from '@jest/globals';
import { of } from 'rxjs';

import { CapacityService } from '../capacity/shared/capacity.service';
import { snapshotFixture } from '../capacity/testing/snapshot.fixture';
import { InstanceService } from '../instance/shared/instance.service';
import { HelperService } from '../shared/helper.service';
import { TopologyComponent } from './topology.component';

describe('TopologyComponent', () => {
  it('joins domains with the snapshot and drives the explorer', () => {
    const getSnapshot = jest.fn(() => of(snapshotFixture()));
    const getSummaries = jest.fn(() =>
      of([
        { name: 'n1', summary: { domain: 'zone=z1,host=h1,instance=n1' } },
        { name: 'n2', summary: { domain: 'zone=z1,host=h2,instance=n2' } },
        { name: 'n3', summary: { domain: 'zone=z3,host=h3,instance=n3' } },
        { name: 'n4', summary: { domain: '' } },
      ]),
    );
    TestBed.configureTestingModule({
      declarations: [TopologyComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: CapacityService, useValue: { getSnapshot } },
        { provide: InstanceService, useValue: { getSummaries } },
        { provide: HelperService, useValue: { showError: jest.fn() } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(TopologyComponent);
    fixture.detectChanges();
    const component = fixture.componentInstance;
    const shown = () => {
      fixture.detectChanges();
      return Array.from(
        fixture.nativeElement.querySelectorAll('tr[data-node]'),
        (tr: Element) => tr.getAttribute('data-node'),
      );
    };

    expect(getSnapshot).toHaveBeenCalledWith('Demo');
    expect(getSummaries).toHaveBeenCalledWith('Demo');
    expect(shown()).toEqual([
      '/zone=z1',
      '/zone=z1/host=h1',
      '/zone=z1/host=h2',
      '/zone=z3',
      '/zone=z3/host=h3',
      'none',
      'instance:n4',
    ]);
    const z1 = component.tree[0];
    expect(component.key).toBe('CU');
    expect(component.averageText(z1)).toBe('60%');
    expect(component.averageText(component.tree[2])).toBe('unknown');
    component.key = 'DISK';
    expect(component.usage(z1)).toMatchObject({ used: 2400, max: 4000 });

    component.setQuery('n2');
    expect(shown()).toEqual(['/zone=z1', '/zone=z1/host=h2', 'instance:n2']);
    component.setQuery('');
    component.expandAll(false);
    expect(shown()).toEqual(['/zone=z1', '/zone=z3', 'none']);
    component.expandAll(true);
    expect(shown()).toHaveLength(10);
    component.toggle(z1);
    expect(shown()).toHaveLength(6);
  });

  it('puts an instance under its current domain and shows both read times', () => {
    // the snapshot is cached and old, n1 has since moved from z1 to z9
    const getSnapshot = jest.fn(() => of(snapshotFixture()));
    const getSummaries = jest.fn(() =>
      of([
        { name: 'n1', summary: { domain: 'zone=z9,host=h1,instance=n1' } },
        { name: 'n2', summary: { domain: 'zone=z1,host=h2,instance=n2' } },
        { name: 'n3', summary: { domain: 'zone=z3,host=h3,instance=n3' } },
        { name: 'n4', summary: { domain: 'zone=z3,host=h4,instance=n4' } },
      ]),
    );
    TestBed.configureTestingModule({
      declarations: [TopologyComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: CapacityService, useValue: { getSnapshot } },
        { provide: InstanceService, useValue: { getSummaries } },
        { provide: HelperService, useValue: { showError: jest.fn() } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(TopologyComponent);
    fixture.detectChanges();
    const component = fixture.componentInstance;

    expect(component.tree.map((n) => n.id)).toEqual([
      '/zone=z1',
      '/zone=z3',
      '/zone=z9',
    ]);
    expect(component.utc(component.updated)).toBe('2026-11-01 10:00:00 UTC');
    expect(component.domainsReadAt).toBeGreaterThan(component.updated);
    const text = fixture.nativeElement
      .querySelector('[data-testid="read-times"]')
      .textContent.replace(/\s+/g, ' ');
    expect(text).toContain('Domains were read at');
    expect(text).toContain('sample taken at 2026-11-01 10:00:00 UTC');
  });
});
