import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatMenuModule } from '@angular/material/menu';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { beforeEach, describe, expect, it } from '@jest/globals';
import { of } from 'rxjs';

import { InstanceListComponent } from './instance-list.component';
import { InstanceService } from '../shared/instance.service';
import { HelperService } from '../../shared/helper.service';
import { Instance, InstanceOperationState } from '../shared/instance.model';

const operations: InstanceOperationState[] = [
  'ENABLE',
  'DISABLE',
  'EVACUATE',
  'SWAP_IN',
  'UNKNOWN',
];

describe('InstanceListComponent Live column', () => {
  let fixture: ComponentFixture<InstanceListComponent>;

  beforeEach(() => {
    const instances = operations.flatMap((operation) =>
      [true, false].map(
        (live) =>
          new Instance(
            `${operation}-${live ? 'live' : 'offline'}`,
            'Demo',
            operation === 'ENABLE',
            live,
            's',
            'v',
            operation,
          ),
      ),
    );
    TestBed.configureTestingModule({
      declarations: [InstanceListComponent],
      imports: [NgxDatatableModule, MatIconModule, MatMenuModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: Router, useValue: { navigate: () => {} } },
        { provide: InstanceService, useValue: {
            getSummaries: () => of(instances),
            getErrorSummaries: () => of({}),
          },
        },
        { provide: HelperService, useValue: { showError: () => {} } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    fixture = TestBed.createComponent(InstanceListComponent);
    fixture.detectChanges();
    fixture.detectChanges();
  });

  it('shows liveness for every operation, not enabled state', () => {
    const rows = Array.from(
      fixture.nativeElement.querySelectorAll('datatable-body-row'),
    ) as HTMLElement[];
    expect(rows).toHaveLength(10);

    for (const row of rows) {
      const name = row.textContent;
      const icon = row.querySelector('mat-icon');
      const live = name.includes('-live');
      expect(icon.textContent.trim()).toBe(live ? 'check_circle' : 'cancel');
      expect(icon.classList.contains('status-healthy')).toBe(live);
      expect(icon.classList.contains('status-offline')).toBe(!live);
    }
  });
});

describe('InstanceListComponent Operations column', () => {
  it('shows only the active operation, with the rest behind its menu', () => {
    const instance = new Instance(
      'n1',
      'Demo',
      false,
      true,
      's',
      'v',
      'EVACUATE',
      {
        host: 'n1',
        port: '1',
        domain: '',
        tags: [],
        capacity: {},
        instanceOperations: [
          { operation: 'ENABLE', source: 'ADMIN', reason: '', timestamp: 1000 },
          {
            operation: 'EVACUATE',
            source: 'AUTOMATION',
            reason: 'drain',
            timestamp: Date.now() - 3 * 3600000,
          },
        ],
      },
    );
    TestBed.configureTestingModule({
      declarations: [InstanceListComponent],
      imports: [
        NgxDatatableModule,
        MatIconModule,
        MatMenuModule,
        NoopAnimationsModule,
      ],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: Router, useValue: { navigate: () => {} } },
        {
          provide: InstanceService,
          useValue: {
            getSummaries: () => of([instance]),
            getErrorSummaries: () => of({}),
          },
        },
        { provide: HelperService, useValue: { showError: () => {} } },
      ],
      schemas: [NO_ERRORS_SCHEMA],
    });
    const fixture = TestBed.createComponent(InstanceListComponent);
    fixture.detectChanges();
    fixture.detectChanges();

    const el: HTMLElement = fixture.nativeElement;
    const chips = el.querySelectorAll('button.op-current');
    expect(chips).toHaveLength(1);
    expect(chips[0].textContent).toContain('EVACUATE · AUTOMATION');
    expect(chips[0].querySelector('.op-age').textContent.trim()).toBe('3h');
    expect(chips[0].querySelector('.op-more').textContent.trim()).toBe('+1');
    // the history opens as a real menu: its rows are focusable menu items
    (chips[0] as HTMLElement).click();
    fixture.detectChanges();
    const items = Array.from(
      document.querySelectorAll('.op-history-entry'),
    ) as HTMLElement[];
    expect(items.map((i) => i.getAttribute('role'))).toEqual([
      'menuitem',
      'menuitem',
    ]);
    // the history rows live in the closed menu, not in the table cell
    expect(
      el.querySelectorAll('datatable-body-row .op-history-entry'),
    ).toHaveLength(0);
  });
});
