import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
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
      imports: [NgxDatatableModule, MatIconModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { snapshot: { params: { name: 'Demo' } } } },
        },
        { provide: Router, useValue: { navigate: () => {} } },
        { provide: InstanceService, useValue: { getAll: () => of(instances) } },
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
