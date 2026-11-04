import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { beforeEach, describe, expect, it, jest, xit } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';
import { ClusterListComponent } from './cluster-list.component';

describe('ClusterListComponent', () => {
  let component: ClusterListComponent;
  let fixture: ComponentFixture<ClusterListComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [ClusterListComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // disable this test until I figure out a way to test
  xit('should create', () => {
    expect(component).toBeTruthy();
  });
});

describe('ClusterListComponent filter', () => {
  let component: ClusterListComponent;
  let navigate: ReturnType<typeof jest.fn>;

  beforeEach(() => {
    navigate = jest.fn();
    component = new ClusterListComponent(
      null as any,
      null as any,
      null as any,
      { navigate } as any,
    );
    component.service = 'helix.default';
    component.clusters = ['Demo', 'ATOMIC_E2E_G', 'atomic_e2e_t'].map(
      (name) => ({ name }) as any,
    );
  });

  it('matches names case-insensitively and opens the first match on enter', () => {
    component.filter = ' Atomic ';
    expect(component.filteredClusters.map((c) => c.name)).toEqual([
      'ATOMIC_E2E_G',
      'atomic_e2e_t',
    ]);
    component.openFirstMatch();
    expect(navigate).toHaveBeenCalledWith(['helix.default', 'ATOMIC_E2E_G']);
  });

  it('shows every cluster and opens nothing without a filter', () => {
    expect(component.filteredClusters.length).toBe(3);
    component.openFirstMatch();
    expect(navigate).not.toHaveBeenCalled();
  });

  it('focuses the filter on "/" unless the user is typing elsewhere', () => {
    const input = document.createElement('input');
    document.body.appendChild(input);
    Object.defineProperty(input, 'offsetParent', { value: document.body });
    component.filterInput = { nativeElement: input };

    const typing = new KeyboardEvent('keydown', { key: '/' });
    Object.defineProperty(typing, 'target', { value: input });
    component.focusFilterOnSlash(typing);
    expect(document.activeElement).not.toBe(input);

    const slash = new KeyboardEvent('keydown', { key: '/', cancelable: true });
    Object.defineProperty(slash, 'target', { value: document.body });
    component.focusFilterOnSlash(slash);
    expect(document.activeElement).toBe(input);
    expect(slash.defaultPrevented).toBe(true);
    input.remove();
  });
});
