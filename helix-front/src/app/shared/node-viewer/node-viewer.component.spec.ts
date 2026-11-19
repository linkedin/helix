import 'zone.js';
import 'zone.js/dist/zone-testing';
import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { TestingModule } from '../../../testing/testing.module';
import { beforeAll, beforeEach, describe, expect, it } from '@jest/globals';
import { of, throwError } from 'rxjs';

import { NodeViewerComponent } from './node-viewer.component';

describe('NodeViewerComponent', () => {
  let component: NodeViewerComponent;
  let fixture: ComponentFixture<NodeViewerComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      declarations: [NodeViewerComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(NodeViewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });

  it('should not be unlockable', () => {
    expect(component).toHaveProperty('unlockable', false);
  });

  describe('editing a list value', () => {
    function viewer(path?: string, write = of({})) {
      const route: any = { snapshot: { data: { path } } };
      const resources: any = { setIdealState: () => write };
      const helper: any = { showSnackBar: () => {}, showError: () => {} };
      const v = new NodeViewerComponent(null, route, resources, helper, null);
      (v as any).node = {
        listFields: [{ name: 'L', value: [{ value: 'a' }, { value: 'b' }] }],
      };
      const sent = [];
      v.change.subscribe((n) => sent.push(n));
      return { v, sent };
    }
    const shown = (v: NodeViewerComponent) =>
      (v as any).node.listFields[0].value.map((i) => i.value);
    const edit = { row: { value: 'a' }, column: { name: 'Value' }, value: 'z' };

    it('sends the new list but shows the stored one until it is written', () => {
      const { v, sent } = viewer();
      v.edited('list', edit, 'L', false);
      expect(sent[0].listFields[0].value.map((i) => i.value)).toEqual([
        'z',
        'b',
      ]);
      expect(shown(v)).toEqual(['a', 'b']);
    });

    it('shows the new ideal state list only after the write succeeds', () => {
      const ok = viewer('idealState');
      ok.v.edited('list', edit, 'L', false);
      expect(shown(ok.v)).toEqual(['z', 'b']);

      const rejected = viewer('idealState', throwError('blocked'));
      rejected.v.edited('list', edit, 'L', false);
      expect(shown(rejected.v)).toEqual(['a', 'b']);
    });
  });
});
