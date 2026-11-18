import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { beforeEach, describe, expect, it, xit } from '@jest/globals';

import { TestingModule } from '../../../testing/testing.module';
import { ResourceDetailComponent } from './resource-detail.component';
import { ResourceService } from '../shared/resource.service';

describe('ResourceDetailComponent', () => {
  let component: ResourceDetailComponent;
  let fixture: ComponentFixture<ResourceDetailComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      providers: [ResourceService],
      declarations: [ResourceDetailComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ResourceDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  xit('should create', () => {
    expect(component).toBeTruthy();
  });

  it('hides the tabs only for a resource that never loaded', () => {
    const page = Object.create(ResourceDetailComponent.prototype);
    Object.assign(page, { isLoading: false, resource: undefined });
    page.loadError = 'Not Found';
    expect(page.missing).toBe(true);
    page.resource = {};
    expect(page.missing).toBe(false);
    Object.assign(page, { resource: undefined, isLoading: true });
    expect(page.missing).toBe(false);
  });
});
