import { NO_ERRORS_SCHEMA } from '@angular/core';
import 'zone.js';
import 'zone.js/dist/zone-testing';

import { ComponentFixture, TestBed, getTestBed } from '@angular/core/testing';
import {
  BrowserDynamicTestingModule,
  platformBrowserDynamicTesting,
} from '@angular/platform-browser-dynamic/testing';
import { of } from 'rxjs';
import { beforeEach, describe, expect, it } from '@jest/globals';

import {} from '@angular/core';

import { TestingModule } from '../../../testing/testing.module';
import { JobDetailComponent } from './job-detail.component';
import { JobService } from '../shared/job.service';

describe('JobDetailComponent', () => {
  let component: JobDetailComponent;
  let fixture: ComponentFixture<JobDetailComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [TestingModule],
      providers: [
        {
          provide: JobService,
          useValue: {
            get: (job) => of(),
          },
        },
      ],
      declarations: [JobDetailComponent],
      schemas: [
        /* avoid importing modules */
        NO_ERRORS_SCHEMA,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(JobDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('taskSummary should be null when there is no summary field', () => {
    component.job = { context: { simpleFields: {} } } as any;
    expect(component.taskSummary).toBeNull();
    expect(component.hasTaskFailures).toBe(false);
  });

  it('taskSummary should parse the TASK_STATUS_SUMMARY JSON string', () => {
    component.job = {
      context: {
        simpleFields: {
          TASK_STATUS_SUMMARY:
            '{"total":6,"completed":4,"failed":2,"timedOut":0,"inProgress":0,"other":0,"byState":{"COMPLETED":4,"TASK_ABORTED":1,"TASK_ERROR":1},"failedTasks":[3,5],"timedOutTasks":[],"inProgressTasks":[]}',
        },
      },
    } as any;
    const summary = component.taskSummary;
    expect(summary).not.toBeNull();
    expect(summary.total).toBe(6);
    expect(summary.completed).toBe(4);
    expect(summary.failed).toBe(2);
    expect(summary.timedOut).toBe(0);
    expect(summary.inProgress).toBe(0);
    expect(summary.failedTasks).toEqual([3, 5]);
    expect(component.hasTaskFailures).toBe(true);
    expect(component.hasTimedOut).toBe(false);
    expect(component.hasInProgress).toBe(false);
  });

  it('taskSummary should expose timed out and in progress counts', () => {
    component.job = {
      context: {
        simpleFields: {
          TASK_STATUS_SUMMARY:
            '{"total":5,"completed":2,"failed":2,"timedOut":1,"inProgress":1,"other":0,"byState":{"COMPLETED":2,"TASK_ERROR":1,"TIMED_OUT":1,"RUNNING":1},"failedTasks":[1,4],"timedOutTasks":[4],"inProgressTasks":[3]}',
        },
      },
    } as any;
    const summary = component.taskSummary;
    expect(summary).not.toBeNull();
    expect(summary.timedOut).toBe(1);
    expect(summary.inProgress).toBe(1);
    expect(summary.timedOutTasks).toEqual([4]);
    expect(summary.inProgressTasks).toEqual([3]);
    // A timed-out task is still a failure, so failure detection stays true.
    expect(component.hasTaskFailures).toBe(true);
    expect(component.hasTimedOut).toBe(true);
    expect(component.hasInProgress).toBe(true);
  });

  it('taskSummary should return null for malformed JSON', () => {
    component.job = {
      context: { simpleFields: { TASK_STATUS_SUMMARY: 'not-json' } },
    } as any;
    expect(component.taskSummary).toBeNull();
  });

  it('taskSummary should be computed live from per-partition states on page entry', () => {
    component.job = {
      context: {
        simpleFields: {},
        mapFields: {
          '0': { STATE: 'COMPLETED' },
          '1': { STATE: 'COMPLETED' },
          '2': { STATE: 'TASK_ERROR' },
          '3': { STATE: 'TIMED_OUT' },
          '4': { STATE: 'RUNNING' },
          '5': { STATE: 'INIT' },
        },
      },
    } as any;
    const summary = component.taskSummary;
    expect(summary).not.toBeNull();
    expect(summary.total).toBe(6);
    expect(summary.completed).toBe(2);
    expect(summary.failed).toBe(2);
    expect(summary.timedOut).toBe(1);
    expect(summary.inProgress).toBe(1);
    expect(summary.pending).toBe(1);
    expect(summary.other).toBe(0);
    expect(summary.failedTasks).toEqual([2, 3]);
    expect(summary.timedOutTasks).toEqual([3]);
    expect(summary.inProgressTasks).toEqual([4]);
    expect(summary.pendingTasks).toEqual([5]);
    expect(component.hasTaskFailures).toBe(true);
    expect(component.hasTimedOut).toBe(true);
    expect(component.hasInProgress).toBe(true);
    expect(component.hasPending).toBe(true);
  });

  it('taskSummary should prefer live per-partition states over a stale stored snapshot', () => {
    component.job = {
      context: {
        // A stale snapshot that no longer matches the current per-partition states.
        simpleFields: {
          TASK_STATUS_SUMMARY:
            '{"total":2,"completed":2,"failed":0,"timedOut":0,"inProgress":0,"pending":0,"other":0,"byState":{"COMPLETED":2},"failedTasks":[],"timedOutTasks":[],"inProgressTasks":[],"pendingTasks":[]}',
        },
        mapFields: {
          '0': { STATE: 'COMPLETED' },
          '1': { STATE: 'RUNNING' },
        },
      },
    } as any;
    const summary = component.taskSummary;
    // The live compute (1 running) must win over the stale snapshot (all completed).
    expect(summary.total).toBe(2);
    expect(summary.completed).toBe(1);
    expect(summary.inProgress).toBe(1);
    expect(component.hasInProgress).toBe(true);
  });

  it('taskSummary should fall back to the stored snapshot when no per-partition states exist', () => {
    component.job = {
      context: {
        simpleFields: {
          TASK_STATUS_SUMMARY:
            '{"total":3,"completed":3,"failed":0,"timedOut":0,"inProgress":0,"pending":0,"other":0,"byState":{"COMPLETED":3},"failedTasks":[],"timedOutTasks":[],"inProgressTasks":[],"pendingTasks":[]}',
        },
        mapFields: {},
      },
    } as any;
    const summary = component.taskSummary;
    expect(summary).not.toBeNull();
    expect(summary.total).toBe(3);
    expect(summary.completed).toBe(3);
    expect(component.hasPending).toBe(false);
  });
});
