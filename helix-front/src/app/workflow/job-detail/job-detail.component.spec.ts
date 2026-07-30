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
});
