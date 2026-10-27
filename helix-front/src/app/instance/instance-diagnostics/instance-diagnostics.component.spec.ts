import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, Router } from '@angular/router';
import { BehaviorSubject, of, Subject, throwError } from 'rxjs';
import { HelperService } from '../../shared/helper.service';
import { ZNRecord } from '../../shared/models/znrecord.model';
import { record } from '../../shared/models/znrecord.fixture';
import { Instance } from '../shared/instance.model';
import { InstanceService } from '../shared/instance.service';
import {
  InstanceDiagnosticsService,
  QueuedMessage,
  RecordedError,
} from '../shared/instance-diagnostics.service';
import { InstanceDiagnosticsComponent } from './instance-diagnostics.component';

describe('Instance diagnostic evidence and bounded requests', () => {
  let fixture: ComponentFixture<InstanceDiagnosticsComponent>;
  let component: InstanceDiagnosticsComponent;
  let routes: BehaviorSubject<ReturnType<typeof convertToParamMap>>;
  let queries: BehaviorSubject<ReturnType<typeof convertToParamMap>>;
  let service: Pick<
    InstanceDiagnosticsService,
    'messages' | 'errors' | 'message' | 'error' | 'currentState'
  >;
  let instances: Pick<InstanceService, 'get'>;
  let messageIndex: QueuedMessage[];
  let errorIndex: RecordedError[];
  let messageReads: { id: string; response: Subject<ZNRecord> }[];

  beforeEach(async () => {
    routes = new BehaviorSubject(
      convertToParamMap({ cluster_name: 'Demo', instance_name: 'node-1' }),
    );
    queries = new BehaviorSubject(convertToParamMap({}));
    messageIndex = [];
    errorIndex = [];
    messageReads = [];
    instances = {
      get: () => of(new Instance('node-1', 'Demo', true, true, 'current')),
    };
    service = {
      messages: () => of(messageIndex),
      errors: () => of(errorIndex),
      message: (_cluster, _instance, id) => {
        const response = new Subject<ZNRecord>();
        messageReads.push({ id, response });
        return response;
      },
      error: () =>
        of(
          record('error', { listFields: { messages: ['transition failed'] } }),
        ),
      currentState: () =>
        of(
          record('Orders', {
            mapFields: {
              Orders_0: { CURRENT_STATE: 'ERROR', INFO: 'failure details' },
            },
          }),
        ),
    };
    await TestBed.configureTestingModule({
      declarations: [InstanceDiagnosticsComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              paramMap: routes,
              parent: {
                paramMap: new BehaviorSubject(
                  convertToParamMap({ helix: 'helix.default' }),
                ),
              },
            },
            queryParamMap: queries,
          },
        },
        { provide: Router, useValue: { navigate: jest.fn() } },
        { provide: HelperService, useValue: { showError: jest.fn() } },
      ],
    })
      .overrideComponent(InstanceDiagnosticsComponent, {
        set: {
          template: '',
          styles: [],
          providers: [
            { provide: InstanceService, useValue: instances },
            { provide: InstanceDiagnosticsService, useValue: service },
          ],
        },
      })
      .compileComponents();
    fixture = TestBed.createComponent(InstanceDiagnosticsComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    fixture.destroy();
  });

  it('limits message detail reads to twenty per page and four concurrent requests', () => {
    messageIndex = Array.from({ length: 45 }, (_, i) => ({
      id: `m${i}`,
      state: 'NEW',
    }));
    queries.next(convertToParamMap({ view: 'messages' }));
    fixture.detectChanges();
    expect(messageReads).toHaveLength(4);
    for (let i = 0; i < 20; i++) {
      expect(
        messageReads.filter((read) => read.response.observers.length > 0)
          .length,
      ).toBeLessThanOrEqual(4);
      messageReads[i].response.next(record(`m${i}`));
      messageReads[i].response.complete();
    }
    expect(messageReads).toHaveLength(20);
    expect(component.messages.map((message) => message.id)).toEqual(
      messageIndex.slice(0, 20).map((message) => message.id),
    );
    component.loadMessages(1);
    expect(messageReads).toHaveLength(24);
  });

  it('keeps error evidence usable while the message index is still loading', () => {
    const index = new Subject<QueuedMessage[]>();
    const indexRead = jest.spyOn(service, 'messages').mockReturnValue(index);
    errorIndex = [
      { session: 'current', resource: 'Orders', partition: 'Orders_0' },
    ];
    fixture.detectChanges();
    expect(indexRead).not.toHaveBeenCalled();
    expect(component.isLoading).toBe(false);
    expect(component.filteredErrors).toHaveLength(1);
    component.setView('messages');
    expect(indexRead).toHaveBeenCalledTimes(1);
    expect(component.indexLoading).toBe(true);
    expect(component.filteredErrors).toHaveLength(1);
    component.setView('errors');
    component.setView('messages');
    expect(indexRead).toHaveBeenCalledTimes(1);

    index.next([{ id: 'late', state: 'NEW' }]);
    index.complete();
    expect(component.indexLoading).toBe(false);
    expect(messageReads.map((read) => read.id)).toEqual(['late']);
  });

  it('scopes a deep link to the exact partition and prefers current-session error evidence', () => {
    errorIndex = [
      { session: 'old', resource: 'Orders', partition: 'Orders_0' },
      { session: 'current', resource: 'Orders', partition: 'Orders_0' },
      { session: 'current', resource: 'Other', partition: 'Other_0' },
    ];
    queries.next(
      convertToParamMap({ resource: 'Orders', partition: 'Orders_0' }),
    );
    fixture.detectChanges();
    expect(component.filteredErrors.map((entry) => entry.session)).toEqual([
      'current',
      'old',
    ]);
    expect(component.selectedError.session).toBe('current');
    expect(component.errorRecord.listFields.messages).toEqual([
      'transition failed',
    ]);
    expect(component.currentPartition.CURRENT_STATE).toBe('ERROR');
    expect(component.sessionLabel('old')).toBe('Historical session');
  });

  it('does not turn a failed error index into a healthy-looking empty array', () => {
    jest
      .spyOn(service, 'errors')
      .mockReturnValue(throwError('permission denied'));
    fixture.detectChanges();
    expect(component.errorIndex).toBeNull();
    expect(component.errorIndexError).toBe('permission denied');
    expect(component.isLoading).toBe(false);
  });

  it('cancels in-flight message reads when navigating to another instance', () => {
    messageIndex = [{ id: 'a', state: 'NEW' }];
    queries.next(convertToParamMap({ view: 'messages' }));
    fixture.detectChanges();
    const old = messageReads[0].response;
    messageIndex = [];
    routes.next(
      convertToParamMap({ cluster_name: 'Demo', instance_name: 'node-2' }),
    );
    expect(old.observers).toHaveLength(0);
    old.next(record('stale'));
    expect(component.messages).toEqual([]);
    expect(component.instanceName).toBe('node-2');
  });
});
