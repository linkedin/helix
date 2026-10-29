import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import {
  combineLatest,
  forkJoin,
  from,
  Observable,
  of,
  Subscription,
} from 'rxjs';
import {
  catchError,
  finalize,
  map,
  mergeMap,
  timeout,
  toArray,
} from 'rxjs/operators';
import { HelperService } from '../../shared/helper.service';
import { ZNRecord } from '../../shared/models/znrecord.model';
import { Instance } from '../shared/instance.model';
import { InstanceService } from '../shared/instance.service';
import {
  InstanceDiagnosticsService,
  QueuedMessage,
  RecordedError,
  recordedErrorMessages,
} from '../shared/instance-diagnostics.service';

interface ReadResult<T> {
  value: T | null;
  error: string;
}
export interface MessageEvidence extends QueuedMessage {
  record: ZNRecord | null;
  error: string;
}

@Component({
  selector: 'hi-instance-diagnostics',
  templateUrl: './instance-diagnostics.component.html',
  styleUrls: ['./instance-diagnostics.component.scss'],
  providers: [InstanceDiagnosticsService, InstanceService],
})
export class InstanceDiagnosticsComponent implements OnInit, OnDestroy {
  readonly pageSize = 20;
  clusterName = '';
  instanceName = '';
  backend = '';
  instance: Instance | null = null;
  instanceError = '';
  messageIndex: QueuedMessage[] | null = null;
  errorIndex: RecordedError[] | null = null;
  messageError = '';
  errorIndexError = '';
  messages: MessageEvidence[] = [];
  filteredErrors: RecordedError[] = [];
  selectedError: RecordedError | null = null;
  errorRecord: ZNRecord | null = null;
  errorMessages: string[] = [];
  errorRecordError = '';
  currentState: ZNRecord | null = null;
  currentStateError = '';
  resourceScope = '';
  partitionScope = '';
  sessionScope = '';
  search = '';
  view: 'errors' | 'messages' = 'errors';
  page = 0;
  messagePage = 0;
  isLoading = false;
  messagesLoading = false;
  indexLoading = false;
  errorLoading = false;
  currentLoading = false;
  updated: number | null = null;
  now = Date.now();
  private routeSub: Subscription;
  private requestSub: Subscription;
  private messagesSub: Subscription;
  private indexSub: Subscription;
  private errorSub: Subscription;
  private currentSub: Subscription;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private service: InstanceDiagnosticsService,
    private instances: InstanceService,
    private helper: HelperService,
  ) {}

  ngOnInit(): void {
    if (!this.route.parent?.parent) return;
    this.routeSub = combineLatest([
      this.route.parent.paramMap,
      this.route.parent.parent.paramMap,
      this.route.queryParamMap,
    ]).subscribe(([params, backend, query]) => {
      const cluster = params.get('cluster_name') || '';
      const instance = params.get('instance_name') || '';
      const key = backend.get('helix') || '';
      const changed =
        cluster !== this.clusterName ||
        instance !== this.instanceName ||
        key !== this.backend;
      const resource = query.get('resource') || '';
      const partition = query.get('partition') || '';
      const scopeChanged =
        resource !== this.resourceScope || partition !== this.partitionScope;
      if (changed) {
        this.cancelReads();
        this.clusterName = cluster;
        this.instanceName = instance;
        this.backend = key;
        this.instance = null;
        this.instanceError = this.errorIndexError = '';
        this.errorIndex = this.selectedError = null;
        this.updated = null;
        this.messagePage = 0;
      }
      this.resourceScope = resource;
      this.partitionScope = partition;
      this.sessionScope = query.get('session') || '';
      this.search = query.get('q') || '';
      this.view = query.get('view') === 'messages' ? 'messages' : 'errors';
      this.page = Math.max(0, Number(query.get('page')) || 0);
      this.applyFilters();
      if (changed) this.refresh();
      else {
        if (scopeChanged) this.loadCurrentState();
        this.ensureMessages();
      }
    });
  }

  ngOnDestroy(): void {
    this.routeSub?.unsubscribe();
    this.cancelReads();
  }

  private cancelReads(): void {
    this.requestSub?.unsubscribe();
    this.messagesSub?.unsubscribe();
    this.indexSub?.unsubscribe();
    this.errorSub?.unsubscribe();
    this.currentSub?.unsubscribe();
    this.indexLoading = false;
  }

  get errorPages(): number {
    return Math.max(1, Math.ceil(this.filteredErrors.length / this.pageSize));
  }
  get messagePages(): number {
    return Math.max(
      1,
      Math.ceil((this.messageIndex?.length || 0) / this.pageSize),
    );
  }
  get visibleErrors(): RecordedError[] {
    return this.filteredErrors.slice(
      this.page * this.pageSize,
      (this.page + 1) * this.pageSize,
    );
  }
  get currentPartition(): Record<string, string | null> | null {
    return this.currentState?.mapFields[this.partitionScope] || null;
  }

  refresh(): void {
    if (!this.clusterName || !this.instanceName || this.isLoading) return;
    this.isLoading = true;
    this.messagesSub?.unsubscribe();
    this.indexSub?.unsubscribe();
    this.errorSub?.unsubscribe();
    this.messages = [];
    this.messageIndex = null;
    this.messageError = '';
    this.errorRecord = null;
    this.errorMessages = [];
    this.errorRecordError = '';
    this.requestSub = forkJoin({
      instance: this.read(
        this.instances.get(this.clusterName, this.instanceName),
      ),
      errors: this.read(
        this.service.errors(this.clusterName, this.instanceName),
      ),
    })
      .pipe(finalize(() => (this.isLoading = false)))
      .subscribe((result) => {
        this.instance = result.instance.value;
        this.instanceError = result.instance.error;
        this.errorIndex = result.errors.value;
        this.errorIndexError = result.errors.error;
        this.updated = this.now = Date.now();
        this.applyFilters();
        if (
          this.selectedError &&
          this.filteredErrors.some((entry) =>
            this.sameError(entry, this.selectedError),
          )
        ) {
          this.selectError(this.selectedError, false);
        } else if (
          this.resourceScope &&
          this.partitionScope &&
          this.filteredErrors.length
        ) {
          this.selectError(this.filteredErrors[0], false);
        } else {
          this.selectedError = null;
        }
      });
    this.loadCurrentState();
    this.ensureMessages();
  }

  applyFilters(): void {
    const search = this.search.trim().toLowerCase();
    this.filteredErrors = (this.errorIndex || [])
      .filter(
        (entry) =>
          (!this.resourceScope || entry.resource === this.resourceScope) &&
          (!this.partitionScope || entry.partition === this.partitionScope) &&
          (!this.sessionScope || entry.session === this.sessionScope) &&
          (!search ||
            [entry.resource, entry.partition, entry.session]
              .join(' ')
              .toLowerCase()
              .includes(search)),
      )
      .sort(
        (a, b) =>
          Number(b.session === this.instance?.sessionId) -
            Number(a.session === this.instance?.sessionId) ||
          a.resource.localeCompare(b.resource) ||
          a.partition.localeCompare(b.partition, undefined, {
            numeric: true,
          }) ||
          a.session.localeCompare(b.session),
      );
    if (this.errorIndex !== null) {
      this.page = Math.max(
        0,
        Math.min(Math.floor(this.page), this.errorPages - 1),
      );
    }
    if (
      this.selectedError &&
      !this.filteredErrors.some((entry) =>
        this.sameError(entry, this.selectedError),
      )
    ) {
      this.errorSub?.unsubscribe();
      this.selectedError = this.errorRecord = null;
      this.errorMessages = [];
      this.errorRecordError = '';
    }
  }

  setFilters(page = 0): void {
    this.page = page;
    this.applyFilters();
    this.updateUrl();
  }

  setView(view: 'messages' | 'errors'): void {
    this.view = view;
    this.ensureMessages();
    this.updateUrl();
  }

  clearScope(): void {
    this.resourceScope = this.partitionScope = this.sessionScope = '';
    this.currentSub?.unsubscribe();
    this.currentState = null;
    this.currentStateError = '';
    this.setFilters();
  }

  private updateUrl(): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: {
        resource: this.resourceScope || null,
        partition: this.partitionScope || null,
        session: this.sessionScope || null,
        q: this.search || null,
        view: this.view === 'errors' ? null : this.view,
        page: this.page || null,
      },
      replaceUrl: true,
    });
  }

  // The message index REST call lists the queue and reads every message from
  // ZooKeeper, so it runs only once the Messages view is open and never gates
  // the error and identity evidence.
  private ensureMessages(): void {
    if (
      this.view !== 'messages' ||
      !this.clusterName ||
      this.indexLoading ||
      this.messagesLoading ||
      this.messages.length
    )
      return;
    if (this.messageIndex === null && !this.messageError) {
      this.loadMessageIndex();
    } else {
      this.loadMessages(this.messagePage);
    }
  }

  private loadMessageIndex(): void {
    this.indexLoading = true;
    this.indexSub = this.read(
      this.service.messages(this.clusterName, this.instanceName),
    )
      .pipe(finalize(() => (this.indexLoading = false)))
      .subscribe((result) => {
        this.messageIndex = result.value;
        this.messageError = result.error;
        this.loadMessages(Math.min(this.messagePage, this.messagePages - 1));
      });
  }

  loadMessages(page: number): void {
    this.messagesSub?.unsubscribe();
    this.messagePage = page;
    this.messages = [];
    if (!this.messageIndex?.length) return;
    this.messagesLoading = true;
    const entries = this.messageIndex.slice(
      page * this.pageSize,
      (page + 1) * this.pageSize,
    );
    this.messagesSub = from(entries)
      .pipe(
        mergeMap(
          (entry) =>
            this.service
              .message(this.clusterName, this.instanceName, entry.id)
              .pipe(
                map(
                  (record): MessageEvidence => ({
                    ...entry,
                    record,
                    error: '',
                  }),
                ),
                catchError((error) =>
                  of<MessageEvidence>({
                    ...entry,
                    record: null,
                    error:
                      error === 'Not Found'
                        ? 'No longer in the queue. Completion is not confirmed by this read.'
                        : this.readError(error),
                  }),
                ),
              ),
          4,
        ),
        toArray(),
        finalize(() => (this.messagesLoading = false)),
      )
      .subscribe((messages) => {
        const order = new Map(entries.map((entry, index) => [entry.id, index]));
        this.messages = messages.sort(
          (a, b) => order.get(a.id) - order.get(b.id),
        );
      });
  }

  selectError(entry: RecordedError, updateUrl = true): void {
    this.errorSub?.unsubscribe();
    this.selectedError = entry;
    this.errorRecord = null;
    this.errorMessages = [];
    this.errorRecordError = '';
    this.errorLoading = true;
    this.errorSub = this.service
      .error(this.clusterName, this.instanceName, entry)
      .pipe(finalize(() => (this.errorLoading = false)))
      .subscribe(
        (record) => {
          this.errorRecord = record;
          this.errorMessages = recordedErrorMessages(record);
        },
        (error) =>
          (this.errorRecordError =
            error === 'Not Found'
              ? 'This error record disappeared between reads. Refresh the index.'
              : this.readError(error)),
      );
    if (updateUrl) {
      this.resourceScope = entry.resource;
      this.partitionScope = entry.partition;
      this.sessionScope = entry.session;
      this.page = 0;
      this.applyFilters();
      this.updateUrl();
      this.loadCurrentState();
    }
  }

  loadCurrentState(): void {
    this.currentSub?.unsubscribe();
    this.currentState = null;
    this.currentStateError = '';
    if (!this.resourceScope || !this.partitionScope) return;
    this.currentLoading = true;
    this.currentSub = this.service
      .currentState(this.clusterName, this.instanceName, this.resourceScope)
      .pipe(finalize(() => (this.currentLoading = false)))
      .subscribe(
        (record) => {
          this.currentState = record;
        },
        (error) =>
          (this.currentStateError =
            error === 'Not Found'
              ? 'No current-session record was returned. The instance may be offline or the resource unassigned.'
              : this.readError(error)),
      );
  }

  sessionLabel(session: string): string {
    if (!this.instance) return 'Live session unknown';
    if (!this.instance.online) return 'Historical session';
    if (!this.instance.sessionId) return 'Live session ID unavailable';
    return session === this.instance.sessionId
      ? 'Current session'
      : 'Historical session';
  }

  messageAge(record: ZNRecord | null): string {
    const created = Number(record?.simpleFields.CREATE_TIMESTAMP);
    if (!Number.isFinite(created) || created <= 0) return 'Unknown';
    const seconds = Math.floor((this.now - created) / 1000);
    return seconds < 60
      ? `${seconds}s`
      : seconds < 3600
        ? `${Math.floor(seconds / 60)}m`
        : `${Math.floor(seconds / 3600)}h`;
  }

  private read<T>(source: Observable<T>): Observable<ReadResult<T>> {
    return source.pipe(
      timeout(15000),
      map((value) => ({ value, error: '' })),
      catchError((error) => of({ value: null, error: this.readError(error) })),
    );
  }

  private readError(error: unknown): string {
    return typeof error === 'string'
      ? error
      : error instanceof Error
        ? error.message
        : 'The read failed';
  }

  private sameError(a: RecordedError, b: RecordedError): boolean {
    return (
      a.session === b.session &&
      a.resource === b.resource &&
      a.partition === b.partition
    );
  }
}
