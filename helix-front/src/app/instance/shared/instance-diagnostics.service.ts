import { Injectable } from '@angular/core';
import { Observable, of, throwError } from 'rxjs';
import { catchError, map, timeout } from 'rxjs/operators';
import { HelixService } from '../../core/helix.service';
import {
  isRecord,
  isZNRecord,
  ZNRecord,
} from '../../shared/models/znrecord.model';

export interface QueuedMessage {
  id: string;
  state: 'NEW' | 'READ';
}

export interface RecordedError {
  session: string;
  resource: string;
  partition: string;
}

export function recordedErrorMessages(record: ZNRecord): string[] {
  const messages = new Set<string>();
  for (const fields of Object.values(record.mapFields)) {
    const lines = (fields?.AdditionalInfo || '')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);
    if (!lines.length) continue;
    const causes = lines.filter((line) => line.startsWith('Caused by:'));
    messages.add(causes.length ? causes[causes.length - 1] : lines[0]);
  }
  return Array.from(messages);
}

function strings(value: unknown): value is string[] {
  return (
    Array.isArray(value) && value.every((entry) => typeof entry === 'string')
  );
}

@Injectable()
export class InstanceDiagnosticsService extends HelixService {
  messages(cluster: string, instance: string): Observable<QueuedMessage[]> {
    return this.request(this.path(cluster, instance, 'messages')).pipe(
      timeout(15000),
      map((data: unknown) => {
        if (
          !isRecord(data) ||
          !strings(data.new_messages) ||
          !strings(data.read_messages)
        ) {
          throw new Error('Invalid message index response.');
        }
        return [
          ...data.new_messages.map(
            (id): QueuedMessage => ({ id, state: 'NEW' }),
          ),
          ...data.read_messages.map(
            (id): QueuedMessage => ({ id, state: 'READ' }),
          ),
        ];
      }),
      // These two index endpoints use 404 for an absent subtree.
      catchError((error) =>
        error === 'Not Found' ? of([]) : throwError(error),
      ),
    );
  }

  errors(cluster: string, instance: string): Observable<RecordedError[]> {
    return this.request(this.path(cluster, instance, 'errors')).pipe(
      timeout(15000),
      map((data: unknown) => {
        if (!isRecord(data) || !isRecord(data.errors)) {
          throw new Error('Invalid error index response.');
        }
        const errors: RecordedError[] = [];
        for (const [session, resources] of Object.entries(data.errors)) {
          if (!isRecord(resources))
            throw new Error('Invalid error session response.');
          for (const [resource, partitions] of Object.entries(resources)) {
            if (!strings(partitions))
              throw new Error('Invalid error partition response.');
            for (const partition of partitions)
              errors.push({ session, resource, partition });
          }
        }
        return errors;
      }),
      catchError((error) =>
        error === 'Not Found' ? of([]) : throwError(error),
      ),
    );
  }

  message(cluster: string, instance: string, id: string): Observable<ZNRecord> {
    return this.record(this.path(cluster, instance, 'messages', id));
  }

  error(
    cluster: string,
    instance: string,
    entry: RecordedError,
  ): Observable<ZNRecord> {
    return this.record(
      this.path(
        cluster,
        instance,
        'errors',
        entry.session,
        entry.resource,
        entry.partition,
      ),
    );
  }

  currentState(
    cluster: string,
    instance: string,
    resource: string,
  ): Observable<ZNRecord> {
    return this.record(this.path(cluster, instance, 'resources', resource));
  }

  private record(path: string): Observable<ZNRecord> {
    return this.request(path).pipe(
      timeout(15000),
      map((data: unknown) => {
        if (!isZNRecord(data))
          throw new Error('Invalid diagnostic record response.');
        return data;
      }),
    );
  }

  private path(
    cluster: string,
    instance: string,
    ...segments: string[]
  ): string {
    return (
      `/clusters/${encodeURIComponent(cluster)}/instances/${encodeURIComponent(instance)}/` +
      segments.map(encodeURIComponent).join('/')
    );
  }
}
