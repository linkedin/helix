import { ZNRecord } from './znrecord.model';

export function record(id: string, overrides: Partial<ZNRecord> = {}): ZNRecord {
  return { id, simpleFields: {}, listFields: {}, mapFields: {}, ...overrides };
}
