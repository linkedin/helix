export interface ZNRecord {
  id: string;
  simpleFields: Record<string, string | null>;
  listFields: Record<string, string[] | null>;
  mapFields: Record<string, Record<string, string | null> | null>;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function isZNRecord(value: unknown): value is ZNRecord {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    isRecord(value.simpleFields) &&
    Object.values(value.simpleFields).every(
      (entry) => entry === null || typeof entry === 'string',
    ) &&
    isRecord(value.listFields) &&
    Object.values(value.listFields).every(
      (entry) =>
        entry === null ||
        (Array.isArray(entry) &&
          entry.every((item) => typeof item === 'string')),
    ) &&
    isRecord(value.mapFields) &&
    Object.values(value.mapFields).every(
      (entry) =>
        entry === null ||
        (isRecord(entry) &&
          Object.values(entry).every(
            (item) => item === null || typeof item === 'string',
          )),
    )
  );
}
