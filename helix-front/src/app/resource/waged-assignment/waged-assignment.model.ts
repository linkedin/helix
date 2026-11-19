// A WAGED assignment as helix-rest decodes it: partition -> instance -> state.
export type PartitionMap = Record<string, Record<string, string>>;

export type AssignmentKind = 'baseline' | 'bestPossible';

export interface Replica {
  instance: string;
  state: string;
}

export interface AssignmentRow {
  partition: string;
  saved: Replica[];
  // the placement compared against, empty when not comparing
  other: Replica[];
  // null when the two placements match, or when not comparing
  difference: string | null;
}

const names = new Intl.Collator(undefined, { numeric: true });

// top states first, then by instance, so two lists line up when compared
function replicas(map: Record<string, string> | undefined): Replica[] {
  return Object.entries(map || {})
    .map(([instance, state]) => ({ instance, state }))
    .sort(
      (a, b) =>
        +(b.state === 'MASTER' || b.state === 'LEADER') -
          +(a.state === 'MASTER' || a.state === 'LEADER') ||
        names.compare(a.instance, b.instance),
    );
}

function differenceOf(
  saved: Replica[],
  live: Replica[],
  otherName: string,
): string | null {
  if (!saved.length) {
    return 'Not in the saved assignment';
  }
  if (!live.length) {
    return `Not in the ${otherName}`;
  }
  // by instance, so a replica moved from one instance to another counts on both sides
  const liveState = new Map(live.map((r) => [r.instance, r.state]));
  const savedNames = new Set(saved.map((r) => r.instance));
  const missing = saved.filter((r) => !liveState.has(r.instance)).length;
  const extra = live.filter((r) => !savedNames.has(r.instance)).length;
  const states = saved.filter(
    (r) => liveState.has(r.instance) && liveState.get(r.instance) !== r.state,
  ).length;
  const plural = (n: number, word: string) =>
    `${n} ${word}${n === 1 ? '' : 's'}`;
  const parts = [
    missing ? `${plural(missing, 'replica')} not placed yet` : '',
    extra ? `${plural(extra, 'extra replica')} still placed` : '',
    states
      ? `${plural(states, 'state')} ${states === 1 ? 'differs' : 'differ'}`
      : '',
  ].filter(Boolean);
  return parts.length ? parts.join(', ') : null;
}

// Every partition in the saved map, and when another map is given, every
// partition in either, compared with it.
export function compareAssignment(
  saved: PartitionMap,
  other: PartitionMap | null = null,
  otherName = 'external view',
): AssignmentRow[] {
  const partitions = new Set([
    ...Object.keys(saved || {}),
    ...Object.keys(other || {}),
  ]);
  return [...partitions]
    .sort((a, b) => names.compare(a, b))
    .map((partition) => {
      const s = replicas(saved?.[partition]);
      const o = other ? replicas(other[partition]) : [];
      return {
        partition,
        saved: s,
        other: o,
        difference: other ? differenceOf(s, o, otherName) : null,
      };
    });
}
