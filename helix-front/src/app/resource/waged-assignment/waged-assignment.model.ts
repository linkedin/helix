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
  const liveState = new Map(live.map((r) => [r.instance, r.state]));
  const moving = saved.filter((r) => !liveState.has(r.instance)).length;
  if (moving) {
    return `${moving} replica${moving === 1 ? '' : 's'} not placed yet`;
  }
  const extra = live.length - saved.length;
  if (extra > 0) {
    return `${extra} extra replica${extra === 1 ? '' : 's'} still placed`;
  }
  const states = saved.filter((r) => liveState.get(r.instance) !== r.state);
  return states.length
    ? `${states.length} state${states.length === 1 ? '' : 's'} differ`
    : null;
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
