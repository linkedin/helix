# waged-sim: WAGED simulation tool

`waged-sim` sets up a Helix cluster locally, runs a scenario of changes in rounds, stops on exit
criteria, and writes an analysis report. It runs the production WAGED code, either in process (dry run)
or on a real local cluster (embedded ZooKeeper, the real controller, simulated participants).

Design: [docs/design/002-waged-simulation-tool.md](../docs/design/002-waged-simulation-tool.md).

## Build

```bash
mvn -pl helix-waged-sim -am install -DskipTests
export JAVA_HOME=$(/usr/libexec/java_home)        # macOS; any JDK 11+
helix-waged-sim/target/helix-waged-sim-pkg/bin/waged-sim.sh presets
```

For large clusters give the JVM more heap: `JAVA_OPTS=-Xmx8g waged-sim.sh ...`.

The Copilot skill in [skill/](skill/) wraps the command line: `helix-waged-sim/skill/install.sh` copies it
to `~/.copilot/skills/waged-sim` and records where this repository is.

## Quick start

```bash
waged-sim.sh setup --from spec --spec helix-waged-sim/src/main/resources/examples/spec-small.yaml
waged-sim.sh inspect ~/.waged-sim/clusters/SIM_SMALL
waged-sim.sh run ~/.waged-sim/clusters/SIM_SMALL --scenario helix-waged-sim/src/main/resources/examples/scenario-small.yaml
open ~/.waged-sim/runs/SIM_SMALL-*/report.html
```

## Commands

| Command | What it does |
|---|---|
| `setup --from spec --spec FILE` | Generates a cluster from given constraints (nodes, resources, partitions, weights, configs) |
| `setup --from folder --path PATH` | Opens copied data: a cluster folder, a Pensieve dump folder, or a legacy single-file snapshot |
| `setup --from rest --url BASE --cluster NAME` | Copies a cluster through helix-rest (`BASE` is the URL part before `/clusters`); `--current-states` fetches per-instance current states instead of deriving them from external views; `--header 'Name: value'` for auth |
| `setup --from pensieve --dump DIR` | Imports a Pensieve pull (see `pensieve-script`), including the WAGED baseline and best possible |
| `pensieve-script --cluster NAME --time 'YYYY-MM-DD HH:MM:SS'` | Prints the pod-side pull script for a ZooKeeper backup pod |
| `inspect DIR` | Stats of the cluster as it is, with no simulation |
| `run DIR --scenario FILE\|preset:NAME` | Runs every variant of a scenario; `--mode local` for a real local cluster; `-j N` runs variants in parallel |
| `report RUN_DIR` | Re-renders the reports from the raw results |
| `up DIR` | Starts the cluster as a real local Helix cluster and keeps it running |
| `presets` | Lists the bundled scenarios |

`setup` takes `-o DIR` (default `~/.waged-sim/clusters/<cluster>`) and `--set key=value` for cluster config
overrides. `run` takes `--until COND`, `--fail-if COND`, `--max-rounds N`, `--timeout 20m`, `--variant NAME`,
`--focus-key KEY`, `--format md,html` and `-o RUN_DIR` (default `~/.waged-sim/runs/...`). Exit codes for
`run`: 0 every variant passed, 1 a variant failed, 2 an error. Set `WAGED_SIM_HOME` to move the workspace.

## Cluster folder

Every source writes the same layout: one ZNRecord JSON file per znode at its ZooKeeper path relative to
the cluster root (`CONFIGS/CLUSTER/<cluster>.json`, `CONFIGS/PARTICIPANT/<instance>.json`,
`CONFIGS/RESOURCE/<resource>.json`, `IDEALSTATES/`, `STATEMODELDEFS/`, `LIVEINSTANCES/`, `EXTERNALVIEW/`,
`INSTANCES/<instance>/HISTORY.json`, `INSTANCES/<instance>/CURRENTSTATES/<resource>.json` without the
session level), plus `ASSIGNMENT_METADATA/BASELINE.json` and `BEST_POSSIBLE.json` as plain
resource → partition → instance → state maps, and `manifest.json` (source, capture time, versions,
fidelity flags, content hash). Names that are not file-safe are percent-encoded.

Fidelity flags say what the source could not provide: no WAGED assignments (helix-rest), current states
derived from external views, or no participant history (offline times unknown).

## Engines

| | dry run (`--mode dry-run`, default) | local cluster (`--mode local`) |
|---|---|---|
| What runs | The controller's resource, current-state and best-possible stages with the production `WagedRebalancer`, on an in-memory copy | Embedded ZooKeeper, the production controller in a child JVM, in-process participants |
| One round | Apply the events due, run the stages once, complete every transition instantly | Apply the events, then wait until no messages are pending and nothing changes for the settle window |
| Time | Virtual clock starting at the capture time; `advanceClock` shifts the recorded offline and disable times | Delay settings divided by `timeCompression`; `advanceClock` waits the compressed time |
| Constraint weights | Explicit per variant, in process | `soft-constraint-weight.properties` on the controller's classpath; a change restarts the controller, as in production |
| Use for | Placement and evenness questions; large clusters; deterministic results | Message flow, transitions, throttling; clusters up to about 100 instances |

Both engines simulate WAGED resources only; other resources stay in the folder untouched. A forced pass
(`settings.pass: partial | global | cold`) runs one WAGED pass directly, which is how the `rca` preset
probes the causes of top-state skew.

WAGED's own failure logs are off (see `src/main/config/log4j2.properties`): every failure is recorded
per round in `rounds.jsonl` and in the report.

## Scenarios

See [skill/SKILL.md](skill/SKILL.md) for the full scenario format, the event and selector vocabulary, and
the stat names usable in `until` and `failIf`. Exit criteria:

- `until` (+ `stableFor`, `minRounds`): PASS when the condition holds; FAIL if `maxRounds` comes first.
- `stableFor` without `until`: PASS when nothing moved for that many rounds.
- Neither: exactly `maxRounds` rounds; PASS when they complete.
- Always: FAIL on `failIf`, a WAGED rebalance failure (unless `failOnRebalanceFailure: false`), `timeout`
  (wall clock) or `maxSimTime` (virtual time).

## Scale-down search

A scenario with a `search` block answers "how many instances can be removed before WAGED can no longer
place every replica?" instead of running rounds. `preset:scale-down` is the ready-made version.

```yaml
name: scale-down
search:
  removeNodes: {strategy: mz-balanced, method: binary, as: evacuate}   # as: delete; linear also takes step; max caps the search
  tolerateZoneOutage: none      # largest | every: also ride out a zone outage within the delay window
  tolerateZoneLoss: none        # largest | every: also survive a zone staying down past the window
  nonServing: keep              # remove: plan without disabled and offline instances; revive: bring them back up
  requireAtLeast: 4             # optional: FAIL when fewer can be removed
variants:                       # per variant: searchStrategy, tolerateZoneOutage, tolerateZoneLoss, nonServing, seed
  mz-balanced: {}
  survive-zone-outage: {tolerateZoneOutage: largest}
  survive-zone-loss: {tolerateZoneLoss: largest}
  worst-case: {searchStrategy: mz-tightest}
```

- **Probe.** Each probe starts from the cluster as it is and takes out the first k serving instances
  of a removal order the way a scale-down does: the EVACUATE operation.
  - The instances stay up but are no longer assignable, so WAGED computes a new baseline over the
    others. That fails with a capacity deficit or an unplaceable replica if they cannot hold
    everything.
  - The controller keeps each old replica serving until its replacement is up (n + 1), hands top
    states over, then drops it.
  - Instances are evacuated in batches. Each batch keeps the instances unable to take replicas
    (down, disabled, evacuating) within `MAX_OFFLINE_INSTANCES_ALLOWED`; past it the controller enters
    maintenance mode and stops moving replicas.
  - Controller rounds run until a batch holds nothing, at most `maxEvacuationRounds` (20) per batch or
    until there is no progress. Then it is removed and one more round runs.
  - WAGED can give a replacement to an instance that is down but still within its delay window. The
    replacement cannot come up there, so the evacuation waits out the window once (the virtual clock
    jumps past it) and reports that it had to.
  - An evacuation that still never empties the instances counts against the probe (`evacuationStuck`).
  - `as: delete` removes the instances at once instead: faster, and the same placement answer.
- **Feasible** (default `feasibleIf`): no rebalance failure, the evacuation finishes, and WAGED's
  assignment (its best possible state) puts every replica on an instance, within capacity, with no two
  replicas of a partition in one fault zone: `rebalanceFailures == 0 and evacuationStuck == 0 and
  unplacedReplicas.added == 0 and overCapacityInstances.added == 0 and zoneConflicts.added == 0`. `.added` counts only problems the probe that removes nothing did not
  have. The probe is judged on the assignment, not the transient layout while replicas move.
- **Search.** Binary search assumes that if removing k fails, removing more fails too. Its first guess
  is the most instances the remaining capacity allows (WAGED's own cluster-wide capacity check), then
  one more to confirm, so a capacity-bound cluster takes about three probes. `linear` checks every step.
- **Removal orders.**
  - `mz-balanced` (default): the next instance comes from the zone that stays least utilized without
    it, smallest instance first, so zones stay even. This is the best case.
  - `mz-tightest`: the opposite. Removals pile up in the zone that is most utilized without the
    instance (usually the smallest), largest and busiest instance first, until it is empty. This is
    the worst case for zone spread and for surviving a zone loss.
  - `mz-single`: empties the largest zone first.
  - `least-loaded`, `most-loaded`, `random` (per-variant `seed`) and `name`: ignore zones.

  The same orders are event selectors: `removeNode: mz-balanced:3`.
- **Worst case.** A real scale-down usually cannot choose which instances go. `preset:scale-down-worst-case`
  runs the best, the adversarial and three random orders, with and without a zone outage, a zone loss
  and down instances. The report's "Across removal orders" table shows the lowest answer per setting, which is
  what such a scale-down can count on.
  - With as many zones as replicas, the answer depends heavily on the order. Every zone must hold one
    replica of each partition, so piling removals into the tightest zone breaks it soonest.
  - With many zones each holding far less than one replica of every partition, and equal instance
    sizes, plain placement depends only on how many instances go. Surviving a zone loss still depends
    on which zone is left largest.
- **Fault zones.** WAGED places at most one replica of a partition per fault zone (a hard constraint).
  With fewer zones left than replicas, it does not fail: it places fewer replicas. Unplaced replicas
  catch that.
- **Surviving a zone going down: two answers.** Both take out the zone with the most capacity left
  after the removal (`largest`), or each zone in turn (`every`).
  - `tolerateZoneOutage`: the zone goes offline and comes back within the delay window. The removal
    settles first (two controller rounds). Then the zone's instances go offline and one more round
    runs. WAGED keeps their replicas in place and only adds temporary replicas where a partition falls
    below its minimum active replicas (`MIN_ACTIVE_REPLICAS`, which defaults to all replicas).
    - Passes when `outageFeasibleIf` holds, counted on what the controller serves. The default
      (`min-active`) is `belowMinActive.added == 0 and noLiveReplica.added == 0`: every partition keeps
      its minimum active replicas serving, for example 2 of 3. Add `rebalanceFailures == 0` to also
      require that WAGED reports no failure. Variants can set their own condition.
    - With 2 of 3 replicas as the minimum, an outage usually costs no capacity. With the default
      minimum (all replicas), it costs as much as losing the zone.
    - When WAGED does add temporary replicas, its capacity check weighs every replica against the
      instances that are up. Partitions already short because another instance is down can make an
      outage fail early.
    - The top-state utilization on the instances that stay up is reported too, since they take over
      the down zone's top states.
  - `tolerateZoneLoss`: the zone stays down past the window, or never comes back. Its instances are
    removed on top of the k, and WAGED must re-place all of their replicas. With as many zones as
    replicas no zone can be lost, and the search says so.
- **Disabled and offline instances** stay by default, as WAGED counts them: their capacity for the
  baseline, and as active while within the delay window. The report says how many replicas WAGED keeps
  on them.
  - `nonServing: remove` plans without them, for nodes that are gone for good.
  - `nonServing: revive` brings them back up and enables them first, for nodes that are briefly down
    (for example in an automated maintenance).

The report's "Scale-down search" section has the answer per variant, why the next removal fails (the
WAGED failure and the hard constraints that blocked it), every probe, and instances and utilization per
zone. `run.json` has the same under `variants[].search`. Searches run in dry-run mode only.

## Reports

Each run folder holds `run.json`, `rounds.jsonl` (one record per variant and round with every stat),
`nodes_<variant>_<round>.csv` (per-instance loads), `report.md` and `report.html` (self-contained, with
inline SVG charts and no external resources). `skill/scripts/render_xlsx.py <run folder>` writes a
colour-coded `report.xlsx`.

Stats are computed with the same WAGED code that resolves weights and capacities. Skew is max/mean of
utilization (load ÷ capacity) over serving instances (enabled, live, assignable), including empty ones.

## Data hygiene

Copied clusters contain real host and cluster names. Keep them in the workspace (outside any repository)
and do not commit them. Sources are only read; the local cluster binds to localhost.
