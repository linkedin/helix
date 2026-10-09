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
  removeNodes: {strategy: mz-balanced, method: binary}   # linear also takes step; max caps the search
  tolerateZoneLoss: none        # largest | every: also survive losing a whole zone afterwards
  nonServing: keep              # remove: plan without disabled and offline instances
  requireAtLeast: 4             # optional: FAIL when fewer can be removed
variants:
  mz-balanced: {}
  survive-zone-loss: {tolerateZoneLoss: largest}
  least-loaded: {searchStrategy: least-loaded}
```

- **Probe.** Each probe starts from the cluster as it is, removes the first k serving instances of a
  removal order, and runs one controller pipeline. The removal changes the topology, so WAGED computes a
  new baseline over the instances left (which fails with a capacity deficit or an unplaceable replica if
  they cannot hold everything), and the partial pass re-homes the removed instances' replicas.
- **Feasible** (default `feasibleIf`): no rebalance failure, and WAGED's assignment (its best possible
  state) puts every replica on an instance, within capacity, with no two replicas of a partition in one
  fault zone: `rebalanceFailures == 0 and unplacedReplicas.added == 0 and overCapacityInstances.added == 0
  and zoneConflicts.added == 0`. `.added` counts only problems the probe that removes nothing did not
  have. The probe is judged on the assignment, not the transient layout while replicas move.
- **Search.** Binary search assumes that if removing k fails, removing more fails too. Its first guess
  is the most instances the remaining capacity allows (WAGED's own cluster-wide capacity check), then
  one more to confirm, so a capacity-bound cluster takes about three probes. `linear` checks every step.
- **Removal orders.** `mz-balanced` (default) takes the next instance from the zone that stays least
  utilized without it, the smallest instance first, so zones stay even; `mz-single` empties the largest
  zone first (the worst case for zone spread); `least-loaded`, `most-loaded`, `random` and `name`
  ignore zones. The same orders are event selectors: `removeNode: mz-balanced:3`.
- **Fault zones.** WAGED places at most one replica of a partition per fault zone (a hard constraint).
  With fewer zones left than replicas, it does not fail: it places fewer replicas. Unplaced replicas
  catch that. `tolerateZoneLoss: largest` also requires that the cluster survives losing the zone with
  the most capacity after the removal, as if it stayed down past the delay window; `every` checks each
  zone. With as many zones as replicas no zone can be lost, and the search says so.
- **Disabled and offline instances** stay by default, as WAGED counts them: their capacity for the
  baseline, and as active while within the delay window. The report says how many replicas WAGED keeps
  on them; `nonServing: remove` plans without them.

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
