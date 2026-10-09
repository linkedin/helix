---
name: waged-sim
description: >-
  Simulate Helix WAGED rebalancing for a cluster and report how placement and top-state (leader)
  load evolve. Use when the user asks to "simulate WAGED", "what happens to leader skew if ...",
  "copy cluster X locally", "run a rebalance scenario", "replay a cluster's placement", "why is
  top-state skewed", "try a constraint weight / preferredScoringKeys change", "kill or disable nodes
  and see where partitions go", "how many nodes can we remove / scale down", "capacity headroom",
  "can the cluster lose a zone (MZ)", or wants a local Helix cluster built from a real one. Sets up a
  cluster from helix-rest, a Pensieve point-in-time pull, a local folder of copied data, or a spec of
  nodes/resources/partitions; runs scenario rounds either as a dry run (production WAGED code in
  process) or on a real local cluster (embedded ZooKeeper, real controller, simulated participants);
  stops on exit criteria (rounds, a condition, a timeout, or a mix), or searches for the most
  instances that can be removed while WAGED still places every replica (fault-zone aware); and writes
  Markdown/HTML (and optional xlsx) reports with per-round stats and the final result.
allowed-tools: Bash
---

# waged-sim: WAGED simulation tool

The work is done by the `waged-sim` command line (module `helix-waged-sim` in the Helix repo). This
skill turns a request into a cluster setup and a scenario file, runs the tool, and explains the
result. Run everything through `scripts/waged-sim` in this skill folder: it finds the Helix repo,
builds the package on first use, and sets `JAVA_HOME`.

## Guardrails (always)

- Read-only toward real clusters. Never write to production ZooKeeper or helix-rest. The local cluster
  binds to localhost only.
- Captured data contains real host and cluster names. Keep it in the workspace (`~/.waged-sim`, or
  `WAGED_SIM_HOME`). Never copy it into a repository or commit it.
- Pensieve pulls run where the production ZooKeeper backups are (for example inside a backup pod).
  That is privileged: get the user's OK for the environment, and never drive their interactive login.
- Publish a report (Google Sheet or Doc) only when the user asks.

## Workflow

1. **Pick the cluster source.** Ask only if it is not clear from the request.
   - helix-rest: `scripts/waged-sim setup --from rest --url <base before /clusters> --cluster <NAME>`
     (add `--header 'Name: value'` for auth). helix-rest has no WAGED baseline/best possible, so the
     report flags that the first rounds start from the served layout.
   - Pensieve (full fidelity, includes the WAGED assignments): generate the pod script with
     `scripts/waged-sim pensieve-script --cluster <NAME> --time 'YYYY-MM-DD HH:MM:SS' --backup-dir <dir> -o pull.sh`,
     run it where the backups are (copy the Pensieve jar and the script in, run, copy
     `/tmp/waged-sim-pull-<NAME>.tgz` back, untar), then
     `scripts/waged-sim setup --from pensieve --dump <untarred folder>`.
   - Local folder of copied data (a cluster folder, a Pensieve dump folder, or a legacy single-file
     snapshot): `scripts/waged-sim setup --from folder --path <PATH>`.
   - A spec (nodes, resources, partitions, configs) from the user's words: write a spec YAML in the
     workspace (format below), show it, then `scripts/waged-sim setup --from spec --spec <FILE>`.
   Show the setup summary, including any fidelity notes.
2. **Look at the starting point:** `scripts/waged-sim inspect <cluster folder> --focus-key <KEY>`.
   The focus key is the capacity key the user cares about (often CU); default is the first key.
3. **Write the scenario** as YAML in the workspace (format below) from the request, or use a preset
   (`scripts/waged-sim presets`). Show it before running.
4. **Run:** `scripts/waged-sim run <cluster folder> --scenario <FILE|preset:NAME> [--mode local] [-j 4]`.
   Use `--mode local` only when message flow, throttling or transition timing matters; the dry run is
   faster and deterministic. For clusters with more than about 100 instances prefer the dry run.
   Exit code 0 means every variant passed, 1 a variant failed its criteria, 2 an error.
5. **Report back:** the verdict per variant, the key stats at the start and end (from the console and
   `report.md`), and the paths of `report.html` and `report.md`. Offer xlsx
   (`python3 scripts/render_xlsx.py <run folder>`) or a Google Sheet only if useful.

### Scale-down questions

"How many nodes can we remove?" is a search, not rounds: `scripts/waged-sim run <cluster folder>
--scenario preset:scale-down -j 4`. Report per variant: how many instances can be removed, how many from
each zone, and why one more fails (the WAGED failure: a capacity deficit, or a replica no instance can
take, with the hard constraints that blocked it, such as FAULT_ZONE and NODE_CAPACITY). Point out:

- `mz-balanced` is the plan to follow (zones stay even); `mz-single` is the worst case for zone spread.
- `mz-balanced-zone-loss` is the answer that still survives losing the largest zone past the delay
  window. With as many zones as replicas no zone can be lost; say so rather than calling it a failure
  of the plan.
- Disabled or offline instances count as capacity for WAGED. If the report says WAGED keeps replicas
  on them, give the answer without them too (`search: {nonServing: remove}`).
- The search assumes that if removing k fails, removing more fails too; `method: linear` checks every
  step when that matters.

## Spec format (cluster from given constraints)

```yaml
cluster:
  name: SIM_A
  capacityKeys: [CU, DISK, PARTCOUNT]
  instanceCapacity: {CU: 10000, DISK: 2000000, PARTCOUNT: 600}
  defaultPartitionWeight: {CU: 10, DISK: 1000, PARTCOUNT: 1}
  rebalancePreference: {EVENNESS: 1, LESS_MOVEMENT: 2}
  delayRebalance: {enabled: true, time: 16h}
  topology: {faultZoneType: zone, path: /zone/instance}
  # preferredScoringKeys: [CU]       maxOfflineInstancesAllowed: 10     config: {simpleFields: {...}}
instances:
  - {count: 100, prefix: node, zones: 20}
  - {count: 10, prefix: big, zones: 20, capacity: {CU: 20000}}
resources:
  - {count: 10, prefix: db, partitions: 256, replicas: 3, stateModel: MasterSlave,
     weights: {CU: "lognormal(40, 1.0)", DISK: "uniform(100, 4000)"}, outliers: {count: 2, factor: 20, key: CU}}
  - {name: hot, partitions: 16, replicas: 3, weights: {CU: 4000}}
liveness: {down: 0, disabled: 0}       # counts or lists of instance names
initialPlacement: cold-start           # cold-start | random | none
seed: 7
```

Weights: a number, `constant(v)`, `uniform(min, max)`, `normal(mean, sd)`, `lognormal(median, sigma)`,
or a list. Zones must be at least the replica count for fault-zone-aware placement.

## Scenario format

```yaml
name: kill-hot-nodes
mode: dry-run                  # or local
focusKey: CU
settings:                      # optional
  pass: auto                   # auto (controller decides) | partial | global | cold (forced WAGED pass)
  activeNodes: delay-aware     # or enabled-live (forced passes only)
  firstRound: steady           # or restart (controller starts fresh: global pass first)
  constraintWeights: {TopState: 3}
  local: {timeCompression: 3600, roundTimeout: 5m, settleQuiet: 2s, transitionLatency: 0ms}
clusterConfig: {}              # overrides for every variant (friendly keys or raw simpleFields)
variants:                      # optional; each runs independently on its own copy
  prod: {}
  ts-x4: {constraintWeights: {TopState: 12}}
  focus-keys: {clusterConfig: {preferredScoringKeys: ["${focusKey}"]}}
events:
  - at: 2
    kill: hottest-top:3
  - at: 3
    advanceClock: 16h
  - at: 4
    revive: previously-killed
  - every: 2
    from: 6
    times: 3
    restart: random:1
exit:
  until: "skew.top.CU <= 1.10 and missingTopState == 0"
  stableFor: 2                 # consecutive rounds; without until it means "nothing moved"
  failIf: "violations.capacity > 0"
  minRounds: 4                 # default: the last event's round
  maxRounds: 30                # without until/stableFor the run is exactly maxRounds rounds and passes
  timeout: 20m                 # wall clock
  maxSimTime: 3d               # virtual time
  failOnRebalanceFailure: true
report: {formats: [md, html], stats: [skew.top.CU, moves.topState], perNode: [start, end]}
```

Events: `disable`, `enable`, `setOperation {nodes, operation}`, `kill`, `revive`, `restart`,
`addNode {count, prefix, zone|zones, capacity, like}`, `removeNode`, `capacity {nodes, set|factor}`,
`weights {resource, partitions, key, factor|set}`, `addResource {name, partitions, replicas, weights}`,
`removeResource`, `partitions {resource, count}`, `replicas {resource, count}`, `clusterConfig {...}`,
`constraintWeights {...}` (restarts the controller, as in production), `maintenance: on|off`,
`advanceClock: 16h`, `restartController: true`, `onDemandRebalance: true`, `random {kind, count}`.

Node selectors: a name or list, `all`, `hottest-top:N`, `hottest-all:N`, `random:N`, `zone:<z>`,
`re:<regex>`, `previously-disabled`, `previously-killed`, the removal orders `mz-balanced:N`,
`mz-single:N`, `least-loaded:N`, `most-loaded:N` (serving instances only), or
`{pick, count, key, zone, names}`.

A scale-down search replaces `events` and `exit` (except `timeout`) with a `search` block:

```yaml
search:
  removeNodes: {strategy: mz-balanced, method: binary, step: 1, max: 50}   # strategy as below
  feasibleIf: "rebalanceFailures == 0 and unplacedReplicas.added == 0 and overCapacityInstances.added == 0 and zoneConflicts.added == 0"
  tolerateZoneLoss: none       # largest | every
  nonServing: keep             # remove
  requireAtLeast: 4            # optional; FAIL below it
variants:                      # per variant: searchStrategy, tolerateZoneLoss, plus the usual fields
  mz-balanced: {}
  mz-single: {searchStrategy: mz-single}
```

Strategies: `mz-balanced`, `mz-single`, `least-loaded`, `most-loaded`, `random`, `name`. Probe stats
(judged on WAGED's assignment): `unplacedReplicas`, `overCapacityInstances`, `zoneConflicts`,
`replicasOnNonServing`, `zones.serving`, `search.k`, each with `.added` (relative to removing nothing),
plus the stats below.

Stats usable in conditions (per capacity key K): `skew.top.K`, `skew.all.K` (max/mean of utilization
over serving instances), `maxUtil.top.K`, `maxUtil.all.K`, `util.top.K`, `util.all.K`,
`util.required.K`, `exposure.K`, `cv.top.K`, `floor.top.<focus>`, `skew.topCount`,
`skew.replicaCount`, `moves.replicas`, `moves.topState`, `moves.kept`, `moves.roleSwap`,
`moves.toNonHolder`, `moves.cumulative`, `drift.baseline`, `yardstick.targetKey`,
`yardstick.misrated`, `missingTopState`, `underReplicated`, `violations.capacity`,
`passes.global`, `passes.partial`, `rebalanceFailures`, `maintenance`, `round`, `simTime.hours`.

## Presets

`replay` (no changes until stable), `rca` (root-cause probes: anchors, TopState weight, scoring keys,
MaxCapacityUsage, cold starts), `levers` (configuration changes with a controller restart), `rehome`
(kill the 3 hottest, wait out the delay, revive), `converge` (controller restart churn),
`restart-all` (rolling restarts), `scale-down` (how many instances can be removed: mz-balanced,
mz-balanced surviving the loss of the largest zone, least-loaded, mz-single).
