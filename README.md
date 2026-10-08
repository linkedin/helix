<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Helix

[![Helix CI](https://github.com/apache/helix/actions/workflows/Helix-CI.yml/badge.svg)](https://github.com/apache/helix/actions/workflows/Helix-CI.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.helix/helix)](https://helix.apache.org)
[![License](https://img.shields.io/github/license/apache/helix)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![codecov.io](https://codecov.io/github/apache/helix/coverage.svg?branch=master)](https://codecov.io/github/apache/helix?branch=master)
[![Flaky Tests Track](https://img.shields.io/github/issues/apache/helix/FailedTestTracking?label=Flaky%20Tests)](https://github.com/apache/helix/issues?q=is%3Aissue+is%3Aopen+label%3AFailedTestTracking)

![Helix Logo](https://helix.apache.org/images/helix-logo.jpg)

Helix is part of the Apache Software Foundation. 

Project page: http://helix.apache.org/

Mailing list: http://helix.apache.org/mail-lists.html

### Build

```bash
mvn clean install -Dmaven.test.skip.exec=true
```

## Configuration compatibility

`ResourceConfigProperty.DELAY_REBALANCE_ENABLED` has been removed. Its ResourceConfig
copy was not consumed by delayed rebalancing, and merging an IdealState into a
ResourceConfig no longer adds it. Existing raw ResourceConfig fields with this name
remain opaque metadata; they are not deleted or migrated into another config.

Delayed rebalancing remains supported through `IdealState.setDelayRebalanceEnabled`,
`InstanceConfig.setDelayRebalanceEnabled`, and `ClusterConfig.setDelayRebalaceEnabled`.
Their serialized `DELAY_REBALANCE_ENABLED` key and default value (`true`) are unchanged.
Code referencing the removed ResourceConfig enum constant must use the corresponding
IdealState, InstanceConfig, or ClusterConfig API/enum instead and be rebuilt before
upgrading Helix. Already-compiled references to the removed enum constant are not
binary compatible. Do not copy an old ResourceConfig value into IdealState as part of
this cleanup: doing so could activate a previously ignored setting.

### ResourceConfig rebalance strategy compatibility

`REBALANCE_STRATEGY` is no longer exposed by the `RebalanceConfig` wrapper used by
`ResourceConfig`. Its enum constant, `getRebalanceStrategy()` and
`setRebalanceStrategy(String)` have been removed. Callers using these APIs must
update and recompile; use `IdealState.IdealStateProperty.REBALANCE_STRATEGY` and
`IdealState.getRebalanceStrategy()` / `setRebalanceStrategy(String)` for strategy
selection. IdealState strategy support and controller behavior are unchanged.

Existing raw `REBALANCE_STRATEGY` fields in ResourceConfig records remain opaque
metadata: wrapping or merging a record preserves them, but the typed
`RebalanceConfig.getConfigsMap()` output and ResourceConfig constructors/builders
using that output no longer emit them. Generic raw-record APIs are unchanged.
There is no automatic deletion or migration of persisted fields. Do not blindly
copy a ResourceConfig value into IdealState: the ResourceConfig value was not
used for strategy selection, and making it effective can change placement.

## WHAT IS HELIX

Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. Helix provides the following features: 

1. Automatic assignment of resource/partition to nodes
2. Node failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions 

## LinkedIn fork compatibility

**IdealState rebalance mode**

`IDEAL_STATE_MODE`, `IdealState.IdealStateModeProperty`,
`IdealState.setIdealStateMode(String)`, and `IdealState.getIdealStateMode()` have
been removed. Use `REBALANCE_MODE` and `IdealState.setRebalanceMode` instead.
The obsolete `IdealState.LEGACY_TASK_REBALANCERS` normalization constant is also
removed. These API removals are source- and binary-incompatible; rebuild and
release downstream callers before upgrading Helix.

The modern setter writes only `REBALANCE_MODE`. The getter does not mutate records,
does not infer a mode from legacy metadata or a rebalancer class, and preserves
the effective `SEMI_AUTO` default for missing or invalid modern values. Invalid
values retain the standard enum-parser warning. Explicit `NONE` is treated like
unset and also resolves to `SEMI_AUTO`, without rewriting the stored value.
Other valid modern modes are respected. This does not restore legacy-derived
`FULL_AUTO` or `CUSTOMIZED` behavior: records that depended on legacy fallback
must explicitly store their intended modern mode before upgrading.

`rebalanceModeFromString` accepts modern enum names only. Invalid inputs (including
the retired `AUTO` and `AUTO_REBALANCE` aliases) are logged and return the caller's
default. Consequently, admin/CLI calls using obsolete aliases must migrate too.
Use this mapping for legacy-only records and callers:

| Legacy mode | Modern mode |
|---|---|
| `AUTO` | `SEMI_AUTO` |
| `AUTO_REBALANCE` | `FULL_AUTO` |
| `CUSTOMIZED` | `CUSTOMIZED` |

Existing raw legacy fields remain opaque metadata: they are not deleted, migrated,
or synchronized by reads or setters, and changing them no longer affects topology
change detection. The UI shows only the modern rebalance mode. Generic raw-record
APIs still accept unknown fields.

Before merging or deploying this retirement, release the downstream reader/API
migrations, then migrate legacy-only persisted records and any `IdealStateRule!`
filters that reference the old key. Deploy tolerant readers before writers stop
emitting the legacy field. Never overwrite a valid modern mode from stale legacy
metadata; in particular, the legacy `AUTO` value can also accompany `TASK` and
`USER_DEFINED`. This PR does not perform a live migration. Historical versioned
website content and generated documentation snapshots describe earlier releases.

`GreedyRebalanceStrategy` and its cluster config
`GLOBAL_MAX_PARTITIONS_ALLOWED_PER_INSTANCE` have been removed from this fork.
Before upgrading, migrate any resource whose IdealState `REBALANCE_STRATEGY` names
that class to an explicitly chosen supported strategy. There is no automatic
fallback: an obsolete selector fails assignment calculation. Existing raw copies
of the retired cluster key are preserved but ignored and no longer impose a cap.
The separate `MAX_PARTITIONS_PER_INSTANCE` settings and WAGED capacity constraints
are unchanged; they are not automatic replacements for Greedy's global count cap.

The ignored job setting `MaxForcedReassignmentsPerTask` has been removed, including
`JobConfig.Builder.setMaxForcedReassignmentsPerTask(int)`,
`JobConfig.DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK`, and its config enum entry.
Remove downstream API references and rebuild/release those callers before upgrading
them to this Helix version. `MaxAttemptsPerTask` continues to control task attempts;
retry, assignment, and `TerminalStateExpiry` behavior are unchanged.

New job configurations and job-ID copies no longer emit the retired key. Legacy raw
records may still contain it: reading them does not rewrite them, and rebuilding
them through the typed builder ignores the key. No stored-record migration is
required, and this change does not add rejection of unknown fields to generic APIs.

## Dependencies

Helix UI has been tested to run well on these versions of node and yarn: 

```json
  "engines": {
    "node": "~14.17.5",
    "yarn": "^1.22.18"
  },
```

## ResourceConfig rebalance configuration compatibility

`REBALANCE_DELAY`, `REBALANCE_MODE`, and `REBALANCER_CLASS_NAME` are no longer
supported by `org.apache.helix.api.config.RebalanceConfig`, the rebalance settings
wrapper used by `ResourceConfig`. Their enum constants, backing fields, and
getters/setters have been removed. They were not consumed from ResourceConfig by
Helix's rebalancers.

Configure resource rebalancing through `IdealState.setRebalanceDelay`,
`IdealState.setRebalanceMode`, and `IdealState.setRebalancerClassName` instead.
Removing these ResourceConfig copies does not change their authoritative
IdealState settings. The separate legacy IdealState mode retirement is described
above. Code referencing the removed RebalanceConfig enum constants or
accessors must migrate and be rebuilt before upgrading Helix; already-compiled
references are not binary compatible. The legacy `RebalanceConfig.RebalanceMode`
enum remains available, deprecated, for callers that only use its mode names;
new callers should use `IdealState.RebalanceMode`.

The ResourceConfig rebalance wrapper and its legacy enum types remain for compatibility,
but no supported settings remain in the wrapper; `getConfigsMap()` returns an empty map.
Periodic rebalance is configured through `ClusterConfig.setRebalanceTimePeriod`;
the resource-level timer has been removed separately.
Building a ResourceConfig from a RebalanceConfig no longer writes the three
retired fields, including the previously synthesized `REBALANCE_MODE=NONE`.
Existing raw ResourceConfig fields remain opaque metadata: reading or merging
a ResourceConfig does not delete or migrate them. Do not automatically copy
these ignored values into IdealState, where they would affect rebalancing.
