# Helix Configuration Audit

| Field       | Value                  |
|-------------|------------------------|
| **Authors** | Karan Tripathi         |
| **Status**  | Draft                  |
| **Created** | 2026-02-18             |
| **Updated** | 2026-02-24             |
| **Modules** | helix-core, helix-rest |

---

## Table of Contents

- [1. Config Inventory Summary](#1-config-inventory-summary)
- [2. Cluster Config — Code References and Pipeline Impact](#2-cluster-config--code-references-and-pipeline-impact)
- [3. Instance Config — Code References and Pipeline Impact](#3-instance-config--code-references-and-pipeline-impact)
- [4. Resource Config / IdealState — Code References and Pipeline Impact](#4-resource-config--idealstate--code-references-and-pipeline-impact)
- [5. Config Resolution Hierarchy](#5-config-resolution-hierarchy)
- [6. Pipeline Stage to Config Dependencies](#6-pipeline-stage-to-config-dependencies)
- [7. Dead / Unused Configs](#7-dead--unused-configs)
- [8. Cross-System Usage: Espresso, Venice, Ambry, Seas](#8-cross-system-usage-espresso-venice-ambry-seas)
- [9. Incident Catalog](#9-incident-catalog)
- [10. Config Interaction Risk Map](#10-config-interaction-risk-map)
- [11. Config Tiering](#11-config-tiering)
- [12. Proposal: Cluster Profiles](#12-proposal-cluster-profiles)
- [13. Key Observations](#13-key-observations)

---

## 1. Config Inventory Summary

Helix exposes **120+ configurable properties** across 7 scopes. All configs extend `HelixProperty` wrapping `ZNRecord` (simple fields, map fields, list fields). `ConfigAccessor` provides CRUD access to configs stored in ZK under `/{clusterName}/CONFIGS/`.

| Scope | Model Class | Count | ZK Path |
|-------|-------------|-------|---------|
| CLUSTER | `ClusterConfig` | ~35+ | `/{cluster}/CONFIGS/CLUSTER/{cluster}` |
| PARTICIPANT | `InstanceConfig` | ~18 | `/{cluster}/CONFIGS/PARTICIPANT/{instance}` |
| RESOURCE | `ResourceConfig` + `IdealState` | ~20+ | `/{cluster}/CONFIGS/RESOURCE/{resource}` + `/{cluster}/IDEALSTATES/{resource}` |
| PARTITION | Via `ResourceConfig` maps | Variable | Embedded in resource config map fields |
| CLOUD | `CloudConfig` | ~5 | `/{cluster}/CONFIGS/CLOUD` |
| REST | Via REST server config | ~5 | Runtime only |
| CUSTOMIZED_STATE | Via customized state config | Variable | `/{cluster}/CONFIGS/CUSTOMIZED_STATE` |

`ResourceConfig` and `IdealState` have significant overlap — many properties exist in both. `Resource.java` merges them with IdealState taking precedence for some fields and ResourceConfig for others. This is historical debt.

---

## 2. Cluster Config — Code References and Pipeline Impact

### 2.1 Rebalancing

#### `DELAY_REBALANCE_ENABLED` — `isDelayRebalaceEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedRebalanceUtil | `rebalancer/util/DelayedRebalanceUtil.java:58` | Cluster-level gate: must be `true` AND `DELAY_REBALANCE_TIME > 0` |
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:69` | Combined with resource-level `IdealState.isDelayRebalanceEnabled()` — resource overrides cluster |

#### `DELAY_REBALANCE_TIME` — `getRebalanceDelayTime()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:57` | Checked `> 0` to enable feature |
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:78` | Cluster default; resource-level overrides if set |
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:94,139` | Computes which offline nodes are still "active" — nodes offline shorter than this duration keep their partitions |
| WagedRebalancer | `rebalancer/waged/WagedRebalancer.java:611` | Schedules future rebalance events |

#### `PERSIST_BEST_POSSIBLE_ASSIGNMENT` — `isPersistBestPossibleAssignment()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| PersistAssignmentStage | `controller/stages/PersistAssignmentStage.java:64` | If both PERSIST flags are false → stage SKIPPED |
| StrictMatchExternalViewVerifier | `tools/ClusterVerifiers/StrictMatchExternalViewVerifier.java:297` | Requires at least one persist flag for FULL_AUTO verification |

#### `PERSIST_INTERMEDIATE_ASSIGNMENT` — `isPersistIntermediateAssignment()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| PersistAssignmentStage | `PersistAssignmentStage.java:64-65` | Combined check with PERSIST_BEST_POSSIBLE |
| PersistAssignmentStage | `PersistAssignmentStage.java:120` | If `true`, writes throttled IntermediateStateOutput instead of BestPossibleStateOutput |
| InstanceValidationUtil | `util/InstanceValidationUtil.java:199` | MANDATORY for instance stability checks |

#### `ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE` — `getErrorOrRecoveryPartitionThresholdForLoadBalance()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| IntermediateStateCalcStage | `controller/stages/IntermediateStateCalcStage.java:338-340` | If error+recovery partitions exceed threshold → ALL load-balance transitions SUSPENDED. Default `-1` means use value of `1` |

#### `RESOURCE_PRIORITY_FIELD` — `getResourcePriorityField()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| IntermediateStateCalcStage | `IntermediateStateCalcStage.java:129-130` | If null → equal priority. Otherwise names a field in ResourceConfig/IdealState used to sort resources for transition priority |

#### `LAST_ON_DEMAND_REBALANCE_TIMESTAMP` — `getLastOnDemandRebalanceTimestamp()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:139` | Forces offline instances to be treated as inactive immediately (overrides delay grace period) |

### 2.2 WAGED Rebalancer

#### `INSTANCE_CAPACITY_KEYS` — `getInstanceCapacityKeys()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| WagedValidationUtil | `rebalancer/util/WagedValidationUtil.java:53` | MANDATORY — throws exception if instance config lacks any required key |
| AssignableNode | `rebalancer/waged/model/AssignableNode.java:420` | Filters instance capacity map to only these dimensions |
| WagedRebalanceUtil | `rebalancer/util/WagedRebalanceUtil.java:74` | Filters partition capacity to match |

#### `DEFAULT_INSTANCE_CAPACITY_MAP` — `getDefaultInstanceCapacityMap()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| WagedValidationUtil | `WagedValidationUtil.java:50` | Provides fallback capacity when instances don't specify their own |

#### `DEFAULT_PARTITION_WEIGHT_MAP` — `getDefaultPartitionWeightMap()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| WagedValidationUtil | `WagedValidationUtil.java:79` | Provides fallback weights when resources don't specify their own |
| ZKHelixAdmin | `manager/zk/ZKHelixAdmin.java:2862-2871` | Validates weights cover all required capacity keys at write time |

#### `REBALANCE_PREFERENCE` — `getGlobalRebalancePreference()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `controller/stages/BestPossibleStateCalcStage.java:416` | Updates WAGED with EVENNESS vs LESS_MOVEMENT vs FORCE_BASELINE_CONVERGE weights (each 0-1000) |

#### `GLOBAL_REBALANCE_ASYNC_MODE` — `isGlobalRebalanceAsyncModeEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:418` | Enables async global rebalance — reduces controller latency, may produce intermediate suboptimal states |

#### `PREFERRED_SCORING_KEYS` — `getPreferredScoringKeys()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ClusterContext | `rebalancer/waged/model/ClusterContext.java:96` | Configures which capacity dimensions are prioritized in evenness scoring |
| MaxCapacityUsageInstanceConstraint | `rebalancer/waged/constraints/MaxCapacityUsageInstanceConstraint.java:40` | Affects capacity utilization scoring |
| TopStateMaxCapacityUsageInstanceConstraint | `rebalancer/waged/constraints/TopStateMaxCapacityUsageInstanceConstraint.java:44` | Affects top-state distribution scoring |

#### `RELAXED_DISABLED_PARTITION_CONSTRAINT` — `isRelaxedDisabledPartitionConstraintEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ClusterContext | `ClusterContext.java:297-304` | Resource-level override first, then cluster fallback |
| ReplicaActivateConstraint | `rebalancer/waged/constraints/ReplicaActivateConstraint.java:45` | If relaxed → disabled partitions stay OFFLINE; if strict → forced reassignment |

### 2.3 Topology

#### `TOPOLOGY_AWARE_ENABLED` — `isTopologyAwareEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ClusterTopologyConfig | `model/ClusterTopologyConfig.java:51-56` | GATE: if `false`, empty topology; if `true`, parses TOPOLOGY and FAULT_ZONE_TYPE |

#### `TOPOLOGY` — `getTopology()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ClusterTopologyConfig | `ClusterTopologyConfig.java:63-80` | Parses hierarchy string (e.g., `/zone/rack/host`) into topology levels |

#### `FAULT_ZONE_TYPE` — `getFaultZoneType()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ClusterTopologyConfig | `ClusterTopologyConfig.java:74-80` | Specifies which level enforces fault isolation |
| CrushRebalanceStrategy | `rebalancer/strategy/CrushRebalanceStrategy.java:174` | CRUSH uses this for cross-fault-zone placement |

### 2.4 Throttling and Messaging

#### `STATE_TRANSITION_THROTTLE_CONFIGS` — `getStateTransitionThrottleConfigs()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| StateTransitionThrottleController | `controller/stages/StateTransitionThrottleController.java:62` | Initializes per-cluster, per-instance, per-resource throttle limits |
| IntermediateStateCalcStage | `IntermediateStateCalcStage.java:117` | Creates throttle controller; enforces during intermediate state calc |
| DelayedAutoRebalancer | `rebalancer/DelayedAutoRebalancer.java:443-447` | Extracts PARTITION-scope throttle for recovery |

#### `STATE_TRANSITION_CANCELLATION_ENABLED` — `isStateTransitionCancelEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| MessageGenerationPhase | `controller/stages/MessageGenerationPhase.java:195` | Generates cancellation messages for pending transitions when ideal state changes |
| ManagementMessageGenerationPhase | `controller/stages/ManagementMessageGenerationPhase.java:62` | During cluster pause, enables cancellation |

#### `P2P_MESSAGE_ENABLED` — `isP2PMessageEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| Resource.isP2PMessageEnabled() | `model/Resource.java:202-214` | Resolution: ResourceConfig > ClusterConfig > `false` |
| MessageSelectionStage | `controller/stages/MessageSelectionStage.java:95` | When enabled + single-top-state model → attaches relay messages to downward transitions |

### 2.5 Maintenance Mode

#### `MAX_OFFLINE_INSTANCES_ALLOWED` — `getMaxOfflineInstancesAllowed()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:371-410` | Counts offline+disabled (excluding SWAP_IN, UNKNOWN). Exceeds threshold → auto maintenance. `-1` disables |

#### `NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT` — `getNumOfflineInstancesForAutoExit()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| MaintenanceRecoveryStage | `controller/stages/MaintenanceRecoveryStage.java:85-99` | Offline drops to/below → auto-exits maintenance. `-1` disables |

#### `MAX_PARTITIONS_PER_INSTANCE` — `getMaxPartitionsPerInstance()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| IntermediateStateCalcStage | `IntermediateStateCalcStage.java:91-95` | Violated → cluster enters maintenance |
| MaintenanceRecoveryStage | `MaintenanceRecoveryStage.java:134-138` | Checks before allowing maintenance exit |
| AssignableNode | `AssignableNode.java:91` | WAGED hard capacity constraint |
| AutoRebalancer | `rebalancer/AutoRebalancer.java:125` | Passed to placement strategy |

### 2.6 Tasks

#### `MAX_CONCURRENT_TASK_PER_INSTANCE` — `getMaxConcurrentTaskPerInstance()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AbstractTaskDispatcher | `task/AbstractTaskDispatcher.java:650-656` | Fallback when instance-level not set. Default: 40. Controls task framework only, not state transitions |

**Note:** Espresso and Seas both use tasks and partitions on the same cluster (Espresso: bulk operations; Seas: index tenure, snapshot bootstrap). Venice and Ambry don't overlap.

---

## 3. Instance Config — Code References and Pipeline Impact

#### `HELIX_ENABLED` — `getInstanceEnabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ReadClusterDataStage | `controller/stages/ReadClusterDataStage.java:101` | Populates disabledInstanceSet |
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:156` | Filters from rebalance candidates |
| Topology | `rebalancer/topology/Topology.java:213` | Only enabled instances in topology tree |

#### `HELIX_INSTANCE_OPERATIONS` — `getInstanceOperation()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BaseControllerDataProvider | `controller/dataproviders/BaseControllerDataProvider.java:416,424` | Segregates by operation (EVACUATE, SWAP_IN, SWAP_OUT, UNKNOWN) |
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:382` | EVACUATE excluded from new assignments |
| RoutingDataCache | `spectator/RoutingDataCache.java:185,194` | Filters for client routing |
| CurrentStateComputationStage | `controller/stages/CurrentStateComputationStage.java:111` | Excludes UNKNOWN from state computation |

#### `TAG_LIST` — `getTags()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AutoRebalancer | `rebalancer/AutoRebalancer.java:94` | Filters nodes to match resource's INSTANCE_GROUP_TAG |
| BaseControllerDataProvider | `BaseControllerDataProvider.java:768,784` | Tag-based filtering |
| AssignableNode | `AssignableNode.java:85` | Copied for WAGED constraints |

#### `INSTANCE_WEIGHT` — `getWeight()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| Topology | `Topology.java:197,385` | Builds tree using weights, calculates zone weights |
| MultiRoundCrushRebalanceStrategy | `MultiRoundCrushRebalanceStrategy.java:197,201,276` | Higher weight → more partitions |
| AbstractEvenDistributionRebalanceStrategy | `AbstractEvenDistributionRebalanceStrategy.java:101` | Zero-weight excluded |

#### `DOMAIN` — `getDomainAsMap()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| Topology | `Topology.java:210,217,258` | Validates domain keys match cluster TOPOLOGY |
| AbstractRebalancer | `AbstractRebalancer.java:603,605,637` | Extracts fault zone for replica diversity |

#### `DELAY_REBALANCE_ENABLED` (instance-level)

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:122,123` | Per-instance override of cluster-level |

#### `MAX_CONCURRENT_TASK` — `getMaxConcurrentTask()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AbstractTaskDispatcher | `AbstractTaskDispatcher.java:651` | Instance-level task capacity, falls back to cluster default |

#### `INSTANCE_CAPACITY_MAP` — `getInstanceCapacityMap()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| WagedValidationUtil | `WagedValidationUtil.java:51` | Overlays on cluster's DEFAULT_INSTANCE_CAPACITY_MAP |

#### `HELIX_DISABLED_PARTITION` — `getDisabledPartitions()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ReadClusterDataStage | `ReadClusterDataStage.java:106,107` | Collects for monitoring |
| BaseControllerDataProvider | `BaseControllerDataProvider.java:1096` | Filters from assignable replicas |
| AssignableNode | `AssignableNode.java:86` | Copied for WAGED constraints |
| ReplicaActivateConstraint | `ReplicaActivateConstraint.java:39,40` | Hard constraint preventing activation |

#### `TARGET_TASK_THREAD_POOL_SIZE` — `getTargetTaskThreadPoolSize()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| TaskUtil | `task/TaskUtil.java:1183` | Thread pool size for task execution; falls back to cluster level |

---

## 4. Resource Config / IdealState — Code References and Pipeline Impact

#### `REBALANCE_MODE` — `getRebalanceMode()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:582,624,630,646,650` | **CRITICAL**: selects rebalancer (FULL_AUTO→Auto, SEMI_AUTO→SemiAuto, USER_DEFINED→custom, CUSTOMIZED→Custom) |
| IntermediateStateCalcStage | `IntermediateStateCalcStage.java:309` | Intermediate calc only for FULL_AUTO |
| PersistAssignmentStage | `PersistAssignmentStage.java:99,219` | Persistence format differs by mode |

#### `REBALANCER_CLASS_NAME` — `getRebalancerClassName()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:630,646` | Loads custom rebalancer for USER_DEFINED |
| WagedValidationUtil | `WagedValidationUtil.java:102` | Detects WagedRebalancer |

#### `REBALANCE_STRATEGY` — `getRebalanceStrategy()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AutoRebalancer | `AutoRebalancer.java:127` | Selects placement algorithm (CRUSH, EvenDistribution, etc.) |
| DelayedAutoRebalancer | `DelayedAutoRebalancer.java:155,205` | Preserved across rebalances |

#### `STATE_MODEL_DEF_REF` — `getStateModelDefRef()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:128,233,515,519` | Fetches state model for valid states |
| CurrentStateComputationStage | `CurrentStateComputationStage.java:203,204,261` | Stores for downstream |
| AutoRebalancer | `AutoRebalancer.java:69` | Looks up valid states |

#### `INSTANCE_GROUP_TAG` — `getInstanceGroupTag()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AutoRebalancer | `AutoRebalancer.java:91,94,105,110,114` | **CRITICAL**: filters instances to matching tag; warns if none |
| DelayedAutoRebalancer | `DelayedAutoRebalancer.java:96,105` | Same for delayed |
| JobDispatcher | `task/JobDispatcher.java:144,145` | Tasks only to tag-matching instances |
| ResourceComputationStage | `ResourceComputationStage.java:121,219,266` | Sets on Resource for downstream |

#### `MIN_ACTIVE_REPLICAS` — `getMinActiveReplica()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedAutoRebalancer | `DelayedAutoRebalancer.java:174` | Ensures minimum during recovery |
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:253,411,432` | Controls activation during recovery |
| WagedRebalancer | `WagedRebalancer.java:629` | WAGED respects this |

#### `MAX_PARTITIONS_PER_INSTANCE` (resource-level)

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| AutoRebalancer | `AutoRebalancer.java:125` | Passed to strategy |
| DelayedAutoRebalancer | `DelayedAutoRebalancer.java:153` | Same |
| AssignableReplica | `AssignableReplica.java:69` | Stored for WAGED |

#### `REBALANCE_DELAY` (resource-level)

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| DelayedRebalanceUtil | `DelayedRebalanceUtil.java:76` | Resource override of cluster DELAY_REBALANCE_TIME |
| DelayedAutoRebalancer | `DelayedAutoRebalancer.java:112,253` | Controls recovery wait per resource |

#### `EXTERNAL_VIEW_DISABLED` — `isExternalViewDisabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| ExternalViewComputeStage | `ExternalViewComputeStage.java:113` | Removes EV ZNode from ZK |

#### `MONITORING_DISABLED` — `isMonitoringDisabled()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| BestPossibleStateCalcStage | `BestPossibleStateCalcStage.java:123` | Skips monitoring registration |
| ExternalViewComputeStage | `ExternalViewComputeStage.java:177` | Skips pending message metrics |

#### `PARTITION_CAPACITY_MAP` — `getPartitionCapacityMap()`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| WagedRebalanceUtil | `WagedRebalanceUtil.java:65` | Partition weights for WAGED |
| CurrentStateComputationStage | `CurrentStateComputationStage.java:343` | Average weight metrics |

#### `ACTIVE_STATES_FOR_MIN_ACTIVE_REPLICA_CHECK`

| Consumer | File:Line | Behavior |
|----------|-----------|----------|
| InstanceValidationUtil | `InstanceValidationUtil.java:530` | Customizes which states count as "active" for health checks |

---

## 5. Config Resolution Hierarchy

Where the same config exists at multiple scopes:

```
P2P_MESSAGE_ENABLED:         ResourceConfig > ClusterConfig > false
DELAY_REBALANCE_ENABLED:     Resource(IdealState) > ClusterConfig; Instance can also override
DELAY_REBALANCE_TIME:        Resource(IdealState.getRebalanceDelay) > ClusterConfig
MAX_PARTITIONS_PER_INSTANCE: Both resource-level and cluster-level checked independently
MAX_CONCURRENT_TASK:         InstanceConfig > ClusterConfig (default 40)
RELAXED_DISABLED_PARTITION:  ResourceConfig > ClusterConfig
```

---

## 6. Pipeline Stage → Config Dependencies

```
ReadClusterDataStage
  └─ HELIX_ENABLED, TAG_LIST, HELIX_DISABLED_PARTITION, INSTANCE_OPERATIONS

ResourceComputationStage
  └─ INSTANCE_GROUP_TAG, REBALANCE_MODE

CurrentStateComputationStage
  └─ STATE_MODEL_DEF_REF, INSTANCE_OPERATIONS, PARTITION_CAPACITY_MAP

BestPossibleStateCalcStage  ← heaviest consumer
  └─ REBALANCE_MODE, REBALANCER_CLASS_NAME, REBALANCE_PREFERENCE,
     GLOBAL_REBALANCE_ASYNC_MODE, MAX_OFFLINE_INSTANCES_ALLOWED,
     REPLICAS, MONITORING_DISABLED

IntermediateStateCalcStage
  └─ STATE_TRANSITION_THROTTLE_CONFIGS, MAX_PARTITIONS_PER_INSTANCE,
     RESOURCE_PRIORITY_FIELD, ERROR_OR_RECOVERY_PARTITION_THRESHOLD

MessageGenerationPhase
  └─ STATE_TRANSITION_CANCELLATION_ENABLED

MessageSelectionStage
  └─ P2P_MESSAGE_ENABLED

PersistAssignmentStage
  └─ PERSIST_BEST_POSSIBLE_ASSIGNMENT, PERSIST_INTERMEDIATE_ASSIGNMENT

ExternalViewComputeStage
  └─ EXTERNAL_VIEW_DISABLED, MONITORING_DISABLED, STATE_MODEL_DEF_REF

MaintenanceRecoveryStage
  └─ NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT, MAX_PARTITIONS_PER_INSTANCE
```

---

## 7. Dead / Unused Configs

### Verified Dead (7 properties — safe to remove)

| Property | Evidence |
|----------|----------|
| `HELIX_DISABLE_PIPELINE_TRIGGERS` | No reads in any module |
| `GLOBAL_MAX_PARTITIONS_ALLOWED_PER_INSTANCE` | Shadowed by per-resource version; no consumer |
| `GLOBAL_TARGET_TASK_THREAD_POOL_SIZE` | No consumer in task dispatcher |
| `TARGET_EXTERNALVIEW_ENABLED` | No consumer in EV computation |
| `PARTICIPANT_DEREGISTRATION_TIMEOUT` | Venice sets it but Helix core doesn't read it |
| `ABNORMAL_STATES_RESOLVER_MAP` | Only in test code |
| `LAST_ON_DEMAND_REBALANCE_TIMESTAMP` | Internal controller state, not customer-set |

### Previously Reported Dead but Verified Alive (6 properties — do NOT remove)

Initial audit scoped search to controller/rebalancer pipeline only. Expanded verification found active consumers:

| Property | Consumer | File:Line |
|----------|----------|-----------|
| `MISS_TOP_STATE_DURATION_THRESHOLD` | Monitoring | `TopStateHandoffReportStage.java:101` |
| `OFFLINE_NODE_TIME_OUT_FOR_MAINTENANCE_MODE` | Maintenance timeout | `BaseControllerDataProvider.java:493` |
| `OFFLINE_DURATION_FOR_PURGE_MS` | Purge API | `ZKHelixAdmin.java:2928` |
| `VIEW_CLUSTER` | View aggregator | `helix-view-aggregator/SourceClusterConfigChangeAction.java:59` |
| `VIEW_CLUSTER_SOURCES` | View aggregator | `ViewClusterRefresher.java` |
| `VIEW_CLUSTER_REFRESH_PERIOD` | View aggregator | `ViewClusterRefresher.java` |

### Needs Verification (1 property)

| Property | Status |
|----------|--------|
| `QUOTA_TYPES` | Task framework uses quota type concepts extensively (`AssignableInstance`, `ThreadCountBasedTaskAssigner`), but the relationship between this ClusterConfig property and the runtime quota type strings needs closer verification |

---

## 8. Cross-System Usage: Espresso, Venice, Ambry, Seas

### 8.1 Comparison Matrix

| Config | Espresso | Venice | Ambry | Seas |
|---|---|---|---|---|
| **Rebalancer** | WagedRebalancer | Controller: Waged; Storage: DelayedAuto | VCR: Waged; Storage: FULL_AUTO | Configurable per UIC client (can be WAGED) |
| **Strategy** | CrushEdRebalanceStrategy | Configurable via `getHelixRebalanceAlg()` | From IdealStateConfigFields | CrushEdRebalanceStrategy |
| **State Model** | LeaderStandby | LeaderStandby | Varies | Custom `IndexerStateModel` (OFFLINE→ASSIGNED→INDEX_DOWNLOADED→CAUGHT_UP) |
| **TOPOLOGY_AWARE** | Yes | Storage: Yes; Controller: No | Yes | Yes (`/mz/host/applicationInstanceId`) |
| **FAULT_ZONE_TYPE** | `helixZoneId` or `mz` | From config (`zone`) | `rack` or `mz` | `mz` |
| **DELAY_REBALANCE** | Yes (resource-level) | Storage: Yes | WAGED: 6 hours | Yes (configurable per cluster AND per resource) |
| **PERSIST_BEST_POSSIBLE** | Yes | Yes | Yes | No (`PERSIST_INTERMEDIATE` = true instead) |
| **MAX_OFFLINE_INSTANCES** | Fault zones + 3 (dynamic) | Not set | From ClusterConfigFields | Configurable per cluster |
| **P2P_MESSAGE_ENABLED** | **Yes** | No | No | No |
| **ST_CANCELLATION** | **Yes** | No | No | **Yes** (except OFFLINE→ASSIGNED) |
| **ST_THROTTLE** | **Yes** — Instance: 1-20; Cluster: 400-6000 dynamic | No (app-level) | Via HelixBootstrapUpgradeUtil | **Yes** — fully configurable per cluster |
| **CAPACITY_KEYS** | `[CU, DISK, PARTCOUNT]` | From HelixCapacityConfig | `[DISK]` | `[partition]` |
| **DEFAULT_CAPACITY_MAP** | `{CU: computed, DISK: computed, PARTCOUNT: 800}` | From HelixCapacityConfig | From WAGED JSON | `{partition: 8}` |
| **PARTITION_WEIGHT_MAP** | Per-partition `{CU, DISK, PARTCOUNT}` from ingraph | From HelixCapacityConfig | `{partitionDiskWeightInGB: 386}` | `{partition: 1}` (uniform) |
| **REBALANCE_PREFERENCE** | FORCE_BASELINE_CONVERGE=0 validated | EVENNESS-biased | evenness=3, lessMovement=4 | EVENNESS=1, LESS_MOVEMENT=2 |
| **INSTANCE_OPERATION** | DISABLE, EVACUATE | UNKNOWN join | UNKNOWN join | Not used |
| **REBALANCE_TIMER** | 4 minutes | Not set | Not set | Not set |
| **ERROR_THRESHOLD** | Not used | Not used | WAGED JSON: 2000 | `maxOffline / 2` (derived) |
| **RESOURCE_PRIORITY_FIELD** | Not used | Not used | Not used | Not used |
| **CLOUD_CONFIG** | No | No | No | **Yes** (INOPS + Inventory Manager) |
| **TASK_FRAMEWORK** | Yes (bulk operations) | No | Lightweight (stats aggregation) | **Yes** (index tenure, snapshot bootstrap) |

### 8.2 Source Files

**Espresso** (repo: `linkedin-multiproduct/espresso`):

| Tool | Purpose |
|------|---------|
| `WAGEDRebalancerConfigTool.java` | Computes CU/DISK weights from live ingraph stats |
| `WAGEDConfigCheckAndFixer.java` | Validates/repairs WAGED config |
| `WAGEDConfigToolV2.java` | Dynamic cluster throttle based on router errors |
| `FaultZoneUpdater.java` | Topology + MAX_OFFLINE management |
| `FullAutoUpgradeV2.java` | Enables P2P, cancellation, throttling |

**Venice** (repo: `linkedin/venice`):

| File | Purpose |
|------|---------|
| `ZkHelixAdminClient.java` | Cluster/resource creation |
| `VeniceControllerClusterConfig.java` | Config source for Helix params |
| `HelixCapacityConfig.java` | WAGED capacity wrapper |

**Ambry** (repo: `linkedin/ambry`):

| File | Purpose |
|------|---------|
| `HelixVcrUtil.java` | VCR cluster setup |
| `HelixBootstrapUpgradeUtil.java` | Storage cluster bootstrap |
| `WagedRebalancerHelixConfig.json` | WAGED config values |
| `HelixFactoryWithMetadata.java` (AmbryLI) | Instance registration with DISK capacity |

**Seas** (repos: `linkedin-multiproduct/seas7`, `linkedin-multiproduct/search-cloud`, `linkedin-multiproduct/search-cloud-utils`):

| File | Purpose |
|------|---------|
| `IndexerCloudClusterResource.java` | Cluster config management (throttle, maintenance, delay, WAGED, cloud) |
| `IndexingRequestHelixResourceManager.java` | Resource/IdealState creation per indexing request |
| `IndexerStateModelDefinition.java` | Custom 4-state model definition |
| `DynamicIndexerCloudService.java` | Participant registration with tags + task config |
| `HelixConfigConverter.java` | ResourceConfig → IndexerInstanceProps conversion |

### 8.3 Observations

1. **Espresso is the power user** — uses nearly every knob including P2P, cancellation, dynamic throttle, 3-dimensional capacity
2. **Seas is the second most sophisticated** — WAGED, throttling, cancellation, delay, cloud config, tasks, AND a custom state model
3. **Venice is most conservative** — basic rebalancing only, app-level throttling
4. **Ambry is in between** — WAGED capacity (DISK-only), throttling, maintenance, no P2P or cancellation
5. **Config validation exists only in Espresso** (`WAGEDConfigCheckAndFixer`)
6. **Dynamic config adjustment exists only in Espresso** (`WAGEDConfigToolV2`)
7. **Seas derives configs from other configs** — `ERROR_THRESHOLD = maxOffline / 2`, `AUTO_EXIT = maxOffline - 1`. This formula-based derivation is a pattern profiles should automate
8. **No system uses:** `RESOURCE_PRIORITY_FIELD`, `RELAXED_DISABLED_PARTITION_CONSTRAINT`, `PREFERRED_SCORING_KEYS`
9. **Only Espresso uses P2P** — all other systems (Venice, Ambry, Seas) do not
10. **All four use FULL_AUTO** with topology-aware placement
11. **Espresso and Seas both use partitions AND tasks** on the same cluster

---

## 9. Incident Catalog

### 9.1 Critical (S0/Blocker)

#### HELIX-1830: Leaderless Partition 12 Hours — P2P Relay Stuck

- **JIRA:** [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830) (Major, Closed)
- **System:** Espresso (prod-lva1, MT-LD-8)
- **Config:** `P2P_MESSAGE_ENABLED` + MissingTopState detection
- **What:** Node with bad disk couldn't complete SLAVE→MASTER relay. Stuck in "Message already exists" loop ~12h. Helix failed to detect MissingTopState
- **Impact:** Partition leaderless 12h in production
- **Resolution:** Node rebooted; fix needed in MissingTopState detection + relay timeout

#### ESPENG-26308: Thread Exhaustion — Cluster-Wide Halt

- **JIRA:** [ESPENG-26308](https://linkedin.atlassian.net/browse/ESPENG-26308) (Blocker, Closed)
- **System:** Espresso DR
- **Config:** State transition thread pool + 6-hour retry timeout
- **What:** Stale resources triggered 6h retries blocking all ST threads
- **Impact:** ALL state transitions halted, deployment failure
- **Resolution:** Reduce retry timeout, make non-blocking, clean stale resources

#### ACTIONITEM-13494: ForceMaster Tool Broken

- **JIRA:** [ACTIONITEM-13494](https://linkedin.atlassian.net/browse/ACTIONITEM-13494) (Blocker, In Progress)
- **System:** Espresso
- **What:** Manual mastership override tool broke after xinfra migration
- **Impact:** Operators lost ability to manually resolve stuck partition leadership

### 9.2 High

#### HELIX-1373/1291/1292: ZK Disconnect → Weeks of Stuck Transitions

- **JIRA:** [HELIX-1373](https://linkedin.atlassian.net/browse/HELIX-1373) / [HELIX-1291](https://linkedin.atlassian.net/browse/HELIX-1291) / [HELIX-1292](https://linkedin.atlassian.net/browse/HELIX-1292) (Major, Resolved)
- **System:** Pinot (CORP)
- **Config:** ZK session + message handling. `STATE_TRANSITION_CANCELLATION_ENABLED` (not used by Pinot) could have mitigated
- **Impact:** Deployments took "many weeks"
- **Resolution:** Improved message handling (HELIX-1301)

#### WAGED Migration Failures

- **Source:** [Espresso WAGED TroubleShooting Guide](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525378245) and [WAGED Weight Model Enhancements](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525356277)
- **System:** Espresso
- **Config:** `INSTANCE_CAPACITY_KEYS`, `DEFAULT_*_MAP`, `REBALANCE_PREFERENCE`
- **What:** Weight misconfiguration during CrushEd→WAGED migration
- **Impact:** Required rollbacks, forced global rebalances, multi-team sign-off
- **Resolution:** Built `WAGEDConfigCheckAndFixer`; Helix still has no server-side validation

#### HELIX-4971: MissingTopState Metric Delay

- **JIRA:** [HELIX-4971](https://linkedin.atlassian.net/browse/HELIX-4971) (Major, Closed)
- **System:** Espresso (MT-MD-4, MT-LEGACY, WATERLOO2)
- **What:** Metric only spiked after state-diff converged — useless for real-time alerting
- **Impact:** Operators couldn't use metric for early warning

#### State Transition Storms (DAAS-3900)

- **JIRA:** [DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900)
- **Source:** [Helix State Transition Priority Support](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525171713)
- **Config:** `STATE_TRANSITION_THROTTLE_CONFIGS` without priority
- **What:** Recovery transitions starved by load-balance during large events
- **Resolution:** Added `RESOURCE_PRIORITY_FIELD`; Venice and Ambry still don't use it

### 9.3 Medium

#### Venice Error Replica Accumulation

- **Source:** [Venice Error Replicas Runbook](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525339110)
- **Config:** No `ERROR_OR_RECOVERY_PARTITION_THRESHOLD` configured
- **What:** Partitions stuck in ERROR; "priority for site up"

#### CICP-1767: Task State Desync

- **JIRA:** [CICP-1767](https://linkedin.atlassian.net/browse/CICP-1767) (Major, Closed)
- **System:** Pinot
- **What:** Tasks completed but stuck at INIT in Helix; purge operations blocked

#### Library Misconfiguration (Systemic)

- **Source:** [Butchering Helix as a Service](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525222679)
- **Quote:** "life with providing complicated cluster management logics as library is rather miserable"

#### ZK Multi-Tenant Starvation

- **Source:** [ZK Multi-Tenancy Quota Throttling Design](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525333468)
- **What:** One app's heavy ZK usage starved others sharing ensemble

#### CICP-1468: 500+ Chronically Non-Approved Node Disruptions (Seas)

- **JIRA:** [CICP-1468](https://linkedin.atlassian.net/browse/CICP-1468) (Blocker, Reopened)
- **System:** Seas (hosted-search pools)
- **What:** Hosted-search pools had 500+ chronically non-approved node disruptions — biggest offender in Nimbus
- **Impact:** Ongoing operational pain; blocks infrastructure maintenance

#### HELIX-2905: WAGED Triggering Unnecessary State Transitions on Node Disable

- **JIRA:** [HELIX-2905](https://linkedin.atlassian.net/browse/HELIX-2905) (Major, Closed)
- **System:** Seas (and other WAGED users)
- **What:** Disabling a node triggered state transitions that weren't needed
- **Impact:** Unnecessary partition movement during planned operations

#### HELIX-3158: Resource Not Assigned to Any Instance

- **JIRA:** [HELIX-3158](https://linkedin.atlassian.net/browse/HELIX-3158) (Major, Closed)
- **System:** Seas
- **What:** Resource not assigned after creation. Workaround: set replicas=0 initially, then rebalance

---

## 10. Config Interaction Risk Map

| Config A | Interacts With | Risk | Evidence |
|----------|---------------|------|----------|
| `P2P_MESSAGE_ENABLED` | MissingTopState detection | Relay masks leaderless from detection | [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830) (12h leaderless) |
| `STATE_TRANSITION_THROTTLE` | Recovery vs load-balance priority | Recovery starved | [DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900) |
| `STATE_TRANSITION_CANCELLATION` | ZK disconnects | Without it, stuck transitions persist indefinitely | [HELIX-1373](https://linkedin.atlassian.net/browse/HELIX-1373) (weeks) |
| `INSTANCE_CAPACITY_KEYS` + weight maps | WAGED rebalancer | Wrong values accepted at write, fail at rebalance | [WAGED TroubleShooting Guide](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525378245) |
| `MAX_OFFLINE_INSTANCES_ALLOWED` | Maintenance + recovery | False maintenance blocks recovery | [S0 Leaderless ITR](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525418840) |
| `ERROR_OR_RECOVERY_THRESHOLD` | Load-balance transitions | Without it, error replicas accumulate | [Venice Error Replicas](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525339110) |
| `DELAY_REBALANCE_TIME` | Recovery speed | Too long = extended leaderless; too short = flapping | [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830), [ESPENG-53908](https://linkedin.atlassian.net/browse/ESPENG-53908) |
| ST thread pool | Retry timeouts | Long retries exhaust threads, halt all transitions | [ESPENG-26308](https://linkedin.atlassian.net/browse/ESPENG-26308) |
| WAGED + InstanceOperation.DISABLE | State transition generation | Unnecessary transitions on node disable | [HELIX-2905](https://linkedin.atlassian.net/browse/HELIX-2905) |

---

## 11. Config Tiering

### Tier 1: Essential (must be set for cluster to function)

| Config | Why Essential | Who Sets |
|--------|-------------|----------|
| `TOPOLOGY` | Defines hierarchy; throws if missing when topology-aware | Customer at creation |
| `TOPOLOGY_AWARE_ENABLED` | Gate for all topology logic | Customer at creation |
| `FAULT_ZONE_TYPE` | Replica placement across fault boundaries | Customer at creation |
| `STATE_MODEL_DEF_REF` | Every pipeline stage depends on it | Customer per resource |
| `REBALANCE_MODE` | Selects which rebalancer runs | Customer per resource |
| `NUM_PARTITIONS` | How many partitions exist | Customer per resource |
| `REPLICAS` | Replication factor | Customer per resource |
| `INSTANCE_GROUP_TAG` / `TAG_LIST` | Instance-to-resource affinity | Customer per resource/instance |
| `DOMAIN` | Instance position in topology tree | Customer per instance |

### Tier 2: Safety (high-blast-radius, must be understood)

| Config | Default | Why Dangerous | Incident Evidence |
|--------|---------|---------------|-------------------|
| `MAX_OFFLINE_INSTANCES_ALLOWED` | `-1` (disabled) | Too low → false maintenance; disabled → no protection | [S0 Leaderless ITR](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525418840); [ESPENG-47905](https://linkedin.atlassian.net/browse/ESPENG-47905) |
| `DELAY_REBALANCE_TIME` | `0` (disabled) | Too long → extended leaderless; too short → flapping | [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830); [ESPENG-53908](https://linkedin.atlassian.net/browse/ESPENG-53908) |
| `STATE_TRANSITION_THROTTLE_CONFIGS` | None | No throttle → storm; wrong throttle → recovery starved | [DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900); [ESPENG-26308](https://linkedin.atlassian.net/browse/ESPENG-26308) |
| `MAX_PARTITIONS_PER_INSTANCE` | `-1` (unlimited) | Violated → cluster enters maintenance | `MaintenanceRecoveryStage.java:134-138` |
| `MIN_ACTIVE_REPLICAS` | `0` | Wrong value → full unavailability during transitions | [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830); all systems set this |
| `INSTANCE_CAPACITY_KEYS` + capacity/weight maps | None | WAGED fails at rebalance (not write) if inconsistent | [WAGED TroubleShooting Guide](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525378245); [Weight Model Enhancements](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525356277) |
| `ERROR_OR_RECOVERY_PARTITION_THRESHOLD` | `-1` (threshold of 1) | Without it, error replicas accumulate unchecked | [Venice Error Replicas](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525339110) |

Configs with incident evidence suggesting promotion from Tier 3:

| Config | Evidence It Should Be Tier 2 |
|--------|------------------------------|
| `P2P_MESSAGE_ENABLED` | [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830) — relay messages created 12h leaderless failure mode |
| `STATE_TRANSITION_CANCELLATION_ENABLED` | [HELIX-1373](https://linkedin.atlassian.net/browse/HELIX-1373) / [HELIX-1291](https://linkedin.atlassian.net/browse/HELIX-1291) / [HELIX-1292](https://linkedin.atlassian.net/browse/HELIX-1292) — without it, Pinot stuck for weeks |
| `RESOURCE_PRIORITY_FIELD` | [DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900) — built to fix recovery starvation; nobody uses it |

### Tier 3: Optimization (safe defaults exist)

| Config | Default | What It Tunes | Who Uses |
|--------|---------|---------------|----------|
| `P2P_MESSAGE_ENABLED` | `false` | Reduces top-state unavailability during handoff | Espresso |
| `STATE_TRANSITION_CANCELLATION_ENABLED` | `false` | Allows cancelling stuck transitions | Espresso |
| `REBALANCE_PREFERENCE` | `{EVENNESS:1, LESS_MOVEMENT:1}` | WAGED objective weights | All WAGED users |
| `GLOBAL_REBALANCE_ASYNC_MODE` | `true` | Async vs sync WAGED global rebalance | Not explicitly set |
| `PERSIST_BEST_POSSIBLE_ASSIGNMENT` | `false` | Write best-possible to ZK | All three set `true` |
| `PERSIST_INTERMEDIATE_ASSIGNMENT` | `false` | Write throttled state to ZK | None explicitly set |
| `RESOURCE_PRIORITY_FIELD` | `null` | Resource ordering during throttled transitions | Nobody uses |
| `PREFERRED_SCORING_KEYS` | All keys | Which capacity dims prioritized in WAGED | Nobody uses |
| `RELAXED_DISABLED_PARTITION_CONSTRAINT` | `false` | Disabled partitions stay OFFLINE in WAGED | Nobody uses |
| `DELAY_REBALANCE_ENABLED` (instance) | Follows cluster | Per-instance delay override | Rarely used |
| `EXTERNAL_VIEW_DISABLED` | `false` | Suppresses EV ZNode | Resource-specific |
| `MONITORING_DISABLED` | `false` | Suppresses monitoring metrics | Resource-specific |
| `REBALANCE_DELAY` (resource) | Follows cluster | Per-resource delay override | Espresso (per DB) |
| `MAX_CONCURRENT_TASK_PER_INSTANCE` | `40` | Task framework parallelism | Espresso (default) |
| `REBALANCE_TIMER_PERIOD` | None | Periodic rebalance trigger | Espresso: 4 min |

### Tier 4: Internal / Remove (should not be customer-facing)

| Config | Status | Action |
|--------|--------|--------|
| `HELIX_DISABLE_PIPELINE_TRIGGERS` | Dead | Remove |
| `GLOBAL_MAX_PARTITIONS_ALLOWED_PER_INSTANCE` | Dead (shadowed) | Remove |
| `GLOBAL_TARGET_TASK_THREAD_POOL_SIZE` | Dead | Remove |
| `TARGET_EXTERNALVIEW_ENABLED` | Dead | Remove |
| `PARTICIPANT_DEREGISTRATION_TIMEOUT` | Venice writes, Helix doesn't read | Remove or implement |
| `ABNORMAL_STATES_RESOLVER_MAP` | Dead | Remove |
| `LAST_ON_DEMAND_REBALANCE_TIMESTAMP` | Internal controller state | Hide from API |

### Tier Summary

| Tier | Count | Customer Visibility |
|------|-------|-------------------|
| Essential | ~9 | Must set |
| Safety | ~7 (+3 borderline) | Should set with guidance |
| Optimization | ~15 | Can set, safe defaults exist |
| Internal/Remove | ~8 | Should not be exposed |
| **Total actively relevant** | **~31** | **Down from 120+** |

---

## 12. Proposal: Cluster Profiles

The tiering analysis shows that out of 120+ configs, only ~16 (Tier 1 + Tier 2) need customer attention, ~15 have safe defaults (Tier 3), and ~8 should be removed (Tier 4). The proposal is to collapse the config surface into **cluster profiles** — opinionated presets that set Tier 2 and Tier 3 configs to tested, validated values based on the workload type.

### 12.1 Profile Definitions

Each profile sets **cluster-level** Tier 2 (Safety) and Tier 3 (Optimization) configs to opinionated defaults. Resource-level configs (state model, rebalancer, strategy) are NOT set by the profile — they are per-resource decisions made by the customer. Profiles are applied as a single atomic `ClusterConfig` ZNRecord write.

#### `STORAGE_LEADER_STANDBY`

For partitioned data stores with leader/standby replication (Espresso, Venice storage).

| Config | Value Set by Profile | Rationale |
|--------|---------------------|-----------|
| `TOPOLOGY_AWARE_ENABLED` | `true` | Fault zone isolation |
| `PERSIST_BEST_POSSIBLE_ASSIGNMENT` | `true` | All four systems already set this |
| `P2P_MESSAGE_ENABLED` | `false` | Disabled by default. Opt-in for systems with single-top-state models after verifying MissingTopState timeout and relay expiry. Only Espresso uses P2P today; Venice, Ambry, Seas do not. [HELIX-1830](https://linkedin.atlassian.net/browse/HELIX-1830) demonstrated the risk. |
| `STATE_TRANSITION_CANCELLATION_ENABLED` | `true` | Prevents [HELIX-1373](https://linkedin.atlassian.net/browse/HELIX-1373) stuck transitions after ZK disconnects |
| `DELAY_REBALANCE_ENABLED` | `true` | Graceful offline handling |
| `STATE_TRANSITION_THROTTLE_CONFIGS` | Recovery: unlimited | Prevents [DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900) recovery starvation. Load-balance throttle values are customer-set (see Section 12.2) |
| `ERROR_OR_RECOVERY_PARTITION_THRESHOLD` | `100` | Prevents [Venice error replica](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525339110) accumulation from blocking all load-balance |
| `GLOBAL_REBALANCE_ASYNC_MODE` | `true` | Reduces controller latency |

#### `STORAGE_ONLINE_OFFLINE`

For blob/object stores with online/offline replicas (Ambry).

| Config | Value Set by Profile | Rationale |
|--------|---------------------|-----------|
| `TOPOLOGY_AWARE_ENABLED` | `true` | Fault zone isolation |
| `PERSIST_BEST_POSSIBLE_ASSIGNMENT` | `true` | Standard |
| `P2P_MESSAGE_ENABLED` | `false` | Not applicable — no single top state |
| `STATE_TRANSITION_CANCELLATION_ENABLED` | `true` | Prevents stuck transitions |
| `DELAY_REBALANCE_ENABLED` | `true` | Graceful offline handling |
| `STATE_TRANSITION_THROTTLE_CONFIGS` | Recovery: unlimited | Load-balance throttle values are customer-set |
| `ERROR_OR_RECOVERY_PARTITION_THRESHOLD` | `2000` | Ambry's WAGED JSON already uses 2000 |
| `GLOBAL_REBALANCE_ASYNC_MODE` | `true` | Standard |

#### `CONTROLLER`

For controller/management clusters (Venice controller, Helix CHO).

| Config | Value Set by Profile | Rationale |
|--------|---------------------|-----------|
| `TOPOLOGY_AWARE_ENABLED` | `false` | Controllers don't need topology isolation |
| `PERSIST_BEST_POSSIBLE_ASSIGNMENT` | `true` | Standard |
| `P2P_MESSAGE_ENABLED` | `false` | Not needed for controller clusters |
| `STATE_TRANSITION_CANCELLATION_ENABLED` | `true` | Standard safety |
| `DELAY_REBALANCE_ENABLED` | `false` | Controllers should rebalance immediately |

#### `CUSTOM`

Escape hatch — full manual config (current behavior). All 120+ configs exposed. For teams that need control beyond what profiles offer, or teams with custom state models (Seas: `IndexerStateModel`).

### 12.2 Customer-Facing Knobs Per Profile

After choosing a profile, the customer sets these:

**Cluster-level (~7 knobs):**

| Knob | Why It Varies | Example |
|------|---------------|---------|
| `topology` | Physical infrastructure layout | `/mz/host/applicationInstanceId` |
| `fault_zone` | Fault isolation boundary | `mz` |
| `rebalance_delay` | Recovery speed vs flapping trade-off | `6h` for Ambry, `15s` for Espresso test |
| `max_offline_threshold` | Maintenance mode trigger — depends on cluster size | `fault_zones + 3` for Espresso |
| `capacity_model` | What dimensions to balance on | `{keys: [DISK], defaults: {DISK: 1000}}` or `{keys: [CU, DISK, PARTCOUNT], ...}` |
| `instance_throttle` | Per-instance load-balance transition limit | Start with 10; tune based on I/O capacity |
| `cluster_throttle` | Cluster-wide load-balance transition limit | Recommendation: `num_instances * instance_throttle`. Seas derives `error_threshold = max_offline / 2` — formula-based derivation is encouraged |

**Resource-level (~6 knobs per resource):**

| Knob | Why It Varies |
|------|---------------|
| `state_model` | Custom (Seas: IndexerStateModel) or built-in (LeaderStandby, OnlineOffline, MasterSlave) |
| `rebalancer` | WAGED vs DelayedAutoRebalancer vs custom — depends on capacity needs. Venice storage uses DelayedAuto; Espresso uses WAGED |
| `rebalance_strategy` | CrushEd, EvenDistribution, etc. |
| `replicas` | Replication factor |
| `min_active_replicas` | Minimum for availability during transitions |
| `partition_weight` | Per-partition capacity (WAGED only) |

**Total: ~13 decisions instead of 120+ knobs.**

### 12.3 Tiering → Profile Mapping

| Tier | What Happens in Profile Model |
|------|------------------------------|
| **Tier 1 (Essential)** | Customer sets these explicitly (~7 cluster + ~6 per resource) |
| **Tier 2 (Safety)** | Profile sets opinionated, incident-informed defaults. Customer CAN override with a warning |
| **Tier 3 (Optimization)** | Profile sets safe defaults. Customer CAN override silently |
| **Tier 4 (Internal/Remove)** | Removed from code or hidden from API entirely |

### 12.4 Scope Clarification: Cluster-Level vs Resource-Level

Profiles set **cluster-level configs only** (single `ClusterConfig` ZNRecord, atomic ZK write). This includes: delay rebalance, maintenance thresholds, cancellation, persist flags, topology, capacity model, recovery throttle.

For resource-level configs (`STATE_MODEL_DEF_REF`, `REBALANCER_CLASS_NAME`, `REBALANCE_STRATEGY`, `MIN_ACTIVE_REPLICAS`, etc.), the profile provides **recommended defaults** that apply:
- At resource creation time (via REST API: "your profile recommends WagedRebalancer, you're using DelayedAuto — proceed?")
- NOT retroactively to existing resources

This avoids:
- Multi-ZNode atomicity problems (profile writes one ZNode only)
- Scope mixing (cluster-level concept doesn't set resource-level configs)
- Venice's use of DelayedAutoRebalancer being overridden
- Seas's per-resource rebalancer choice being overridden

### 12.5 Override Model

- **Resolution order:** `explicit_override > profile_default > helix_default`
- **Storage:** Overrides stored in a distinct `PROFILE_OVERRIDES` map field within `ClusterConfig` ZNRecord. Profile application writes defaults to regular config fields; overrides are stored separately and never touched by profile application.
- **Persistence:** Profile version upgrades preserve existing overrides.
- **Tier semantics:** Tier 2 (Safety) overrides emit a warning metric. Tier 3 (Optimization) overrides are silent.
- **REST API:**
  - `GET /clusters/{cluster}/profile/compliance` — returns base profile, version, list of overrides with `{config_name, profile_default, override_value, source, timestamp}`, and list of configs matching defaults
  - `DELETE /clusters/{cluster}/profile/overrides/{config_name}` — reverts a single override to profile default

### 12.6 Profile Versioning and Lifecycle

- Profiles are **one-shot templates** — applied at creation time or via explicit upgrade, NOT reconciliation loops. This is critical for compatibility with Espresso's `WAGEDConfigToolV2` (dynamic throttle adjustment) and Seas's per-cluster throttle tuning.
- `ClusterConfig` stores: `{profile: "STORAGE_LEADER_STANDBY", profile_version: 1, profile_applied_at: <timestamp>}`
- New profile versions ship with Helix releases but don't auto-propagate to existing clusters.
- **Upgrade API:** `POST /clusters/{cluster}/profile/upgrade` — shows diff ("these N configs will change, these M already match, these K have overrides that will be preserved"), requires operator confirmation, logs old/new values for audit trail.
- **Rollback:** `POST /clusters/{cluster}/profile/rollback` — restores pre-upgrade config from snapshot taken at upgrade time.

### 12.7 Profile → Incident Prevention

| Incident | Which Profile Config Prevents It |
|----------|--------------------------------|
| Throttle starving recovery ([DAAS-3900](https://linkedin.atlassian.net/browse/DAAS-3900)) | All profiles set recovery throttle unlimited |
| ZK disconnect stuck transitions ([HELIX-1373](https://linkedin.atlassian.net/browse/HELIX-1373)) | All profiles enable `STATE_TRANSITION_CANCELLATION` |
| WAGED weight misconfiguration ([WAGED Guide](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525378245)) | `capacity_model` is a single validated object — Helix validates at write time |
| Venice error replicas ([Runbook](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525339110)) | All storage profiles set `ERROR_OR_RECOVERY_PARTITION_THRESHOLD` |
| Library misconfiguration ([Confluence](https://linkedin.atlassian.net/wiki/spaces/ENGS/pages/525222679)) | Profiles eliminate the problem for non-CUSTOM users |
| Unnecessary transitions on disable ([HELIX-2905](https://linkedin.atlassian.net/browse/HELIX-2905)) | Validated config model prevents misconfigured WAGED + disable interaction |

### 12.8 How Existing Systems Map to Profiles

| System | Profile | What Changes |
|--------|---------|-------------|
| Espresso storage | `STORAGE_LEADER_STANDBY` | Gets ERROR_THRESHOLD they currently lack. Cancellation + persist already enabled. Throttle values set per-cluster (existing). CU capacity via REST endpoint. P2P opt-in via override. |
| Venice storage | `STORAGE_LEADER_STANDBY` | Gets cancellation, error threshold, persist they currently lack. Rebalancer stays DelayedAuto (per-resource choice). No P2P. Throttle values set per-cluster. |
| Venice controller | `CONTROLLER` | Minimal change — already close to profile |
| Ambry storage | `STORAGE_ONLINE_OFFLINE` | Gets cancellation they currently lack. DISK capacity via self-reporting. |
| Ambry VCR | `STORAGE_ONLINE_OFFLINE` | Same profile |
| Seas indexer cloud | `CUSTOM` | Custom state model (`IndexerStateModel`) not covered by profiles. Already well-configured — uses throttle, cancellation, delay, WAGED, error threshold derived from maxOffline. CloudConfig is unique. |
| Pinot | `CUSTOM` | MasterSlave state model not covered by current profiles. Gets cancellation benefit if adopted. |

### 12.9 Native Capacity Management

Today, every WAGED customer builds their own tooling to populate capacity values. Espresso built `WAGEDRebalancerConfigTool` (computes CU/DISK weights from InGraph), Venice uses a static POJO (`HelixCapacityConfig`), Ambry uses a JSON file + `HelixFactoryWithMetadata`. All three write via `ConfigAccessor` directly to ZK with no validation.

The ideal system eliminates this external tooling for common cases by making capacity a first-class Helix concept.

#### Participant Self-Reporting

The participant process runs on the host and has access to hardware info. Instead of requiring external tools to set `INSTANCE_CAPACITY_MAP`, participants self-report capacity at join time:

```java
// Ideal participant setup
manager = HelixManagerFactory.getZKHelixManager(cluster, instance, PARTICIPANT, zkAddr);
manager.getCapacityReporter()
    .enableAutoDetect(CapacityDimension.DISK)           // reads filesystem stats
    .enableAutoDetect(CapacityDimension.MEMORY)          // reads runtime memory
    .setStatic(CapacityDimension.PARTITION_COUNT, 800)   // operator-set limit
    .setRefreshInterval(Duration.ofMinutes(30));          // periodic re-report
manager.connect();
```

On connect, the participant writes to `InstanceConfig.INSTANCE_CAPACITY_MAP` automatically:

```json
{"DISK": 2900, "MEMORY": 64000, "PARTITION_COUNT": 800}
```

This is analogous to Kubernetes node capacity reporting — nodes self-report `capacity.cpu`, `capacity.memory`, `capacity.ephemeral-storage` at registration. No external tool needed.

#### Auto-Derived Partition Weights

For the common case (homogeneous partitions — Venice, Ambry), the controller derives weights automatically. Customers specify **resource-level intent**, not per-partition maps:

```java
// Today: customer sets per-partition weights (error-prone, requires external computation)
Map<String, Map<String, Integer>> partitionCapacity = new HashMap<>();
for (int i = 0; i < 100; i++) {
    partitionCapacity.put("partition_" + i, Map.of("DISK", 10, "PARTCOUNT", 1));
}
resourceConfig.setPartitionCapacityMap(partitionCapacity);

// Ideal: customer sets resource-level intent, Helix derives partition weights
resourceConfig.setTotalResourceWeight(Map.of("DISK", 1000));  // "this resource uses ~1TB"
// Helix auto-derives: each of 100 partitions weighs DISK=10
```

For heterogeneous partitions (Espresso where each DB partition has different CU/DISK weights), the per-partition override path remains available.

#### REST Endpoint for Custom Capacity Dimensions

Espresso's CU dimension requires querying InGraph — genuinely application-specific. Rather than running customer code inside the controller (which creates failure isolation concerns — timeout, OOM, classloading conflicts), Helix provides a validated REST endpoint for external capacity updates:

```
POST /clusters/{cluster}/capacity/instances/{instance}
Body: {"CU": 500, "source": "espresso-cu-tool"}

POST /clusters/{cluster}/capacity/resources/{resource}/partitions
Body: {"partition_0": {"CU": 10}, "partition_1": {"CU": 15}, "source": "espresso-cu-tool"}
```

The endpoint validates at write time (positive values, required dimensions present, consistency with capacity model) and writes to ZK. The controller reads from ZK as it does today — it never calls external services. This follows the Kubernetes model: kubelet reports capacity, scheduler reads — the scheduler never calls external services.

Espresso's `WAGEDRebalancerConfigTool` becomes an HTTP client instead of a direct ZK writer. The validation replaces `WAGEDConfigCheckAndFixer`.

**Per-dimension ownership model:**
- Each dimension annotated as `PARTICIPANT_AUTO` (written by participant self-reporting) or `EXTERNAL` (written by external tool via REST)
- Participant can't overwrite EXTERNAL dimensions; external tools can't overwrite PARTICIPANT_AUTO
- Operator can "pin" a value to prevent auto-reporting from overwriting during incident response

**Note:** The existing `org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider` interface (used by `ConstraintRebalanceStrategy`) is unrelated to this capacity reporting mechanism. No naming collision.

#### Capacity Model — Single Config Object

The 3-config dance (`INSTANCE_CAPACITY_KEYS` + `DEFAULT_INSTANCE_CAPACITY_MAP` + `DEFAULT_PARTITION_WEIGHT_MAP`) becomes a single `CapacityModel` config with built-in templates:

| Model | What Helix Does | Who Needs It |
|-------|----------------|--------------|
| `PARTITION_COUNT` | Balances partition count across instances. Weight = 1 per partition. | Venice (controller cluster), Seas |
| `DISK` | Participants self-report disk. Partition weight = total_resource_disk / num_partitions. | Ambry |
| `CUSTOM` | External tools post capacity via REST. Auto-detect for standard dimensions. | Espresso (CU + DISK + PARTCOUNT) |

Registration at cluster creation:

```java
// Ambry: zero external tooling needed
clusterConfig.setCapacityModel(new CapacityModel.Builder()
    .autoDetect(DISK)
    .build());

// Venice: zero external tooling needed
clusterConfig.setCapacityModel(new CapacityModel.Builder()
    .autoDetect(PARTITION_COUNT)
    .build());

// Espresso: DISK/PARTCOUNT auto-detected, CU via REST endpoint
clusterConfig.setCapacityModel(new CapacityModel.Builder()
    .autoDetect(DISK)
    .autoDetect(PARTITION_COUNT)
    .addExternalDimension("CU")
    .build());
```

#### Write-Time Validation

`ConfigAccessor` and REST API validate capacity model consistency at write time:

- All `INSTANCE_CAPACITY_KEYS` have entries in `DEFAULT_INSTANCE_CAPACITY_MAP`
- All values are positive integers
- `DEFAULT_PARTITION_WEIGHT_MAP` covers all keys
- If WAGED rebalancer is configured, capacity model is complete
- If P2P enabled, MissingTopState detection timeout is adequate
- If throttle configured, recovery priority is not starved

Additionally, a **dry-run rebalance API** provides semantic validation:

```
POST /clusters/{cluster}/validate-rebalance
Body: {proposed config changes}
Response: {convergent: true/false, violations: [...], estimated_movements: N}
```

This runs WAGED's solver against current state with proposed config and returns whether it converges — catching the semantic failures (total weight exceeds capacity, extreme skew) that structural validation cannot. Replaces Espresso's external `WAGEDConfigCheckAndFixer`.

#### What Each System Gains

| System | Today | With Native Capacity |
|--------|-------|---------------------|
| Espresso | `WAGEDRebalancerConfigTool` (CLI) + `WAGEDConfigCheckAndFixer` + `WAGEDConfigToolV2` | CU via REST capacity endpoint; DISK and PARTCOUNT automatic via self-reporting; validation built-in; dry-run rebalance API replaces WAGEDConfigCheckAndFixer |
| Venice | `HelixCapacityConfig` POJO + manual `setDefaultInstanceCapacityMap()` | `capacity_model=PARTITION_COUNT`; zero config code |
| Ambry | `WagedRebalancerHelixConfig.json` + `HelixFactoryWithMetadata` disk calculation | `capacity_model=DISK`; participant self-reports; zero external tooling |
| Seas | `IndexerCloudClusterResource.java` manually sets capacity keys/maps | `capacity_model=PARTITION_COUNT`; zero external tooling; formula derivation (`error_threshold = maxOffline/2`) could be built into profile |

### 12.10 Implementation Phases

**Phase 1: Remove dead code** (low risk)
- Delete 7 verified dead properties from `ClusterConfig.java`
- Grep-verify across ALL modules + external repos (Jarvis) before deletion
- Remove getters/setters, update tests
- Testing: full-codebase consumer search per property

**Phase 2: Server-side validation + dry-run rebalance API** (low risk)
- Structural validation in `ConfigAccessor`: positive values, required fields present, capacity key consistency
- REST API validation in `ClusterAccessor.java`: warn on known-dangerous combinations
- Dry-run rebalance endpoint: `POST /clusters/{cluster}/validate-rebalance` — runs WAGED solver against current state with proposed config
- Warn-only on existing configs; enforce on new writes
- Tiered: syntactic validation (blocking) → semantic validation (on-demand via dry-run) → cross-config (report-only)
- Testing: integration tests per validation rule; verify existing valid configs pass; verify known-bad configs from incident history are caught

**Phase 3: Participant capacity self-reporting** (medium risk)
- Add `CapacityReporter` to `HelixManager` participant role
- Built-in auto-detect for DISK (filesystem stats) and MEMORY (runtime)
- Per-dimension ownership: `PARTICIPANT_AUTO` vs `EXTERNAL` — prevents write conflicts
- Participant writes `INSTANCE_CAPACITY_MAP` at join and refreshes periodically
- Backward compatible — existing participants continue with external capacity setting
- Testing: mixed-mode (some self-reporting, others external), ZK session expiry re-report, write frequency impact

**Phase 4: CapacityModel abstraction + auto-derived weights + REST capacity endpoint** (medium risk)
- Add `CapacityModel` to `ClusterConfig` with templates: `PARTITION_COUNT`, `DISK`, `CUSTOM`
- Controller auto-derives partition weights from `totalResourceWeight / numPartitions` when no explicit weights set
- REST endpoint for external capacity updates (replaces direct ZK writes by customer tools)
- Validation at REST write time
- Existing clusters with explicit capacity keys/maps continue to work (backward compatible)
- Testing: auto-derived weights match manual weights for known clusters

**Phase 5: Introduce `ClusterProfile` enum + override model + versioning** (medium risk)
- Add `ClusterProfile` to `ClusterConfig`: `STORAGE_LEADER_STANDBY`, `STORAGE_ONLINE_OFFLINE`, `CONTROLLER`, `CUSTOM`
- Profile sets cluster-level Tier 2 + Tier 3 configs to opinionated defaults
- Resource-level configs provided as recommended defaults at resource creation, not retroactively applied
- Override model: `PROFILE_OVERRIDES` map field, compliance API, per-override audit trail
- Profile versioning: one-shot template, explicit upgrade API with diff preview, rollback support
- Existing clusters get `CUSTOM` profile (zero behavioral change)
- Testing: migration test (CUSTOM → profile, verify zero behavioral change), override persistence across version upgrade

**Phase 6: Future — Deprecate raw config access** (deferred)
- Deferred until all major consumers have migrated to profiles
- Requires explicit sign-off from Espresso, Venice, Ambry, Seas, and Pinot teams
- REST API warns when setting profile-internal configs directly
- Raw API kept behind flag, not deleted

---

## 13. Key Observations

### Config Surface vs Actual Usage

- 120+ defined, 7 verified dead, 6 alive but narrowly consumed, 1 needs verification
- 4 systems analyzed: Espresso uses ~30 configs, Seas ~25, Ambry ~20, Venice ~15
- No system uses: `RESOURCE_PRIORITY_FIELD`, `RELAXED_DISABLED_PARTITION_CONSTRAINT`, `PREFERRED_SCORING_KEYS`, `GLOBAL_MAX_PARTITIONS_ALLOWED_PER_INSTANCE`
- Only Espresso uses P2P messaging — all other systems do not
- Customer-facing knobs reduce from 120+ to ~13 (7 cluster + 6 per-resource) with profiles

### Standardization Gaps

- No standard config profiles across systems
- Each system wraps Helix in its own abstraction (Espresso: 5 tools, Venice: 3 wrappers, Ambry: JSON+util, Seas: Rest.li resource)
- No documentation on recommended values per system
- No server-side validation for high-blast-radius configs
- Seas derives configs from other configs (`ERROR_THRESHOLD = maxOffline / 2`) — a pattern that should be standardized

### Incident Patterns

- Highest-severity incidents from config interactions, not individual wrong values
- P2P + MissingTopState, throttle + recovery priority, capacity + weight validation, WAGED + instance disable
- Venice doesn't use features that would help it (cancellation, error threshold, priority)
- Espresso built external validation because Helix lacks server-side validation
- Seas has 500+ chronically non-approved node disruptions ([CICP-1468](https://linkedin.atlassian.net/browse/CICP-1468))

### Task vs Partition Parallelism

- `MAX_CONCURRENT_TASK_PER_INSTANCE` and `STATE_TRANSITION_THROTTLE` are separate systems
- Espresso and Seas both use tasks + partitions on the same cluster
- Venice/Ambry don't overlap — configs can remain separate
