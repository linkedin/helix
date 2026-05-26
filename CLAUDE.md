# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Helix

Apache Helix is a generic cluster management framework for automatic management of partitioned, replicated, and distributed resources. It handles resource/partition assignment to nodes, failure detection and recovery, dynamic resource/node addition, pluggable state machines for state transitions, and automatic load balancing with throttling.

## Build and Test

- Full build: `mvn clean install`
- Build without tests: `mvn clean install -Dmaven.test.skip.exec=true`
- Run specific module tests: `mvn test -pl helix-core`
- Run specific test class: `mvn test -pl helix-core -Dtest=TestClassName`
- Run specific test method: `mvn test -pl helix-core -Dtest=TestClassName#testMethodName`
- Integration tests: `mvn verify -pl helix-core -P integration-test`
- Frontend (Angular/Node 14, Yarn 1.22+): `cd helix-front && yarn install && yarn test`
- Frontend e2e: `cd helix-front && yarn cypress:open`

## Module Structure

Build order matters — modules listed earlier are dependencies of later ones:

| Module | Purpose |
|--------|---------|
| `metrics-common` | Common metrics and monitoring interfaces (JMX MBeans) |
| `metadata-store-directory-common` | Shared metadata store directory utilities |
| `zookeeper-api` | ZooKeeper abstraction layer; wraps raw ZK client |
| `helix-common` | Shared utilities, data models (`ZNRecord`), exceptions, constants |
| `helix-core` | **Main engine**: controller, participant, spectator, rebalancers, state machines, pipeline |
| `helix-rest` | REST API server for cluster management operations |
| `helix-lock` | Distributed lock primitives built on ZooKeeper |
| `helix-agent` | Command execution agent for running tasks on cluster nodes |
| `recipes` | Example implementations (distributed lock manager, etc.) |
| `helix-view-aggregator` | Aggregates customized views across the cluster |
| `meta-client` | Client for interacting with metadata stores |
| `helix-front` | Angular frontend UI for cluster administration |

## Architecture Overview

### Three Roles

Every process joins a Helix cluster via `HelixManagerFactory.getZKHelixManager()` in one of three roles:

- **PARTICIPANT** — cluster nodes that host resource partitions and execute state transitions
- **CONTROLLER** — manages cluster state, computes rebalancing, dispatches state transition messages
- **SPECTATOR** — reads cluster state for client-side routing (e.g., `RoutingTableProvider`)

### ZooKeeper Data Model

All cluster state lives as ZNodes under `/{clusterName}/`:

```
CONFIGS/           — Cluster, instance, and resource configuration (persistent)
LIVEINSTANCES/     — Ephemeral nodes; appear on connect, vanish on disconnect
INSTANCES/         — Instance definitions + per-instance subtrees:
  {instance}/MESSAGES/         — State transition commands from controller
  {instance}/CURRENTSTATES/    — Actual partition states reported by participant
  {instance}/ERRORS/           — Error reports
IDEALSTATES/       — Desired partition-to-instance assignments (persistent, cached)
EXTERNALVIEW/      — Controller-computed actual state (for spectators)
STATEMODELDEFS/    — State machine definitions (persistent, cached)
CONTROLLER/        — Leader election (ephemeral), history, pause/maintenance signals
```

Key properties like `CONFIGS`, `IDEALSTATES`, `STATEMODELDEFS`, and `CURRENTSTATES` are cached in-memory for performance.

### Controller Pipeline

The controller processes cluster events through an ordered pipeline of stages (in `helix-core/.../controller/stages/`):

1. `ReadClusterDataStage` — read cluster state from ZK into cache
2. `ResourceComputationStage` — determine which resources need rebalancing
3. `CurrentStateComputationStage` — compile current state from all instances
4. `BestPossibleStateCalcStage` — **invoke Rebalancer** to compute ideal assignment
5. `IntermediateStateCalcStage` — calculate intermediate states for safe transitions
6. `MessageGenerationPhase` → `MessageSelectionStage` → `MessageThrottleStage` → `MessageDispatchStage` — generate, filter, throttle, and send state transition messages
7. `ExternalViewComputeStage` — update ExternalView for spectators

Events are queued and processed sequentially. Multiple events of the same type coalesce.

### State Machine Model

State models define valid states and transitions. Built-in models in `helix-core/.../model/builder/`:

- **MasterSlave** — Master(1), Slave(R-1), Offline(N-R). Used for databases.
- **LeaderStandby** — Leader(1), Standby(R-1), Offline(N-R). Used for services.
- **OnlineOffline** — Online(R), Offline(N-R). Simplest model.
- **Task** — RUNNING, COMPLETED, FAILED. For job execution.

Participants implement state transitions via `@Transition(from, to)` annotated methods on `StateModel` subclasses, registered through `StateModelFactory` on the `StateMachineEngine`.

### Rebalancing

Rebalance modes (set on `IdealState`):

- **FULL_AUTO** — controller computes both partition→instance mapping and state assignment
- **SEMI_AUTO** — operator provides preference lists; controller assigns states
- **CUSTOMIZED** — operator fully controls the ideal state
- **USER_DEFINED** — custom `Rebalancer` implementation class

Key rebalancer implementations in `helix-core/.../controller/rebalancer/`:

- `AutoRebalancer` — default FULL_AUTO rebalancer
- `DelayedAutoRebalancer` — adds recovery delay to avoid flapping
- `WagedRebalancer` (`waged/` subpackage) — weighted, globally-efficient multi-objective rebalancer with capacity constraints
- `CrushRebalanceStrategy` — CRUSH-based consistent hashing for placement

### Message Flow

```
Controller computes transition → writes Message to /{cluster}/INSTANCES/{instance}/MESSAGES/
  → Participant reads message → HelixTaskExecutor dispatches to StateModel handler
  → Handler executes @Transition method → updates CURRENTSTATES → deletes message
```

### REST API (helix-rest)

Key endpoints under `HelixRestServer`:
- `/clusters` — list/manage clusters
- `/clusters/{cluster}/resources` — manage resources
- `/clusters/{cluster}/instances` — manage instances
- `/clusters/{cluster}/configs` — configuration CRUD

## Key Source Locations

| What | Where |
|------|-------|
| HelixManager interface | `helix-core/src/main/java/org/apache/helix/HelixManager.java` |
| Controller logic | `helix-core/.../controller/GenericHelixController.java` |
| Pipeline stages | `helix-core/.../controller/stages/` |
| Rebalancers | `helix-core/.../controller/rebalancer/` |
| Data model classes | `helix-core/.../model/` (IdealState, CurrentState, ExternalView, Message, etc.) |
| ZNode path definitions | `helix-core/.../PropertyType.java`, `PropertyPathBuilder.java` |
| State machine framework | `helix-core/.../participant/statemachine/` |
| Message handling | `helix-core/.../messaging/handling/` |
| REST endpoints | `helix-rest/.../rest/server/resources/` |
| ZK abstraction | `zookeeper-api/src/main/java/org/apache/helix/zookeeper/` |
| Monitoring MBeans | `helix-core/.../monitoring/mbeans/` |

## Design Documents

When creating or modifying design documents:

1. **Use the template**: All design docs MUST follow `docs/design/000-TEMPLATE.md`.
2. **Location**: `docs/design/NNN-short-title.md` with sequential numbering.
3. **Required sections**: Summary, Problem Statement, Goals/Non-Goals, Design, Implementation Plan (with file paths and validation commands), Testing Strategy.
4. **Status tracking**: Set the Status field (Draft / In Review / Approved / Implementing / Done).
5. **Module references**: List affected modules in the metadata table.
6. **Implementation steps**: Each step must reference specific file paths, the module, and a validation command.
7. **Diagrams**: Use Mermaid syntax. ASCII art only when Mermaid cannot express the concept.
8. **Cross-references**: Use relative paths: `[Title](./NNN-title.md)`.
