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

## WHAT IS HELIX

Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. Helix provides the following features: 

1. Automatic assignment of resource/partition to nodes
2. Node failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions 

## LinkedIn fork compatibility

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
