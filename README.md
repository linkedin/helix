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

## WHAT IS HELIX

Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. Helix provides the following features: 

1. Automatic assignment of resource/partition to nodes
2. Node failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions 

## Convergence status

Helix REST reports whether the external views of a cluster match the mapping their ideal states
imply, without the caller running a cluster verifier of its own:

```text
GET /clusters/exampleCluster/convergence-status?matchMode=LENIENT&resources=db0,db1
```

```json
{
  "id": "exampleCluster",
  "scope": "RESOURCES",
  "matchMode": "LENIENT",
  "status": "PENDING",
  "observedAtMillis": 1750000000000,
  "evaluatedResourceCount": 2,
  "pendingResourceCount": 1,
  "failedResourceCount": 0,
  "unknownResourceCount": 0,
  "skippedResourceCount": 0,
  "pendingResources": { "db0": "MAPPING_MISMATCH" },
  "failedResources": {},
  "unknownResources": [],
  "skippedResources": [],
  "detailTruncated": false
}
```

The calculation is shared with `StrictMatchExternalViewVerifier`. `matchMode` defaults to `STRICT`;
`LENIENT` ignores replicas in the state model's initial state and in `DROPPED`. `resources`
restricts the evaluation, and the response says which scope was used. `CONVERGED` is claimed only
when every evaluated resource matched. `PENDING` means retrying can change the answer, and
`FAILED` means a resource could not be evaluated at all, for example because it does not exist or
its state model definition is missing, so neither may be read as convergence. Task resources
without an external view and resources with external-view publication disabled are reported as
skipped rather than counted as evaluated. A leftover task external view is compared against an
empty ideal state and counted as evaluated. An invalid
`matchMode` or an empty `resources` list is rejected rather than defaulted, a missing cluster
returns 404, and a failed metadata read returns an error instead of an empty successful result.
The status has its own path, so a server that predates it answers 404 instead of a successful
response that carries no status, which matters while a deployment is still rolling.

This is a single bounded read: it never waits for convergence, so a caller that wants to wait
polls at an interval it chooses. It opens no connection of its own and leaves behind no verifier,
callback or background task. The read never writes cluster or participant metadata and never
triggers a rebalance. `observedAtMillis` is when the evaluation started reading; its inputs are
collected with several requests, so the result is not an atomic snapshot and is not a statement
about what the controller has processed. Per resource detail is capped, which `detailTruncated`
reports, while the counts stay exact. Responses use `Cache-Control: no-store`.

## Dependencies

Helix UI has been tested to run well on these versions of node and yarn: 

```json
  "engines": {
    "node": "~14.17.5",
    "yarn": "^1.22.18"
  },
```
