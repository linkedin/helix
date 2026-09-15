package org.apache.helix.rest.server.service;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.model.IdealState;
import org.apache.helix.rest.server.service.ResourceReplicaCountService.BulkReplicaCountUpdateResult;
import org.apache.helix.rest.server.service.ResourceReplicaCountService.ResourceUpdateStatus;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises the bulk replica count service against an in-memory stand-in for the metadata store
 * that reproduces ZooKeeper's version and creation-id behaviour, so that races can be triggered
 * deterministically instead of being hoped for.
 */
public class TestResourceReplicaCountService {
  private static final String CLUSTER = "TestBulkReplicaCluster";
  private static final String RESOURCE_1 = "resource1";
  private static final String RESOURCE_2 = "resource2";
  private static final String RESOURCE_3 = "resource3";
  private static final String UNRELATED_FIELD = "UNRELATED_FIELD";
  private static final String REPLICAS_FIELD =
      IdealState.IdealStateProperty.REPLICAS.name();
  private static final String MIN_ACTIVE_FIELD =
      IdealState.IdealStateProperty.MIN_ACTIVE_REPLICAS.name();

  /** A stored znode: the record plus the two pieces of ZooKeeper metadata this service relies on. */
  private static class StoredNode {
    private ZNRecord _record;
    private int _version;
    private final long _creationId;

    StoredNode(ZNRecord record, long creationId) {
      _record = record;
      _version = 0;
      _creationId = creationId;
    }
  }

  private final Map<String, StoredNode> _store = new HashMap<>();
  private long _nextCreationId;
  private Consumer<String> _afterReadHook;
  private Consumer<String> _afterWriteHook;
  private BaseDataAccessor<ZNRecord> _accessor;
  private ResourceReplicaCountService _service;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void beforeMethod() {
    _store.clear();
    _nextCreationId = 100L;
    _afterReadHook = path -> {
    };
    _afterWriteHook = path -> {
    };
    _accessor = mock(BaseDataAccessor.class);

    when(_accessor.get(anyString(), any(Stat.class), anyInt())).thenAnswer(invocation -> {
      String path = invocation.getArgument(0);
      Stat stat = invocation.getArgument(1);
      StoredNode node = _store.get(path);
      ZNRecord read = null;
      if (node != null) {
        stat.setVersion(node._version);
        stat.setCzxid(node._creationId);
        read = new ZNRecord(node._record);
      }
      // Simulates another writer committing after this read and before the guarded write.
      _afterReadHook.accept(path);
      return read;
    });

    when(_accessor.set(anyString(), any(ZNRecord.class), anyInt(), anyInt()))
        .thenAnswer(invocation -> {
          String path = invocation.getArgument(0);
          ZNRecord record = invocation.getArgument(1);
          int expectedVersion = invocation.getArgument(2);
          StoredNode node = _store.get(path);
          if (node == null) {
            // A version guarded write never creates a node, so an absent node simply fails.
            Assert.assertTrue(expectedVersion != -1,
                "The service must never issue an unguarded write that could create a resource.");
            return false;
          }
          if (node._version != expectedVersion) {
            // Matches ZkBaseDataAccessor, which rethrows a bad-version failure rather than
            // reporting it through the return value.
            throw new ZkBadVersionException(
                "version mismatch on " + path + ", expected " + expectedVersion);
          }
          node._record = new ZNRecord(record);
          node._version++;
          _afterWriteHook.accept(path);
          return true;
        });

    when(_accessor.getStat(anyString(), anyInt())).thenAnswer(invocation -> {
      StoredNode node = _store.get(invocation.<String>getArgument(0));
      if (node == null) {
        return null;
      }
      Stat stat = new Stat();
      stat.setVersion(node._version);
      stat.setCzxid(node._creationId);
      return stat;
    });

    when(_accessor.getChildNames(anyString(), anyInt())).thenAnswer(invocation -> {
      String parentPath = invocation.<String>getArgument(0) + "/";
      List<String> children = new ArrayList<>();
      for (String path : _store.keySet()) {
        if (path.startsWith(parentPath)) {
          children.add(path.substring(parentPath.length()));
        }
      }
      Collections.sort(children);
      return children;
    });

    _service = new ResourceReplicaCountService(_accessor);
  }

  @Test
  public void testAppliesDesiredValuesToEverySelectedResource() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 2, 1);

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_2), 3, 2);

    Assert.assertTrue(result.isAllAtDesiredValues());
    Assert.assertEquals(result.getSelectedResources(), Arrays.asList(RESOURCE_1, RESOURCE_2));
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    assertStatus(result, RESOURCE_2, ResourceUpdateStatus.APPLIED);
    assertStored(RESOURCE_1, "3", 2);
    assertStored(RESOURCE_2, "3", 2);
    Assert.assertEquals(result.getStatusCounts().get(ResourceUpdateStatus.APPLIED).intValue(), 2);
    // Every status is reported, so an absent key cannot be mistaken for a zero count.
    Assert.assertEquals(result.getStatusCounts().size(), ResourceUpdateStatus.values().length);
    Assert.assertEquals(result.getOutcomes().get(RESOURCE_1).getVersion(), 1);
  }

  @Test
  public void testAllResourcesSelectionUsesASnapshotTakenBeforeTheWrites() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 1, 1);

    List<String> selected = _service.listResources(CLUSTER);
    Assert.assertEquals(selected, Arrays.asList(RESOURCE_1, RESOURCE_2));
    // A resource added after the selection is outside this operation and must not be reported.
    givenResource(RESOURCE_3, 1, 1);

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER, selected, 3, 2);

    Assert.assertTrue(result.isAllAtDesiredValues());
    Assert.assertFalse(result.getOutcomes().containsKey(RESOURCE_3));
    assertStored(RESOURCE_3, "1", 1);
  }

  @Test
  public void testResourceRemovedAfterSelectionIsReportedAndNotRecreated() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 1, 1);

    List<String> selected = _service.listResources(CLUSTER);
    _store.remove(path(RESOURCE_2));

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER, selected, 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    assertStatus(result, RESOURCE_2, ResourceUpdateStatus.NOT_FOUND);
    Assert.assertFalse(_store.containsKey(path(RESOURCE_2)),
        "A missing resource must not be recreated by the update.");
  }

  @Test
  public void testRerunAfterAPartialFailureOnlyWritesWhatIsStillBehind() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 1, 1);
    _afterReadHook = path -> {
      if (path.equals(path(RESOURCE_2))) {
        throw new IllegalStateException("metadata store is unavailable");
      }
    };

    BulkReplicaCountUpdateResult first = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_2), 3, 2);
    Assert.assertFalse(first.isAllAtDesiredValues());
    assertStatus(first, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    assertStatus(first, RESOURCE_2, ResourceUpdateStatus.FAILED);
    Assert.assertNotNull(first.getOutcomes().get(RESOURCE_2).getMessage());
    Assert.assertEquals(first.getOutcomes().get(RESOURCE_2).getVersion(), -1);

    _afterReadHook = path -> {
    };
    BulkReplicaCountUpdateResult second = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_2), 3, 2);

    Assert.assertTrue(second.isAllAtDesiredValues());
    // The resource that already carried the desired values is not written a second time.
    assertStatus(second, RESOURCE_1, ResourceUpdateStatus.UNCHANGED);
    assertStatus(second, RESOURCE_2, ResourceUpdateStatus.APPLIED);
    Assert.assertEquals(_store.get(path(RESOURCE_1))._version, 1);
    assertStored(RESOURCE_2, "3", 2);
  }

  @Test
  public void testRerunOnAnAlreadyUpdatedClusterWritesNothing() {
    givenResource(RESOURCE_1, 3, 2);
    givenResource(RESOURCE_2, 3, 2);

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_2), 3, 2);

    Assert.assertTrue(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.UNCHANGED);
    assertStatus(result, RESOURCE_2, ResourceUpdateStatus.UNCHANGED);
    Assert.assertEquals(result.getOutcomes().get(RESOURCE_1).getVersion(), 0);
    verify(_accessor, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testConcurrentUnrelatedChangeIsRetriedAndPreserved() {
    givenResource(RESOURCE_1, 1, 1);
    boolean[] alreadyRan = new boolean[1];
    _afterReadHook = path -> {
      if (alreadyRan[0]) {
        return;
      }
      alreadyRan[0] = true;
      // Another writer changes a field this operation does not own, after this operation read.
      StoredNode node = _store.get(path);
      node._record.setSimpleField(UNRELATED_FIELD, "set-by-another-writer");
      node._version++;
    };

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    assertStored(RESOURCE_1, "3", 2);
    Assert.assertEquals(_store.get(path(RESOURCE_1))._record.getSimpleField(UNRELATED_FIELD),
        "set-by-another-writer", "The concurrent change to an unrelated field must survive.");
  }

  @Test
  public void testUnrelatedListAndMapFieldsAreLeftIntact() {
    ZNRecord record = newRecord(RESOURCE_1, 1, 1);
    record.setListField("partition_0", Arrays.asList("instance1", "instance2"));
    record.setMapField("partition_0", Collections.singletonMap("instance1", "MASTER"));
    record.setSimpleField(UNRELATED_FIELD, "keep-me");
    _store.put(path(RESOURCE_1), new StoredNode(record, _nextCreationId++));

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    ZNRecord stored = _store.get(path(RESOURCE_1))._record;
    Assert.assertEquals(stored.getListField("partition_0"),
        Arrays.asList("instance1", "instance2"));
    Assert.assertEquals(stored.getMapField("partition_0"),
        Collections.singletonMap("instance1", "MASTER"));
    Assert.assertEquals(stored.getSimpleField(UNRELATED_FIELD), "keep-me");
    assertStored(RESOURCE_1, "3", 2);
  }

  @Test
  public void testSustainedContentionEndsAsConflictWithoutOverwritingTheOtherWriter() {
    givenResource(RESOURCE_1, 1, 1);
    _afterReadHook = path -> {
      StoredNode node = _store.get(path);
      node._record.setSimpleField(REPLICAS_FIELD, "9");
      node._version++;
    };

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.CONFLICT);
    Assert.assertEquals(result.getOutcomes().get(RESOURCE_1).getVersion(), -1);
    Assert.assertEquals(_store.get(path(RESOURCE_1))._record.getSimpleField(REPLICAS_FIELD), "9",
        "A resource held by another writer must not be overwritten.");
    verify(_accessor, times(ResourceReplicaCountService.MAX_ATTEMPTS_PER_RESOURCE))
        .set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testRecreatedResourceIsReportedAsConflictRatherThanSilentlyAccepted() {
    givenResource(RESOURCE_1, 1, 1);
    boolean[] alreadyRan = new boolean[1];
    _afterReadHook = path -> {
      if (alreadyRan[0]) {
        return;
      }
      alreadyRan[0] = true;
      // A delete and recreate resets the data version, so the version guard alone cannot tell the
      // new incarnation apart from the one that was read.
      _store.put(path, new StoredNode(newRecord(RESOURCE_1, 1, 1), _nextCreationId++));
    };

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.CONFLICT);
    Assert.assertTrue(
        result.getOutcomes().get(RESOURCE_1).getMessage().contains("recreated"),
        "The conflict must say that the resource was recreated.");
  }

  @Test
  public void testResourceRemovedRightAfterTheWriteIsReportedAsConflict() {
    givenResource(RESOURCE_1, 1, 1);
    _afterWriteHook = _store::remove;

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.CONFLICT);
  }

  @Test
  public void testResourceRemovedBetweenTheReadAndTheWriteIsNotRecreated() {
    givenResource(RESOURCE_1, 1, 1);
    boolean[] alreadyRan = new boolean[1];
    _afterReadHook = path -> {
      if (alreadyRan[0]) {
        return;
      }
      alreadyRan[0] = true;
      // The guarded write now reaches a node that no longer exists, which the accessor reports
      // through the return value rather than as a version conflict.
      _store.remove(path);
    };

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.NOT_FOUND);
    Assert.assertFalse(_store.containsKey(path(RESOURCE_1)),
        "A resource removed mid-update must not be brought back by the write.");
  }

  @Test
  public void testWriteFailureIsReportedAsFailedForThatResourceOnly() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 1, 1);
    when(_accessor.set(eq(path(RESOURCE_1)), any(ZNRecord.class), anyInt(), anyInt()))
        .thenThrow(new IllegalStateException("write rejected"));

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_2), 3, 2);

    Assert.assertFalse(result.isAllAtDesiredValues());
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.FAILED);
    // A failure on one resource must not stop the rest of the selection from being reported.
    assertStatus(result, RESOURCE_2, ResourceUpdateStatus.APPLIED);
    assertStored(RESOURCE_2, "3", 2);
  }

  @Test
  public void testOnlyTheRequestedFieldsAreChanged() {
    givenResource(RESOURCE_1, 1, 1);
    givenResource(RESOURCE_2, 1, 1);

    _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, null);
    _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_2), null, 4);

    assertStored(RESOURCE_1, "3", 1);
    assertStored(RESOURCE_2, "1", 4);
  }

  @Test
  public void testDerivedReplicaCountIsNotMistakenForTheStoredValue() {
    ZNRecord record = new ZNRecord(RESOURCE_1);
    record.setSimpleField(MIN_ACTIVE_FIELD, "2");
    record.setListField("partition_0", Arrays.asList("instance1", "instance2", "instance3"));
    _store.put(path(RESOURCE_1), new StoredNode(record, _nextCreationId++));

    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.singletonList(RESOURCE_1), 3, 2);

    // The replica count field is absent, so it must be written even though a count of 3 could be
    // derived from the preference list.
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
    assertStored(RESOURCE_1, "3", 2);
  }

  @Test
  public void testEmptySelectionLeavesNothingUnconfirmed() {
    BulkReplicaCountUpdateResult result =
        _service.updateReplicaCounts(CLUSTER, Collections.emptyList(), 3, 2);

    Assert.assertTrue(result.isAllAtDesiredValues());
    Assert.assertTrue(result.getSelectedResources().isEmpty());
    verify(_accessor, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  @Test
  public void testDuplicateResourceNamesAreCollapsed() {
    givenResource(RESOURCE_1, 1, 1);

    BulkReplicaCountUpdateResult result = _service.updateReplicaCounts(CLUSTER,
        Arrays.asList(RESOURCE_1, RESOURCE_1), 3, 2);

    Assert.assertEquals(result.getSelectedResources(), Collections.singletonList(RESOURCE_1));
    assertStatus(result, RESOURCE_1, ResourceUpdateStatus.APPLIED);
  }

  @Test
  public void testUnknownClusterIsDistinguishedFromAnEmptyCluster() {
    Assert.assertTrue(_service.listResources(CLUSTER).isEmpty(),
        "A cluster whose IdealState path exists but holds nothing selects no resource.");

    when(_accessor.getChildNames(anyString(), anyInt())).thenReturn(null);
    Assert.assertNull(_service.listResources(CLUSTER),
        "An absent IdealState path must not be reported as an empty selection.");
  }

  @Test
  public void testInvalidInputsAreRejectedBeforeAnyWrite() {
    givenResource(RESOURCE_1, 1, 1);
    List<String> resources = Collections.singletonList(RESOURCE_1);

    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, resources, null, null));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, resources, 0, null));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, resources, -1, null));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, resources, 3, -1));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, resources, 2, 3));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER, null, 3, 2));
    assertRejected(
        () -> _service.updateReplicaCounts(CLUSTER, Collections.singletonList(" "), 3, 2));
    assertRejected(() -> _service.updateReplicaCounts(CLUSTER,
        Collections.singletonList("../CONFIGS/RESOURCE"), 3, 2));

    assertStored(RESOURCE_1, "1", 1);
    verify(_accessor, never()).set(anyString(), any(ZNRecord.class), anyInt(), anyInt());
  }

  private void assertRejected(Runnable call) {
    try {
      call.run();
      Assert.fail("Expected the request to be rejected before any write.");
    } catch (IllegalArgumentException expected) {
      Assert.assertNotNull(expected.getMessage());
    }
  }

  private void assertStatus(BulkReplicaCountUpdateResult result, String resourceName,
      ResourceUpdateStatus expected) {
    Assert.assertEquals(result.getOutcomes().get(resourceName).getStatus(), expected,
        "Unexpected outcome for " + resourceName + ": "
            + result.getOutcomes().get(resourceName).getMessage());
  }

  private void assertStored(String resourceName, String replicas, int minActiveReplicas) {
    ZNRecord stored = _store.get(path(resourceName))._record;
    Assert.assertEquals(stored.getSimpleField(REPLICAS_FIELD), replicas);
    Assert.assertEquals(new IdealState(stored).getMinActiveReplicas(), minActiveReplicas);
  }

  private void givenResource(String resourceName, int replicas, int minActiveReplicas) {
    _store.put(path(resourceName),
        new StoredNode(newRecord(resourceName, replicas, minActiveReplicas), _nextCreationId++));
  }

  private static ZNRecord newRecord(String resourceName, int replicas, int minActiveReplicas) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setReplicas(Integer.toString(replicas));
    idealState.setMinActiveReplicas(minActiveReplicas);
    return idealState.getRecord();
  }

  private static String path(String resourceName) {
    return PropertyPathBuilder.idealState(CLUSTER, resourceName);
  }
}
