package org.apache.helix.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestCheckLiveInstancesObservationDeferred {

  private List<LiveInstance> createMockLiveInstances(int count) {
    List<LiveInstance> instances = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      LiveInstance li = mock(LiveInstance.class);
      when(li.getInstanceName()).thenReturn("instance" + i);
      when(li.getEphemeralOwner()).thenReturn("session" + i);
      instances.add(li);
    }
    return instances;
  }

  @Test
  public void testInitDefersPerInstanceListenerRegistration() throws Exception {
    GenericHelixController controller = new GenericHelixController("testCluster");

    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getInstanceName()).thenReturn("controller0");
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    List<LiveInstance> liveInstances = createMockLiveInstances(5);

    NotificationContext initContext = new NotificationContext(manager);
    initContext.setType(NotificationContext.Type.INIT);

    controller.checkLiveInstancesObservation(liveInstances, initContext);

    // No addXxxListener calls should have been made on the manager
    verify(manager, never()).addCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
        org.mockito.ArgumentMatchers.anyString());
    verify(manager, never()).addMessageListener(
        org.mockito.ArgumentMatchers.any(org.apache.helix.api.listeners.MessageListener.class),
        org.mockito.ArgumentMatchers.anyString());

    // Pending listeners should be populated
    GenericHelixController.PendingInstanceListeners pending =
        controller.takePendingInstanceListeners();
    Assert.assertNotNull(pending);
    Assert.assertFalse(pending.isEmpty());
    Assert.assertEquals(pending.getSessionToInstance().size(), 5);
    Assert.assertEquals(pending.getNewInstances().size(), 5);

    // Verify instance-to-session mapping
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(pending.getSessionToInstance().get("session" + i), "instance" + i);
      Assert.assertTrue(pending.getNewInstances().contains("instance" + i));
    }
  }

  @Test
  public void testCallbackRegistersInline() throws Exception {
    GenericHelixController controller = new GenericHelixController("testCluster");

    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getInstanceName()).thenReturn("controller0");
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    // First do INIT to populate _lastSeenInstances
    List<LiveInstance> liveInstances = createMockLiveInstances(3);
    NotificationContext initContext = new NotificationContext(manager);
    initContext.setType(NotificationContext.Type.INIT);
    controller.checkLiveInstancesObservation(liveInstances, initContext);
    controller.takePendingInstanceListeners(); // clear pending

    // Now CALLBACK with one new instance - should register inline, not defer
    LiveInstance newInstance = mock(LiveInstance.class);
    when(newInstance.getInstanceName()).thenReturn("instance3");
    when(newInstance.getEphemeralOwner()).thenReturn("session3");
    List<LiveInstance> updatedInstances = new ArrayList<>(liveInstances);
    updatedInstances.add(newInstance);

    NotificationContext callbackContext = new NotificationContext(manager);
    callbackContext.setType(NotificationContext.Type.CALLBACK);

    controller.checkLiveInstancesObservation(updatedInstances, callbackContext);

    // No pending listeners should be created for CALLBACK type
    Assert.assertNull(controller.takePendingInstanceListeners());
  }

  @Test
  public void testTakePendingClearsState() {
    GenericHelixController controller = new GenericHelixController("testCluster");

    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getInstanceName()).thenReturn("controller0");
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    List<LiveInstance> liveInstances = createMockLiveInstances(3);
    NotificationContext initContext = new NotificationContext(manager);
    initContext.setType(NotificationContext.Type.INIT);
    controller.checkLiveInstancesObservation(liveInstances, initContext);

    // First take returns the pending data
    Assert.assertNotNull(controller.takePendingInstanceListeners());

    // Second take returns null
    Assert.assertNull(controller.takePendingInstanceListeners());
  }

  @Test
  public void testForgetTriggersReRegistrationOnNextChange() throws Exception {
    GenericHelixController controller = new GenericHelixController("testCluster");

    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getInstanceName()).thenReturn("controller0");
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    // INIT with 3 instances - all deferred, _lastSeen populated.
    List<LiveInstance> liveInstances = createMockLiveInstances(3);
    NotificationContext initContext = new NotificationContext(manager);
    initContext.setType(NotificationContext.Type.INIT);
    controller.checkLiveInstancesObservation(liveInstances, initContext);
    controller.takePendingInstanceListeners(); // pretend the async registration ran

    // A CALLBACK with the SAME instances registers nothing new - they are already in _lastSeen.
    NotificationContext cb1 = new NotificationContext(manager);
    cb1.setType(NotificationContext.Type.CALLBACK);
    controller.checkLiveInstancesObservation(liveInstances, cb1);
    verify(manager, never()).addCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(
            org.apache.helix.api.listeners.CurrentStateChangeListener.class),
        org.mockito.ArgumentMatchers.eq("instance1"),
        org.mockito.ArgumentMatchers.eq("session1"));

    // Simulate that registration for instance1/session1 failed: forget it.
    controller.forgetSessionForReregistration("session1");
    controller.forgetInstanceForReregistration("instance1");

    // The next CALLBACK with the same instances must now re-register ONLY instance1/session1.
    NotificationContext cb2 = new NotificationContext(manager);
    cb2.setType(NotificationContext.Type.CALLBACK);
    controller.checkLiveInstancesObservation(liveInstances, cb2);

    verify(manager).addCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(
            org.apache.helix.api.listeners.CurrentStateChangeListener.class),
        org.mockito.ArgumentMatchers.eq("instance1"),
        org.mockito.ArgumentMatchers.eq("session1"));
    verify(manager).addTaskCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(
            org.apache.helix.api.listeners.CurrentStateChangeListener.class),
        org.mockito.ArgumentMatchers.eq("instance1"),
        org.mockito.ArgumentMatchers.eq("session1"));
    verify(manager).addMessageListener(
        org.mockito.ArgumentMatchers.any(org.apache.helix.api.listeners.MessageListener.class),
        org.mockito.ArgumentMatchers.eq("instance1"));
    // A different instance that did NOT fail must not be re-registered.
    verify(manager, never()).addCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(
            org.apache.helix.api.listeners.CurrentStateChangeListener.class),
        org.mockito.ArgumentMatchers.eq("instance0"),
        org.mockito.ArgumentMatchers.eq("session0"));
  }
}
