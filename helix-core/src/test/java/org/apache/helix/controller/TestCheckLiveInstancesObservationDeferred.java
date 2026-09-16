package org.apache.helix.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.model.LiveInstance;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestCheckLiveInstancesObservationDeferred {

  private static final String FEATURE_FLAG =
      SystemPropertyKeys.CONTROLLER_PARALLEL_INSTANCE_LISTENER_REGISTRATION_ENABLED;

  private String _prevFlag;

  // The feature gate is read once when GenericHelixController is constructed, so set it before each
  // test builds its controller. Tests that need the feature OFF clear it at their own start. Restore
  // the prior value (not a blanket clear) so this cannot leak into other test classes in the fork.
  @BeforeMethod
  public void enableFeature() {
    _prevFlag = System.getProperty(FEATURE_FLAG);
    System.setProperty(FEATURE_FLAG, "true");
  }

  @AfterMethod
  public void restoreFeature() {
    if (_prevFlag == null) {
      System.clearProperty(FEATURE_FLAG);
    } else {
      System.setProperty(FEATURE_FLAG, _prevFlag);
    }
  }

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

  @DataProvider(name = "featureFlag")
  public static Object[][] featureFlag() {
    // flagOn true  -> INIT defers every per-instance listener (0 inline)
    // flagOn false -> INIT registers all per-instance listeners inline (legacy), defers nothing
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "featureFlag")
  public void testInitRegistration(boolean flagOn) throws Exception {
    // @BeforeMethod set the flag ON; clear it for the OFF case before the controller reads it.
    if (!flagOn) {
      System.clearProperty(FEATURE_FLAG);
    }
    GenericHelixController controller = new GenericHelixController("testCluster");

    HelixManager manager = mock(HelixManager.class);
    when(manager.getClusterName()).thenReturn("testCluster");
    when(manager.getInstanceName()).thenReturn("controller0");
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);

    int count = 4;
    List<LiveInstance> liveInstances = createMockLiveInstances(count);
    NotificationContext initContext = new NotificationContext(manager);
    initContext.setType(NotificationContext.Type.INIT);
    controller.checkLiveInstancesObservation(liveInstances, initContext);

    if (flagOn) {
      // Feature ON: nothing registered inline - everything deferred.
      verify(manager, never()).addCurrentStateChangeListener(
          org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
          org.mockito.ArgumentMatchers.anyString());
      verify(manager, never()).addMessageListener(
          org.mockito.ArgumentMatchers.any(org.apache.helix.api.listeners.MessageListener.class),
          org.mockito.ArgumentMatchers.anyString());

      GenericHelixController.PendingInstanceListeners pending =
          controller.takePendingInstanceListeners();
      Assert.assertNotNull(pending);
      Assert.assertFalse(pending.isEmpty());
      Assert.assertEquals(pending.getSessionToInstance().size(), count);
      Assert.assertEquals(pending.getNewInstances().size(), count);
      for (int i = 0; i < count; i++) {
        Assert.assertEquals(pending.getSessionToInstance().get("session" + i), "instance" + i);
        Assert.assertTrue(pending.getNewInstances().contains("instance" + i));
      }
      // The take clears the pending set: a second take returns null.
      Assert.assertNull(controller.takePendingInstanceListeners());
    } else {
      // Feature OFF (legacy): every instance registered inline via the unchanged path, none deferred.
      Assert.assertNull(controller.takePendingInstanceListeners());
      verify(manager, times(count)).addCurrentStateChangeListener(
          org.mockito.ArgumentMatchers.any(
              org.apache.helix.api.listeners.CurrentStateChangeListener.class),
          org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString());
      verify(manager, times(count)).addMessageListener(
          org.mockito.ArgumentMatchers.any(org.apache.helix.api.listeners.MessageListener.class),
          org.mockito.ArgumentMatchers.anyString());
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

    // No pending listeners should be created for CALLBACK type (nothing deferred)...
    Assert.assertNull(controller.takePendingInstanceListeners());

    // ...and the new instance must actually be registered INLINE via the else branch (not merely
    // "not deferred") - a no-op'd inline path would still pass the assertNull above.
    verify(manager).addCurrentStateChangeListener(
        org.mockito.ArgumentMatchers.any(
            org.apache.helix.api.listeners.CurrentStateChangeListener.class),
        org.mockito.ArgumentMatchers.eq("instance3"),
        org.mockito.ArgumentMatchers.eq("session3"));
    verify(manager).addMessageListener(
        org.mockito.ArgumentMatchers.any(org.apache.helix.api.listeners.MessageListener.class),
        org.mockito.ArgumentMatchers.eq("instance3"));
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
