package org.apache.helix.manager.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestInitHandlersParallel {

  @Test
  public void testInitHandlersWithNullList() {
    ZKHelixManager manager = mock(ZKHelixManager.class);
    doCallRealMethod().when(manager).initHandlers(any());
    manager.initHandlers(null);
  }

  @Test
  public void testInitHandlersWithEmptyList() {
    ZKHelixManager manager = mock(ZKHelixManager.class);
    doCallRealMethod().when(manager).initHandlers(any());
    manager.initHandlers(new ArrayList<>());
  }

  @Test
  public void testInitHandlersWithSingleHandler() {
    ZKHelixManager manager = mock(ZKHelixManager.class);
    CallbackHandler handler = mock(CallbackHandler.class);
    when(handler.getPath()).thenReturn("/test/path");
    when(handler.getListener()).thenReturn("testListener");

    doCallRealMethod().when(manager).initHandlers(any());

    List<CallbackHandler> handlers = new ArrayList<>();
    handlers.add(handler);
    manager.initHandlers(handlers);

    verify(handler, times(1)).init();
  }

  @Test
  public void testInitHandlersRunsInParallel() throws Exception {
    ZKHelixManager manager = mock(ZKHelixManager.class);
    doCallRealMethod().when(manager).initHandlers(any());

    int handlerCount = 50;
    AtomicInteger maxConcurrency = new AtomicInteger(0);
    AtomicInteger currentConcurrency = new AtomicInteger(0);
    AtomicInteger initCount = new AtomicInteger(0);

    List<CallbackHandler> handlers = new ArrayList<>();
    for (int i = 0; i < handlerCount; i++) {
      CallbackHandler handler = mock(CallbackHandler.class);
      when(handler.getPath()).thenReturn("/test/path/" + i);
      when(handler.getListener()).thenReturn("listener" + i);
      doAnswer(invocation -> {
        int concurrent = currentConcurrency.incrementAndGet();
        maxConcurrency.updateAndGet(max -> Math.max(max, concurrent));
        Thread.sleep(50); // simulate ZK roundtrip
        currentConcurrency.decrementAndGet();
        initCount.incrementAndGet();
        return null;
      }).when(handler).init();
      handlers.add(handler);
    }

    long start = System.currentTimeMillis();
    manager.initHandlers(handlers);
    long elapsed = System.currentTimeMillis() - start;

    // All handlers should have been initialized
    Assert.assertEquals(initCount.get(), handlerCount);

    // Should have run with some parallelism (max concurrency > 1)
    Assert.assertTrue(maxConcurrency.get() > 1,
        "Expected parallel execution but max concurrency was " + maxConcurrency.get());

    // With 50 handlers at 50ms each, sequential would take 2500ms.
    // Parallel with 20 threads should take ~150ms (50/20 * 50ms).
    // Allow generous margin but should be well under sequential time.
    Assert.assertTrue(elapsed < 1500,
        "Expected parallel execution to be faster than sequential. Took " + elapsed + "ms");
  }

  @Test
  public void testInitHandlersContinuesOnException() {
    ZKHelixManager manager = mock(ZKHelixManager.class);
    doCallRealMethod().when(manager).initHandlers(any());

    AtomicInteger initCount = new AtomicInteger(0);

    List<CallbackHandler> handlers = new ArrayList<>();

    // First handler succeeds
    CallbackHandler goodHandler1 = mock(CallbackHandler.class);
    when(goodHandler1.getPath()).thenReturn("/test/good1");
    when(goodHandler1.getListener()).thenReturn("good1");
    doAnswer(inv -> { initCount.incrementAndGet(); return null; }).when(goodHandler1).init();
    handlers.add(goodHandler1);

    // Second handler throws
    CallbackHandler badHandler = mock(CallbackHandler.class);
    when(badHandler.getPath()).thenReturn("/test/bad");
    when(badHandler.getListener()).thenReturn("bad");
    doAnswer(inv -> { throw new RuntimeException("test exception"); }).when(badHandler).init();
    handlers.add(badHandler);

    // Third handler succeeds
    CallbackHandler goodHandler2 = mock(CallbackHandler.class);
    when(goodHandler2.getPath()).thenReturn("/test/good2");
    when(goodHandler2.getListener()).thenReturn("good2");
    doAnswer(inv -> { initCount.incrementAndGet(); return null; }).when(goodHandler2).init();
    handlers.add(goodHandler2);

    // Should not throw - exception in one handler should not block others
    manager.initHandlers(handlers);

    // The two good handlers should have been initialized
    Assert.assertEquals(initCount.get(), 2);
  }

}
