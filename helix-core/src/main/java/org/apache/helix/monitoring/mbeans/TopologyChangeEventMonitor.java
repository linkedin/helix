package org.apache.helix.monitoring.mbeans;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.JMException;

import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

/**
 * Per-{@link ClusterEventType} counter MBean for topology-change events.
 *
 * <p>One instance is registered for each event type returned by
 * {@link ClusterEventType#topologyChangeEventTypes()}. Each instance exposes:
 * <ul>
 *   <li>{@code ReceivedCounter} -- incremented when the controller enqueues an event of this
 *       type onto the cluster event queue (post ZK-callback, pre coalescing).</li>
 *   <li>{@code ProcessedCounter} -- incremented when the controller's resource pipeline
 *       finishes processing an event of this type without failure.</li>
 * </ul>
 *
 * <p>Received and processed counts diverge under load because the controller's event queue
 * coalesces events of the same {@link ClusterEventType}; that gap is the intended signal --
 * it reflects controller load and rebalance throughput against topology churn.
 *
 * <p>{@code pushToEventQueues} runs on independent ZK callback threads (one per
 * controller listener), so increments must be safe under concurrent calls. Both
 * counters are backed by {@link AtomicLong} for that reason.
 */
public class TopologyChangeEventMonitor extends DynamicMBeanProvider {

  public static final String EVENT_NAME = "TopologyChangeEvent";
  public static final String EVENT_NAME_KEY = "eventName";
  public static final String EVENT_TYPE_KEY = "eventType";

  private final ClusterStatusMonitor _clusterStatusMonitor;
  private final ClusterEventType _eventType;

  private final AtomicLongMetric _receivedCounter;
  private final AtomicLongMetric _processedCounter;

  public TopologyChangeEventMonitor(ClusterStatusMonitor clusterStatusMonitor,
      ClusterEventType eventType) {
    if (!eventType.isTopologyChange()) {
      throw new IllegalArgumentException(
          "TopologyChangeEventMonitor only supports topology-change event types, got: "
              + eventType);
    }
    _clusterStatusMonitor = clusterStatusMonitor;
    _eventType = eventType;
    _receivedCounter = new AtomicLongMetric("ReceivedCounter");
    _processedCounter = new AtomicLongMetric("ProcessedCounter");
  }

  public void incrementReceived() {
    _receivedCounter.increment();
  }

  public void incrementProcessed() {
    _processedCounter.increment();
  }

  public ClusterEventType getEventType() {
    return _eventType;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", EVENT_NAME, _clusterStatusMonitor.getClusterName(),
        _eventType.name());
  }

  private String getBeanName() {
    return String.format("%s,%s=%s,%s=%s", _clusterStatusMonitor.clusterBeanName(),
        EVENT_NAME_KEY, EVENT_NAME, EVENT_TYPE_KEY, _eventType.name());
  }

  @Override
  public TopologyChangeEventMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_receivedCounter);
    attributeList.add(_processedCounter);
    doRegister(attributeList, _clusterStatusMonitor.getObjectName(getBeanName()));
    return this;
  }

  /**
   * {@link SimpleDynamicMetric} variant whose value is backed by an {@link AtomicLong},
   * making concurrent increments safe. The MBean attribute remains a {@code Long} so
   * downstream JMX scrapers (and the LinkedIn OTel adaptor) consume it identically to
   * the existing simple-Long counters in {@link ClusterEventMonitor}.
   */
  static final class AtomicLongMetric extends SimpleDynamicMetric<Long> {
    private final AtomicLong _value = new AtomicLong(0L);

    AtomicLongMetric(String metricName) {
      super(metricName, 0L);
    }

    void increment() {
      _value.incrementAndGet();
    }

    @Override
    public Long getValue() {
      return _value.get();
    }

    @Override
    public Long getAttributeValue(String attributeName) {
      if (!_metricName.equals(attributeName)) {
        return null;
      }
      return _value.get();
    }

    @Override
    public void updateValue(Long newValue) {
      _value.set(newValue == null ? 0L : newValue);
    }
  }
}
