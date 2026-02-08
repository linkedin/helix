package org.apache.helix.messaging;

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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.messaging.handling.AsyncCallbackService;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.messaging.handling.TaskExecutor;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.monitoring.mbeans.MessageQueueMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMessagingService implements ClusterMessagingService {
  private final HelixManager _manager;
  private final CriteriaEvaluator _evaluator;
  private final HelixTaskExecutor _taskExecutor;
  // TODO:rename to factory, this is not a service
  private final AsyncCallbackService _asyncCallbackService;
  private final int _taskThreadpoolResetTimeout;

  private static Logger _logger = LoggerFactory.getLogger(DefaultMessagingService.class);
  ConcurrentHashMap<String, MessageHandlerFactory> _messageHandlerFactoriestobeAdded =
      new ConcurrentHashMap<>();

  public DefaultMessagingService(HelixManager manager) {
    _manager = manager;
    _evaluator = new CriteriaEvaluator();

    boolean isParticipant = false;
    if (manager.getInstanceType() == InstanceType.PARTICIPANT || manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
      isParticipant = true;
    }

    _taskExecutor = new HelixTaskExecutor(
        new ParticipantStatusMonitor(isParticipant, manager.getInstanceName()),
        new MessageQueueMonitor(manager.getClusterName(), manager.getInstanceName()));
    _asyncCallbackService = new AsyncCallbackService();

    _taskThreadpoolResetTimeout = HelixUtil
        .getSystemPropertyAsInt(SystemPropertyKeys.TASK_THREADPOOL_RESET_TIMEOUT,
            TaskExecutor.DEFAULT_MSG_HANDLER_RESET_TIMEOUT_MS);
    _taskExecutor.registerMessageHandlerFactory(_asyncCallbackService, TaskExecutor.DEFAULT_PARALLEL_TASKS,
        _taskThreadpoolResetTimeout);
  }

  @Override
  public int send(Criteria recipientCriteria, final Message messageTemplate) {
    return send(recipientCriteria, messageTemplate, null, -1);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut) {
    return send(recipientCriteria, message, callbackOnReply, timeOut, 0);
  }

  @Override
  public int send(final Criteria recipientCriteria, final Message message,
      AsyncCallback callbackOnReply, int timeOut, int retryCount) {
    Map<InstanceType, List<Message>> generateMessage = generateMessage(recipientCriteria, message);

    HelixDataAccessor targetDataAccessor = getRecipientDataAccessor(recipientCriteria);

    ParticipantMessageOptions opts = ParticipantMessageOptions.builder()
        .callbackOnReply(callbackOnReply)
        .timeoutMs(timeOut)
        .retryCount(retryCount)
        .build();

    return sendMessagesInternalWithInstanceType(generateMessage, opts, targetDataAccessor);
  }

  private HelixDataAccessor getRecipientDataAccessor(final Criteria recipientCriteria) {
    return getRecipientDataAccessor(recipientCriteria.getClusterName());
  }

  private HelixDataAccessor getRecipientDataAccessor(String clusterName) {
    HelixDataAccessor dataAccessor = _manager.getHelixDataAccessor();
    if (clusterName != null && !clusterName.equals(_manager.getClusterName())) {
      // for cross cluster message, create new DataAccessor for sending message.
      /*
        TODO On frequent cross clsuter messaging request, keeping construct data accessor may cause
        performance issue. We should consider adding cache in this service or HelixManager. --JJ
       */
      dataAccessor = new ZKHelixDataAccessor(clusterName, dataAccessor.getBaseDataAccessor());
    }
    return dataAccessor;
  }

  public Map<InstanceType, List<Message>> generateMessage(final Criteria recipientCriteria,
      final Message message) {
    Map<InstanceType, List<Message>> messagesToSendMap = new HashMap<InstanceType, List<Message>>();
    InstanceType instanceType = recipientCriteria.getRecipientInstanceType();

    HelixDataAccessor targetDataAccessor = getRecipientDataAccessor(recipientCriteria);

      List<Message> messages = Collections.EMPTY_LIST;
      if (instanceType == InstanceType.CONTROLLER) {
        messages = generateMessagesForController(message);
      } else if (instanceType == InstanceType.PARTICIPANT) {
        messages =
            generateMessagesForParticipant(recipientCriteria, message, targetDataAccessor);
      }
      messagesToSendMap.put(instanceType, messages);
      return messagesToSendMap;
  }

  private List<Message> generateMessagesForParticipant(Criteria recipientCriteria, Message message,
      HelixDataAccessor targetDataAccessor) {
    List<Message> messages = new ArrayList<Message>();
    List<Map<String, String>> matchedList =
        _evaluator.evaluateCriteria(recipientCriteria, targetDataAccessor);

    if (!matchedList.isEmpty()) {
      Map<String, String> sessionIdMap = new HashMap<String, String>();
      if (recipientCriteria.isSessionSpecific()) {
        Builder keyBuilder = targetDataAccessor.keyBuilder();
        // For backward compatibility, allow partial read for the live instances.
        // Note that this may cause the pending message to be sent with null target session Id.
        List<LiveInstance> liveInstances =
            targetDataAccessor.getChildValues(keyBuilder.liveInstances(), false);

        for (LiveInstance liveInstance : liveInstances) {
          sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getEphemeralOwner());
        }
      }
      for (Map<String, String> map : matchedList) {
        String tgtInstanceName = map.get("instanceName");
        // Don't send message to self
        if (recipientCriteria.isSelfExcluded() && _manager.getInstanceName().equalsIgnoreCase(tgtInstanceName)) {
          continue;
        }
        String sessionId = recipientCriteria.isSessionSpecific() ? sessionIdMap.get(tgtInstanceName) : null;
        Message newMessage = createMessage(tgtInstanceName, message, map.get("resourceName"),
            map.get("partitionName"), sessionId);
        messages.add(newMessage);
      }
    }
    return messages;
  }

  private List<Message> generateMessagesForController(Message message) {
    List<Message> messages = new ArrayList<Message>();
    String id = (message.getMsgId() == null) ? UUID.randomUUID().toString() : message.getMsgId();
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setMsgId(id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName(InstanceType.CONTROLLER.name());
    messages.add(newMessage);
    return messages;
  }

  @Override
  public synchronized void registerMessageHandlerFactory(String type,
      MessageHandlerFactory factory) {
    registerMessageHandlerFactory(Collections.singletonList(type), factory);
  }

  @Override
  public synchronized void registerMessageHandlerFactory(List<String> types,
      MessageHandlerFactory factory) {
    if (_manager.isConnected()) {
      for (String type : types) {
        registerMessageHandlerFactoryInternal(type, factory);
      }
    } else {
      for (String type : types) {
        _messageHandlerFactoriestobeAdded.put(type, factory);
      }
    }
  }

  public synchronized void onConnected() {
    for (String type : _messageHandlerFactoriestobeAdded.keySet()) {
      registerMessageHandlerFactoryInternal(type, _messageHandlerFactoriestobeAdded.get(type));
    }
    _messageHandlerFactoriestobeAdded.clear();
  }

  void registerMessageHandlerFactoryInternal(String type, MessageHandlerFactory factory) {
    _logger.info("registering msg factory for type " + type);
    int threadpoolSize = HelixTaskExecutor.DEFAULT_PARALLEL_TASKS;
    String threadpoolSizeStr = null;
    String key = type + "." + HelixTaskExecutor.MAX_THREADS;

    ConfigAccessor configAccessor = _manager.getConfigAccessor();
    if (configAccessor != null) {
      ConfigScope scope = null;

      // Read the participant config and cluster config for the per-message type thread pool size.
      // participant config will override the cluster config.

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        scope =
            new ConfigScopeBuilder().forCluster(_manager.getClusterName())
                .forParticipant(_manager.getInstanceName()).build();
        threadpoolSizeStr = configAccessor.get(scope, key);
      }

      if (threadpoolSizeStr == null) {
        scope = new ConfigScopeBuilder().forCluster(_manager.getClusterName()).build();
        threadpoolSizeStr = configAccessor.get(scope, key);
      }
    }

    if (threadpoolSizeStr != null) {
      try {
        threadpoolSize = Integer.parseInt(threadpoolSizeStr);
        if (threadpoolSize <= 0) {
          threadpoolSize = 1;
        }
      } catch (Exception e) {
        _logger.error("", e);
      }
    }

    _taskExecutor.registerMessageHandlerFactory(type, factory, threadpoolSize);
    // Self-send a no-op message, so that the onMessage() call will be invoked
    // again, and
    // we have a chance to process the message that we received with the new
    // added MessageHandlerFactory
    // before the factory is added.
    sendNopMessageInternal();
  }

  @Deprecated
  public void sendNopMessage() {
    sendNopMessageInternal();
  }

  private void sendNopMessageInternal() {
    try {
      Message nopMsg = new Message(MessageType.NO_OP, UUID.randomUUID().toString());
      nopMsg.setSrcName(_manager.getInstanceName());

      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      if (_manager.getInstanceType() == InstanceType.CONTROLLER
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName(InstanceType.CONTROLLER.name());
        accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg);
      }

      if (_manager.getInstanceType() == InstanceType.PARTICIPANT
          || _manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT) {
        nopMsg.setTgtName(_manager.getInstanceName());
        accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg);
      }
    } catch (Exception e) {
      _logger.error(e.toString());
    }
  }

  public HelixTaskExecutor getExecutor() {
    return _taskExecutor;
  }

  @VisibleForTesting
  int getTaskThreadpoolResetTimeout() {
    return _taskThreadpoolResetTimeout;
  }

  @Override
  // TODO if the manager is not Participant or Controller, no reply, so should fail immediately
  public int sendAndWait(Criteria recipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut, int retryCount) {
    int messagesSent = send(recipientCriteria, message, asyncCallback, timeOut, retryCount);
    if (messagesSent > 0) {
      synchronized (asyncCallback) {
        while (!asyncCallback.isDone() && !asyncCallback.isTimedOut()) {
          try {
            asyncCallback.wait();
          } catch (InterruptedException e) {
            _logger.error(e.toString());
            asyncCallback.setInterrupted(true);
            break;
          }
        }
      }
    } else {
      _logger.warn("No messages sent. For Criteria:" + recipientCriteria);
    }
    return messagesSent;
  }

  @Override
  public int sendAndWait(Criteria recipientCriteria, Message message, AsyncCallback asyncCallback,
      int timeOut) {
    return sendAndWait(recipientCriteria, message, asyncCallback, timeOut, 0);
  }

  @Override
  public int sendToParticipantInstance(String clusterName, String instanceName, Message message,
      ParticipantMessageOptions options) {
    if (instanceName == null || instanceName.isEmpty()) {
      _logger.warn("Instance name is null or empty. No message sent.");
      return 0;
    }

    HelixDataAccessor dataAccessor = getRecipientDataAccessor(clusterName);
    Builder keyBuilder = dataAccessor.keyBuilder();
    boolean isLive = dataAccessor.getBaseDataAccessor()
        .exists(keyBuilder.liveInstance(instanceName).getPath(), 0);
    if (!isLive) {
      _logger.info("Instance " + instanceName + " is not live. No message sent.");
      return 0;
    }

    return sendToParticipantInstances(clusterName, message, options,
        Collections.singletonList(instanceName));
  }

  @Override
  public int sendToAllParticipantInstances(String clusterName, Message message,
      ParticipantMessageOptions options) {
    HelixDataAccessor dataAccessor = getRecipientDataAccessor(clusterName);
    Builder keyBuilder = dataAccessor.keyBuilder();
    List<String> liveInstanceNames = dataAccessor.getChildNames(keyBuilder.liveInstances());
    if (liveInstanceNames == null || liveInstanceNames.isEmpty()) {
      _logger.info("No live participant instances found in cluster: " + clusterName);
      return 0;
    }
    return sendToParticipantInstances(clusterName, message, options, liveInstanceNames);
  }

  /**
   * Shared helper to send messages to participant instances.
   *
   * @param clusterName the target cluster name
   * @param message message template
   * @param options participant message options (defaults applied when null)
   * @param liveInstanceNames list of live instance names
   * @return number of messages sent
   */
  private int sendToParticipantInstances(String clusterName, Message message,
      ParticipantMessageOptions options, List<String> liveInstanceNames) {
    ParticipantMessageOptions opts =
        options == null ? ParticipantMessageOptions.defaults() : options;

    HelixDataAccessor dataAccessor = getRecipientDataAccessor(clusterName);
    Builder keyBuilder = dataAccessor.keyBuilder();

    List<Message> participantMessages = new ArrayList<>();
    for (String instanceName : liveInstanceNames) {
      if (opts.isSelfExcluded() && instanceName.equalsIgnoreCase(_manager.getInstanceName())) {
        _logger.info("Message to self excluded for instance: " + instanceName);
        continue;
      }

      Optional<Message> msg = generateMessageForSingleInstance(instanceName, message, opts,
          dataAccessor, keyBuilder);
      if (msg.isPresent()) {
        participantMessages.add(msg.get());
      } else {
        _logger.warn("Failed to generate message for instance: " + instanceName);
      }
    }

    if (participantMessages.isEmpty()) {
      _logger.info("No participant messages generated for cluster: " + clusterName);
      return 0;
    }

    Map<InstanceType, List<Message>> messagesByType = new HashMap<>();
    messagesByType.put(InstanceType.PARTICIPANT, participantMessages);

    return sendMessagesInternalWithInstanceType(messagesByType, opts, dataAccessor);
  }

  /**
   * Common helper to create a message with standard fields.
   *
   * @param instanceName target instance name
   * @param message message template
   * @param resourceName resource name
   * @param partitionName partition name
   * @param sessionId target session ID (can be null)
   * @return new message with fields set
   */
  private Message createMessage(String instanceName, Message message, String resourceName,
      String partitionName, String sessionId) {
    String id = UUID.randomUUID().toString();
    Message newMessage = new Message(message.getRecord(), id);
    newMessage.setSrcName(_manager.getInstanceName());
    newMessage.setTgtName(instanceName);
    newMessage.setResourceName(resourceName);
    newMessage.setPartitionName(partitionName);
    if (sessionId != null) {
      newMessage.setTgtSessionId(sessionId);
    }
    return newMessage;
  }

  /**
   * Optimized helper to generate a single message for a specific instance.
   * Bypasses the criteria evaluator and directly creates message.
   *
   *
   * @param instanceName target instance name
   * @param message message template
   * @param opts participant message options
   * @param dataAccessor data accessor to use
   * @param keyBuilder key builder for ZK paths
   * @return Optional containing the message, or empty if failed
   */
  private Optional<Message> generateMessageForSingleInstance(String instanceName, Message message,
      ParticipantMessageOptions opts, HelixDataAccessor dataAccessor, Builder keyBuilder) {
    String sessionId = null;
    if (opts.isSessionSpecific()) {
      LiveInstance liveInstance = dataAccessor.getProperty(keyBuilder.liveInstance(instanceName));
      if (liveInstance != null) {
        sessionId = liveInstance.getEphemeralOwner();
      } else {
        _logger.warn("Failed to fetch session ID for instance: " + instanceName);
        return Optional.empty();
      }
    }

    return Optional.of(createMessage(instanceName, message, "", "", sessionId));
  }


  /**
   * Internal helper method to send messages grouped by InstanceType.
   * This handles both CONTROLLER and PARTICIPANT message types.
   *
   * @param messagesByType messages grouped by instance type
   * @param opts participant message options
   * @param targetDataAccessor data accessor to use
   * @return number of messages sent
   */
  private int sendMessagesInternalWithInstanceType(
      Map<InstanceType, List<Message>> messagesByType, ParticipantMessageOptions opts,
      HelixDataAccessor targetDataAccessor) {
    int totalMessageCount = 0;
    for (List<Message> messages : messagesByType.values()) {
      totalMessageCount += messages.size();
    }
    if (totalMessageCount == 0) {
      return 0;
    }

    // Setup callback
    String correlationId = setupCallback(messagesByType, opts);

    // Send messages for each instance type
    for (InstanceType receiverType : messagesByType.keySet()) {
      List<Message> messages = messagesByType.get(receiverType);
      sendMessagesToRecipients(messages, receiverType, correlationId, opts, targetDataAccessor);
    }

    // Start timer after sending
    if (opts.getCallbackOnReply() != null) {
      opts.getCallbackOnReply().startTimer();
    }

    return totalMessageCount;
  }

  /**
   * Shared helper to setup callback for message sending.
   *
   * @param messagesByType messages that will be sent
   * @param opts participant message options
   * @return correlation ID if callback is registered, null otherwise
   */
  private String setupCallback(Map<InstanceType, List<Message>> messagesByType,
      ParticipantMessageOptions opts) {
    AsyncCallback callbackOnReply = opts.getCallbackOnReply();
    if (callbackOnReply == null) {
      return null;
    }

    int totalTimeout = opts.getTimeoutMs() * (opts.getRetryCount() + 1);
    if (totalTimeout < 0) {
      totalTimeout = -1;
    }
    callbackOnReply.setTimeout(totalTimeout);
    String correlationId = UUID.randomUUID().toString();

    // Collect all messages from all instance types
    List<Message> allMessages = new ArrayList<>();
    for (List<Message> messages : messagesByType.values()) {
      allMessages.addAll(messages);
    }
    callbackOnReply.setMessagesSent(allMessages);

    _asyncCallbackService.registerAsyncCallback(correlationId, callbackOnReply);
    return correlationId;
  }

  /**
   * Shared helper to send messages to recipients based on instance type.
   *
   * @param messages list of messages to send
   * @param receiverType type of receiver (CONTROLLER or PARTICIPANT)
   * @param correlationId correlation ID for callbacks (can be null)
   * @param opts participant message options
   * @param targetDataAccessor data accessor for ZK operations
   */
  private void sendMessagesToRecipients(List<Message> messages, InstanceType receiverType,
      String correlationId, ParticipantMessageOptions opts, HelixDataAccessor targetDataAccessor) {
    Builder keyBuilder = targetDataAccessor.keyBuilder();

    for (Message tempMessage : messages) {
      tempMessage.setRetryCount(opts.getRetryCount());
      tempMessage.setExecutionTimeout(opts.getTimeoutMs());
      tempMessage.setSrcInstanceType(_manager.getInstanceType());
      if (correlationId != null) {
        tempMessage.setCorrelationId(correlationId);
      }
      tempMessage.setSrcClusterName(_manager.getClusterName());

      // Send to appropriate ZK path based on receiver type
      if (receiverType == InstanceType.CONTROLLER) {
        targetDataAccessor
            .setProperty(keyBuilder.controllerMessage(tempMessage.getId()), tempMessage);
      } else if (receiverType == InstanceType.PARTICIPANT) {
        targetDataAccessor
            .setProperty(keyBuilder.message(tempMessage.getTgtName(), tempMessage.getId()),
                tempMessage);
      }
    }
  }
}
