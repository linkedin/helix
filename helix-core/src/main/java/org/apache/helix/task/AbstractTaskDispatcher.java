package org.apache.helix.task;

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
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.monitoring.mbeans.JobMonitor;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskDispatcher.class);
  private static final String TASK_LATENCY_TAG = "Latency";

  // For connection management
  protected HelixManager _manager;
  protected static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();
  protected ClusterStatusMonitor _clusterStatusMonitor;

  public void init(HelixManager manager) {
    _manager = manager;
  }

  // Job Update related methods

  public void updatePreviousAssignedTasksStatus(
      Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments,
      Set<String> excludedInstances, String jobResource, CurrentStateOutput currStateOutput,
      JobContext jobCtx, JobConfig jobCfg, TaskState jobState,
      Map<String, Set<Integer>> assignedPartitions, Set<Integer> partitionsToDropFromIs,
      Map<Integer, PartitionAssignment> paMap, TargetState jobTgtState,
      Set<Integer> skippedPartitions, WorkflowControllerDataProvider cache,
      Map<String, Set<Integer>> tasksToDrop) {

    // If a job is in one of the following states and its tasks are in RUNNING states, the tasks
    // will be aborted.
    Set<TaskState> jobStatesForAbortingTasks =
        new HashSet<>(Arrays.asList(TaskState.TIMING_OUT, TaskState.TIMED_OUT, TaskState.FAILING,
            TaskState.FAILED, TaskState.ABORTED));

    // Get AssignableInstanceMap for releasing resources for tasks in terminal states
    AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();

    Set<Integer> allTasksToDrop = new HashSet<>();
    for (Set<Integer> taskToDropForInstance: tasksToDrop.values()) {
      allTasksToDrop.addAll(taskToDropForInstance);
    }

    // Iterate through all instances
    for (String instance : currentInstanceToTaskAssignments.keySet()) {
      assignedPartitions.put(instance, new HashSet<>());

      // Set all dropping transitions first. These are tasks coming from Participant disconnects
      // and have the requestedState of DROPPED.
      // These need to be prioritized over any other state transitions because of the race condition
      // with the same pId (task) running on other instances. This is because in paMap, we can only
      // define one transition per pId
      if (tasksToDrop.containsKey(instance)) {
        for (int pIdToDrop : tasksToDrop.get(instance)) {
          paMap.put(pIdToDrop,
              new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));
          assignedPartitions.get(instance).add(pIdToDrop);
        }
      }

      if (excludedInstances.contains(instance)) {
        continue;
      }

      // If not an excluded instance, we must instantiate its entry in assignedPartitions
      Set<Integer> pSet = currentInstanceToTaskAssignments.get(instance);

      // We need to remove all task pId's to be dropped because we already made an assignment in
      // paMap above for them to be dropped. The following does this.
      pSet.removeAll(allTasksToDrop);

      // Used to keep track of partitions that are in either INIT or DROPPED states
      Set<Integer> donePartitions = new TreeSet<>();
      for (int pId : pSet) {
        final String pName = pName(jobResource, pId);
        TaskPartitionState currState = getTaskCurrentState(currStateOutput,
            jobResource, pId, pName, instance, jobCtx, jobTgtState);

        // Check for pending state transitions on this (partition, instance). If there is a pending
        // state transition, we prioritize this pending state transition and set the assignment from
        // this pending state transition, essentially "waiting" until this pending message clears
        // If there is a pending message, we should not continue to update the context because from
        // controller prospective, state transition has not been completed yet if pending message
        // still existed.
        // If context gets updated here, controller might remove the job from RunTimeJobDAG which
        // can cause the task's CurrentState not being removed when there is a pending message for
        // that task.
        Message pendingMessage =
            currStateOutput.getPendingMessage(jobResource, new Partition(pName), instance);
        if (pendingMessage != null) {
          processTaskWithPendingMessage(pId, pName, instance, pendingMessage, jobState, currState,
              paMap, assignedPartitions);
          continue;
        }

        // Update job context based on current state
        updatePartitionInformationInJobContext(currStateOutput, jobResource, currState, jobCtx,
            pId, pName, instance);

        if (!instance.equals(jobCtx.getAssignedParticipant(pId))) {
          LOG.warn(
              "Instance {} does not match the assigned participant for pId {} in the job context (job: {}). Skipping task scheduling.",
              instance, pId, jobCtx.getName());
          continue;
        }

        // Get AssignableInstance for this instance and TaskConfig for releasing resources
        String quotaType = jobCfg.getJobType();
        String taskId;
        if (TaskUtil.isGenericTaskJob(jobCfg)) {
          taskId = jobCtx.getTaskIdForPartition(pId);
        } else {
          taskId = pName;
        }
        TaskConfig taskConfig = jobCfg.getTaskConfig(taskId);

        // Process any requested state transitions. If there is a requested state transition, just
        // "wait" until this state transition is complete
        String requestedStateStr =
            currStateOutput.getRequestedState(jobResource, new Partition(pName), instance);
        if (requestedStateStr != null && !requestedStateStr.isEmpty()) {
          TaskPartitionState requestedState = TaskPartitionState.valueOf(requestedStateStr);
          if (requestedState.equals(currState)) {
            LOG.warn("Requested state {} is the same as the current state for instance {}.",
                requestedState, instance);
          }

          // For STOPPED tasks, if the targetState is STOP, we should not honor requestedState
          // transition and make it a NOP
          if (currState == TaskPartitionState.STOPPED && jobTgtState == TargetState.STOP) {
            // This task is STOPPED and not going to be re-run, so release this task
            assignableInstanceManager.release(instance, taskConfig, quotaType);
            continue;
          }

          // This contains check is necessary because we have already traversed pIdsToDrop at the
          // beginning of this method. If we already have a dropping transition, we do not want to
          // overwrite it. Any other requestedState transitions (for example, INIT to RUNNING or
          // RUNNING to COMPLETE, can wait without affecting correctness - they will be picked up
          // in ensuing runs of the Task pipeline)
          if (!paMap.containsKey(pId)) {
            paMap.put(pId, new PartitionAssignment(instance, requestedState.name()));
          }
          assignedPartitions.get(instance).add(pId);
          LOG.debug("Instance {} requested a state transition to {} for partition {}.", instance,
              requestedState, pName);
          continue;
        }

        switch (currState) {
        case RUNNING: {
          TaskPartitionState nextState = TaskPartitionState.RUNNING;
          if (jobStatesForAbortingTasks.contains(jobState)) {
            nextState = TaskPartitionState.TASK_ABORTED;
          } else if (jobTgtState == TargetState.STOP) {
            nextState = TaskPartitionState.STOPPED;
          }
          paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
          assignedPartitions.get(instance).add(pId);
          LOG.debug("Setting task partition {} state to {} on instance {}.", pName, nextState,
              instance);
        }
          break;
        case STOPPED: {
          // TODO: This case statement might be unreachable code - Hunter
          // This code may need to be removed because once a task is STOPPED and its workflow's
          // targetState is STOP, we do not assign that stopped task. Not assigning means it will
          // not be included in previousAssignment map in the next rebalance. If it is not in
          // prevInstanceToTaskAssignments, it will never hit this part of the code
          // When the parent workflow is to be resumed (target state is START), then it will just be
          // assigned as if it were being assigned for the first time
          TaskPartitionState nextState;
          if (jobTgtState.equals(TargetState.START)) {
            nextState = TaskPartitionState.RUNNING;
          } else {
            nextState = TaskPartitionState.STOPPED;
            // This task is STOPPED and not going to be re-run, so release this task
            assignableInstanceManager.release(instance, taskConfig, quotaType);
          }
          paMap.put(pId, new JobRebalancer.PartitionAssignment(instance, nextState.name()));
          assignedPartitions.get(instance).add(pId);

          LOG.debug("Setting job {} task partition {} state to {} on instance {}.",
              jobCtx.getName(), pName, nextState, instance);
        }
        break;
        case COMPLETED: {
          // The task has completed on this partition. Drop it from the instance and add it to assignedPartitions in
          // order to avoid scheduling it again in this pipeline.
          assignedPartitions.get(instance).add(pId);
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));
          LOG.debug(
              "Task partition {} has completed with state {}. Marking as such in rebalancer context.",
              pName, currState);
          partitionsToDropFromIs.add(pId);
          // This task is COMPLETED, so release this task
          assignableInstanceManager.release(instance, taskConfig, quotaType);
        }
        break;
        case TIMED_OUT:

        case TASK_ERROR:

        case TASK_ABORTED:

        case ERROR: {
          // First make this task which is in terminal state to be dropped.
          // Later on, in next pipeline in handleAdditionalAssignments, the task will be retried if possible.
          // (meaning it is not ABORTED and max number of attempts has not been reached yet)
          assignedPartitions.get(instance).add(pId);
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));
          LOG.debug(
              "Task partition {} has error state {} with msg {}. Marking as such in rebalancer context.",
              pName, currState, jobCtx.getPartitionInfo(pId));
          // The error policy is to fail the task as soon a single partition fails for a specified
          // maximum number of attempts or task is in ABORTED state.
          // But notice that if job is TIMED_OUT, aborted task won't be treated as fail and won't
          // cause job fail.
          // After all tasks are aborted, they will be dropped, because of job timeout.
          if (jobState != TaskState.TIMED_OUT && jobState != TaskState.TIMING_OUT) {
            if (jobCtx.getPartitionNumAttempts(pId) >= jobCfg.getMaxAttemptsPerTask()
                || currState.equals(TaskPartitionState.TASK_ABORTED)
                || currState.equals(TaskPartitionState.ERROR)) {
              skippedPartitions.add(pId);
              partitionsToDropFromIs.add(pId);
              LOG.debug("skippedPartitions: {}", skippedPartitions);
            } else {
              // Mark the task to be started at some later time (if enabled)
              markPartitionDelayed(jobCfg, jobCtx, pId);
            }
          }
          // Release this task
          assignableInstanceManager.release(instance, taskConfig, quotaType);
        }
          break;
        case INIT: {
          // INIT is a temporary state for tasks
          // Two possible scenarios for INIT:
          // 1. Task is getting scheduled for the first time. In this case, Task's state will go
          // from null->INIT->RUNNING, and this INIT state will be transient and very short-lived
          // 2. Task is getting scheduled for the first time, but in this case, job is timed out or
          // timing out. In this case, it will be sent back to INIT state to be removed. Here we
          // ensure that this task then goes from INIT to DROPPED so that it will be released from
          // AssignableInstance to prevent resource leak
          if (jobState == TaskState.TIMED_OUT || jobState == TaskState.TIMING_OUT
              || jobTgtState == TargetState.DELETE) {
            // Job is timed out or timing out or targetState is to be deleted, so its tasks will be
            // sent back to INIT
            // In this case, tasks' IdealState will be removed, and they will be sent to DROPPED
            partitionsToDropFromIs.add(pId);

            assignedPartitions.get(instance).add(pId);
            paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));

            // Also release resources for these tasks
            assignableInstanceManager.release(instance, taskConfig, quotaType);
            break;
          } else if (jobState == TaskState.IN_PROGRESS
              && (jobTgtState != TargetState.STOP && jobTgtState != TargetState.DELETE)) {
            // Job is in progress, implying that tasks are being re-tried, so set it to RUNNING
            paMap.put(pId,
                new JobRebalancer.PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
            assignedPartitions.get(instance).add(pId);
            break;
          }
        }

        case DROPPED: {
          // currState in [INIT, DROPPED]. Do nothing, the partition is eligible to be reassigned.
          donePartitions.add(pId);
          LOG.debug(
              "Task partition {} has state {}. It will be dropped from the current ideal state.",
              pName, currState);
          // If it's DROPPED, release this task. If INIT, do not release
          if (currState == TaskPartitionState.DROPPED) {
            assignableInstanceManager.release(instance, taskConfig, quotaType);
          }
        }
          break;
        default:
          throw new AssertionError("Unknown enum symbol: " + currState);
        }
      }

      // Remove the set of task partitions that are completed or in one of the error states.
      pSet.removeAll(donePartitions);
    }
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  protected String pName(String resource, int pId) {
    return String.format("%s_%s", resource, pId);
  }

  /**
   * An (instance, state) pair.
   */
  protected static class PartitionAssignment {
    public final String _instance;
    public final String _state;

    PartitionAssignment(String instance, String state) {
      _instance = instance;
      _state = state;
    }
  }

  private TaskPartitionState getTaskCurrentState(
      CurrentStateOutput currentStateOutput, String jobResource, Integer pId, String pName,
      String instance, JobContext jobCtx, TargetState jobTgtState) {
    String currentStateString =
        currentStateOutput.getCurrentState(jobResource, new Partition(pName), instance);
    if (currentStateString == null) {
      // Task state is either DROPPED or INIT
      TaskPartitionState stateFromContext = jobCtx.getPartitionState(pId);
      // If jobTgtState is START: Since currentstate is null, this function will return INIT to
      // start the task or it will return the stateFromContext (the current context) and there is no
      // need to update the context.
      // If jobTgtState is DELETE: JobDispatcher handles this case and this part of the code will
      // not be triggered.
      // If jobTgtState is STOP:
      // If context is equal to INIT or RUNNING: Here context is set to be STOPPED.
      // Other states don't need special handling and context can remain unchanged.
      if (jobTgtState == TargetState.STOP && (stateFromContext == TaskPartitionState.RUNNING
          || stateFromContext == TaskPartitionState.INIT)) {
        jobCtx.setPartitionState(pId, TaskPartitionState.STOPPED);
        return TaskPartitionState.STOPPED;
      }
      return stateFromContext == null ? TaskPartitionState.INIT : stateFromContext;
    }
    TaskPartitionState currentState = TaskPartitionState.valueOf(currentStateString);
    return currentState;
  }

  /**
   * Based on the CurrentState of this task and Context information, the task information in the job
   * context gets updated.
   * @param currentStateOutput
   * @param jobResource
   * @param currentState
   * @param jobCtx
   * @param pId
   * @param pName
   * @param instance
   */
  private void updatePartitionInformationInJobContext(CurrentStateOutput currentStateOutput,
      String jobResource, TaskPartitionState currentState, JobContext jobCtx, Integer pId,
      String pName, String instance) {
    // The assignedParticipant field needs to be updated regardless of the current state and context
    // information because it will prevent controller to assign the task to the wrong participant
    // for targeted tasks when two CurrentStates exist for one task.
    // In the updatePreviousAssignedTasksStatus, we check
    // instance.equals(jobCtx.getAssignedParticipant(pId)) and bypass the assignment if instance is
    // not equal to job context's AssignedParticipant for this pId.
    jobCtx.setAssignedParticipant(pId, instance);
    // If job context needs to be updated with new state, update it accordingly
    // This check is necessary because we are relying on current state and we do not want to update
    // context as long as current state existed. We just want to update context information
    // (specially finish time) once.
    // This condition checks whether jobContext's state is out of date or not.
    if (!currentState.equals(jobCtx.getPartitionState(pId))) {
      jobCtx.setPartitionState(pId, currentState);
      String taskMsg = currentStateOutput.getInfo(jobResource, new Partition(pName), instance);
      if (taskMsg != null) {
        jobCtx.setPartitionInfo(pId, taskMsg);
      }
      if (currentState == TaskPartitionState.COMPLETED) {
        markPartitionCompleted(jobCtx, pId);
      }
      // This avoids a race condition in the case that although currentState is in the following
      // error condition, the pending message (INIT->RUNNNING) might still be present.
      // This is undesirable because this prevents JobContext from getting the proper update of
      // fields including task state and task's NUM_ATTEMPTS
      if (currentState == TaskPartitionState.ERROR || currentState == TaskPartitionState.TASK_ERROR
          || currentState == TaskPartitionState.TIMED_OUT
          || currentState == TaskPartitionState.TASK_ABORTED) {
        // Do not increment the task attempt count here - it will be incremented at scheduling
        // time
        markPartitionError(jobCtx, pId, currentState);
      }
    }
  }

  /**
   * Create an assignment based on an already-existing pending message. This effectively lets the
   * Controller to "wait" until the pending state transition has been processed.
   * @param pId
   * @param pName
   * @param instance
   * @param pendingMessage
   * @param jobState
   * @param currState
   * @param paMap
   * @param assignedPartitions
   */
  private void processTaskWithPendingMessage(Integer pId, String pName, String instance,
      Message pendingMessage, TaskState jobState, TaskPartitionState currState,
      Map<Integer, PartitionAssignment> paMap, Map<String, Set<Integer>> assignedPartitions) {

    if (jobState == TaskState.TIMING_OUT && currState == TaskPartitionState.INIT
        && pendingMessage.getToState().equals(TaskPartitionState.RUNNING.name())) {
      // While job is timing out, if the task is pending on INIT->RUNNING, set it back to INIT,
      // so that Helix will cancel the transition.
      paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.INIT.name()));
      assignedPartitions.get(instance).add(pId);
      LOG.debug(
          "Task partition {} has a pending state transition on instance {} INIT->RUNNING. CurrentState is {} "
              + "Setting it back to INIT so that Helix can cancel the transition(if enabled).",
          pName, instance, currState.name());
    } else {
      // Otherwise, Just copy forward
      // the state assignment from the pending message
      paMap.put(pId, new PartitionAssignment(instance, pendingMessage.getToState()));
      assignedPartitions.get(instance).add(pId);
      LOG.debug(
          "Task partition {} has a pending state transition on instance {}. Using the pending message ToState which was {}.",
          pName, instance, pendingMessage.getToState());
    }
  }

  protected static void markPartitionCompleted(JobContext ctx, int pId) {
    ctx.setPartitionState(pId, TaskPartitionState.COMPLETED);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
  }

  protected static void markPartitionError(JobContext ctx, int pId, TaskPartitionState state) {
    ctx.setPartitionState(pId, state);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
  }

  protected static void markAllPartitionsError(JobContext ctx) {
    for (int pId : ctx.getPartitionSet()) {
      markPartitionError(ctx, pId, TaskPartitionState.ERROR);
    }
  }

  protected static void markPartitionDelayed(JobConfig cfg, JobContext ctx, int p) {
    long delayInterval = cfg.getTaskRetryDelay();
    if (delayInterval <= 0) {
      return;
    }
    long nextStartTime = ctx.getPartitionFinishTime(p) + delayInterval;
    ctx.setNextRetryTime(p, nextStartTime);
  }

  protected void handleJobTimeout(JobContext jobCtx, WorkflowContext workflowCtx,
      String jobResource, JobConfig jobCfg) {
    jobCtx.setFinishTime(System.currentTimeMillis());
    workflowCtx.setJobState(jobResource, TaskState.TIMED_OUT);
    // Mark all INIT task to TASK_ABORTED
    for (int pId : jobCtx.getPartitionSet()) {
      if (jobCtx.getPartitionState(pId) == TaskPartitionState.INIT) {
        jobCtx.setPartitionState(pId, TaskPartitionState.TASK_ABORTED);
      }
    }
    _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.TIMED_OUT);
    _rebalanceScheduler.removeScheduledRebalance(jobResource);
    TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
    // New pipeline trigger for workflow status update
    // TODO: Enhance the pipeline and remove this because this operation is expansive
    RebalanceUtil.scheduleOnDemandPipeline(_manager.getClusterName(),0L,false);
  }

  protected void failJob(String jobName, WorkflowContext workflowContext, JobContext jobContext,
      WorkflowConfig workflowConfig, Map<String, JobConfig> jobConfigMap,
      WorkflowControllerDataProvider dataProvider) {
    markJobFailed(jobName, jobContext, workflowConfig, workflowContext, jobConfigMap, dataProvider);

    // Mark all INIT task to TASK_ABORTED
    for (int pId : jobContext.getPartitionSet()) {
      if (jobContext.getPartitionState(pId) == TaskPartitionState.INIT) {
        jobContext.setPartitionState(pId, TaskPartitionState.TASK_ABORTED);
      }
    }
    _clusterStatusMonitor.updateJobCounters(jobConfigMap.get(jobName), TaskState.FAILED);
    _rebalanceScheduler.removeScheduledRebalance(jobName);
    TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobName);
    // New pipeline trigger for workflow status update
    // TODO: Enhance the pipeline and remove this because this operation is expansive
    RebalanceUtil.scheduleOnDemandPipeline(_manager.getClusterName(),0L,false);
  }

  // Compute real assignment from theoretical calculation with applied throttling
  // This is the actual assigning part
  protected void handleAdditionalTaskAssignment(
      Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments,
      Set<String> excludedInstances, String jobResource, CurrentStateOutput currStateOutput,
      JobContext jobCtx, final JobConfig jobCfg, final WorkflowConfig workflowConfig,
      WorkflowContext workflowCtx, final WorkflowControllerDataProvider cache,
      Map<String, Set<Integer>> assignedPartitions, Map<Integer, PartitionAssignment> paMap,
      Set<Integer> skippedPartitions, TaskAssignmentCalculator taskAssignmentCal,
      Set<Integer> allPartitions, final long currentTime, Collection<String> liveInstances) {

    // See if there was LiveInstance change and cache LiveInstances from this iteration of pipeline
    boolean existsLiveInstanceOrCurrentStateOrMessageChangeChange =
        cache.getExistsLiveInstanceOrCurrentStateOrMessageChange();

    // The excludeSet contains the set of task partitions that must be excluded from consideration
    // when making any new assignments.
    // This includes all completed, failed, delayed, and already assigned partitions.
    Set<Integer> excludeSet = Sets.newTreeSet();
    // Add all assigned partitions to excludeSet
    for (Set<Integer> assignedSet : assignedPartitions.values()) {
      excludeSet.addAll(assignedSet);
    }
    addCompletedTasks(excludeSet, jobCtx, allPartitions);
    addPartitionsReachedMaximumRetries(excludeSet, jobCtx, allPartitions, jobCfg);
    excludeSet.addAll(skippedPartitions);
    Set<Integer> partitionsWithDelay = TaskUtil.getNonReadyPartitions(jobCtx, currentTime);
    excludeSet.addAll(partitionsWithDelay);

    // The following is filtering of tasks before passing them to the assigner
    // Only feed in tasks that need to be assigned (have state equal to null, STOPPED, TIMED_OUT,
    // TASK_ERROR, or DROPPED) or their assigned participant is disabled or not live anymore
    Set<Integer> filteredTaskPartitionNumbers = filterTasks(jobResource, allPartitions, jobCtx,
        liveInstances, cache.getDisabledInstances(), currStateOutput, paMap);
    // Remove all excludeSet tasks to be safer because some STOPPED tasks have been already
    // re-started (excludeSet includes already-assigned partitions). Also tasks with their retry
    // limit exceed (addGiveupPartitions) will be removed as well
    filteredTaskPartitionNumbers.removeAll(excludeSet);

    Set<Integer> partitionsToRetryOnLiveInstanceChangeForTargetedJob = new HashSet<>();
    // If the job is a targeted job, in case of live instance change, we need to assign
    // non-terminal tasks so that they could be re-scheduled
    if (!TaskUtil.isGenericTaskJob(jobCfg)
        && existsLiveInstanceOrCurrentStateOrMessageChangeChange) {
      // This job is a targeted job, so FixedAssignmentCalculator will be used
      // There has been a live instance change. Must re-add incomplete task partitions to be
      // re-assigned and re-scheduled
      for (int partitionNum : allPartitions) {
        TaskPartitionState taskPartitionState = jobCtx.getPartitionState(partitionNum);
        if (isTaskNotInTerminalState(taskPartitionState)
            && !partitionsWithDelay.contains(partitionNum)
            && !isTaskGivenup(jobCtx, jobCfg, partitionNum)) {
          // Some targeted tasks may have timed-out due to Participants (instances) not being
          // live, so we give tasks like these another try
          // If some of these tasks are already scheduled and running, they will be dropped as
          // well. Also, do not include partitions with delay that are not ready to be assigned and
          // scheduled and the partitions that cannot be retried (given up)
          partitionsToRetryOnLiveInstanceChangeForTargetedJob.add(partitionNum);
        }
      }
    }
    filteredTaskPartitionNumbers.addAll(partitionsToRetryOnLiveInstanceChangeForTargetedJob);

    // The actual assignment is computed here
    // Get instance->[partition, ...] mappings for the target resource.
    Map<String, SortedSet<Integer>> tgtPartitionAssignments =
        taskAssignmentCal.getTaskAssignment(currStateOutput, liveInstances, jobCfg, jobCtx,
            workflowConfig, workflowCtx, filteredTaskPartitionNumbers, cache.getIdealStates());

    if (!TaskUtil.isGenericTaskJob(jobCfg) && jobCfg.isRebalanceRunningTask()) {
      // TODO: Revisit the logic for isRebalanceRunningTask() and valid use cases for it
      // TODO: isRebalanceRunningTask() was originally put in place to allow users to move
      // ("rebalance") long-running tasks, but there hasn't been a clear use case for this
      // Previously, there was a bug in the condition above (it was || where it should have been &&)
      dropRebalancedRunningTasks(tgtPartitionAssignments, currentInstanceToTaskAssignments, paMap,
          jobCtx);
    }

    // If this is a targeted job and if there was a live instance change
    if (!TaskUtil.isGenericTaskJob(jobCfg)
        && existsLiveInstanceOrCurrentStateOrMessageChangeChange) {
      // Drop current jobs only if they are assigned to a different instance, regardless of
      // the jobCfg.isRebalanceRunningTask() setting
      dropRebalancedRunningTasks(tgtPartitionAssignments, currentInstanceToTaskAssignments, paMap,
          jobCtx);
    }
    // Go through ALL instances and assign/throttle tasks accordingly
    for (Map.Entry<String, SortedSet<Integer>> entry : currentInstanceToTaskAssignments.entrySet()) {
      String instance = entry.getKey();
      if (!tgtPartitionAssignments.containsKey(instance)) {
        // There is no assignment made for this instance, so it is safe to skip
        continue;
      }
      if (excludedInstances.contains(instance)) {
        // There is a task assignment made for this instance, but for some reason, we cannot
        // assign to this instance. So we must skip the actual scheduling, but we must also
        // release the prematurely assigned tasks from AssignableInstance
        if (!cache.getAssignableInstanceManager().getAssignableInstanceMap()
            .containsKey(instance)) {
          continue; // This should not happen; skip!
        }
        AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();
        String quotaType = jobCfg.getJobType();
        for (int partitionNum : tgtPartitionAssignments.get(instance)) {
          // Get the TaskConfig for this partitionNumber
          String taskId = getTaskId(jobCfg, jobCtx, partitionNum);
          TaskConfig taskConfig = jobCfg.getTaskConfig(taskId);
          assignableInstanceManager.release(instance, taskConfig, quotaType);
        }
        continue;
      }
      // 1. throttled by job configuration
      // Contains the set of task partitions currently assigned to the instance.
      int jobCfgLimitation =
          jobCfg.getNumConcurrentTasksPerInstance() - assignedPartitions.get(instance).size();
      // 2. throttled by participant capacity
      int participantCapacity =
          cache.getAssignableInstanceConfigMap().get(instance).getMaxConcurrentTask();
      if (participantCapacity == InstanceConfig.MAX_CONCURRENT_TASK_NOT_SET) {
        participantCapacity = cache.getClusterConfig().getMaxConcurrentTaskPerInstance();
      }
      int participantLimitation =
          participantCapacity - cache.getParticipantActiveTaskCount(instance);
      // New tasks to be assigned
      int numToAssign = Math.min(jobCfgLimitation, participantLimitation);
      LOG.debug(
          "Throttle tasks to be assigned to instance {} using limitation: Job Concurrent Task({}), "
              + "Participant Max Task({}). Remaining capacity {}.", instance, jobCfgLimitation,
          participantCapacity, numToAssign);
      Set<Integer> throttledSet = new HashSet<>();
      if (numToAssign > 0) {
        List<Integer> nextPartitions = getNextPartitions(tgtPartitionAssignments.get(instance),
            excludeSet, throttledSet, numToAssign);
        for (Integer pId : nextPartitions) {
          // The following is the actual scheduling of the tasks
          String pName = pName(jobResource, pId);
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
          excludeSet.add(pId);
          jobCtx.setAssignedParticipant(pId, instance);
          jobCtx.setPartitionState(pId, TaskPartitionState.INIT);
          final long currentTimestamp = System.currentTimeMillis();
          jobCtx.setPartitionStartTime(pId, currentTimestamp);
          if (jobCtx.getExecutionStartTime() == WorkflowContext.NOT_STARTED) {
            // This means this is the very first task scheduled for this job
            jobCtx.setExecutionStartTime(currentTimestamp);
            reportSubmissionToScheduleDelay(cache, _clusterStatusMonitor, workflowConfig, jobCfg,
                currentTimestamp);
          }
          // Increment the task attempt count at schedule time
          jobCtx.incrementNumAttempts(pId);
          LOG.debug("Setting job {} task partition {} state to {} on instance {}.",
              jobCtx.getName(), pName, TaskPartitionState.RUNNING, instance);
        }
        cache.setParticipantActiveTaskCount(instance,
            cache.getParticipantActiveTaskCount(instance) + nextPartitions.size());
      } else {
        // No assignment was actually scheduled, so this assignment needs to be released
        // Put all assignments in throttledSet. Be sure to subtract excludeSet because excludeSet is
        // already applied at filteringPartitions (excludeSet may contain partitions that are
        // currently running)
        Set<Integer> throttledSetWithExcludeSet =
            new HashSet<>(tgtPartitionAssignments.get(instance));
        throttledSetWithExcludeSet.removeAll(excludeSet); // Remove excludeSet
        throttledSet.addAll(throttledSetWithExcludeSet);
      }
      if (!throttledSet.isEmpty()) {
        // Release the tasks in throttledSet because they weren't actually assigned
        if (!cache.getAssignableInstanceManager().getAssignableInstanceMap()
            .containsKey(instance)) {
          continue;
        }
        AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();
        String quotaType = jobCfg.getJobType();
        for (int partitionNum : throttledSet) {
          // Get the TaskConfig for this partitionNumber
          String taskId = getTaskId(jobCfg, jobCtx, partitionNum);
          TaskConfig taskConfig = jobCfg.getTaskConfig(taskId);
          assignableInstanceManager.release(instance, taskConfig, quotaType);
        }
        LOG.debug(
            "tasks for job {} are ready but throttled (size: {}) when assigned to participant.",
            jobCfg.getJobId(), throttledSet.size());
      }
    }
  }

  protected void scheduleForNextTask(String job, JobContext jobCtx, long now) {
    // Figure out the earliest schedulable time in the future of a non-complete job
    boolean shouldSchedule = false;
    long earliestTime = Long.MAX_VALUE;
    for (int p : jobCtx.getPartitionSet()) {
      long retryTime = jobCtx.getNextRetryTime(p);
      TaskPartitionState state = jobCtx.getPartitionState(p);
      state = (state != null) ? state : TaskPartitionState.INIT;
      Set<TaskPartitionState> errorStates = Sets.newHashSet(TaskPartitionState.ERROR,
          TaskPartitionState.TASK_ERROR, TaskPartitionState.TIMED_OUT);
      if (errorStates.contains(state) && retryTime > now && retryTime < earliestTime) {
        earliestTime = retryTime;
        shouldSchedule = true;
      }
    }

    // If any was found, then schedule it
    if (shouldSchedule) {
      long scheduledTime = _rebalanceScheduler.getRebalanceTime(job);
      if (scheduledTime == -1 || earliestTime < scheduledTime) {
        _rebalanceScheduler.scheduleRebalance(_manager, job, earliestTime);
      }
    }
  }

  // Add all partitions/tasks that are cannot be retried. These tasks are:
  // 1- Task is in ABORTED or ERROR state.
  // 2- Task has just gone to TIMED_OUT, ERROR or DROPPED states and has reached to its
  // maxNumberAttempts
  // These tasks determine whether the job needs to FAILED or not.
  protected static void addGivenUpPartitions(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds, JobConfig cfg) {
    for (Integer pId : pIds) {
      if (isTaskGivenup(ctx, cfg, pId)) {
        set.add(pId);
      }
    }
  }

  // Add all partitions that have reached their maxNumberAttempts. These tasks should not be
  // considered for scheduling again.
  protected static void addPartitionsReachedMaximumRetries(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds, JobConfig cfg) {
    for (Integer pId : pIds) {
      if (ctx.getPartitionNumAttempts(pId) >= cfg.getMaxAttemptsPerTask()) {
        set.add(pId);
      }
    }
  }

  private static List<Integer> getNextPartitions(SortedSet<Integer> candidatePartitions,
      Set<Integer> excluded, Set<Integer> throttled, int n) {
    List<Integer> result = new ArrayList<>();
    for (Integer pId : candidatePartitions) {
      if (!excluded.contains(pId)) {
        if (result.size() < n) {
          result.add(pId);
        } else {
          throttled.add(pId);
        }
      }
    }
    return result;
  }

  private static void addCompletedTasks(Set<Integer> set, JobContext ctx, Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  /**
   * Returns a filtered Iterable of tasks. To filter tasks in this context means to only allow tasks
   * whose contexts are either null or in STOPPED, TIMED_OUT, TASK_ERROR, or DROPPED state because
   * only the tasks whose contexts are in these states are eligible to be assigned or re-tried.
   * Also, for those tasks in non-terminal states whose previously assigned instances are no longer
   * LiveInstances are re-added so that they could be re-assigned. Since in the Task Pipeline,
   * LiveInstance list contains instances that are live and enable, if instance is not among live
   * instance, it is either not live or not enabled. If the instance is not enabled, the controller
   * should first drop the task on the disabled participant. After the task is dropped, then the
   * task can be filtered out for new assignment. Otherwise, once the participant is re-enabled,
   * the controller see the task is running state on two different participants and that cause quota
   * and scheduling issues.
   */
  private Set<Integer> filterTasks(String jobResource, Iterable<Integer> allPartitions,
      JobContext jobContext, Collection<String> liveInstances, Set<String> disableInstances,
      CurrentStateOutput currStateOutput, Map<Integer, PartitionAssignment> paMap) {
    Set<Integer> filteredTasks = new HashSet<>();
    for (int partitionNumber : allPartitions) {
      TaskPartitionState state = jobContext.getPartitionState(partitionNumber);
      // Allow tasks eligible for scheduling
      if (state == null || state == TaskPartitionState.STOPPED
          || state == TaskPartitionState.TIMED_OUT || state == TaskPartitionState.TASK_ERROR
          || state == TaskPartitionState.DROPPED) {
        filteredTasks.add(partitionNumber);
      }
      // Allow tasks that their assigned instances are no longer live or enabled for rescheduling
      if (isTaskNotInTerminalState(state)) {
        String assignedParticipant = jobContext.getAssignedParticipant(partitionNumber);
        final String partitionName = pName(jobResource, partitionNumber);
        if (assignedParticipant != null && !liveInstances.contains(assignedParticipant)) {
          // The assigned instance is no longer in the liveInstance list. It is either not live or
          // disabled. If instance is disabled and current state still exist on the instance,
          // then controller needs to drop the current state, otherwise, the task can be marked as
          // dropped and be reassigned to other instances
          if (disableInstances.contains(assignedParticipant) &&
              currStateOutput.getCurrentState(jobResource, new Partition(partitionName),
                  assignedParticipant) != null) {
            paMap.put(partitionNumber,
                new PartitionAssignment(assignedParticipant, TaskPartitionState.DROPPED.name()));
          } else {
            if (_manager.getHelixDataAccessor().getProperty(
                _manager.getHelixDataAccessor().keyBuilder().liveInstance(assignedParticipant))
                != null && !disableInstances.contains(assignedParticipant)) {
              continue;
            }
            // Only drop the task if the instance is not alive, otherwise, the task will be continued
            jobContext.setPartitionState(partitionNumber, TaskPartitionState.DROPPED);
            filteredTasks.add(partitionNumber);
          }
        }
      }
    }
    return filteredTasks;
  }

  /**
   * Returns whether if the task is not in a terminal state and could be re-scheduled.
   * @param state
   * @return
   */
  protected static boolean isTaskNotInTerminalState(TaskPartitionState state) {
    return state != TaskPartitionState.COMPLETED && state != TaskPartitionState.TASK_ABORTED
        && state != TaskPartitionState.ERROR;
  }

  protected static boolean isTaskGivenup(JobContext ctx, JobConfig cfg, int pId) {
    TaskPartitionState state = ctx.getPartitionState(pId);
    if (state == TaskPartitionState.TASK_ABORTED || state == TaskPartitionState.ERROR) {
      return true;
    }
    if (state == TaskPartitionState.TIMED_OUT || state == TaskPartitionState.TASK_ERROR
        || state == TaskPartitionState.DROPPED) {
      return ctx.getPartitionNumAttempts(pId) >= cfg.getMaxAttemptsPerTask();
    }
    return false;
  }

  /**
   * If assignment is different from previous assignment, drop the old running task if it's no
   * longer assigned to the same instance, but not removing it from excludeSet because the same task
   * should not be assigned to the new instance right away.
   * Also only drop if the old and the new assignments both have the partition (task) and they
   * differ (because that means the task has been assigned to a different instance).
   */
  private void dropRebalancedRunningTasks(Map<String, SortedSet<Integer>> newAssignment,
      Map<String, SortedSet<Integer>> oldAssignment, Map<Integer, PartitionAssignment> paMap,
      JobContext jobContext) {

    for (String instance : oldAssignment.keySet()) {
      for (int pId : oldAssignment.get(instance)) {
        if (jobContext.getPartitionState(pId) == TaskPartitionState.RUNNING) {
          // Check if the new assignment has this task on a different instance
          boolean existsInNewAssignment = false;
          for (Map.Entry<String, SortedSet<Integer>> entry : newAssignment.entrySet()) {
            if (!entry.getKey().equals(instance) && entry.getValue().contains(pId)) {
              // Found the partition number; new assignment has been made
              existsInNewAssignment = true;
              LOG.info(
                  "Currently running task partition number: {} (job: {}) is being dropped from instance: {} and will be newly assigned to instance: {}. This is due to a LiveInstance/CurrentState change, and because this is a targeted task.",
                  jobContext.getName(), pId, instance, entry.getKey());
              break;
            }
          }
          if (existsInNewAssignment
              && instance.equals(jobContext.getAssignedParticipant(pId))
          ) {
            // We need to drop this task in the old assignment
            paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));
            jobContext.setPartitionState(pId, TaskPartitionState.DROPPED);
            // Now it will be dropped and be rescheduled
          }
        }
      }
    }
  }

  protected void markJobComplete(final String jobName, final JobContext jobContext,
      final WorkflowConfig workflowConfig, WorkflowContext workflowContext,
      final Map<String, JobConfig> jobConfigMap,
      final WorkflowControllerDataProvider dataProvider) {
    finishJobInRuntimeJobDag(dataProvider.getTaskDataCache(), workflowConfig.getWorkflowId(),
        jobName);
    final long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.COMPLETED);
    jobContext.setFinishTime(currentTime);
    if (isWorkflowFinished(workflowContext, workflowConfig, jobConfigMap, dataProvider)) {
      workflowContext.setFinishTime(currentTime);
      updateWorkflowMonitor(workflowContext, workflowConfig);
    }
    scheduleJobCleanUp(jobConfigMap.get(jobName).getExpiry(), workflowConfig, currentTime);

    // Job has completed successfully so report ControllerInducedDelay
    JobConfig jobConfig = jobConfigMap.get(jobName);
    if (jobConfig != null) {
      reportControllerInducedDelay(dataProvider, _clusterStatusMonitor, workflowConfig, jobConfig,
          currentTime);
    }
  }

  protected void markJobFailed(String jobName, JobContext jobContext, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext, Map<String, JobConfig> jobConfigMap,
      WorkflowControllerDataProvider clusterDataCache) {
    finishJobInRuntimeJobDag(clusterDataCache.getTaskDataCache(), workflowConfig.getWorkflowId(),
        jobName);
    long currentTime = System.currentTimeMillis();
    LOG.debug("Mark job: {} FAILED.", jobName);
    workflowContext.setJobState(jobName, TaskState.FAILED);
    if (jobContext != null) {
      jobContext.setFinishTime(currentTime);
    }
    if (isWorkflowFinished(workflowContext, workflowConfig, jobConfigMap, clusterDataCache)) {
      workflowContext.setFinishTime(currentTime);
      updateWorkflowMonitor(workflowContext, workflowConfig);
    }
    scheduleJobCleanUp(jobConfigMap.get(jobName).getTerminalStateExpiry(), workflowConfig,
        currentTime);
  }

  protected void scheduleJobCleanUp(long expiry, WorkflowConfig workflowConfig,
      long currentTime) {
    if (expiry < 0) {
      // If the expiry is negative, it's an invalid clean up. Return.
      return;
    }
    long currentScheduledTime =
        _rebalanceScheduler.getRebalanceTime(workflowConfig.getWorkflowId()) == -1 ? Long.MAX_VALUE
            : _rebalanceScheduler.getRebalanceTime(workflowConfig.getWorkflowId());
    if (currentTime + expiry < currentScheduledTime) {
      _rebalanceScheduler.scheduleRebalance(_manager, workflowConfig.getWorkflowId(),
          currentTime + expiry);
    }
  }

  // Workflow related methods

  /**
   * Checks if the workflow has finished (either completed or failed).
   * Set the state in workflow context properly.
   * @param ctx Workflow context containing job states
   * @param cfg Workflow config containing set of jobs
   * @return returns true if the workflow
   *         1. completed (all tasks are {@link TaskState#COMPLETED})
   *         2. failed (any task is {@link TaskState#FAILED}
   *         3. workflow is {@link TaskState#TIMED_OUT}
   *         returns false otherwise.
   */
  protected boolean isWorkflowFinished(WorkflowContext ctx, WorkflowConfig cfg,
      Map<String, JobConfig> jobConfigMap, WorkflowControllerDataProvider clusterDataCache) {
    boolean incomplete = false;

    TaskState workflowState = ctx.getWorkflowState();
    if (TaskState.TIMED_OUT.equals(workflowState)) {
      // We don't update job state here as JobRebalancer will do it
      return true;
    }

    // Check if failed job count is beyond threshold and if so, fail the workflow
    // and abort in-progress jobs
    int failedJobs = 0;
    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (jobState == TaskState.FAILED || jobState == TaskState.TIMED_OUT) {
        failedJobs++;
        if (!cfg.isJobQueue() && failedJobs > cfg.getFailureThreshold()) {
          ctx.setWorkflowState(TaskState.FAILED);
          LOG.info("Workflow {} reached the failure threshold, so setting its state to FAILED.",
              cfg.getWorkflowId());
          for (String jobToFail : cfg.getJobDag().getAllNodes()) {
            if (ctx.getJobState(jobToFail) == TaskState.IN_PROGRESS) {
              ctx.setJobState(jobToFail, TaskState.ABORTED);

              // Skip aborted jobs latency since they are not accurate latency for job running time
              if (_clusterStatusMonitor != null) {
                _clusterStatusMonitor.updateJobCounters(jobConfigMap.get(jobToFail),
                    TaskState.ABORTED);
              }

              // Since the job is aborted, release resources occupied by it
              // Otherwise, we run the risk of resource leak
              if (clusterDataCache != null) {
                AssignableInstanceManager assignableInstanceManager =
                    clusterDataCache.getAssignableInstanceManager();
                JobConfig jobConfig = jobConfigMap.get(jobToFail);
                String quotaType = jobConfig.getJobType();
                Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
                // Iterate over all tasks and release them
                for (Map.Entry<String, TaskConfig> taskEntry : taskConfigMap.entrySet()) {
                  TaskConfig taskConfig = taskEntry.getValue();
                  for (String assignableInstanceName : assignableInstanceManager
                      .getAssignableInstanceNames()) {
                    assignableInstanceManager.release(assignableInstanceName, taskConfig,
                        quotaType);
                  }
                }
              }
            }
          }
          return true;
        }
      }
      if (jobState != TaskState.COMPLETED && jobState != TaskState.FAILED
          && jobState != TaskState.TIMED_OUT) {
        incomplete = true;
      }
    }
    if (!incomplete && cfg.isTerminable()) {
      ctx.setWorkflowState(TaskState.COMPLETED);
      return true;
    }
    return false;
  }

  protected void updateWorkflowMonitor(WorkflowContext context, WorkflowConfig config) {
    if (_clusterStatusMonitor != null) {
      _clusterStatusMonitor.updateWorkflowCounters(config, context.getWorkflowState(),
          context.getFinishTime() - context.getStartTime());
    }
  }

  // Common methods

  protected Set<String> getExcludedInstances(String currentJobName, WorkflowConfig workflowCfg,
      WorkflowContext workflowContext, WorkflowControllerDataProvider cache) {
    Set<String> ret = new HashSet<>();

    if (!workflowCfg.isAllowOverlapJobAssignment()) {
      // exclude all instances that has been assigned other jobs' tasks
      for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
        if (jobName.equals(currentJobName)) {
          continue;
        }
        JobContext jobContext = cache.getJobContext(jobName);
        if (jobContext == null) {
          continue;
        }
        // Also skip if the job is not currently running
        // For example, if the job here is in a terminal state (such as ABORTED), then its tasks are
        // practically not running, so we do not need to exclude instances who have tasks from dead
        // jobs
        TaskState jobState = workflowContext.getJobState(jobName);
        if (jobState != TaskState.IN_PROGRESS) {
          continue;
        }
        for (int pId : jobContext.getPartitionSet()) {
          TaskPartitionState partitionState = jobContext.getPartitionState(pId);
          if (partitionState == TaskPartitionState.INIT
              || partitionState == TaskPartitionState.RUNNING) {
            ret.add(jobContext.getAssignedParticipant(pId));
          }
        }
      }
    }
    return ret;
  }

  /**
   * Schedule the rebalancer timer for task framework elements
   * @param resourceId The resource id
   * @param startTime The resource start time
   * @param timeoutPeriod The resource timeout period. Will be -1 if it is not set.
   */
  protected void scheduleRebalanceForTimeout(String resourceId, long startTime,
      long timeoutPeriod) {
    long nextTimeout = getTimeoutTime(startTime, timeoutPeriod);
    long nextRebalanceTime = _rebalanceScheduler.getRebalanceTime(resourceId);
    if (nextTimeout >= System.currentTimeMillis()
        && (nextRebalanceTime == TaskConstants.DEFAULT_NEVER_TIMEOUT
            || nextTimeout < nextRebalanceTime)) {
      _rebalanceScheduler.scheduleRebalance(_manager, resourceId, nextTimeout);
    }
  }

  /**
   * Basic function to check task framework resources, workflow and job, are timeout
   * @param startTime Resources start time
   * @param timeoutPeriod Resources timeout period. Will be -1 if it is not set.
   * @return
   */
  protected boolean isTimeout(long startTime, long timeoutPeriod) {
    long nextTimeout = getTimeoutTime(startTime, timeoutPeriod);
    return nextTimeout != TaskConstants.DEFAULT_NEVER_TIMEOUT
        && nextTimeout <= System.currentTimeMillis();
  }

  private long getTimeoutTime(long startTime, long timeoutPeriod) {
    return (timeoutPeriod == TaskConstants.DEFAULT_NEVER_TIMEOUT
        || timeoutPeriod > Long.MAX_VALUE - startTime)
            // check long overflow
            ? TaskConstants.DEFAULT_NEVER_TIMEOUT
            : startTime + timeoutPeriod;
  }

  /**
   * Set the ClusterStatusMonitor for metrics update
   */
  public void setClusterStatusMonitor(ClusterStatusMonitor clusterStatusMonitor) {
    _clusterStatusMonitor = clusterStatusMonitor;
  }

  /**
   * Returns an appropriate TaskId depending on whether the job is targeted or not.
   * @param jobCfg
   * @param jobCtx
   * @param partitionNum
   * @return
   */
  private String getTaskId(JobConfig jobCfg, JobContext jobCtx, int partitionNum) {
    if (TaskUtil.isGenericTaskJob(jobCfg)) {
      return jobCtx.getTaskIdForPartition(partitionNum);
    }
    // This is a targeted task
    return pName(jobCfg.getJobId(), partitionNum);
  }

  /**
   * Checks if the workflow has been stopped.
   * In the case of a recurrent workflow template, we look at its TargetState.
   * @param ctx Workflow context containing task states
   * @param cfg Workflow config containing set of tasks
   * @return returns true if all tasks are {@link TaskState#STOPPED}, false otherwise.
   */
  protected boolean isWorkflowStopped(WorkflowContext ctx, WorkflowConfig cfg) {
    if (cfg.isRecurring()) {
      return cfg.getTargetState() == TargetState.STOP;
    }

    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (jobState != null
          && (jobState.equals(TaskState.IN_PROGRESS) || jobState.equals(TaskState.STOPPING))) {
        return false;
      }
    }
    return true;
  }

  protected ResourceAssignment buildEmptyAssignment(String name,
      CurrentStateOutput currStateOutput) {
    ResourceAssignment assignment = new ResourceAssignment(name);
    Set<Partition> partitions = currStateOutput.getCurrentStateMappedPartitions(name);
    for (Partition partition : partitions) {
      Map<String, String> currentStateMap = currStateOutput.getCurrentStateMap(name, partition);
      Map<String, String> replicaMap = Maps.newHashMap();
      for (String instanceName : currentStateMap.keySet()) {
        replicaMap.put(instanceName, HelixDefinedState.DROPPED.toString());
      }
      assignment.addReplicaMap(partition, replicaMap);
    }
    return assignment;
  }

  /**
   * Check all the dependencies of a job to determine whether the job is ready to be scheduled.
   * @param job
   * @param workflowCfg
   * @param workflowCtx
   * @return
   */
  protected boolean isJobReadyToSchedule(String job, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, int incompleteAllCount, Map<String, JobConfig> jobConfigMap,
      WorkflowControllerDataProvider clusterDataCache,
      AssignableInstanceManager assignableInstanceManager) {
    int notStartedCount = 0;
    int failedOrTimeoutCount = 0;
    int incompleteParentCount = 0;
    JobConfig jobConfig = jobConfigMap.get(job);

    if (jobConfig == null) {
      LOG.error(String.format("The job config is missing for job %s", job));
      return false;
    }

    String quotaType = TaskAssignmentCalculator.getQuotaType(workflowCfg, jobConfig);
    if (quotaType == null || !assignableInstanceManager.hasQuotaType(quotaType)) {
      quotaType = AssignableInstance.DEFAULT_QUOTA_TYPE;
    }

    if (!assignableInstanceManager.hasGlobalCapacity(quotaType)) {
      LOG.info(
          "Job {} not ready to schedule due to not having enough quota for quota type {}", job,
          quotaType);
      return false;
    }

    for (String parent : workflowCfg.getJobDag().getDirectParents(job)) {
      TaskState jobState = workflowCtx.getJobState(parent);
      if (jobState == null || jobState == TaskState.NOT_STARTED) {
        ++notStartedCount;
      } else if (jobState == TaskState.FAILED || jobState == TaskState.TIMED_OUT) {
        ++failedOrTimeoutCount;
      } else if (jobState != TaskState.COMPLETED) {
        incompleteParentCount++;
      }
    }

    // If there is any parent job not started, this job should not be scheduled
    if (notStartedCount > 0) {
        LOG.debug("Job {} is not ready to start, notStartedParent(s)={}.", job,
            notStartedCount);
      return false;
    }

    // If there is parent job failed, schedule the job only when ignore dependent
    // job failure enabled
    if (failedOrTimeoutCount > 0 && !jobConfig.isIgnoreDependentJobFailure()) {
      markJobFailed(job, null, workflowCfg, workflowCtx, jobConfigMap, clusterDataCache);
        LOG.debug("Job {} is not ready to start, failedCount(s)={}.", job,
            failedOrTimeoutCount);
      return false;
    }

    if (workflowCfg.isJobQueue()) {
      // If job comes from a JobQueue, it should apply the parallel job logics
      if (incompleteAllCount >= workflowCfg.getParallelJobs()) {
          LOG.debug("Job {} is not ready to schedule, inCompleteJobs(s)={}.", job,
              incompleteAllCount);
        return false;
      }
    } else {
      // If this job comes from a generic workflow, job will not be scheduled until
      // all the direct parent jobs finished
      if (incompleteParentCount > 0) {
          LOG.debug("Job {} is not ready to start, notFinishedParent(s)={}.", job,
              incompleteParentCount);
        return false;
      }
    }

    return true;
  }

  /**
   * Check if a workflow is ready to schedule.
   * @param workflowCfg the workflow to check
   * @return true if the workflow is ready for schedule, false if not ready
   */
  protected boolean isWorkflowReadyForSchedule(WorkflowConfig workflowCfg) {
    Date startTime = workflowCfg.getStartTime();
    // Workflow with non-scheduled config or passed start time is ready to schedule.
    return (startTime == null || startTime.getTime() <= System.currentTimeMillis());
  }

  public void updateBestPossibleStateOutput(String resource,
      ResourceAssignment partitionStateAssignment, BestPossibleStateOutput output) {
    // Use the internal MappingCalculator interface to compute the final assignment
    // The next release will support rebalancers that compute the mapping from start to finish
    for (Partition partition : partitionStateAssignment.getMappedPartitions()) {
      Map<String, String> newStateMap = partitionStateAssignment.getReplicaMap(partition);
      output.setState(resource, partition, newStateMap);
    }
  }

  protected void finishJobInRuntimeJobDag(TaskDataCache clusterDataCache, String workflowName,
      String jobName) {
    RuntimeJobDag runtimeJobDag = clusterDataCache.getRuntimeJobDag(workflowName);
    if (runtimeJobDag != null) {
      runtimeJobDag.finishJob(jobName);
      LOG.debug(
          "Finish job {} of workflow {} for runtime job DAG", jobName, workflowName);
    } else {
      LOG.warn("Failed to find runtime job DAG for workflow {} and job {}",
          workflowName, jobName);
    }
  }

  /**
   * TODO: Move this logic to Task Framework metrics class for refactoring.
   * Computes and passes on submissionToProcessDelay to the dynamic metric.
   * @param dataProvider
   * @param clusterStatusMonitor
   * @param workflowConfig
   * @param jobConfig
   * @param currentTimestamp
   */
  protected static void reportSubmissionToProcessDelay(BaseControllerDataProvider dataProvider,
      final ClusterStatusMonitor clusterStatusMonitor, final WorkflowConfig workflowConfig,
      final JobConfig jobConfig, final long currentTimestamp) {
    AbstractBaseStage.asyncExecute(dataProvider.getAsyncTasksThreadPool(), () -> {
      // Asynchronously update the appropriate JobMonitor
      JobMonitor jobMonitor = clusterStatusMonitor
          .getJobMonitor(TaskAssignmentCalculator.getQuotaType(workflowConfig, jobConfig));
      if (jobMonitor == null) {
        return null;
      }

      // Compute SubmissionToProcessDelay
      long submissionToProcessDelay = currentTimestamp - jobConfig.getStat().getCreationTime();
      jobMonitor.updateSubmissionToProcessDelayGauge(submissionToProcessDelay);
      return null;
    });
  }

  /**
   * TODO: Move this logic to Task Framework metrics class for refactoring.
   * Computes and passes on submissionToScheduleDelay to the dynamic metric.
   * @param dataProvider
   * @param clusterStatusMonitor
   * @param workflowConfig
   * @param jobConfig
   * @param currentTimestamp
   */
  private static void reportSubmissionToScheduleDelay(BaseControllerDataProvider dataProvider,
      final ClusterStatusMonitor clusterStatusMonitor, final WorkflowConfig workflowConfig,
      final JobConfig jobConfig, final long currentTimestamp) {
    AbstractBaseStage.asyncExecute(dataProvider.getAsyncTasksThreadPool(), () -> {
      // Asynchronously update the appropriate JobMonitor
      JobMonitor jobMonitor = clusterStatusMonitor
          .getJobMonitor(TaskAssignmentCalculator.getQuotaType(workflowConfig, jobConfig));
      if (jobMonitor == null) {
        return null;
      }

      // Compute SubmissionToScheduleDelay
      long submissionToStartDelay = currentTimestamp - jobConfig.getStat().getCreationTime();
      jobMonitor.updateSubmissionToScheduleDelayGauge(submissionToStartDelay);
      return null;
    });
  }

  /**
   * TODO: Move this logic to Task Framework metrics class for refactoring.
   * Computes and passes on controllerInducedDelay to the dynamic metric.
   * @param dataProvider
   * @param clusterStatusMonitor
   * @param workflowConfig
   * @param jobConfig
   * @param currentTimestamp
   */
  private static void reportControllerInducedDelay(BaseControllerDataProvider dataProvider,
      final ClusterStatusMonitor clusterStatusMonitor, final WorkflowConfig workflowConfig,
      final JobConfig jobConfig, final long currentTimestamp) {
    AbstractBaseStage.asyncExecute(dataProvider.getAsyncTasksThreadPool(), () -> {
      // Asynchronously update the appropriate JobMonitor
      JobMonitor jobMonitor = clusterStatusMonitor
          .getJobMonitor(TaskAssignmentCalculator.getQuotaType(workflowConfig, jobConfig));
      if (jobMonitor == null) {
        return null;
      }

      // Compute ControllerInducedDelay only if the workload is a test load
      // NOTE: this metric cannot be computed for general user-submitted workloads because
      // the actual runtime of the tasks vary, and there could exist multiple tasks per
      // job
      // NOTE: a test workload will have the "latency" field in the mapField of the
      // JobConfig (taskConfig)
      String firstTask = jobConfig.getTaskConfigMap().keySet().iterator().next();
      if (jobConfig.getTaskConfig(firstTask).getConfigMap().containsKey(TASK_LATENCY_TAG)) {
        long taskDuration =
            Long.valueOf(jobConfig.getTaskConfig(firstTask).getConfigMap().get(TASK_LATENCY_TAG));
        long controllerInducedDelay =
            currentTimestamp - jobConfig.getStat().getCreationTime() - taskDuration;
        jobMonitor.updateControllerInducedDelayGauge(controllerInducedDelay);
      }
      return null;
    });
  }
}
