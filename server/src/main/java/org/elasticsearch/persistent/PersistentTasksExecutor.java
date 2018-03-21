/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.persistent;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.persistent.decider.AssignmentDecider;
import org.elasticsearch.persistent.decider.DataNodeAssignmentDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.List;
import java.util.Map;

/**
 * An executor of tasks that can survive restart of requesting or executing node.
 * These tasks are using cluster state rather than only transport service to send requests and responses.
 */
public abstract class PersistentTasksExecutor<Params extends PersistentTaskParams> extends AbstractComponent {

    private final String executor;
    private final String taskName;

    protected PersistentTasksExecutor(Settings settings, String taskName, String executor) {
        super(settings);
        this.taskName = taskName;
        this.executor = executor;
    }

    public String getTaskName() {
        return taskName;
    }

    /**
     * Checks the current cluster state for compatibility with the params
     * <p>
     * Throws an exception if the supplied params cannot be executed on the cluster in the current state.
     */
    public void validate(Params params, ClusterState clusterState) {}

    /**
     * Creates a AllocatedPersistentTask for communicating with task manager
     */
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTask<Params> taskInProgress, Map<String, String> headers) {
        return new AllocatedPersistentTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    /**
     * Returns task description that will be available via task manager
     */
    protected String getDescription(PersistentTask<Params> taskInProgress) {
        return "id=" + taskInProgress.getId();
    }

    /**
     * This operation will be executed on the executor node.
     * <p>
     * NOTE: The nodeOperation has to throw an exception, trigger task.markAsCompleted() or task.completeAndNotifyIfNeeded() methods to
     * indicate that the persistent task has finished.
     */
    protected abstract void nodeOperation(AllocatedPersistentTask task, @Nullable Params params, @Nullable Task.Status status);

    public String getExecutor() {
        return executor;
    }

    /**
     * Returns the {@link AssignmentDecider} to use when assigning persistent tasks to nodes for execution.
     *
     * @param currentState the current {@link ClusterState}
     * @return a {@link AssignmentDecider}
     */
    public AssignmentDecider<PersistentTaskParams> getTaskAssignmentDecider(final ClusterState currentState) {
        return new DataNodeAssignmentDecider<>(settings);
    }

    protected DiscoveryNode getAssignedNode(final String taskName, final @Nullable Params taskParams,
                                            final ClusterState currentState, final List<DiscoveryNode> nodes) {
        if (nodes.isEmpty()) {
            return null;
        }

        final PersistentTasksCustomMetaData persistentTasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        Long minLoad = Long.MAX_VALUE;
        DiscoveryNode minLoadedNode = null;
        for (DiscoveryNode node : nodes) {
            if (persistentTasks == null) {
                // We don't have any task running yet, pick the first available node
                return node;
            }
            long numberOfTasks = persistentTasks.getNumberOfTasksOnNode(node.getId(), taskName);
            if (numberOfTasks == 0) {
                return node;
            } else if (minLoad > numberOfTasks) {
                minLoad = numberOfTasks;
                minLoadedNode = node;
            }
        }
        return minLoadedNode;
    }
}
