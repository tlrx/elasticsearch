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
package org.elasticsearch.persistent.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksDecidersTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DataNodeAssignmentDeciderTests  extends PersistentTasksDecidersTestCase {

    public void testEnableAssignment() {
        final int nbNodes = 10;randomIntBetween(0, 10);
        final int nbDataNodes = 1;//nbNodes > 0 ? randomInt(nbNodes) : 0;

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < nbNodes; i++) {
            Set<DiscoveryNode.Role> roles = new HashSet<>();
            if (i < nbDataNodes) {
                roles.add(DiscoveryNode.Role.DATA);
            } else {
                roles.add(randomFrom(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.INGEST));
            }
            nodes.add(new DiscoveryNode("_node_" + i, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT));
        }

        final int nbTasks = 5;//randomIntBetween(1, 10);
        PersistentTasksCustomMetaData.Builder tasks = PersistentTasksCustomMetaData.builder();
        for (int i = 0; i < nbTasks; i++) {
            tasks.addTask("_task_" + i, "test", null, new PersistentTasksCustomMetaData.Assignment(null, "initialized"));
        }

        MetaData metaData = MetaData.builder()
            .putCustom(PersistentTasksCustomMetaData.TYPE, tasks.build())
            .build();

        ClusterState clusterState = reassign(ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metaData(metaData).build());
        if (nbDataNodes > 0) {
            assertNbAssignedTasks(nbTasks, clusterState);
        } else {
            assertNbUnassignedTasks(nbTasks, clusterState);
        }
    }
}
