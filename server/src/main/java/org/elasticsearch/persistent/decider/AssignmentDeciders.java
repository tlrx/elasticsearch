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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * An {@link AssignmentDecider} composed of one or more deciders. Deciders are checked sequentially
 * within each deciding method, and the first {@code Type.NO} decision encountered will be immediately
 * returned to the caller. Otherwise, it returns the decision of the last decider.
 */
public class AssignmentDeciders<Params extends PersistentTaskParams> extends AssignmentDecider<Params> {

    private final List<AssignmentDecider<Params>> deciders;

    public AssignmentDeciders(final Settings settings, final List<AssignmentDecider<Params>> deciders) {
        super(settings);
        this.deciders = Collections.unmodifiableList(deciders);
    }

    @Override
    public AssignmentDecision canAssign(final String taskName, final @Nullable Params taskParams) {
        return decide(decider -> decider.canAssign(taskName, taskParams));
    }

    @Override
    public AssignmentDecision canAssign(String taskName, Params taskParams, DiscoveryNode node) {
        return decide(decider -> decider.canAssign(taskName, taskParams, node));
    }

    /** Iterate over every deciders and stop iterating once a NO decision is found **/
    private AssignmentDecision decide(final Function<AssignmentDecider<Params>, AssignmentDecision> fn) {
        AssignmentDecision decision = AssignmentDecision.YES;
        for (AssignmentDecider<Params> decider : deciders) {
            decision = fn.apply(decider);
            if (decision == AssignmentDecision.NO || decision.getType() == AssignmentDecision.Type.NO) {
                break;
            }
        }
        return decision;
    }
}
