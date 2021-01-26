/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;

import java.util.Map;
import java.util.Set;

public interface RackStandbyTaskAssignor {

    /**
     * Computes desired standby task distribution for a different {@link StreamsConfig#RACK_ID_CONFIG}s.
     * @param sourceTasks - Source {@link TaskId}s with a corresponding rack IDs that are eligible for standby task creation.
     * @param clientRackIds - Client rack IDs that were received during assignment.
     * @return - Map of the rack IDs to set of {@link TaskId}s. The return value can be used by {@link TaskAssignor}
     *           implementation to decide if the {@link TaskId} can be assigned to a client that is located in a given rack.
     */
    Map<String, Set<TaskId>> computeStandbyTaskDistribution(final Map<TaskId, String> sourceTasks,
                                                            final Set<String> clientRackIds);
}
