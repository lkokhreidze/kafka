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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultRackStandbyTaskAssignor implements RackStandbyTaskAssignor {

    @Override
    public Map<String, Set<TaskId>> computeStandbyTaskDistribution(final Map<TaskId, String> sourceTasks,
                                                                   final Set<String> clientRackIds) {
        final Map<String, Set<TaskId>> standbyTaskDistribution = new HashMap<>();
        for (final Map.Entry<TaskId, String> sourceTaskEntry : sourceTasks.entrySet()) {
            final TaskId taskId = sourceTaskEntry.getKey();
            final String taskRackId = sourceTaskEntry.getValue();

            for (final String clientRackId : clientRackIds) {
                if (!taskRackId.equals(clientRackId)) {
                    final Set<TaskId> rackTasks = standbyTaskDistribution.getOrDefault(clientRackId, new HashSet<>());
                    rackTasks.add(taskId);
                    standbyTaskDistribution.put(clientRackId, rackTasks);
                } else {
                    standbyTaskDistribution.put(clientRackId, new HashSet<>());
                }
            }
        }

        return Collections.unmodifiableMap(standbyTaskDistribution);
    }
}
