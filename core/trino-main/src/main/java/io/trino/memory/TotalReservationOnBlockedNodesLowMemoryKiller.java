/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.memory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.execution.TaskId;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Comparator.comparingLong;

public class TotalReservationOnBlockedNodesLowMemoryKiller
        implements LowMemoryKiller
{
    @Override
    public Optional<KillTarget> chooseQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Optional<KillTarget> killTarget = chooseTasksToKill(runningQueries, nodes);
        if (killTarget.isEmpty()) {
            killTarget = chooseWholeQueryToKill(runningQueries, nodes);
        }
        return killTarget;
    }

    private Optional<KillTarget> chooseTasksToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Set<QueryId> queriesWithTaskRetryPolicy = runningQueries.stream()
                                                          .filter(query -> query.getRetryPolicy() == RetryPolicy.TASK)
                                                          .map(QueryMemoryInfo::getQueryId)
                                                          .collect(toImmutableSet());

        if (queriesWithTaskRetryPolicy.isEmpty()) {
            return Optional.empty();
        }

        ImmutableSet.Builder<TaskId> tasksToKillBuilder = ImmutableSet.builder();
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPool = node.getPool();
            if (memoryPool == null) {
                continue;
            }
            if (memoryPool.getFreeBytes() + memoryPool.getReservedRevocableBytes() > 0) {
                continue;
            }

            memoryPool.getTaskMemoryReservations().entrySet().stream()
                    // consider only tasks from queries with task retries enabled
                    .map(entry -> new SimpleEntry<>(TaskId.valueOf(entry.getKey()), entry.getValue()))
                    .filter(entry -> queriesWithTaskRetryPolicy.contains(entry.getKey().getQueryId()))
                    .max(Map.Entry.comparingByValue())
                    .map(SimpleEntry::getKey)
                    .ifPresent(tasksToKillBuilder::add);
        }
        Set<TaskId> tasksToKill = tasksToKillBuilder.build();
        if (tasksToKill.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(KillTarget.selectedTasks(tasksToKill));
    }

    private Optional<KillTarget> chooseWholeQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Map<QueryId, QueryMemoryInfo> queriesById = Maps.uniqueIndex(runningQueries, QueryMemoryInfo::getQueryId);
        Map<QueryId, Long> memoryReservationOnBlockedNodes = new HashMap<>();
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPool = node.getPool();
            if (memoryPool == null) {
                continue;
            }
            if (memoryPool.getFreeBytes() + memoryPool.getReservedRevocableBytes() > 0) {
                continue;
            }
            Map<QueryId, Long> queryMemoryReservations = memoryPool.getQueryMemoryReservations();
            queryMemoryReservations.forEach((queryId, memoryReservation) -> {
                QueryMemoryInfo queryMemoryInfo = queriesById.get(queryId);
                if (queryMemoryInfo != null && queryMemoryInfo.getRetryPolicy() == RetryPolicy.TASK) {
                    // Do not kill whole queries which run with task retries enabled
                    // Most of the time if query with task retries enabled is a root cause of cluster out-of-memory error
                    // individual tasks should be already picked for killing by `chooseTasksToKill`. Yet sometimes there is a discrepancy between
                    // tasks listing and determining memory pool size. Pool may report it is fully reserved by Q, yet there are no running tasks from Q reported
                    // for given node.
                    return;
                }
                memoryReservationOnBlockedNodes.compute(queryId, (id, oldValue) -> oldValue == null ? memoryReservation : oldValue + memoryReservation);
            });
        }

        return memoryReservationOnBlockedNodes.entrySet().stream()
                .max(comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .map(KillTarget::wholeQuery);
    }
}
