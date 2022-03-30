/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.queue.MaxQueueSize;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Map;
import java.util.function.Supplier;

public class FluentLoadBalancingPartitioner implements FlowFilePartitioner {
    private final Supplier<MaxQueueSize> maxSize;
    private final FluentLoadBalancingDataSource dataSource;

    private final double loadBalanceThreshold = 0.3D; // percentage
    private final double differenceThreshold = 0.1D; // percentage

    public FluentLoadBalancingPartitioner(final Supplier<MaxQueueSize> maxSize, final FluentLoadBalancingDataSource dataSource) {
        this.maxSize = maxSize;
        this.dataSource = dataSource;
    }

    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
        if (!reachesLoadBalanceThreshold(localPartition.size())) {
            return localPartition;
        }

        final Map<String, QueueSize> snapshot = dataSource.getSnapshot();

        QueuePartition candidate = localPartition;

        for (final QueuePartition partition : partitions) {
            if (partition.equals(localPartition)) {
                continue;
            }

            final QueueSize partitionSize = snapshot.get(partition.getNodeIdentifier().get().getId());
            final QueueSize candidateSize = candidate.equals(localPartition)
                ? localPartition.size()
                : snapshot.get(candidate.getNodeIdentifier().get().getId());

            if (isCandidateSuitable(candidateSize, partitionSize)) { // TODO partitonSize might be null after start
                candidate = partition;
            }
        }

        return candidate;
    }

    private boolean reachesLoadBalanceThreshold(final QueueSize actualSize) {
        final MaxQueueSize maxSizeSnapshot = maxSize.get();
        final double l = (double) actualSize.getObjectCount() / (double) maxSizeSnapshot.getMaxCount();
        final double l2 = (double) actualSize.getByteCount() / (double) maxSizeSnapshot.getMaxBytes();
        return l >= loadBalanceThreshold || l2 >= loadBalanceThreshold;
    }


    // max 100; actual 60; candidate 20; threshold: 20% >>>> yes
    // max 100; actual 60; candidate 50; threshold: 20% >>>> no
    // max 100; actual 60; candidate 70; threshold: 20% >>>> no
    private boolean isCandidateSuitable(final QueueSize actualSize, final QueueSize candidateSize) {
        final MaxQueueSize maxSizeSnapshot = maxSize.get();

        // TODO as a test, we only use the count!
//        boolean p1 = candidateSize.getByteCount() < actualSize.getByteCount() || candidateSize.getObjectCount() < actualSize.getObjectCount();

        double x = (double) (actualSize.getObjectCount() - candidateSize.getObjectCount()) / (double) maxSizeSnapshot.getMaxCount();
//        double y = (double) (actualSize.getByteCount() - candidateSize.getByteCount()) / (double) maxSizeSnapshot.getMaxBytes();

        return x > differenceThreshold;
//        return x > differenceThreshold && y > differenceThreshold;
    }

    // TODO last bits

    @Override
    public boolean isRebalanceOnClusterResize() {
        return false;
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return true;
    }
}
