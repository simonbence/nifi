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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.state.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateBasedLoadBalancingDataSourceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateBasedLoadBalancingDataSourceFactory.class);
    private final StateBasedLoadBalancingDataSourceRecorder recorder;

    public StateBasedLoadBalancingDataSourceFactory(StateBasedLoadBalancingDataSourceRecorder recorder) {
        this.recorder = recorder;
    }

    // TODO where to stop this
//    public void start() {
//        LOGGER.warn("Starting recorder");
//        recorder.start();
//        LOGGER.warn("Starting recorder finished");
//    }

    public StateBasedLoadBalancingDataSource getInstance(final NodeIdentifier localNodeIdentifier, final LocalQueuePartition localQueuePartition, final StateManager stateManager) {
        return new StateBasedLoadBalancingDataSource(localNodeIdentifier, localQueuePartition, stateManager, recorder);
    }
}
