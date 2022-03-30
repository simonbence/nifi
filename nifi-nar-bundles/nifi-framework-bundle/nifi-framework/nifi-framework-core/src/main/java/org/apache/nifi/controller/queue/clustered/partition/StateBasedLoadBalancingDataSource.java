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
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.queue.QueueSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.components.state.Scope.CLUSTER;

public class StateBasedLoadBalancingDataSource implements FluentLoadBalancingDataSource {
    private static Logger LOGGER = LoggerFactory.getLogger(StateBasedLoadBalancingDataSource.class);
    private static final String STATE_MAP_KEY_PREFIX = "queues.loadbalancing.";

    private final NodeIdentifier localNodeIdentifier;
    private final StateManager stateManager;
    private final LocalQueuePartition localQueuePartition; // TODO
    private final StateBasedLoadBalancingDataSourceRecorder recorder;

    public StateBasedLoadBalancingDataSource(final NodeIdentifier localNodeIdentifier, final LocalQueuePartition localQueuePartition, final StateManager stateManager, final StateBasedLoadBalancingDataSourceRecorder recorder) {
        this.localNodeIdentifier = localNodeIdentifier;
        this.localQueuePartition = localQueuePartition;
        this.stateManager = stateManager;
        this.recorder = recorder;
    }

    final public void startRecording() {
        // NEEDS TO BE INITIALIZED
        try {
            // This is to initialize ZK...
            stateManager.setState(new HashMap<>(), CLUSTER);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        recorder.register(this);
    }

    final public void stopRecording() {
        recorder.deregister(this);
    }

    @Override
    public Map<String, QueueSize> getSnapshot() {
        try {
            final Map<String, QueueSize> result = new HashMap<>();
            final Map<String, String> state = stateManager.getState(CLUSTER).toMap(); // check if null / before first write

            final Set<String> ids = state.keySet()
                    .stream()
                    .map(s -> s.substring(STATE_MAP_KEY_PREFIX.length()))
                    .map(s -> s.substring(0, s.indexOf('.')))
                    .collect(Collectors.toSet());

            for (final String id : ids) {
                final QueueSize size = new QueueSize(
                    Integer.valueOf(state.get(STATE_MAP_KEY_PREFIX + id + ".count")),
                    Integer.valueOf(state.get(STATE_MAP_KEY_PREFIX + id + ".size"))
                );

                result.put(id, size);
            }

            return result;
        } catch (IOException e) {
            e.printStackTrace();
            // TODO
            throw new RuntimeException(e);
        }
    }

    // Every node saves the data about it's local partition. As a sum, the ZK behind will know about all the sizes and
    // when acquiring state, will have all the information.

    @Override
    public void recordSnapshot() {
        try {
            final QueueSize size = localQueuePartition.size();

            StateMap oldState;
            Map<String, String> state;

            // TODO an escape mechanism is needed
            do {
                oldState = stateManager.getState(CLUSTER);
                state = new HashMap<>(oldState.toMap());
                state.put(STATE_MAP_KEY_PREFIX + localNodeIdentifier.getId() + ".count", size.getObjectCount() + "");
                state.put(STATE_MAP_KEY_PREFIX + localNodeIdentifier.getId() + ".size", size.getByteCount() + "");
            } while (!stateManager.replace(oldState, state, CLUSTER));

                LOGGER.warn("Recording balancing info: " + localNodeIdentifier.getId() + "(" + size.getObjectCount()  + "/" + size.getByteCount() + ")");

            //stateManager.
        } catch (IOException e) {
            e.printStackTrace();
            // TODO
        }
    }
}
