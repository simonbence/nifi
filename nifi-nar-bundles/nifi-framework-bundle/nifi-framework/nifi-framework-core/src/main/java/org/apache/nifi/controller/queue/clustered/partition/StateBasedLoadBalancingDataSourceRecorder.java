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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class StateBasedLoadBalancingDataSourceRecorder  {
    private final static Logger LOGGER = LoggerFactory.getLogger(StateBasedLoadBalancingDataSourceRecorder.class);
    private final Set<StateBasedLoadBalancingDataSource> dataSources = ConcurrentHashMap.newKeySet();

    public void register(final StateBasedLoadBalancingDataSource dataSource) {
        dataSources.add(dataSource); // As soon as something is registered, the RECORD THREAD goes into waiting state
        LOGGER.warn("Register data source: " + dataSource);
    }

    public void deregister(final StateBasedLoadBalancingDataSource dataSource) {
        dataSources.remove(dataSource);
        LOGGER.warn("Deregister data source: " + dataSource);
    }

    public void start() {
        LOGGER.warn("Starting data source recorder");


        final StateBasedLoadBalancingDataSourceWorker worker = new StateBasedLoadBalancingDataSourceWorker(dataSources, 10000); // TODO
        final Thread t = new Thread(worker);
        t.setName("RECORDER THREAD");
        t.setDaemon(true);
        t.start();
    }

    private static class StateBasedLoadBalancingDataSourceWorker implements Runnable {
        private final Set<StateBasedLoadBalancingDataSource> dataSources;
        private final long interval;

        private StateBasedLoadBalancingDataSourceWorker(final Set<StateBasedLoadBalancingDataSource> dataSources, long interval) {
            this.dataSources = dataSources;
            this.interval = interval;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Thread.sleep(interval); // after registerint one, thread goes to WAIT state
                    final Set<StateBasedLoadBalancingDataSource> sources = new HashSet<>(dataSources);
                    sources.forEach(dataSource -> {dataSource.recordSnapshot();});
                    LOGGER.warn("Recording snapshots for " + sources.size() + " data sources");
                }
            } catch (InterruptedException e) { // other errors like error within recordSnapshot
                e.printStackTrace();
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOGGER.error("ERRORRRRRRR");
                LOGGER.error(t.toString());
            }

            LOGGER.warn("Finishing recorder");
        }
    }
}
