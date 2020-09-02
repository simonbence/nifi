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

package org.apache.nifi.controller;

import java.util.Set;

/**
 * <p>
 * This interface provides a set of methods for checking NiFi node type.
 * <p>
 */
public interface NodeTypeProvider {

    /**
     * @return true if this instance is clustered, false otherwise.
     * Clustered means that a node is either connected or trying to connect to the cluster.
     */
    boolean isClustered();

    /**
     * @return true if this instance is the primary node in the cluster; false otherwise
     */
    boolean isPrimary();

    /**
     * @return In case of the instance is clustered, returns the collection of the host of the expected members
     * in the cluster, regardless of their state. This includes the current host. In case of non-clustered instance
     * the result will be an empty set.
     */
    Set<String> getClusterMembers();

}
