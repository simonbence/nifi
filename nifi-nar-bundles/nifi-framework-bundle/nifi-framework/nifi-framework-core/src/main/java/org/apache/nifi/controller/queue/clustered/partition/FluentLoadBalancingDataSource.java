package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.queue.QueueSize;

import java.util.Map;

// TODO rebalancing should start this as well, as well as stop
public interface FluentLoadBalancingDataSource {

    // Where string is the node identifier
    Map<String, QueueSize> getSnapshot();

    void recordSnapshot();
}
