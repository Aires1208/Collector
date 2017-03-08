package com.navercorp.pinpoint.collector.handler;

import com.google.common.base.Preconditions;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Created by root on 17-3-1.
 */
@Component
public class AgentInfoFilter {
    private static final long DEFAULT_INTERVAL_FOR_AGENTINFO = 10 * 60 * 1000L;

    private Map<AgentInstance, Long> agentMap = newHashMap();

    public boolean needPersistent(AgentInstance agentInstance, long acceptTime) {
        Preconditions.checkNotNull(agentInstance != null, "agentInstance must not be empty.");

        if (agentMap.get(agentInstance) == null) {
            return true;
        }

        long persistentElapsed = acceptTime - agentMap.get(agentInstance);

        return persistentElapsed > DEFAULT_INTERVAL_FOR_AGENTINFO;
    }

    public void persistent(AgentInstance agentInstance, long timestamp) {
        agentMap.put(agentInstance, timestamp);
    }
}
