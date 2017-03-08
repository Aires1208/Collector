package com.navercorp.pinpoint.collector.handler;

/**
 * Created by root on 17-3-1.
 */
public class AgentInstance {
    private String agentId;
    private long agentStartTime;

    public AgentInstance(String agentId, long agentStartTime) {
        this.agentId = agentId;
        this.agentStartTime = agentStartTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AgentInstance that = (AgentInstance) o;

        if (agentStartTime != that.agentStartTime) return false;
        return agentId.equals(that.agentId);
    }

    @Override
    public int hashCode() {
        int result = agentId.hashCode();
        result = 31 * result + (int) (agentStartTime ^ (agentStartTime >>> 32));
        return result;
    }
}
