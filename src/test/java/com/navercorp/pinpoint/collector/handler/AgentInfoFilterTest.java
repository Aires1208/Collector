package com.navercorp.pinpoint.collector.handler;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 17-3-1.
 */
public class AgentInfoFilterTest {
    @Test
    public void needStore() throws Exception {
        long startTime = 1488361417330L;
        long acceptTime1 = 1488361417440L;
        AgentInstance agentInstance = new AgentInstance("agent1", startTime);

        AgentInfoFilter agentInfoFilter = new AgentInfoFilter();

        boolean needStoreAgent1 = agentInfoFilter.needPersistent(agentInstance, acceptTime1);
        assertEquals(true, needStoreAgent1);
        agentInfoFilter.persistent(agentInstance, acceptTime1);

        long acceptTime2 = 1488361418440L;
        boolean needStoreAgent2 = agentInfoFilter.needPersistent(agentInstance, acceptTime1);
        assertEquals(false, needStoreAgent2);

        long acceptTime3 = 1488369417440L;
        boolean needStoreAgent3 = agentInfoFilter.needPersistent(agentInstance, acceptTime3);
        assertEquals(true, needStoreAgent3);

    }

}