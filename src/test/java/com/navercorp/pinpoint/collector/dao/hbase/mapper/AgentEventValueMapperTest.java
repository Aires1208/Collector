package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.bo.AgentEventBo;
import com.navercorp.pinpoint.common.util.AgentEventType;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-20.
 */
public class AgentEventValueMapperTest {
    @InjectMocks
    private AgentEventValueMapper mapper = new AgentEventValueMapper();

    @Test
    public void mapValue() throws Exception {
        //given
        AgentEventBo agentEventBo = new AgentEventBo("fm-active", System.currentTimeMillis(), System.currentTimeMillis(), AgentEventType.AGENT_PING);

        //then
        byte[] agentEventValues = mapper.mapValue(agentEventBo);

        //then
        assertEquals(31, agentEventValues.length);
    }

}