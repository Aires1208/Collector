package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.bo.AgentLifeCycleBo;
import com.navercorp.pinpoint.common.util.AgentLifeCycleState;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-20.
 */
public class AgentLifeCycleValueMapperTest {
    @Test
    public void mapValue() throws Exception {
        //given
        AgentLifeCycleBo agentLifeCycleBo =
                new AgentLifeCycleBo("fm-active", System.currentTimeMillis(), System.currentTimeMillis(), 111L, AgentLifeCycleState.RUNNING);

        //then
        AgentLifeCycleValueMapper mapper = new AgentLifeCycleValueMapper();
        byte[] agentLifeCycleBytes = mapper.mapValue(agentLifeCycleBo);

        assertEquals(40, agentLifeCycleBytes.length);
    }

}