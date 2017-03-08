package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.AgentLifeCycleDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.AgentLifeCycleValueMapper;
import com.navercorp.pinpoint.common.bo.AgentLifeCycleBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.AgentLifeCycleState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Created by root on 16-12-23.
 */
public class HbaseAgentLifeCycleDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private AgentLifeCycleValueMapper valueMapper;

    @InjectMocks
    private AgentLifeCycleDao agentLifeCycleDao = new HbaseAgentLifeCycleDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        AgentLifeCycleBo agentLifeCycleBo = new AgentLifeCycleBo(0, "fm-agent", 2222L, 2226L, 101010L,
                AgentLifeCycleState.RUNNING);

        //when
        agentLifeCycleDao.insert(agentLifeCycleBo);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(byte[].class), any(byte[].class), any(byte[].class),
                any(AgentLifeCycleBo.class), any(AgentLifeCycleValueMapper.class));
    }
}