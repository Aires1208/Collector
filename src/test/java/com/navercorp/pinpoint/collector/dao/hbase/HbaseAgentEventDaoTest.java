package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.AgentEventValueMapper;
import com.navercorp.pinpoint.common.bo.AgentEventBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.AgentEventType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;

/**
 * Created by root on 16-12-22.
 */
public class HbaseAgentEventDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private AgentEventValueMapper valueMapper;

    @InjectMocks
    private AgentEventDao agentEventDao = new HbaseAgentEventDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        AgentEventBo agentEventBo = new AgentEventBo(0, "fm-agent", 1L, 2L, AgentEventType.AGENT_PING);

        //when
        agentEventDao.insert(agentEventBo);

        //then
        verify(hbaseTemplate).put(anyString(), any(byte[].class), any(byte[].class), any(byte[].class), any(AgentEventBo.class), any(AgentEventValueMapper.class));
    }
}