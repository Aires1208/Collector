package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TResult;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class AgentInfoHandlerTest {

    @Mock
    private AcceptedTimeService timeService;

    @Mock
    private AgentInfoFilter agentInfoFilter;

    @Mock
    private AgentInfoDao agentInfoDao;

    @Mock
    private ApplicationIndexDao applicationIndexDao;

    @InjectMocks
    private AgentInfoHandler handler = new AgentInfoHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shoud_return_null_when_input_a_non_TAgentInfo_object() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        handler.handleSimple(tBase);

        //then
        verify(agentInfoDao, times(0)).insert(any(TAgentInfo.class));
        verify(applicationIndexDao, times(0)).insert(any(TAgentInfo.class));
    }

    @Test
    public void shoud_return_TResult_True_when_input_a_TAgentInfo_object() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        when(agentInfoFilter.needPersistent(any(AgentInstance.class), anyLong())).thenReturn(true);
        TBase result = handler.handleRequest(tBase);

        //then
        verify(agentInfoDao, times(1)).insert(any(TAgentInfo.class));
        verify(applicationIndexDao, times(1)).insert(any(TAgentInfo.class));
        assertTrue(result instanceof TResult);
        assertTrue(((TResult) result).isSuccess());
    }

    @Test
    public void shoud_return_TResult_false_when_input_a_TAgentInfo_object() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        when(agentInfoFilter.needPersistent(any(AgentInstance.class), anyLong())).thenReturn(true);
        doThrow(new RuntimeException("exception")).when(agentInfoDao).insert(any(TAgentInfo.class));
        TBase result = handler.handleRequest(tBase);

        //then
        verify(agentInfoDao, times(1)).insert(any(TAgentInfo.class));
        verify(applicationIndexDao, times(0)).insert(any(TAgentInfo.class));
        assertTrue(result instanceof TResult);
        assertFalse(((TResult) result).isSuccess());
    }
}