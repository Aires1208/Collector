package com.navercorp.pinpoint.collector.receiver;

import com.navercorp.pinpoint.collector.handler.AgentInfoHandler;
import com.navercorp.pinpoint.collector.handler.RequestResponseHandler;
import com.navercorp.pinpoint.collector.handler.SimpleHandler;
import com.navercorp.pinpoint.collector.handler.SqlMetaDataHandler;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.thrift.dto.*;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class TcpDispatchHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private AcceptedTimeService acceptedTimeService;

    @Mock
    private AgentInfoHandler agentInfoHandler;

    @Mock
    private RequestResponseHandler sqlMetaDataHandler;

    @Mock
    private RequestResponseHandler apiMetaDataHandler;

    @Mock
    private RequestResponseHandler stringMetaDataHandler;

    @InjectMocks
    private AbstractDispatchHandler tcpDispatchHandler = new TcpDispatchHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(logger.isTraceEnabled()).thenReturn(true);
    }

    @Test
    public void should_return_agentInfoHandler_when_input_TAgentInfo_Object() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        SimpleHandler handler = tcpDispatchHandler.getSimpleHandler(tBase);

        //then
        assertTrue(handler instanceof AgentInfoHandler);
    }

    @Test
    public void should_return_sqlMetaDataHandler_when_input_TSqlMetaData_Object() throws Exception {
        //given
        TBase tBaseSql = mock(TSqlMetaData.class);
        TBase tBaseString = mock(TStringMetaData.class);
        TBase tBaseApi = mock(TApiMetaData.class);

        //when
        RequestResponseHandler sqlHandler = tcpDispatchHandler.getRequestResponseHandler(tBaseSql);
        RequestResponseHandler stringHandler = tcpDispatchHandler.getRequestResponseHandler(tBaseString);
        RequestResponseHandler apiHandler = tcpDispatchHandler.getRequestResponseHandler(tBaseApi);

        //then
        assertTrue(sqlHandler instanceof RequestResponseHandler);
        assertTrue(stringHandler instanceof RequestResponseHandler);
        assertTrue(apiHandler instanceof RequestResponseHandler);

    }

    @Test
    public void should_return_agentInfoHandler_when_input_TAgentInfo_Object_for_requestHanlder() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        RequestResponseHandler handler = tcpDispatchHandler.getRequestResponseHandler(tBase);

        //then
        assertTrue(handler instanceof AgentInfoHandler);
    }

    @Test
    public void should_invoke_handleRequest_when_input_TAgentInfo_Object_for_requestHanlder() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        tcpDispatchHandler.dispatchRequestMessage(tBase);

        //then
        verify(agentInfoHandler).handleRequest(any(TBase.class));
    }

    @Test
    public void should_invoke_handleSimple_when_input_TAgentInfo_Object_for_requestHanlder() throws Exception {
        //given
        TBase tBase = mock(TAgentInfo.class);

        //when
        tcpDispatchHandler.dispatchSendMessage(tBase);

        //then
        verify(agentInfoHandler).handleSimple(any(TBase.class));
    }

    @Test
    public void should_throw_UnsupportedOperationException_when_input_non_TAgentInfo_Object() throws Exception {
        //given
        TBase tBase = mock(TAgentStat.class);

        //when
        try {
            tcpDispatchHandler.dispatchSendMessage(tBase);
        } catch (UnsupportedOperationException e) {
        }

        verify(agentInfoHandler, times(0)).handleRequest(any(TBase.class));
        verify(agentInfoHandler, times(0)).handleSimple(any(TBase.class));
    }
}