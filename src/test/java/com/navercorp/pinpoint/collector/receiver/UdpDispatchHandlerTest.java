package com.navercorp.pinpoint.collector.receiver;

import com.navercorp.pinpoint.collector.handler.Handler;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.thrift.dto.TAgentStat;
import com.navercorp.pinpoint.thrift.dto.TAgentStatBatch;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by root on 16-12-28.
 */
public class UdpDispatchHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private AcceptedTimeService acceptedTimeService;

    @Mock
    private Handler agentStatHandler;

    @InjectMocks
    private AbstractDispatchHandler udpDispatchHandler = new UdpDispatchHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_null_when_input_non_TAgentStat_object() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        Handler handler = udpDispatchHandler.getHandler(tBase);

        //then
        assertNull(handler);
    }

    @Test
    public void should_return_agentStatHandler_when_input_TAgentStat_object() throws Exception {
        //given
        TBase tAgentStat = mock(TAgentStat.class);
        TBase tAgentStatBatch = mock(TAgentStatBatch.class);

        //when
        Handler handler1 = udpDispatchHandler.getHandler(tAgentStat);
        Handler handler2 = udpDispatchHandler.getHandler(tAgentStatBatch);

        //then
        assertTrue(handler1.equals(agentStatHandler));
        assertTrue(handler2.equals(agentStatHandler));
    }

    @Test
    public void should_invoke_handle_when_given_TAgentStat_Object() throws Exception {
        //given
        TBase tAgentStat = mock(TAgentStat.class);

        //when
        udpDispatchHandler.dispatchSendMessage(tAgentStat);

        //then
        verify(agentStatHandler).handle(tAgentStat);
    }

}