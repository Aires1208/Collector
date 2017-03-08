package com.navercorp.pinpoint.collector.cluster.route.filter;

import com.navercorp.pinpoint.collector.cluster.route.ResponseEvent;
import com.navercorp.pinpoint.collector.rpc.handler.AgentEventHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;

/**
 * Created by root on 16-12-27.
 */
public class AgentEventHandlingFilterTest {

    @Mock
    private AgentEventHandler agentEventHandler;

    @InjectMocks
    private AgentEventHandlingFilter agentEventHandlingFilter = new AgentEventHandlingFilter();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void doEvent() throws Exception {
        //given
        ResponseEvent event = Mockito.mock(ResponseEvent.class);;

        //when
        agentEventHandlingFilter.doEvent(event);

        //then
        Mockito.verify(agentEventHandler).handleResponseEvent(any(ResponseEvent.class), anyLong());
    }
}