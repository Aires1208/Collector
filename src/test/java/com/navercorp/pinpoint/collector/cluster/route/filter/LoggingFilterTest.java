package com.navercorp.pinpoint.collector.cluster.route.filter;

import com.navercorp.pinpoint.collector.cluster.route.RequestEvent;
import com.navercorp.pinpoint.collector.cluster.route.RouteEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by root on 17-1-11.
 */
public class LoggingFilterTest {

    @Mock
    private Logger logger;

    @InjectMocks
    private LoggingFilter filter = new LoggingFilter();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void doEvent() throws Exception {
        //given
        RequestEvent event = mock(RequestEvent.class);

        filter.doEvent(event);

//        verify(logger).info(anyString(), Matchers.anyObject(), any(RouteEvent.class));
    }

}