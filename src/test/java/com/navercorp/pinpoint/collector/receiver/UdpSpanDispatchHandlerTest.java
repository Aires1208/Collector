package com.navercorp.pinpoint.collector.receiver;

import com.navercorp.pinpoint.collector.handler.SimpleHandler;
import com.navercorp.pinpoint.collector.handler.SpanHandler;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Created by root on 16-12-28.
 */
public class UdpSpanDispatchHandlerTest {

    @Mock
    private SimpleHandler spanDataHandler;

    @Mock
    private SimpleHandler spanChunkHandler;

    @InjectMocks
    private UdpSpanDispatchHandler handler = new UdpSpanDispatchHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_null_when_input_non_span_object() throws Exception {
        //given
        TBase tBase = mock(TBase.class);

        //when
        SimpleHandler simpleHandler = handler.getSimpleHandler(tBase);

        //then
        assertNull(simpleHandler);
    }

    @Test
    public void should_return_spanDataHandler_when_input_span_object() throws Exception {
        //given
        TBase tBase = mock(TSpan.class);

        //when
        SimpleHandler simpleHandler = handler.getSimpleHandler(tBase);

        //then
        assertThat(simpleHandler, is(spanDataHandler));
    }

    @Test
    public void should_return_spanChunkHandler_when_input_spanChunk_object() throws Exception {
        //given
        TBase tBase = mock(TSpanChunk.class);

        //when
        SimpleHandler simpleHandler = handler.getSimpleHandler(tBase);

        //then
        assertThat(simpleHandler, is(spanChunkHandler));
    }

}