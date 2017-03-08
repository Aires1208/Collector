package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.TracesDao;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class SpanChunkHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private TracesDao traceDao;

    @InjectMocks
    private SimpleHandler handler = new SpanChunkHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void handleSimple() throws Exception {
        //given
        TSpanChunk spanChunk = mock(TSpanChunk.class);

        //when
        when(logger.isDebugEnabled()).thenReturn(true);
        handler.handleSimple(spanChunk);

        //then
        verify(traceDao).insertSpanChunk(spanChunk);
    }

}