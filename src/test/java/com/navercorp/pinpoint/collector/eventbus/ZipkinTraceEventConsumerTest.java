package com.navercorp.pinpoint.collector.eventbus;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.manage.controller.ZipkinHttpCollector;
import com.navercorp.pinpoint.collector.service.ZipkinSpanConsumerService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import zipkin.Span;

import java.util.List;

import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by root on 17-1-10.
 */
public class ZipkinTraceEventConsumerTest {
    @Mock
    private ZipkinTraceDao zipkinTraceDao;

    @Mock
    private ZipkinSpanConsumerService zipkinSpanConsumerService;

    @InjectMocks
    private ZipkinTraceEventConsumer consumer = new ZipkinTraceEventConsumer();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void lister() throws Exception {
        //given
        TransactionEventKey key = new TransactionEventKey("fm-active", 1L, 1L);
        TransactionEventValue value = new TransactionEventValue(key, "fm_history", 1L);
        List<Span> spen = mock(List.class);

        //when
        when(zipkinTraceDao.selectSpan(anyLong())).thenReturn(spen);
        consumer.lister(value);

        //then
        verify(zipkinSpanConsumerService).consume(anyList());
    }

}