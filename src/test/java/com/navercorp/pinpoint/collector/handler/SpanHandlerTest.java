package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.*;
import com.navercorp.pinpoint.collector.eventbus.EventConsumer;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventCache;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Created by root on 16-12-28.
 */
public class SpanHandlerTest {

    @Mock
    private Logger logger;

    @Mock
    private TransactionListDao transactionListDao;

    @Mock
    private TracesDao traceDao;

    @Mock
    private ApplicationTraceIndexDao applicationTraceIndexDao;

    @Mock
    private ServiceTraceIdIndexDao serviceTraceIdIndexDao;

    @Mock
    private InstanceTraceIdIndexDao instanceTraceIdIndexDao;

    @Mock
    private EventConsumer consumer;

    @Mock
    private RpcStatisticDao rpcStatisticDao;

    @Mock
    private TransactionEventCache cache;

    @InjectMocks
    private SimpleHandler handler = new SpanHandler();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void handleSimple() throws Exception {
        //given
        TSpan tSpan = mock(TSpan.class);
        byte[] transactionId = TransactionIdUtils.formatBytes("fm-agent", 1L, 2L);

        //when
        when(tSpan.getParentSpanId()).thenReturn(-1L);
        when(tSpan.getTransactionId()).thenReturn(transactionId);
        when(logger.isDebugEnabled()).thenReturn(true);

        handler.handleSimple(tSpan);

        //then
        verify(traceDao).insert(any(TSpan.class));
        verify(applicationTraceIndexDao).insert(any(TSpan.class));
        verify(rpcStatisticDao).update(any(TSpan.class));
        verify(transactionListDao).insert(any(TSpan.class));
        verify(instanceTraceIdIndexDao).update(any(TSpan.class));
        verify(serviceTraceIdIndexDao).update(any(TSpan.class));
        verify(cache).put(any(TransactionEventKey.class), Matchers.any(TransactionEventValue.class), anyLong(), any(TimeUnit.class));
    }

}