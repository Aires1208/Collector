package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.TracesDao;
import com.navercorp.pinpoint.collector.dao.hbase.filter.SpanEventFilter;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.bo.SpanEventBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.*;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;

/**
 * Created by root on 16-12-23.
 */
public class HbaseTraceDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private AcceptedTimeService acceptedTimeService;

    @Mock
    private SpanEventFilter spanEventFilter;

    @Mock
    private AbstractRowKeyDistributor rowKeyDistributor;

    @InjectMocks
    private TracesDao tracesDao = new HbaseTraceDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert_while_spanEvent_filter_return_true() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        TSpan span = getSpan(timestamp);

        //when
        Mockito.when(acceptedTimeService.getAcceptedTime()).thenReturn(timestamp);
        Mockito.when(rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(span.getTransactionId());
        Mockito.when(spanEventFilter.filter(any(SpanEventBo.class))).thenReturn(true);

        tracesDao.insert(span);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    @Test
    public void insert_while_spanEvent_filter_return_false() {
        long timestamp = System.currentTimeMillis();
        TSpan span = getSpan(timestamp);

        //when
        Mockito.when(acceptedTimeService.getAcceptedTime()).thenReturn(timestamp);
        Mockito.when(rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(span.getTransactionId());
        Mockito.when(spanEventFilter.filter(any(SpanEventBo.class))).thenReturn(false);

        tracesDao.insert(span);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    @Test
    public void shoule_invoke_0_time() throws Exception {
        //given
        long timestmap = System.currentTimeMillis();
        TSpanChunk spanChunk = buildSpanChunk(timestmap);

        //when
        Mockito.when(rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(spanChunk.getTransactionId());
        tracesDao.insertSpanChunk(spanChunk);
        //then
        Mockito.verify(hbaseTemplate, times(0)).put(anyString(), any(Put.class));
    }

    @Test
    public void shoule_invoke_1_time() throws Exception {
        //given
        long timestmap = System.currentTimeMillis();
        TSpanChunk spanChunk = buildSpanChunkwithAnnotation(timestmap);

        //when
        Mockito.when(rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(spanChunk.getTransactionId());
        Mockito.when(spanEventFilter.filter(any(SpanEventBo.class))).thenReturn(true);
        tracesDao.insertSpanChunk(spanChunk);

        //then
        Mockito.verify(hbaseTemplate, times(1)).put(anyString(), any(Put.class));
    }

    private TSpanChunk buildSpanChunkwithAnnotation(long timestmap) {
        TSpanChunk spanChunk = new TSpanChunk();
        spanChunk.setAgentId("fm-agent");
        spanChunk.setAgentStartTime(timestmap);
        spanChunk.setServiceType(ServiceType.SPRING.getCode());
        spanChunk.setTransactionId(TransactionIdUtils.formatBytes("fm-agent", timestmap, 123L));
        spanChunk.setApplicationName("fm_active");
        spanChunk.setSpanId(1234L);

        spanChunk.setSpanEventList(buildSpanEvents());

        return spanChunk;
    }

    private TSpanChunk buildSpanChunk(long timestmap) {
        TSpanChunk spanChunk = new TSpanChunk();
        spanChunk.setAgentId("fm-agent");
        spanChunk.setAgentStartTime(timestmap);
        spanChunk.setServiceType(ServiceType.SPRING.getCode());
        spanChunk.setTransactionId(TransactionIdUtils.formatBytes("fm-agent", timestmap, 123L));
        spanChunk.setApplicationName("fm_active");
        spanChunk.setSpanId(1234L);

        return spanChunk;
    }

    private TSpan getSpan(long timestamp) {
        TSpan tSpan = new TSpan();

        tSpan.setSpanId(111111L);
        tSpan.setStartTime(timestamp + 3000);
        tSpan.setAgentId("fm-agent");
        tSpan.setApplicationName("fm_active");
        tSpan.setRpc("/apm/html/transaction.html");
        tSpan.setElapsed(2000);

        tSpan.setAnnotations(buildAnnotations());

        tSpan.setTransactionId(TransactionIdUtils.formatBytes("fm-agent", timestamp, 234L));

        tSpan.setSpanEventList(buildSpanEvents());

        return tSpan;
    }

    private List<TSpanEvent> buildSpanEvents() {
        TSpanEvent event1 = new TSpanEvent((short) 0, 230, ServiceType.SPRING.getCode());
        event1.setDepth(1);
        event1.setRpc("/getTimestamp");
        event1.setEndElapsed(288);
        event1.setNextSpanId(-1L);

        TSpanEvent event2 = new TSpanEvent((short) 0, 230, ServiceType.SPRING.getCode());
        event2.setDepth(1);
        event2.setRpc("/getTimestamp");
        event2.setEndElapsed(288);
        event2.setNextSpanId(-1L);

        return newArrayList(event1, event2);
    }

    private List<TAnnotation> buildAnnotations() {
        TAnnotation annotation = new TAnnotation(40);
        annotation.setKey(32);
        annotation.setValue(TAnnotationValue.byteValue((byte) 1));

        return newArrayList(annotation);
    }

}