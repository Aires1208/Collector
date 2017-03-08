package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.collect.ImmutableSet;
import com.navercorp.pinpoint.collector.dao.InstanceTraceIdIndexDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.TraceIdMapper;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-22.
 */
public class HbaseInstanceTraceIdIndexDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private TimeSlot timeSlot;

    @Mock
    private TraceIdMapper traceIdMapper;

    @InjectMocks
    private InstanceTraceIdIndexDao instanceTraceIdIndexDao = new HbaseInstanceTraceIdIndexDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void update() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        TSpan span = getSpan(timestamp);
        TransactionId transactionId = new TransactionId("fm-agent", timestamp, 222L);

        //when
        when(timeSlot.getTimeSlot(anyLong())).thenReturn(timestamp);
        when(hbaseTemplate.get(anyString(), any(byte[].class), any(TraceIdMapper.class))).thenReturn(ImmutableSet.of(transactionId));

        //then
        instanceTraceIdIndexDao.update(span);

        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    private TSpan getSpan(long timestamp) {
        TSpan tSpan = new TSpan();

        tSpan.setStartTime(timestamp);
        tSpan.setAgentId("fm-agent");
        tSpan.setApplicationName("fm_active");
        tSpan.setRpc("/apm/html/transaction.html");

        tSpan.setTransactionId(TransactionIdUtils.formatBytes("fm-agent", timestamp, 234L));

        return tSpan;
    }

}