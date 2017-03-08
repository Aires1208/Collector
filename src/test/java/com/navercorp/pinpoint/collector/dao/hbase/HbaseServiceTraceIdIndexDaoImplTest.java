package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.ServiceTraceIdIndexDao;
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
import org.springframework.data.hadoop.hbase.RowMapper;

import static com.google.common.collect.Sets.newHashSet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-11-14.
 */
public class HbaseServiceTraceIdIndexDaoImplTest {
    @Mock
    private HbaseOperations2 hbaseOperations2;

    @Mock
    private TimeSlot timeSlot;

    @Mock
    private TraceIdMapper traceIdMapper;

    @InjectMocks
    private ServiceTraceIdIndexDao serviceTraceIdIndexDao = new HbaseServiceTraceIdIndexDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }


    private final long timestamp = System.currentTimeMillis();
    private TransactionId id1 = new TransactionId("fm-active", timestamp, 1L);
    private TransactionId id2 = new TransactionId("fm-active", timestamp, 2L);

    @Test
    public void show_update_existed_column_when_input_given_span() throws Exception {
        //given

        TSpan span = new TSpan();
        span.setAgentId("fm-agent");
        span.setApplicationName("fm_active");
        span.setStartTime(timestamp);
        span.setRpc("/getAlarm");
        span.setTransactionId(TransactionIdUtils.formatBytes(id1));

        //when
        when(this.hbaseOperations2.get(anyString(), any(byte[].class), any(byte[].class), any(byte[].class), any(RowMapper.class))).thenReturn(newHashSet(id1, id2));

        //then
        serviceTraceIdIndexDao.update(span);
        verify(hbaseOperations2).put(anyString(), any(Put.class));
    }

}