package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-22.
 */
public class HbaseApplicationTraceIndexDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private AbstractRowKeyDistributor rowKeyDistributor;

    @InjectMocks
    private ApplicationTraceIndexDao applicationTraceIndexDao = new HbaseApplicationTraceIndexDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        TSpan span = getSpan(timestamp);

        //when
        when(rowKeyDistributor.getDistributedKey(any(byte[].class))).thenReturn(span.getTransactionId());

        applicationTraceIndexDao.insert(span);


        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));

    }

    private TSpan getSpan(long timestamp) {
        TSpan tSpan = new TSpan();

        tSpan.setStartTime(timestamp);
        tSpan.setAgentId("fm-agent");
        tSpan.setApplicationName("fm_active");
        tSpan.setElapsed(1000);
        tSpan.setErr(1);

        byte[] traceId = TransactionIdUtils.formatBytes("fm-agent", timestamp, 1234L);
        tSpan.setTransactionId(traceId);

        return tSpan;
    }
}