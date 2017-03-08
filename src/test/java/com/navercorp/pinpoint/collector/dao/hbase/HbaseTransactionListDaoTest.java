package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.TransactionListDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class HbaseTransactionListDaoTest {
    @Mock
    private HbaseOperations2 hbaseTemplate;

    @InjectMocks
    private TransactionListDao transactionListDao = new HbaseTransactionListDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testInsert() {
        //given
        long timestamp = System.currentTimeMillis();
        TSpan span = getSpan(timestamp);

        //when
        transactionListDao.insert(span);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
        System.out.println("end");

    }

    private TSpan getSpan(long timestamp) {
        TSpan tSpan = new TSpan();

        tSpan.setStartTime(timestamp);
        tSpan.setAgentId("fm-agent");
        tSpan.setApplicationName("fm_active");
        tSpan.setRpc("/apm/html/transaction.html");

        return tSpan;
    }
}
