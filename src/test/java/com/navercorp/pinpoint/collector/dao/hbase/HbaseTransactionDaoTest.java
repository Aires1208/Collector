package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.TransactionsDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XSpanMapper;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import org.apache.hadoop.hbase.client.Get;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class HbaseTransactionDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private XSpanMapper spanMapper;

    @InjectMocks
    private TransactionsDao transactionsDao = new HbaseTransactionDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void selectSpanTest() {
        //given
        TransactionEventKey key = new TransactionEventKey("test-agent", 1467022742471L, 2);

        //when
        when(hbaseTemplate.get(anyString(), any(Get.class), any(XSpanMapper.class))).thenReturn(getSpanBos());
        List<SpanBo> spanboLists = transactionsDao.selectSpans(key);

        //then
        assertThat(spanboLists.size(), is(2));
        System.out.println("end");
    }

    private List<SpanBo> getSpanBos() {
        long timestamp = System.currentTimeMillis();
        SpanBo spanBo1 = new SpanBo("test-agent", timestamp, 111L, timestamp + 1000L, 2400, 222L);
        SpanBo spanBo2 = new SpanBo("test-agent", timestamp, 444L, timestamp + 1200L, 339, 998L);

        return newArrayList(spanBo1, spanBo2);
    }
}
