package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Created by root on 16-12-22.
 */
public class HbaseApplicationIndexDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @InjectMocks
    private ApplicationIndexDao applicationIndexDao = new HbaseApplicationIndexDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        TAgentInfo agentInfo = getAgentInfo(timestamp);

        //then
        applicationIndexDao.insert(agentInfo);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    private TAgentInfo getAgentInfo(long timestamp) {
        TAgentInfo agentInfo = new TAgentInfo();
        agentInfo.setAgentId("jmz_Test1111111111111");
        agentInfo.setStartTimestamp(timestamp);
        agentInfo.setApplicationName("TESTAPP");
        agentInfo.setEndStatus(0);

        return agentInfo;
    }

}