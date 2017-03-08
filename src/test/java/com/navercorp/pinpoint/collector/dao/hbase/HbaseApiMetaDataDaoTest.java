package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.common.bo.ApiMetaDataBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
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
public class HbaseApiMetaDataDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    @InjectMocks
    private ApiMetaDataDao apiMetaDataDao = new HbaseApiMetaDataDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        TApiMetaData apiMetaData = getTApiMetaData();
        ApiMetaDataBo apiMetaDataBo = new ApiMetaDataBo(apiMetaData.getAgentId(), apiMetaData.getAgentStartTime(), apiMetaData.getApiId());

        //when
        when(rowKeyDistributorByHashPrefix.getDistributedKey(any(byte[].class))).thenReturn(apiMetaDataBo.toRowKey());
        apiMetaDataDao.insert(apiMetaData);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

    private TApiMetaData getTApiMetaData() {
        TApiMetaData apiMetaData = new TApiMetaData("fm-agent", System.currentTimeMillis(), -1, "");
        apiMetaData.setLine(33);
        return apiMetaData;
    }
}