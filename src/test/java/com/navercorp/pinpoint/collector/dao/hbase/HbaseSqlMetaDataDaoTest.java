package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.SqlMetaDataDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-22.
 */
public class HbaseSqlMetaDataDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    @InjectMocks
    private SqlMetaDataDao sqlMetaDataDao = new HbaseSqlMetaDataDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        TSqlMetaData sqlMetaData = new TSqlMetaData("fm-agent", System.currentTimeMillis(), -1, "SELECT * FROM HISTORY");

        //when
        when(rowKeyDistributorByHashPrefix.getDistributedKey(any(byte[].class))).thenReturn(Bytes.toBytes("fm-agent"));

        sqlMetaDataDao.insert(sqlMetaData);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

}