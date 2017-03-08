package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 16-12-23.
 */
public class HbaseStringMetaDataDaoTest {

    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;


    @InjectMocks
    private StringMetaDataDao stringMetaDataDao = new HbaseStringMetaDataDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insert() throws Exception {
        //given
        TStringMetaData metaData = new TStringMetaData("fm-agent", 3456L, -2, "NullPointerException");
        byte[] rowkey = Bytes.toBytes("fm");

        //when
        when(this.rowKeyDistributorByHashPrefix.getDistributedKey(any(byte[].class))).thenReturn(rowkey);
        stringMetaDataDao.insert(metaData);

        //then
        Mockito.verify(hbaseTemplate).put(anyString(), any(Put.class));
    }

}