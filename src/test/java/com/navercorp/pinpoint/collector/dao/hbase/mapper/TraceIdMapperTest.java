package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


/**
 * Created by root on 16-11-14.
 */
public class TraceIdMapperTest {
    @Test
    public void mapRow() throws Exception {
        //given
        TransactionId id1 = new TransactionId("fm-active", 12345L, 1L);
        TransactionId id2 = new TransactionId("fm-active", 12345L, 2L);
        TransactionId id3 = new TransactionId("fm-active", 12345L, 3L);
        Set<TransactionId> expect_list = newHashSet(id1, id2, id3);

        final Result result = Result.create(newArrayList(createCell(newArrayList(id1, id2, id3))));

        //when
        TraceIdMapper mapper = new TraceIdMapper();
        Set<TransactionId> transactionIds = mapper.mapRow(result, 0);

        //then
        assertThat(transactionIds, is(expect_list));

    }

    private Cell createCell(ArrayList<TransactionId> transactionIds) {
        final Buffer buffer = new AutomaticBuffer();
        byte version = 0;
        buffer.put(version);
        buffer.put(transactionIds.size());
        for (TransactionId transactionId : transactionIds) {
            buffer.put2PrefixedBytes(TransactionIdUtils.formatBytes(transactionId));
        }
        return CellUtil.createCell(Bytes.toBytes("fm_active"), Bytes.toBytes("T"), Bytes.toBytes("/gettime"), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), buffer.getBuffer());
    }

}