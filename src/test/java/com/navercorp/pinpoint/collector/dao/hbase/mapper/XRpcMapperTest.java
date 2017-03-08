package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.topo.domain.XRpc;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-28.
 */
public class XRpcMapperTest {
    @Test
    public void mapRow() throws Exception {
        //given
        final Result result = getResult();
        //when
        XRpcMapper mapper = new XRpcMapper();
        XRpc rpc = mapper.mapRow(result, 0);

        //then
        assertEquals(2, rpc.getCount());
        assertEquals("GET", rpc.getMethod());
    }

    private Result getResult() {
        XRpc rpc = new XRpc("GET", 2, 2, 200, 300, 250, "/getCurrentTimestamp");
        Cell cell = CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, HBaseTables.RPC_STATISTIC_CF_NAME, Bytes.toBytes(rpc.getRpc()), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), rpc.writeValue());
        return Result.create(newArrayList(cell));
    }

}