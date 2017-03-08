package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.topo.domain.XRpc;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

/**
 * Created by ${10183966} on 11/23/16.
 */
@Component
public class XRpcMapper implements RowMapper<XRpc> {
    @Override
    public XRpc mapRow(Result result, int i) throws Exception {
        XRpc xRpc = new XRpc();
        final Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            xRpc.readValue(cell.getValueArray(), cell.getValueOffset());
        }
        return xRpc;
    }
}
