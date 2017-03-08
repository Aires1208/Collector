package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.topo.domain.XNode;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
@Component
public class XNodeMapper implements RowMapper<List<XNode>> {

    @Override
    public List<XNode> mapRow(Result result, int rowNum) throws Exception {
        if (result.isEmpty()) {
            return newArrayList();
        }

        final Cell[] cells = result.rawCells();
        List<XNode> nodeList = newArrayList();

        for (Cell cell : cells) {
            XNode xNode = new XNode(Bytes.toString(cell.getQualifier()));
            xNode.readValue(cell.getValueArray(), cell.getValueOffset());

            nodeList.add(xNode);
        }

        return nodeList;
    }
}
