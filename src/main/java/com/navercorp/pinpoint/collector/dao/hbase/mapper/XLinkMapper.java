package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.buffer.OffsetFixedBuffer;
import com.navercorp.pinpoint.common.topo.domain.XLink;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

@Component
public class XLinkMapper implements RowMapper<List<XLink>> {
    @Override
    public List<XLink> mapRow(Result result, int rowNum) throws Exception {
        if (result.isEmpty()) {
            return newArrayList();
        }

        final Cell[] cells = result.rawCells();
        List<XLink> xlinkList = newArrayList();
        for (Cell cell : cells) {
            Buffer buffer = new OffsetFixedBuffer(cell.getQualifierArray(), cell.getQualifierOffset());
            String from = buffer.readPrefixedString();
            String to = buffer.readPrefixedString();
            XLink xLink = new XLink(from, to);
            xLink.readValue(cell.getValueArray(), cell.getValueOffset());

            xlinkList.add(xLink);
        }

        return xlinkList;
    }
}
