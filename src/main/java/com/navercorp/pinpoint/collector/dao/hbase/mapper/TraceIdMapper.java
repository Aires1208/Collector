package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.buffer.OffsetFixedBuffer;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Created by root on 16-11-12.
 */
@Component
public class TraceIdMapper implements RowMapper<Set<TransactionId>>{
    @Override
    public Set<TransactionId> mapRow(Result result, int i) throws Exception {
        if (result.isEmpty()) {
            return newHashSet();
        }

        final Cell[] cells = result.rawCells();
        Set<TransactionId> transactionIds = newHashSet();

        for (Cell cell : cells) {
            Buffer buffer = new OffsetFixedBuffer(cell.getValueArray(), cell.getValueOffset());
            byte version = buffer.readByte();
            if (version == 0) {
                int length = buffer.readInt();
                transactionIds.addAll(readTraceIdList(buffer, length));
            }
        }

        return transactionIds;
    }

    private Set<TransactionId> readTraceIdList(Buffer buffer, int length) {
        int index = 0;
        Set<TransactionId> transactionIds = newHashSet();
        while (index < length) {
            transactionIds.add(TransactionIdUtils.parseTransactionId(buffer.read2PrefixedBytes()));
            index++;
        }
        return transactionIds;
    }

}
