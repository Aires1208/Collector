package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.InstanceTraceIdIndexDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.TraceIdMapper;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.util.*;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Created by root on 16-11-14.
 */
@Repository
public class HbaseInstanceTraceIdIndexDao implements InstanceTraceIdIndexDao {
    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    private TraceIdMapper traceIdMapper;

    @Override
    public void update(TSpan span) {
        Preconditions.checkArgument(span != null, new NullPointerException("argument error."));

        long timeslot = timeSlot.getTimeSlot(span.getStartTime());
        byte[] rowkey = RowKeyUtils.createTimeSlotRowKey(span.getAgentId(), timeslot);

        TransactionId transactionId = SpanUtils.getTransactionIdObj(span);

        Set<TransactionId> traceList = hbaseTemplate.get(HBaseTables.INSTANCE_TRACEID_INDEX, rowkey, HBaseTables.INSTANCE_TRACEID_CF_NAME, Bytes.toBytes(span.getRpc()), traceIdMapper);

        if (traceList == null) {
            traceList = newHashSet(transactionId);
        } else {
            traceList.add(transactionId);
        }

        byte[] traceIdValue = TransactionIdUtils.writeTraceIdIndexValue(traceList);

        Put put = new Put(rowkey);
        put.addColumn(HBaseTables.INSTANCE_TRACEID_CF_NAME, Bytes.toBytes(span.getRpc()), traceIdValue);

        hbaseTemplate.put(HBaseTables.INSTANCE_TRACEID_INDEX, put);
    }

}
