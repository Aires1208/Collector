package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.ServiceTraceIdIndexDao;
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

/**
 * Created by root on 16-11-12.
 */
@Repository
public class HbaseServiceTraceIdIndexDao implements ServiceTraceIdIndexDao {

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    private TraceIdMapper traceIdMapper;

    @Override
    public void update(TSpan span) {
        Preconditions.checkArgument(span != null,
                new NullPointerException("HbaseServiceTraceIdIndexDao.update() argument error."));

        long traceTimeSlot = this.timeSlot.getTimeSlot(span.getStartTime());
        byte[] rowkey = RowKeyUtils.createTimeSlotRowKey(span.getApplicationName(), traceTimeSlot);

        TransactionId transactionId = SpanUtils.getTransactionIdObj(span);

        Set<TransactionId> traceList = hbaseTemplate.get(HBaseTables.SERVICE_TRACEID_INDEX, rowkey, HBaseTables.SERVICE_TRACEID_CF_NAME, Bytes.toBytes(span.getRpc()), traceIdMapper);

        traceList.add(transactionId);

        byte[] traceIdValue = TransactionIdUtils.writeTraceIdIndexValue(traceList);

        Put put = new Put(rowkey);
        put.addColumn(HBaseTables.SERVICE_TRACEID_CF_NAME, Bytes.toBytes(span.getRpc()), traceIdValue);

        hbaseTemplate.put(HBaseTables.SERVICE_TRACEID_INDEX, put);
    }

}
