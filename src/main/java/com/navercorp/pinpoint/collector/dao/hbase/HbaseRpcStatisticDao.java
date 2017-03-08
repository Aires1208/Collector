package com.navercorp.pinpoint.collector.dao.hbase;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.dao.RpcStatisticDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.XRpcMapper;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.common.topo.domain.XRpc;
import com.navercorp.pinpoint.common.topo.domain.XRpcBuilder;
import com.navercorp.pinpoint.common.util.RowKeyUtils;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class HbaseRpcStatisticDao implements RpcStatisticDao {

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private XRpcMapper xRpcMapper;

    @Override
    public void update(TSpan span) {
        Preconditions.checkArgument(span != null, new NullPointerException("argument error."));

        long timeslot = timeSlot.getTimeSlot(span.getStartTime());
        byte[] rowkey = RowKeyUtils.createTimeSlotRowKey(span.getApplicationName(), timeslot);

        XRpc xRpc = hbaseTemplate.get(HBaseTables.RPC_STATISTIC, rowkey, HBaseTables.RPC_STATISTIC_CF_NAME, Bytes.toBytes(span.getRpc()), xRpcMapper);

        XRpc resultXRpc;
        if (null != xRpc) {
            resultXRpc = mergerTSpanToRpc(span, xRpc);
        } else {
            resultXRpc = fetchXRpcByTSpan(span);
        }
        Put put = new Put(rowkey);
        put.addColumn(HBaseTables.RPC_STATISTIC_CF_NAME, Bytes.toBytes(span.getRpc()), resultXRpc.writeValue());

        hbaseTemplate.put(HBaseTables.RPC_STATISTIC, put);

    }

    private XRpc mergerTSpanToRpc(TSpan span, XRpc rpc) {

        return new XRpcBuilder()
                .Duration(rpc.getDuration() + span.getElapsed())
                .Count(rpc.getCount() + 1)
                .MinTime(span.getElapsed() < rpc.getMin_time() ? span.getElapsed() : rpc.getMin_time())
//                        (rpc.getMin_time() == 0 ? span.getElapsed() : rpc.getMin_time()))
                .MaxTime(span.getElapsed() > rpc.getMax_time() ? span.getElapsed() : rpc.getMax_time())
                .SuccessCount(rpc.getSuccessCount() + span.getErr())
                .Rpc(null != rpc.getRpc() ? rpc.getRpc() : span.getRpc())
                .Method(span.getRestControlName())
                .AvgTime((rpc.getDuration() + span.getElapsed()) / (rpc.getCount() + 1))     //lost precision
                .build();
    }

    private XRpc fetchXRpcByTSpan(TSpan span) {

        return new XRpcBuilder()
                .Duration(span.getElapsed())
                .Count(1)
                .MinTime(span.getElapsed())
                .MaxTime(span.getElapsed())
                .SuccessCount(span.getErr())
                .Rpc(span.getRpc())
                .AvgTime(span.getElapsed())
                .Method(span.getRestControlName()).build();
    }


}
