package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.bo.SpanEventBo;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSpanEvent;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-28.
 */
public class XSpanMapperTest {
    @Test
    public void mapRow() throws Exception {
//        given
        final Result result = createResult();

        //then
        XSpanMapper mapper = new XSpanMapper();
        List<SpanBo> spanBos = mapper.mapRow(result, 0);

        //then
        assertEquals(1, spanBos.size());
    }

    private Result createResult() {
        TSpan tSpan = gettSpan();

        TSpanEvent event = gettSpanEvent();

        SpanBo spanBo = new SpanBo(tSpan);
        SpanEventBo spanEventBo = new SpanEventBo(tSpan, event);

        Cell cellSpan = CellUtil.createCell(buildTraceId(), HBaseTables.TRACES_CF_SPAN, Bytes.toBytes(tSpan.getSpanId()), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), spanBo.writeValue());
        Cell cellEvent = CellUtil.createCell(buildTraceId(), HBaseTables.TRACES_CF_TERMINALSPAN, Bytes.toBytes(tSpan.getSpanId()), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), spanEventBo.writeValue());

        return Result.create(newArrayList(cellSpan, cellEvent));
    }

    private TSpan gettSpan() {
        byte[] traceId = TransactionIdUtils.formatBytes("fm-history", 1L, 100L);
        TSpan tSpan = new TSpan();
        tSpan.setAgentId("fm-history");
        tSpan.setTransactionId(traceId);
        tSpan.setElapsed(1000);
        tSpan.setSpanId(2L);
        tSpan.setRpc("/getCurrentTimestamp");
        tSpan.setAgentStartTime(1L);
        tSpan.setApiId(-1);
        tSpan.setApplicationName("fm_history");
        tSpan.setErr(0);
        tSpan.setStartTime(10L);
        tSpan.setServiceType(ServiceType.SPRING.getCode());
        tSpan.setApplicationServiceType(ServiceType.SPRING.getCode());
        tSpan.setParentSpanId(-1L);
        return tSpan;
    }

    private TSpanEvent gettSpanEvent() {
        TSpanEvent event = new TSpanEvent((short) 0, 2, ServiceType.SPRING_ORM_IBATIS.getCode());
        event.setNextSpanId(-1L);
        event.setRpc("/doGet");
        event.setDepth(1);
        event.setApiId(1);
        event.setEndElapsed(20);
        return event;
    }

    private byte[] buildTraceId() {

        TransactionEventKey key = new TransactionEventKey("fm-history", 1L, 100L);

        AbstractRowKeyDistributor rowKeyDistributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(64));
        return rowKeyDistributor.getDistributedKey(key.getBytes());
    }

}