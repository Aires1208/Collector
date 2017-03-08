package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.bo.SpanEventBo;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.util.TransactionId;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class XSpanMapper implements RowMapper<List<SpanBo>> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public List<SpanBo> mapRow(Result result, int rowNum) throws Exception {
        if (result.isEmpty()) {
            return Collections.emptyList();
        }

        byte[] rowKey = result.getRow();
        final TransactionEventKey transactionId = TransactionEventKey.buildTransacionKey(rowKey, TransactionId.DISTRIBUTE_HASH_SIZE);

        final Cell[] rawCells = result.rawCells();
        List<SpanBo> spanList = new ArrayList<>();
        Map<Long, SpanBo> spanMap = new HashMap<>();
        List<SpanEventBo> spanEventBoList = new ArrayList<>();
        for (Cell cell : rawCells) {
            // only if family name is "span"
            if (CellUtil.matchingFamily(cell, HBaseTables.TRACES_CF_SPAN)) {
                getSpan(transactionId, spanList, spanMap, cell);
            } else if (CellUtil.matchingFamily(cell, HBaseTables.TRACES_CF_TERMINALSPAN)) {
                getSpanEventList(transactionId, spanEventBoList, cell);
            }
        }

        for (SpanEventBo spanEventBo : spanEventBoList) {
            SpanBo spanBo = spanMap.get(spanEventBo.getSpanId());
            if (spanBo != null) {
                spanBo.addSpanEvent(spanEventBo);
            }
        }

        return spanList;

    }

    private void getSpan(TransactionEventKey transactionId, List<SpanBo> spanList, Map<Long, SpanBo> spanMap, Cell cell) {
        SpanBo spanBo = new SpanBo();
        spanBo.setTraceAgentId(transactionId.getAgentId());
        spanBo.setTraceAgentStartTime(transactionId.getAgentStartTime());
        spanBo.setTraceTransactionSequence(transactionId.getTransactionSequence());
        spanBo.setCollectorAcceptTime(cell.getTimestamp());

        spanBo.setSpanID(Bytes.toLong(cell.getQualifierArray(), cell.getQualifierOffset()));
        spanBo.readValue(cell.getValueArray(), cell.getValueOffset());
        if (logger.isDebugEnabled()) {
            logger.debug("read span :{}", spanBo);
        }
        spanList.add(spanBo);
        spanMap.put(spanBo.getSpanId(), spanBo);
    }

    private void getSpanEventList(TransactionEventKey transactionId, List<SpanEventBo> spanEventBoList, Cell cell) {
        SpanEventBo spanEventBo = new SpanEventBo();
        spanEventBo.setTraceAgentId(transactionId.getAgentId());
        spanEventBo.setTraceAgentStartTime(transactionId.getAgentStartTime());
        spanEventBo.setTraceTransactionSequence(transactionId.getTransactionSequence());

        int offset = 0;
        // qualifier : spanId(long) + sequence(short) + asyncId(int)
        long spanId = Bytes.toLong(cell.getQualifierArray(), cell.getQualifierOffset());

        // because above spanId type is "long", so offset is 8
        offset += Bytes.SIZEOF_LONG;
        short sequence = Bytes.toShort(cell.getQualifierArray(), cell.getQualifierOffset() + offset);
        int asyncId = -1;
        offset += Bytes.SIZEOF_SHORT;
        if (cell.getQualifierLength() > offset) {
            asyncId = Bytes.toInt(cell.getQualifierArray(), cell.getQualifierOffset() + offset);
        }
        short asyncSequence = -1;
        offset += Bytes.SIZEOF_INT;
        if (cell.getQualifierLength() > offset) {
            asyncSequence = Bytes.toShort(cell.getQualifierArray(), cell.getQualifierOffset() + offset);
        }
        spanEventBo.setSpanId(spanId);
        spanEventBo.setSequence(sequence);
        spanEventBo.setAsyncId(asyncId);
        spanEventBo.setAsyncSequence(asyncSequence);

        spanEventBo.readValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        if (logger.isDebugEnabled()) {
            logger.debug("read spanEvent :{}", spanEventBo);
        }
        spanEventBoList.add(spanEventBo);
    }
}

