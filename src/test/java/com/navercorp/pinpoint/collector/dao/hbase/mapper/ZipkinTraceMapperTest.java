package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.collector.manage.controller.ConverterTest;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import zipkin.Span;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.navercorp.pinpoint.collector.util.ZipkinConvertUtils.writeZipkinSpan;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-19.
 */
public class ZipkinTraceMapperTest {
    @Test
    public void mapRow() throws Exception {
        //given
        final Result result = Result.create(createRows());
        List<Span> originSpans = ConverterTest.ZipkinSpanBuilder.read("frontend-middleend-backendX2.json");

        //when
        ZipkinTraceMapper mapper = new ZipkinTraceMapper();
        List<Span> spans = mapper.mapRow(result, 0);

        //then
        assertEquals(originSpans.size(), spans.size());
        for (int i = 0; i < originSpans.size(); i++) {
            assertEquals(originSpans.get(i), spans.get(i));
        }

    }

    private List<Cell> createRows() {
        List<Span> zipkinSpans = ConverterTest.ZipkinSpanBuilder.read("frontend-middleend-backendX2.json");
        List<Cell> cells = newArrayList();
        for (Span zipkinSpan : zipkinSpans) {
            byte[] values = writeZipkinSpan(zipkinSpan);
            Cell cell = CellUtil.createCell(Bytes.toBytes(zipkinSpan.traceId), HBaseTables.ZIPKIN_TRACES_CF_S, createQualifier(zipkinSpan.id, zipkinSpan.timestamp), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), values);
            cells.add(cell);
        }

        return cells;
    }

    private byte[] createQualifier(long id, Long timestamp) {
        long time = timestamp == null ? System.nanoTime() / 1000L : timestamp;

        final Buffer buffer = new AutomaticBuffer();
        buffer.put(id);
        buffer.put(time);

        return buffer.getBuffer();
    }

}