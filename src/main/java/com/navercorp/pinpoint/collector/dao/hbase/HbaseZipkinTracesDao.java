package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.dao.hbase.mapper.ZipkinTraceMapper;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import zipkin.Span;

import java.io.IOException;
import java.util.List;

import static com.navercorp.pinpoint.collector.util.ZipkinConvertUtils.writeZipkinSpan;

/**
 * Created by root on 16-12-1.
 */
@Repository
public class HbaseZipkinTracesDao implements ZipkinTraceDao {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private AcceptedTimeService acceptedTimeService;

    @Autowired
    private HbaseOperations2 hbaseTemplate;

    @Autowired
    private ZipkinTraceMapper zipkinTraceMapper;

    @Override
    public void insert(Span span) {
        byte[] rowkey = Bytes.toBytes(span.traceId);
        Put put = new Put(rowkey);

        byte[] qualifier = createQualifier(span.id, span.timestamp);

        byte[] value = writeZipkinSpan(span);

        put.addColumn(HBaseTables.ZIPKIN_TRACES_CF_S, qualifier, value);

        hbaseTemplate.put(HBaseTables.ZIPKIN_TRACES, put);
        logger.info("insert zipkin span: {}", span);
    }

    @Override
    public List<Span>selectSpan(long traceId) throws IOException {
        byte[] rowKey = Bytes.toBytes(traceId);

        Get get = new Get(rowKey);
        get.setMaxVersions(2);

        return hbaseTemplate.get(HBaseTables.ZIPKIN_TRACES, get, zipkinTraceMapper);
    }

    private byte[] createQualifier(long id, Long timestamp) {
        long time = timestamp == null ? acceptedTimeService.getAcceptedTime() * 1000L : timestamp;

        final Buffer buffer = new AutomaticBuffer();
        buffer.put(id);
        buffer.put(time);

        return buffer.getBuffer();
    }

}
