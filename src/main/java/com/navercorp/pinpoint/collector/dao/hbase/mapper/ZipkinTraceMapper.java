package com.navercorp.pinpoint.collector.dao.hbase.mapper;

import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.buffer.OffsetFixedBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import org.springframework.data.hadoop.hbase.RowMapper;

import org.springframework.stereotype.Component;

import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;
import zipkin.Span;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Created by root on 16-12-8.
 */
@Component
public class ZipkinTraceMapper implements RowMapper<List<Span>> {

    @Override
    public List<Span> mapRow(Result result, int i) throws Exception {
        final Cell[] rawCells = result.rawCells();
        List<Span> spans = newArrayList();
        for (Cell cell : rawCells) {
            Span.Builder builder = Span.builder();
            final Buffer buffer = new OffsetFixedBuffer(cell.getValueArray(), cell.getValueOffset());
            builder.traceId(buffer.readLong());
            builder.name(buffer.read2PrefixedString());
            builder.id(buffer.readLong());
            if (buffer.readBoolean()) {
                builder.parentId(buffer.readLong());
            }

            long timestamp = buffer.readLong();
            if (timestamp != -1L) {
                builder.timestamp(timestamp);
            }

            long duration = buffer.readLong();
            if (duration != -1L) {
                builder.duration(duration);
            }

            if (buffer.readBoolean()) {
                List<Annotation> annotations = readAnnotations(buffer);
                builder.annotations(annotations);
            }

            if (buffer.readBoolean()) {
                List<BinaryAnnotation> binaryAnnotations = readBinaryAnnotations(buffer);
                builder.binaryAnnotations(binaryAnnotations);
            }

            spans.add(builder.build());
        }
        return spans;
    }

    private List<Annotation> readAnnotations(Buffer buffer) {
        final int size = buffer.readInt();
        List<Annotation> annotations = newArrayList();
        for (int i = 0; i < size; i++) {
            Annotation.Builder builder = Annotation.builder();
            builder.timestamp(buffer.readLong());
            builder.value(buffer.read2PrefixedString());
            if (buffer.readBoolean()) {
                Endpoint endpoint = readEndpoint(buffer);
                builder.endpoint(endpoint);
            }

            annotations.add(builder.build());
        }
        return annotations;
    }

    private List<BinaryAnnotation> readBinaryAnnotations(Buffer buffer) {
        final int size = buffer.readInt();
        List<BinaryAnnotation> binaryAnnotations = newArrayList();
        for (int i = 0; i < size; i++) {
            BinaryAnnotation.Builder builder = BinaryAnnotation.builder();
            builder.key(buffer.read2PrefixedString());
            builder.value(buffer.read2PrefixedString());
            int typeCode = buffer.readInt();
            builder.type(BinaryAnnotation.Type.fromValue(typeCode));
            if (buffer.readBoolean()) {
                Endpoint endpoint = readEndpoint(buffer);
                builder.endpoint(endpoint);
            }
            binaryAnnotations.add(builder.build());
        }
        return binaryAnnotations;
    }

    private Endpoint readEndpoint(Buffer buffer) {
        String serviceName = buffer.read2PrefixedString();
        int ipv4 = buffer.readInt();
        short port = buffer.readShort();
        // todo: 16-12-8 for future use
        if (buffer.readBoolean()) {
        }

        return port == (short) -1 ? Endpoint.create(serviceName, ipv4) :
                Endpoint.create(serviceName, ipv4, port);
    }

}
