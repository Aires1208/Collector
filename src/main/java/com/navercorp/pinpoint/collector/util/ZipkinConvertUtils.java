package com.navercorp.pinpoint.collector.util;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.collector.usercase.ServiceInfo;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.trace.ServiceType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import sun.misc.CRC16;
import zipkin.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Created by root on 16-12-13.
 */
public final class ZipkinConvertUtils {
    private static final String DEFAULT_SERVICE_NAME_SEPARATER = "_";
    private static final String DEFAULT_SERVICE_TYPE = "undefined";
    private static final String DEFAULT_PORT = "0";

    private static final String JDBC_QUERY = "jdbc.query";
    private static final String SQL_QUERY = "sql.query";

    public static ServiceInfo parseServiceName(String serviceName) {
        String[] services = serviceName.split(DEFAULT_SERVICE_NAME_SEPARATER);

        switch (services.length) {
            case 4:
            case 3:
                return new ServiceInfo(services[0], services[1], services[2].toLowerCase());
            case 2:
                return new ServiceInfo(services[0], services[1], DEFAULT_SERVICE_TYPE);
            default:
                return new ServiceInfo(serviceName, serviceName, DEFAULT_SERVICE_TYPE);
        }
    }

    public static String buildAgentId(String serviceName, Endpoint endpoint) {
        return serviceName + DEFAULT_SERVICE_NAME_SEPARATER +
                (endpoint.port != null ? endpoint.port : DEFAULT_PORT);
    }

    public static Integer getCRC16(String str) {
        CRC16 crc16 = new CRC16();
        byte[] bytesStr = Bytes.toBytes(str);

        for (byte b : bytesStr) {
            crc16.update(b);
        }

        return crc16.value;
    }

    public static short findServiceType(String type) {
        switch (type) {
            case "python":
                return ServiceType.PYTHON.getCode();
            case "go":
                return ServiceType.GO.getCode();
            default:
                return ServiceType.UNDEFINED.getCode();
        }
    }

    public static long getSpanStartTimeMs(Span span) {
        return TimeUnit.MICROSECONDS.toMillis(span.timestamp != null ? span.timestamp :
                span.annotations.get(0).timestamp);
    }

    public static Span findNodeRoot(Set<Span> spanSet) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(spanSet), new IllegalArgumentException("spans must not be empty."));

        //only one zipkin span in this node
        if (spanSet.size() == 1) {
            return newArrayList(spanSet).get(0);
        }

        //root span(trace root) found.
        for (Span span : spanSet) {
            if (span.parentId == null) {
                return span;
            }
        }

        //first span(not root span) in this node
        Map<Long, Span> spanMap = newHashMap();
        for (Span span : spanSet) {
            spanMap.put(span.id, span);
        }

        for (Span span : spanSet) {
            Span parent = spanMap.get(span.parentId);
            if (parent == null) {
                return span;
            }
        }

        //otherwise
        return null;
    }

    public static boolean containSqlInfo(List<BinaryAnnotation> binaryAnnotations) {
        if (binaryAnnotations.isEmpty()) {
            return false;
        }

        for (BinaryAnnotation binaryAnnotation : binaryAnnotations) {
            if (binaryAnnotation.key.equals(JDBC_QUERY) || binaryAnnotation.key.equals(SQL_QUERY))
                return true;
        }

        return false;
    }

    public static boolean isClientSpan(List<Annotation> annotations) {
        if (annotations.isEmpty()) {
            return false;
        }

        for (Annotation annotation : annotations) {
            if (annotation.value.equals(Constants.CLIENT_SEND)
                    || annotation.value.equals(Constants.CLIENT_RECV)
                    || annotation.value.equals(Constants.CLIENT_RECV_FRAGMENT)
                    || annotation.value.equals(Constants.CLIENT_SEND_FRAGMENT)) {
                return true;
            }
        }

        return false;
    }

    public static List<String> getSqls(Span span) {
        List<String> sqls = newArrayList();
        for (BinaryAnnotation binaryAnnotation : span.binaryAnnotations) {
            if (binaryAnnotation.key.equals(SQL_QUERY) || binaryAnnotation.key.equals(JDBC_QUERY)) {
                sqls.add(Bytes.toString(binaryAnnotation.value));
            }
        }
        return sqls;
    }

    public static byte[] writeZipkinSpan(Span span) {
        final Buffer buffer = new AutomaticBuffer();
        buffer.put(span.traceId);
        buffer.put2PrefixedString(span.name);
        buffer.put(span.id);
        //should put a boolean flag: hasParentId
        if (null != span.parentId) {
            buffer.put(true);
            buffer.put(span.parentId);
        } else {
            buffer.put(false);
        }

        buffer.put(null == span.timestamp ? -1L : span.timestamp);
        buffer.put(null == span.duration ? -1L : span.duration);
        if (null != span.annotations && !span.annotations.isEmpty()) {
            writeAnnotation(buffer, span.annotations);
        } else {
            buffer.put(false);
        }

        if (null != span.binaryAnnotations && !span.binaryAnnotations.isEmpty()) {
            writeBinaryAnnotations(buffer, span.binaryAnnotations);
        } else {
            buffer.put(false);
        }

        return buffer.getBuffer();
    }

    private static void writeBinaryAnnotations(Buffer buffer, List<BinaryAnnotation> binaryAnnotations) {
        buffer.put(true);
        buffer.put(binaryAnnotations.size());
        for (BinaryAnnotation binaryAnnotation : binaryAnnotations) {
            buffer.put2PrefixedString(binaryAnnotation.key);
            buffer.put2PrefixedBytes(binaryAnnotation.value);
            buffer.put(binaryAnnotation.type.value);
            //should put a boolean flag: hasEndPoint
            if (null != binaryAnnotation.endpoint) {
                buffer.put(true);
                writeEndPoint(buffer, binaryAnnotation.endpoint);
            } else {
                buffer.put(false);
            }
        }
    }

    private static void writeAnnotation(Buffer buffer, List<Annotation> annotations) {
        buffer.put(true);
        buffer.put(annotations.size());
        for (Annotation annotation : annotations) {
            buffer.put(annotation.timestamp);
            buffer.put2PrefixedString(annotation.value);
            //should put a boolean flag: hasEndPoint
            if (null != annotation.endpoint) {
                buffer.put(true);
                writeEndPoint(buffer, annotation.endpoint);
            } else {
                buffer.put(false);
            }
        }
    }

    private static void writeEndPoint(Buffer buffer, Endpoint endpoint) {
        buffer.put2PrefixedString(endpoint.serviceName);
        buffer.put(endpoint.ipv4);
        buffer.put(endpoint.port == null ? (short) -1 : endpoint.port);
        //current version(1.1.3) of zipkin has no ipv6
        buffer.put(false);
    }
}
