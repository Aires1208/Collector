package com.navercorp.pinpoint.collector.usercase;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.*;
import org.apache.hadoop.hbase.util.CollectionUtils;
import zipkin.Annotation;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.Span;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.navercorp.pinpoint.collector.util.ZipkinConvertUtils.*;

public class Converter {
    private static final String ERROR_VALUE = "error";

    private byte[] traceId;
    private Long startTime;
    private List<Span> spans;
    private Map<Long, List<Span>> spanIdMap = newHashMap();

    public Converter(List<Span> spans) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(spans),
                new NullPointerException("Span List must not be empty."));

        this.spans = spans;

        this.startTime = getSpanStartTimeMs(findTraceRoot());
        this.traceId = findTraceId();
        this.spanIdMap = buildSpanIdMap(spans);
    }

    public List<TSqlMetaData> buildSqlInfo() {
        Map<String, Set<Span>> serviceSpansMap = buildServiceSpanMap();

        ConverterSqlHelper sqlHelper = new ConverterSqlHelper(serviceSpansMap, spanIdMap, startTime);
        return sqlHelper.getSqlMetaData();
    }

    public byte[] getTraceId() {
        return traceId;
    }

    public Long getStartTime() {
        return startTime;
    }

    public List<TAgentInfo> buildAgentInfos() {
        Map<String, Set<Span>> serviceSpanMap = buildServiceSpanMap();

        ConverterAgentInfoHelper agentInfoHelper = new ConverterAgentInfoHelper(serviceSpanMap, spanIdMap);

        return agentInfoHelper.getAgentInfos();
    }

    public List<TSpan> convert() {
        List<TSpan> tSpans = newArrayList();

        Map<String, Set<Span>> serviceSpanMap = buildServiceSpanMap();
        for (Map.Entry<String, Set<Span>> entry : serviceSpanMap.entrySet()) {
            TSpan tSpan = new TSpan();
            tSpan.setTransactionId(traceId);
            tSpan.setAgentStartTime(this.startTime);
            tSpan.setApplicationName(entry.getKey());

            Span nodeRoot = findNodeRoot(entry.getValue());
            Preconditions.checkArgument(nodeRoot != null,
                    new IllegalStateException("there must be a root zipkin span in one node."));

            ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(nodeRoot, spanIdMap);
            Endpoint endpoint = endPointHelper.getEndPoint();
            ServiceInfo serviceInfo = parseServiceName(endpoint.serviceName);

            short serviceType = findServiceType(serviceInfo.getServiceType());
            tSpan.setServiceType(serviceType);
            tSpan.setApplicationServiceType(serviceType);

            tSpan.setSpanId(nodeRoot.id);
            // 16-12-7 parentSpanId is incorrect, but it's not related to CallTree and Topology
            tSpan.setParentSpanId(nodeRoot.parentId == null ? -1L : nodeRoot.parentId);

            // 16-12-8 we suppose that root spans or "SR" spans should have timestamp/duration and annotation.endpoint
            long startTimeMs = getSpanStartTimeMs(nodeRoot);
            tSpan.setStartTime(startTimeMs);
            tSpan.setElapsed((int) TimeUnit.MICROSECONDS.toMillis(nodeRoot.duration == null ?
                    getAnnotationElapsedUs(nodeRoot.annotations) : nodeRoot.duration));

            String agentId = buildAgentId(entry.getKey(), endpoint);
            tSpan.setAgentId(agentId);

            tSpan.setRpc(nodeRoot.name);
            tSpan.setApiId(getCRC16(nodeRoot.name));

            int errCode = getErrors(entry.getValue());
            tSpan.setErr(errCode);

            List<TSpanEvent> tSpanEventList = newArrayList();
            buildSpanEventList(tSpanEventList, entry.getValue(), nodeRoot, startTimeMs, (short) 0, 1);
            tSpan.setSpanEventList(tSpanEventList);

            tSpans.add(tSpan);
        }

        return tSpans;
    }

    private long getAnnotationElapsedUs(List<Annotation> annotations) {
        int size = annotations.size();
        return annotations.get(size - 1).timestamp - annotations.get(0).timestamp;
    }

    public List<TApiMetaData> buildApiInfo() {
        List<TApiMetaData> apis = newArrayList();

        Map<String, Set<Span>> serviceSpanMap = buildServiceSpanMap();
        for (Map.Entry<String, Set<Span>> entry : serviceSpanMap.entrySet()) {
            for (Span span : entry.getValue()) {

                ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(span, spanIdMap);
                Endpoint endpoint = endPointHelper.getEndPoint();
                String agentId = buildAgentId(entry.getKey(), endpoint);

                Integer apiId = getCRC16(span.name);

                apis.add(new TApiMetaData(agentId, this.startTime, apiId, span.name));
            }
        }

        return apis;
    }

    private Long getAnnotationStartTimeUs(List<Annotation> annotations) {
        return annotations.get(0).timestamp;
    }

    private Map<Long, List<Span>> buildSpanIdMap(List<Span> zSpans) {
        Map<Long, List<Span>> spanMap = newHashMap();
        for (Span span : zSpans) {
            List<Span> spanList = spanMap.get(span.id);
            if (spanList == null) {
                spanMap.put(span.id, newArrayList(span));
            } else {
                spanList.add(span);
            }
        }
        return spanMap;
    }

    private Map<String, Set<Span>> buildServiceSpanMap() {
        Map<String, Set<Span>> serviceMap = newHashMap();
        for (Span span : spans) {
            ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(span, spanIdMap);
            Endpoint endpoint = endPointHelper.getEndPoint();
            ServiceInfo serviceInfo = parseServiceName(endpoint.serviceName);

            Set<Span> spanSet = serviceMap.get(serviceInfo.getServiceName());
            if (null == spanSet) {
                serviceMap.put(serviceInfo.getServiceName(), newHashSet(span));
            } else {
                spanSet.add(span);
            }
        }
        return serviceMap;
    }

    private byte[] findTraceId() {
        Span rootSpan = findTraceRoot();

        ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(rootSpan, spanIdMap);
        Endpoint endpoint = endPointHelper.getEndPoint();

        ServiceInfo serviceInfo = parseServiceName(endpoint.serviceName);
        String traceAgentId = buildAgentId(serviceInfo.getServiceName(), endpoint);

        return TransactionIdUtils.formatBytes(traceAgentId, this.startTime, rootSpan.traceId);
    }

    private Span findTraceRoot() {
        List<Span> rootSpans = newArrayList();
        for (Span span : spans) {
            if (span.parentId == null) {
                rootSpans.add(span);
            }
        }

        Preconditions.checkNotNull(!rootSpans.isEmpty(), new IllegalStateException("no root span found."));
        Preconditions.checkNotNull(rootSpans.size() < 2, new IllegalStateException("more than 2 root spans were  found."));

        return rootSpans.get(0);
    }

    private void buildSpanEventList(List<TSpanEvent> tSpanEventList,
                                    final Set<Span> spanSet,
                                    Span span,
                                    long spanStartTimeMs,
                                    short sequence,
                                    int depth) {
        //build spanId<-->childSpans map
        Preconditions.checkArgument(!spanSet.isEmpty(), "spanSet(zipkin spans in an certain endpoint) must not be empty.");
        Map<Long, List<Span>> childSpanMap = buildChildSpanMap(spanSet);
        List<Long> nextSpanIds = getNextSpanIds(span, childSpanMap);

        // convert zipkin span to smartsight spanEvent and remove current span for spanSet
        for (Long nextSpanId : nextSpanIds) {
            tSpanEventList.add(buildEvent(span, spanStartTimeMs, sequence, depth, nextSpanId, nextSpanIds.size() > 1));
            sequence++;
        }

        spanSet.remove(span);

        List<Span> childes = childSpanMap.get(span.id);
        if (!CollectionUtils.isEmpty(childes)) {
            int childDepth = depth + 1;
            for (Span child : childes) {
                buildSpanEventList(tSpanEventList, spanSet, child, spanStartTimeMs, sequence, childDepth);
                sequence++;
            }
        }

    }

    private List<Long> getNextSpanIds(Span currentSpan, Map<Long, List<Span>> childSpanMap) {

        if (childSpanMap.get(currentSpan.id) != null) {
            return newArrayList(-1L);
        }

        if (isClientSpan(currentSpan.annotations) && this.spanIdMap.get(currentSpan.id).size() == 2) {
            return newArrayList(currentSpan.id);
        }

        if (!isClientSpan(currentSpan.annotations)) {
            List<Long> childIds = newArrayList();
            for (Span span : this.spans) {
                if (span.parentId != null && span.parentId.equals(currentSpan.id)) {
                    childIds.add(span.id);
                }
            }

            return childIds.isEmpty() ? newArrayList(-1L) : childIds;
        }

        return newArrayList(-1L);
    }

    private TSpanEvent buildEvent(Span span,
                                  long spanStartTimeMs,
                                  short sequence,
                                  int depth,
                                  Long nextSpanId,
                                  boolean isMultiChild) {

        long eventStartTimeUs = getSpanEventStartTime(isMultiChild ? getChild(nextSpanId) : span);
        int startElapsed = (int) (TimeUnit.MICROSECONDS.toMillis(eventStartTimeUs) - spanStartTimeMs);
        TSpanEvent event = new TSpanEvent(sequence, startElapsed, ServiceType.INTERNAL_METHOD.getCode());
        event.setDepth(depth);

        final int elapsed = getSpanEventEndElapsed(isMultiChild ? getChild(nextSpanId) : span);
        event.setEndElapsed(elapsed);

        event.setRpc(span.name);
        Integer apiId = getCRC16(span.name);
        event.setApiId(apiId);

        event.setNextSpanId(nextSpanId);

        if (containSqlInfo(span.binaryAnnotations)) {
            List<String> sqls = getSqls(span);

            event.setAnnotations(buildSqlAnnotaions(sqls));
        }

        return event;
    }

    private List<TAnnotation> buildSqlAnnotaions(List<String> sqls) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(sqls), new NullPointerException("Sql list must not be empty."));

        List<TAnnotation> annotations = newArrayList();
        for (String sql : sqls) {
            int sqlId = getCRC16(sql);
            TAnnotation sqlAnnotation = new TAnnotation();
            sqlAnnotation.setKey(AnnotationKey.SQL_ID.getCode());
            TAnnotationValue value = new TAnnotationValue();
            value.setIntStringStringValue(new TIntStringStringValue(sqlId));

            sqlAnnotation.setValue(value);

            annotations.add(sqlAnnotation);
        }

        return annotations;
    }

    private int getSpanEventEndElapsed(Span span) {
        return (int) TimeUnit.MICROSECONDS.toMillis(span.duration != null ?
                span.duration : getAnnotationElapsedUs(span.annotations));
    }

    private Long getSpanEventStartTime(Span span) {
        return span.annotations.isEmpty() ? span.timestamp : getAnnotationStartTimeUs(span.annotations);
    }

    private Span getChild(long nextSpanId) {
        List<Span> children = this.spanIdMap.get(nextSpanId);
        Preconditions.checkArgument(!children.isEmpty() && children.size() == 1, "child span must be size 1.");
        return children.get(0);
    }

    private Map<Long, List<Span>> buildChildSpanMap(Set<Span> spanSet) {
        Preconditions.checkArgument(spanSet != null && !spanSet.isEmpty(), new NullPointerException("span set must not be empty."));
        Map<Long, List<Span>> childMap = newHashMap();
        for (Span span : spanSet) {
            Optional<Long> parentId = Optional.fromNullable(span.parentId);
            List<Span> childSpan = childMap.get(parentId.or(-1L));
            if (childSpan == null) {
                childMap.put(parentId.or(-1L), newArrayList(span));
            } else {
                childSpan.add(span);
            }
        }
        return childMap;
    }

    private int getErrors(Set<Span> spanSet) {
        int errorCode = 0;
        for (Span span : spanSet) {
            errorCode += span.annotations.isEmpty() ? 0 : getErrorCode(span.annotations);
        }
        return errorCode;
    }

    private int getErrorCode(List<Annotation> annotations) {
        for (Annotation annotation : annotations) {
            if (annotation.value.equals(ERROR_VALUE)) {
                return 1;
            }
        }

        return 0;
    }
}
