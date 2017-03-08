package com.navercorp.pinpoint.collector.usercase;

import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import zipkin.Endpoint;
import zipkin.Span;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.navercorp.pinpoint.collector.util.ZipkinConvertUtils.*;

/**
 * Created by root on 17-2-3.
 */
public class ConverterSqlHelper {
    private Map<String, Set<Span>> serviceSpanMap = newHashMap();
    private Map<Long, List<Span>> spanIdMap = newHashMap();
    private Long startTime;

    public ConverterSqlHelper(Map<String, Set<Span>> serviceSpanMap, Map<Long, List<Span>> spanIdMap, Long startTime) {
        this.serviceSpanMap = serviceSpanMap;
        this.spanIdMap = spanIdMap;
        this.startTime = startTime;
    }

    public List<TSqlMetaData> getSqlMetaData() {
        List<TSqlMetaData> tSqlMetaDatas = newArrayList();

        for (Map.Entry<String, Set<Span>> entry : serviceSpanMap.entrySet()) {
            for (Span span : entry.getValue()) {
                if (!containSqlInfo(span.binaryAnnotations)) {
                    continue;
                }

                ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(span, spanIdMap);
                Endpoint endpoint = endPointHelper.getEndPoint();
                String agentId = buildAgentId(entry.getKey(), endpoint);

                List<String> sqls = getSqls(span);
                for (String sql : sqls) {
                    tSqlMetaDatas.add(new TSqlMetaData(agentId, this.startTime, getCRC16(sql), sql));
                }
            }
        }

        return tSqlMetaDatas;
    }
}
