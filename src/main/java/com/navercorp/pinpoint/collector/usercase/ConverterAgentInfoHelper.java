package com.navercorp.pinpoint.collector.usercase;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import zipkin.Endpoint;
import zipkin.Span;

import java.net.Inet4Address;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.navercorp.pinpoint.collector.util.ZipkinConvertUtils.*;

/**
 * Created by root on 17-2-3.
 */
public class ConverterAgentInfoHelper {
    private Map<String, Set<Span>> serviceSpanMap = newHashMap();
    private Map<Long, List<Span>> spanIdMap = newHashMap();

    public ConverterAgentInfoHelper(Map<String, Set<Span>> serviceSpanMap, Map<Long, List<Span>> spanIdMap) {
        this.serviceSpanMap = serviceSpanMap;
        this.spanIdMap = spanIdMap;
    }

    public List<TAgentInfo> getAgentInfos() {
        List<TAgentInfo> agentInfos = newArrayList();

        for (Map.Entry<String, Set<Span>> entry : serviceSpanMap.entrySet()) {
            TAgentInfo agentInfo = new TAgentInfo();
            agentInfo.setApplicationName(entry.getKey());

            Span nodeRoot = findNodeRoot(entry.getValue());
            Preconditions.checkArgument(nodeRoot != null,
                    new IllegalStateException("root span of this node :" + entry.getKey() + " not found"));

            ConverterEndPointHelper endPointHelper = new ConverterEndPointHelper(nodeRoot, spanIdMap);
            Endpoint endpoint = endPointHelper.getEndPoint();
            int ipv4 = endpoint.ipv4;
            Inet4Address ipAddr = InetAddresses.fromInteger(ipv4);
            String ip = ipAddr.getHostAddress();
            agentInfo.setHostname(ip);
            agentInfo.setIp(ip);

            final String agentId = buildAgentId(entry.getKey(), endpoint);
            agentInfo.setAgentId(agentId);
            long agentStartTime = getSpanStartTimeMs(nodeRoot);
            agentInfo.setStartTimestamp(agentStartTime);

            ServiceInfo serviceInfo = parseServiceName(endpoint.serviceName);
            short serviceType = findServiceType(serviceInfo.getServiceType());
            agentInfo.setServiceType(serviceType);

            agentInfo.setAgentVersion("1.1.5-SNAPSHOT");

            agentInfos.add(agentInfo);
        }

        return agentInfos;
    }
}
