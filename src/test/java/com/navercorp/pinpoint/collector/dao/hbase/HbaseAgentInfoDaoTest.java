package com.navercorp.pinpoint.collector.dao.hbase;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.mapper.thrift.ThriftBoMapper;
import com.navercorp.pinpoint.common.bo.AgentInfoBo;
import com.navercorp.pinpoint.common.bo.JvmInfoBo;
import com.navercorp.pinpoint.common.bo.ServerMetaDataBo;
import com.navercorp.pinpoint.common.bo.ServiceInfoBo;
import com.navercorp.pinpoint.common.hbase.HbaseOperations2;
import com.navercorp.pinpoint.thrift.dto.*;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseAgentInfoDaoTest {
    @Mock
    private HbaseOperations2 hbaseTemplate;

    @Mock
    private ThriftBoMapper<AgentInfoBo, TAgentInfo> agentInfoBoMapper;

    @Mock
    private ThriftBoMapper<ServerMetaDataBo, TServerMetaData> serverMetaDataBoMapper;

    @Mock
    private ThriftBoMapper<JvmInfoBo, TJvmInfo> jvmInfoBoMapper;

    @InjectMocks
    private AgentInfoDao agentInfoDao = new HbaseAgentInfoDao();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void insertTest() {
        //given
        long timestamp = System.currentTimeMillis();
        TAgentInfo agentInfo = getAgentInfo(timestamp);
        AgentInfoBo agentInfoBo = buildAgentInfoBo(timestamp);

        //when
        when(agentInfoBoMapper.map(any(TAgentInfo.class))).thenReturn(agentInfoBo);
        when(serverMetaDataBoMapper.map(any(TServerMetaData.class))).thenReturn(buildServerMeta());
        when(jvmInfoBoMapper.map(any(TJvmInfo.class))).thenReturn(buildJvmInfo());

        agentInfoDao.insert(agentInfo);

        //then
        verify(hbaseTemplate).put(anyString(), any(Put.class));
    }


    private TAgentInfo getAgentInfo(long timestamp) {
        TAgentInfo agentInfo = new TAgentInfo();
        agentInfo.setAgentId("jmz_Test1111111111111");
        agentInfo.setStartTimestamp(timestamp);
        agentInfo.setHostname("pinpoint2");
        agentInfo.setIp("10.63.212.88");
        agentInfo.setMac("52:54:00:e0:17:a3");
        agentInfo.setAgentVersion("1.5.2-SNAPSHOT");
        agentInfo.setApplicationName("TESTAPP");
        agentInfo.setEndStatus(0);
        agentInfo.setEndTimestamp(0L);
        agentInfo.setPid(21123);
        agentInfo.setVmVersion("1.7.0_75");
        agentInfo.setPorts("");
        TJvmInfo jvmInfo = new TJvmInfo();
        jvmInfo.setGcType(TJvmGcType.PARALLEL);
        jvmInfo.setVersion((short) 0);
        jvmInfo.setVmVersion("1.7.0_75");
        agentInfo.setJvmInfo(jvmInfo);
        agentInfo.setOs("Linux amd64 3.19.0-66-generic");
        agentInfo.setServerMetaData(buildTServerMeta());

        return agentInfo;
    }

    private AgentInfoBo buildAgentInfoBo(long timestamp) {
        AgentInfoBo.Builder builder = new AgentInfoBo.Builder();
        builder.setAgentId("jmz_Test1111111111111");
        builder.setStartTime(timestamp);
        builder.setHostName("pinpoint2");
        builder.setIp("10.63.212.88");
        builder.setMac("52:54:00:e0:17:a3");
        builder.setAgentVersion("1.5.2-SNAPSHOT");
        builder.setApplicationName("TESTAPP");
        builder.setEndStatus(0);
        builder.setEndTimeStamp(0L);
        builder.setPid(21123);
        builder.setVmVersion("1.7.0_75");
        builder.setPorts("");
        JvmInfoBo jvmInfo = buildJvmInfo();
        builder.setJvmInfo(jvmInfo);
        builder.setOs("Linux amd64 3.19.0-66-generic");
        return builder.build();
    }

    private JvmInfoBo buildJvmInfo() {
        JvmInfoBo jvmInfo = new JvmInfoBo((short) 0);
        jvmInfo.setGcTypeName(TJvmGcType.PARALLEL.name());
        jvmInfo.setJvmVersion("1.7.0_75");
        return jvmInfo;
    }

    private ServerMetaDataBo buildServerMeta() {
        ServerMetaDataBo.Builder builder = new ServerMetaDataBo.Builder();
        builder.vmArgs(newArrayList("-Xmm:200m", "-Xms:200m"));
        List<ServiceInfoBo> serviceInfoBos = newArrayList(buildServerInfos());
        builder.serviceInfos(serviceInfoBos);
        builder.serverInfo("tomcat 8.0");

        return builder.build();
    }

    private TServerMetaData buildTServerMeta() {
        TServerMetaData tServerMetaData = new TServerMetaData();
        tServerMetaData.setVmArgs(newArrayList("-Xmm:200m", "-Xms:200m"));
        tServerMetaData.setServerInfo("tomcat");

        return tServerMetaData;
    }

    private ServiceInfoBo buildServerInfos() {
        ServiceInfoBo.Builder builder = new ServiceInfoBo.Builder();
        builder.serviceLibs(newArrayList("tools.jar", "hbase.jar"));
        builder.serviceName("tomcat");
        return builder.build();
    }
}
