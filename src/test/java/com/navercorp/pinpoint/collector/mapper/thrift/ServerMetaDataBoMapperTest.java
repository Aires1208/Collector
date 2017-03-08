package com.navercorp.pinpoint.collector.mapper.thrift;

import com.google.common.collect.Lists;
import com.navercorp.pinpoint.common.bo.ServerMetaDataBo;
import com.navercorp.pinpoint.common.bo.ServiceInfoBo;
import com.navercorp.pinpoint.thrift.dto.TServerMetaData;
import com.navercorp.pinpoint.thrift.dto.TServiceInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-21.
 */
public class ServerMetaDataBoMapperTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void map() throws Exception {
        //given
        TServerMetaData serverMetaData = new TServerMetaData();
        serverMetaData.setServerInfo("tomcat 8.1");
        serverMetaData.setVmArgs(newArrayList("-Xms:200m", "-Xmm:200m"));
        serverMetaData.setServiceInfos(newArrayList(buildServiceInfos()));

        //when
        ServerMetaDataBoMapper mapper = new ServerMetaDataBoMapper();
        ServerMetaDataBo serverMetaDataBo = mapper.map(serverMetaData);

        //then
        assertEquals("tomcat 8.1", serverMetaDataBo.getServerInfo());
        assertEquals(2, serverMetaData.getVmArgsSize());
        assertEquals(1, serverMetaDataBo.getServiceInfos().size());
    }

    private TServiceInfo buildServiceInfos() {
        TServiceInfo serviceInfo = new TServiceInfo();
        serviceInfo.setServiceName("test_service");
        serviceInfo.setServiceLibs(newArrayList("spring-tx_4.1.jar", "jackson-core.jar"));

        return serviceInfo;
    }
}