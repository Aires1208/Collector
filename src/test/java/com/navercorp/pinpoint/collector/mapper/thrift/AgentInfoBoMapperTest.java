package com.navercorp.pinpoint.collector.mapper.thrift;

import com.navercorp.pinpoint.common.bo.AgentInfoBo;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-21.
 */
public class AgentInfoBoMapperTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void map() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        TAgentInfo agentInfo = buildTagentInfo(timestamp);
        AgentInfoBo expectAgentInfoBo = buildAgentInfoBo(timestamp);

        //then
        AgentInfoBoMapper mapper = new AgentInfoBoMapper();
        AgentInfoBo agentInfoBo = mapper.map(agentInfo);


        assertEquals(expectAgentInfoBo, agentInfoBo);
    }

    private TAgentInfo buildTagentInfo(long timestamp) {
        return new TAgentInfo("test_localhost", "127.0.0.1", "0", "test_agent", "test_app", (short) 5000, 23234, "1.5.2-snapshot", "1.8.0_111", timestamp);
    }

    private AgentInfoBo buildAgentInfoBo(long timestamp) {
        AgentInfoBo.Builder builder = new AgentInfoBo.Builder();
        builder.setHostName("test_localhost");
        builder.setIp("127.0.0.1");
        builder.setPorts("0");
        builder.setAgentId("test_agent");
        builder.setApplicationName("test_app");
        builder.setServiceTypeCode((short) 5000);
        builder.setPid(23234);
        builder.setAgentVersion("1.5.2-snapshot");
        builder.setVmVersion("1.8.0_111");
        builder.setStartTime(timestamp);
        builder.setMac("12-e4-32-a9");
        builder.setOs("Linux");

        return builder.build();
    }

}