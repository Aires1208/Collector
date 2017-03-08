package com.navercorp.pinpoint.collector.config;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CollectorConfigurationTest {

    private Properties properties = new Properties();

    @InjectMocks
    private CollectorConfiguration collectorConfiguration = new CollectorConfiguration();

    @Before
    public void setUp() throws Exception {
        InputStream propertiesStrean = this.getClass().getClassLoader().getResourceAsStream("pinpoint-collector.properties");
        properties.load(propertiesStrean);

        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void configGetTest() throws Exception {
        //given
        collectorConfiguration.setProperties(properties);
        collectorConfiguration.afterPropertiesSet();


        //then
        assertEquals("0.0.0.0", collectorConfiguration.getTcpListenIp());
        assertEquals(29994, collectorConfiguration.getTcpListenPort());
        assertEquals(8, collectorConfiguration.getTcpWorkerThread());
        assertEquals(1024, collectorConfiguration.getTcpWorkerQueueSize());
        assertEquals(1, collectorConfiguration.getTcpThreadCount());
        assertEquals("0.0.0.0", collectorConfiguration.getUdpStatListenIp());
        assertEquals(9995, collectorConfiguration.getUdpStatListenPort());
        assertEquals("0.0.0.0", collectorConfiguration.getUdpSpanListenIp());
        assertEquals(9996, collectorConfiguration.getUdpSpanListenPort());
        assertEquals(4194304, collectorConfiguration.getUdpStatSocketReceiveBufferSize());
        assertEquals(512, collectorConfiguration.getUdpStatWorkerQueueSize());
        assertEquals(16, collectorConfiguration.getUdpStatWorkerThread());
        assertEquals(4194304, collectorConfiguration.getUdpSpanSocketReceiveBufferSize());
        assertEquals(1024, collectorConfiguration.getUdpSpanWorkerQueueSize());
        assertEquals(32, collectorConfiguration.getUdpSpanWorkerThread());
        assertEquals(1, collectorConfiguration.getUdpthreadNumber());
        assertEquals(-1, collectorConfiguration.getClusterListenPort());
        assertEquals("", collectorConfiguration.getClusterListenIp());
        assertEquals("localhost", collectorConfiguration.getClusterAddress());
        assertEquals(true, collectorConfiguration.isClusterEnable());
        assertEquals(30000, collectorConfiguration.getClusterSessionTimeout());
        assertEquals(1024, collectorConfiguration.getAgentEventWorkerQueueSize());
        assertEquals(8, collectorConfiguration.getAgentEventWorkerThreadSize());
        assertEquals("127.0.0.1:2181", collectorConfiguration.getKafkaZK());
        assertEquals("group1", collectorConfiguration.getKafkaGroupId());
        assertEquals("udpspan", collectorConfiguration.getSpanTopic());
        assertEquals("udpstat", collectorConfiguration.getStatTopic());
        assertEquals("tcpmeta", collectorConfiguration.getTcpTopic());
        assertEquals(Collections.emptyList(), collectorConfiguration.getL4IpList());
    }

    @Test
    public void configSetTest() throws Exception {
        collectorConfiguration.setTcpListenIp("127.0.0.1");
        assertEquals("127.0.0.1", collectorConfiguration.getTcpListenIp());

        collectorConfiguration.setTcpListenPort(65535);
        assertEquals(65535, collectorConfiguration.getTcpListenPort());

        collectorConfiguration.setTcpWorkerThread(16);
        assertEquals(16, collectorConfiguration.getTcpWorkerThread());

        collectorConfiguration.setTcpWorkerQueueSize(2048);
        assertEquals(2048, collectorConfiguration.getTcpWorkerQueueSize());

        collectorConfiguration.setTcpThreadCount(2);
        assertEquals(2, collectorConfiguration.getTcpThreadCount());

        collectorConfiguration.setUdpStatListenIp("127.0.0.1");
        assertEquals("127.0.0.1", collectorConfiguration.getUdpStatListenIp());

        collectorConfiguration.setUdpStatListenPort(-1);
        assertEquals(-1, collectorConfiguration.getUdpStatListenPort());

        collectorConfiguration.setUdpSpanListenIp("127.0.0.1");
        assertEquals("127.0.0.1", collectorConfiguration.getUdpSpanListenIp());

        collectorConfiguration.setUdpSpanListenPort(-1);
        assertEquals(-1, collectorConfiguration.getUdpSpanListenPort());

        collectorConfiguration.setUdpStatSocketReceiveBufferSize(4194304);
        assertEquals(4194304, collectorConfiguration.getUdpStatSocketReceiveBufferSize());

        collectorConfiguration.setUdpStatWorkerQueueSize(512);
        assertEquals(512, collectorConfiguration.getUdpStatWorkerQueueSize());

        collectorConfiguration.setUdpStatWorkerThread(16);
        assertEquals(16, collectorConfiguration.getUdpStatWorkerThread());

        collectorConfiguration.setUdpSpanSocketReceiveBufferSize(4194304);
        assertEquals(4194304, collectorConfiguration.getUdpSpanSocketReceiveBufferSize());

        collectorConfiguration.setUdpSpanWorkerQueueSize(1024);
        assertEquals(1024, collectorConfiguration.getUdpSpanWorkerQueueSize());

        collectorConfiguration.setUdpSpanWorkerThread(32);
        assertEquals(32, collectorConfiguration.getUdpSpanWorkerThread());

        collectorConfiguration.setUdpthreadNumber(2);
        assertEquals(2, collectorConfiguration.getUdpthreadNumber());

        collectorConfiguration.setClusterListenPort(-1);
        assertEquals(-1, collectorConfiguration.getClusterListenPort());

        collectorConfiguration.setClusterListenIp("127.0.0.1");
        assertEquals("127.0.0.1", collectorConfiguration.getClusterListenIp());

        collectorConfiguration.setClusterAddress("localhost");
        assertEquals("localhost", collectorConfiguration.getClusterAddress());

        collectorConfiguration.setClusterEnable(true);
        assertEquals(true, collectorConfiguration.isClusterEnable());

        collectorConfiguration.setClusterSessionTimeout(40000);
        assertEquals(40000, collectorConfiguration.getClusterSessionTimeout());

        collectorConfiguration.setAgentEventWorkerQueueSize(1024);
        assertEquals(1024, collectorConfiguration.getAgentEventWorkerQueueSize());

        collectorConfiguration.setAgentEventWorkerThreadSize(8);
        assertEquals(8, collectorConfiguration.getAgentEventWorkerThreadSize());

        collectorConfiguration.setKafkaZK("127.0.0.1:2181");
        assertEquals("127.0.0.1:2181", collectorConfiguration.getKafkaZK());

        collectorConfiguration.setKafkaGroupId("group2");
        assertEquals("group2", collectorConfiguration.getKafkaGroupId());

        collectorConfiguration.setSpanTopic("udpspan");
        assertEquals("udpspan", collectorConfiguration.getSpanTopic());

        collectorConfiguration.setStatTopic("udpstat");
        assertEquals("udpstat", collectorConfiguration.getStatTopic());

        collectorConfiguration.setTcpTopic("tcpmeta");
        assertEquals("tcpmeta", collectorConfiguration.getTcpTopic());

        collectorConfiguration.setL4IpList(newArrayList("127.0.0.1"));
        assertEquals(newArrayList("127.0.0.1"), collectorConfiguration.getL4IpList());

        collectorConfiguration.toString();
    }
}
