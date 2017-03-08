package com.navercorp.pinpoint.collector.cluster.zookeeper;

import com.google.common.collect.Maps;
import com.navercorp.pinpoint.collector.cluster.ClusterPointRepository;
import com.navercorp.pinpoint.collector.cluster.PinpointServerClusterPoint;
import com.navercorp.pinpoint.collector.receiver.tcp.AgentHandshakePropertyType;
import com.navercorp.pinpoint.common.util.concurrent.CommonState;
import com.navercorp.pinpoint.common.util.concurrent.CommonStateContext;
import com.navercorp.pinpoint.rpc.common.SocketStateCode;
import com.navercorp.pinpoint.rpc.server.PinpointServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 17-1-14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ZookeeperProfilerClusterManager.class)
//@PrepareForTest({CommonStateContext.class, ZookeeperJobWorker.class})
public class ZookeeperProfilerClusterManagerTest {
    CommonStateContext stateContext = PowerMockito.mock(CommonStateContext.class);
    ZookeeperJobWorker worker = PowerMockito.mock(ZookeeperJobWorker.class);
    ZookeeperClient zookeeperClient = PowerMockito.mock(ZookeeperClient.class);
    ClusterPointRepository repository = PowerMockito.mock(ClusterPointRepository.class);
    ZookeeperProfilerClusterManager profilerClusterManager;

    @Before
    public void setUp() throws Exception {
        PowerMockito.whenNew(CommonStateContext.class).withNoArguments().thenReturn(stateContext);
        PowerMockito.whenNew(ZookeeperJobWorker.class).withAnyArguments().thenReturn(worker);

        profilerClusterManager = new ZookeeperProfilerClusterManager(zookeeperClient, "", repository);
    }

    @Test
    public void should_result_start_stop_complete_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.NEW);
        PowerMockito.when(stateContext.changeStateInitializing()).thenReturn(true);
        PowerMockito.when(stateContext.changeStateDestroying()).thenReturn(true);
        profilerClusterManager.start();
        profilerClusterManager.stop();

        Mockito.verify(worker).start();
        Mockito.verify(worker).stop();
    }

    @Test
    public void should_result_start_complete_stop_failed_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.NEW);
        PowerMockito.when(stateContext.changeStateInitializing()).thenReturn(true);
        profilerClusterManager.start();
        profilerClusterManager.stop();

        Mockito.verify(worker).start();
        Mockito.verify(worker, Mockito.times(0)).stop();
    }

    @Test
    public void should_result_initializing_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.INITIALIZING);
        profilerClusterManager.start();

        Mockito.verify(worker, Mockito.times(0)).start();
    }

    @Test
    public void should_result_started_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.STARTED);
        profilerClusterManager.start();

        Mockito.verify(worker, Mockito.times(0)).start();
    }

    @Test
    public void should_result_stopped_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.STOPPED);

        try {
            profilerClusterManager.start();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), "Already stopped.");
        }

        Mockito.verify(worker, Mockito.times(0)).start();
    }

    @Test
    public void should_result_destroying_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.DESTROYING);

        try {
            profilerClusterManager.start();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), "Already destroying.");
        }

        Mockito.verify(worker, Mockito.times(0)).start();
    }

    @Test
    public void should_result_Invalid_State_when_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.ILLEGAL_STATE);

        try {
            profilerClusterManager.start();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), "Invalid State.");
        }


        Mockito.verify(worker, Mockito.times(0)).start();
    }

    @Test
    public void should_add_addClusterPoint_when_perform_given_pinpointServer() throws Exception {
        PowerMockito.when(stateContext.isStarted()).thenReturn(true);
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        PowerMockito.when(pinpointServer.getChannelProperties()).thenReturn(getProperties());

        profilerClusterManager.eventPerformed(pinpointServer, SocketStateCode.RUN_DUPLEX);

        Mockito.verify(worker).addPinpointServer(Mockito.any(PinpointServer.class));
    }

    @Test
    public void should_add_removeClusterPoint_when_perform_given_pinpointServer() throws Exception {
        PowerMockito.when(stateContext.isStarted()).thenReturn(true);
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        PowerMockito.when(pinpointServer.getChannelProperties()).thenReturn(getProperties());

        profilerClusterManager.eventPerformed(pinpointServer, SocketStateCode.CLOSED_BY_CLIENT);

        Mockito.verify(worker).removePinpointServer(Mockito.any(PinpointServer.class));
    }

    @Test
    public void should_skip_agent_when_perform_given_pinpointServer() throws Exception {
        PowerMockito.when(stateContext.isStarted()).thenReturn(true);
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        PowerMockito.when(pinpointServer.getChannelProperties()).thenReturn(Maps.newHashMap());

        profilerClusterManager.eventPerformed(pinpointServer, SocketStateCode.CONNECTED);

        Mockito.verify(worker, Mockito.times(0)).removePinpointServer(Mockito.any(PinpointServer.class));
        Mockito.verify(worker, Mockito.times(0)).addPinpointServer(Mockito.any(PinpointServer.class));
    }

    @Test
    public void should_return_empty_list_when_get_cluster_data() throws Exception {
        List<String> results = profilerClusterManager.getClusterData();

        assertTrue(results.isEmpty());
    }

    @Test
    public void should_return_expect_list_when_get_cluster_data() throws Exception {
        PowerMockito.when(worker.getClusterData()).thenReturn(Bytes.toBytes("test1\r\ntest2"));

        List<String> results = profilerClusterManager.getClusterData();

        assertEquals(2, results.size());
    }

    @Test
    public void exceptionCaught() throws Exception {
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        profilerClusterManager.exceptionCaught(pinpointServer, SocketStateCode.CLOSED_BY_SERVER, new RuntimeException());
    }

    @Test
    public void initZookeeperClusterData() throws Exception {
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        PowerMockito.when(pinpointServer.getCurrentStateCode()).thenReturn(SocketStateCode.RUN_DUPLEX);
        PinpointServerClusterPoint clusterPoint = PowerMockito.mock(PinpointServerClusterPoint.class);
        PowerMockito.when(clusterPoint.getPinpointServer()).thenReturn(pinpointServer);
        PowerMockito.when(repository.getClusterPointList()).thenReturn(newArrayList(clusterPoint));

        profilerClusterManager.initZookeeperClusterData();

        Mockito.verify(worker).addPinpointServer(Mockito.any(PinpointServer.class));
    }

    private Map<Object, Object> getProperties() {
        Map<Object, Object> properties = newHashMap();

        properties.put(AgentHandshakePropertyType.VERSION.getName(), "1.6.0");
        properties.put(AgentHandshakePropertyType.APPLICATION_NAME.getName(), "test_APP");
        properties.put(AgentHandshakePropertyType.AGENT_ID.getName(), "test-agent");
        properties.put(AgentHandshakePropertyType.START_TIMESTAMP.getName(), 333L);
        return properties;
    }

}