package com.navercorp.pinpoint.collector.cluster.zookeeper;

import com.navercorp.pinpoint.collector.cluster.connection.CollectorClusterConnectionManager;
import com.navercorp.pinpoint.common.util.concurrent.CommonState;
import com.navercorp.pinpoint.common.util.concurrent.CommonStateContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ZookeeperWebClusterManager.class)
public class ZookeeperWebClusterManagerTest {
    CommonStateContext stateContext = PowerMockito.mock(CommonStateContext.class);
    ZookeeperClient zookeeperClient = PowerMockito.mock(DefaultZookeeperClient.class);
    CollectorClusterConnectionManager connectionManager = PowerMockito.mock(CollectorClusterConnectionManager.class);

    ZookeeperWebClusterManager webClusterManager;

    @Before
    public void setUp() throws Exception {
        PowerMockito.whenNew(CommonStateContext.class).withAnyArguments().thenReturn(stateContext);
        PowerMockito.whenNew(CollectorClusterConnectionManager.class).withAnyArguments().thenReturn(connectionManager);

        webClusterManager = new ZookeeperWebClusterManager(zookeeperClient, "/test", "idTest", connectionManager);
    }

    @Test
    public void should_start_complete_and_stop_complete() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.NEW);
        PowerMockito.when(stateContext.changeStateInitializing()).thenReturn(true);
        PowerMockito.when(stateContext.changeStateDestroying()).thenReturn(true);

        webClusterManager.start();
        webClusterManager.stop();

        Mockito.verify(connectionManager).start();
        Mockito.verify(connectionManager).stop();
        Mockito.verify(stateContext).changeStateStopped();
    }

    @Test
    public void should_already_stopped_when_invoke_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.STOPPED);

        try {
            webClusterManager.start();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), "Already stopped.");
        }
        webClusterManager.stop();

        Mockito.verify(connectionManager, Mockito.times(0)).stop();
    }

    @Test
    public void should_already_destroying_when_invoke_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.DESTROYING);

        try {
            webClusterManager.start();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals(e.getMessage(), "Already destroying.");
        }

        Mockito.verify(connectionManager, Mockito.times(0)).start();
    }

    @Test
    public void should_already_started_when_invoke_start() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.STARTED);

        webClusterManager.start();

        Mockito.verify(connectionManager, Mockito.times(0)).start();
    }

    @Test
    public void should_add_to_queue_when_state_is_start_handleAndRegisterWatcher() throws Exception {
        PowerMockito.when(stateContext.isStarted()).thenReturn(true);

        webClusterManager.handleAndRegisterWatcher("/test");

        webClusterManager.handleAndRegisterWatcher("/path");
    }

    @Test
    public void handleAndRegisterWatcher_while_state_is_not_started() throws Exception {
        PowerMockito.when(stateContext.getCurrentState()).thenReturn(CommonState.STOPPED);

        webClusterManager.handleAndRegisterWatcher("/test");
    }
}