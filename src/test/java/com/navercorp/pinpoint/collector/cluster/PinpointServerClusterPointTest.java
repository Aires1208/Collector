package com.navercorp.pinpoint.collector.cluster;

import com.navercorp.pinpoint.collector.receiver.tcp.AgentHandshakePropertyType;
import com.navercorp.pinpoint.rpc.DefaultFuture;
import com.navercorp.pinpoint.rpc.Future;
import com.navercorp.pinpoint.rpc.FutureListener;
import com.navercorp.pinpoint.rpc.server.PinpointServer;
import org.junit.Test;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by root on 17-1-12.
 */
public class PinpointServerClusterPointTest {
    @Test
    public void testProperties() throws Exception {
        PinpointServer pinpointServer = mock(PinpointServer.class);
        Map<Object, Object> properties = createProperties1();
        when(pinpointServer.getChannelProperties()).thenReturn(properties);

        PinpointServerClusterPoint clusterPoint = new PinpointServerClusterPoint(pinpointServer);
        clusterPoint.send(new byte[0]);
        verify(pinpointServer).send(any(byte[].class));

        when(pinpointServer.request(any(byte[].class))).thenReturn(new MockFuture());
        Future future = clusterPoint.request(new byte[0]);
        assertEquals(future.isSuccess(), true);

        assertEquals(clusterPoint.gerVersion(), "1.6.0");
        assertEquals(clusterPoint.getAgentId(), "test-agent");
        assertEquals(clusterPoint.getStartTimeStamp(), 111L);
        assertEquals(clusterPoint.getApplicationName(), "testAPP");
        assertEquals(clusterPoint.getPinpointServer(), pinpointServer);
        System.out.println(clusterPoint.toString());
    }

    @Test
    public void testEquals() throws Exception {
        PinpointServer pinpointServer1 = mock(PinpointServer.class);
        Map<Object, Object> properties1 = createProperties1();
        when(pinpointServer1.getChannelProperties()).thenReturn(properties1);
        PinpointServerClusterPoint clusterPoint1 = new PinpointServerClusterPoint(pinpointServer1);

        PinpointServer pinpointServer2 = mock(PinpointServer.class);
        Map<Object, Object> properties2 = createProperties2();
        when(pinpointServer2.getChannelProperties()).thenReturn(properties2);
        PinpointServerClusterPoint clusterPoint2 = new PinpointServerClusterPoint(pinpointServer2);

        assertFalse(clusterPoint1.equals(clusterPoint2));
        assertFalse(clusterPoint1.hashCode() == clusterPoint2.hashCode());
    }

    private Map<Object, Object> createProperties1() {
        Map<Object, Object> properties = newHashMap();

        properties.put(AgentHandshakePropertyType.VERSION.getName(), "1.6.0");
        properties.put(AgentHandshakePropertyType.APPLICATION_NAME.getName(), "testAPP");
        properties.put(AgentHandshakePropertyType.AGENT_ID.getName(), "test-agent");
        properties.put(AgentHandshakePropertyType.START_TIMESTAMP.getName(), 111L);
        return properties;
    }

    private Map<Object, Object> createProperties2() {
        Map<Object, Object> properties = newHashMap();

        properties.put(AgentHandshakePropertyType.VERSION.getName(), "1.5.2");
        properties.put(AgentHandshakePropertyType.APPLICATION_NAME.getName(), "testAPP");
        properties.put(AgentHandshakePropertyType.AGENT_ID.getName(), "test-agent");
        properties.put(AgentHandshakePropertyType.START_TIMESTAMP.getName(), 111L);
        return properties;
    }

    private class MockFuture implements Future {
        @Override
        public Object getResult() {
            return null;
        }

        @Override
        public Throwable getCause() {
            return null;
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public boolean setListener(FutureListener futureListener) {
            return false;
        }

        @Override
        public boolean await(long l) {
            return false;
        }

        @Override
        public boolean await() {
            return false;
        }
    }
}