package com.navercorp.pinpoint.collector.cluster.route;

import com.navercorp.pinpoint.collector.cluster.ClusterPointLocator;
import com.navercorp.pinpoint.collector.cluster.PinpointServerClusterPoint;
import com.navercorp.pinpoint.collector.cluster.TargetClusterPoint;
import com.navercorp.pinpoint.collector.cluster.route.filter.RouteFilter;
import com.navercorp.pinpoint.collector.receiver.tcp.AgentHandshakePropertyType;
import com.navercorp.pinpoint.rpc.Future;
import com.navercorp.pinpoint.rpc.packet.stream.StreamCreateFailPacket;
import com.navercorp.pinpoint.rpc.server.PinpointServer;
import com.navercorp.pinpoint.rpc.stream.*;
import com.navercorp.pinpoint.thrift.dto.TResult;
import com.navercorp.pinpoint.thrift.dto.command.*;
import org.apache.thrift.TBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

/**
 * Created by root on 17-1-16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamRouteHandler.class)
public class StreamRouteHandlerTest {
    ClusterPointLocator clusterPointLocator = PowerMockito.mock(ClusterPointLocator.class);
    RouteFilterChain streamCreateChain = PowerMockito.mock(RouteFilterChain.class);
    RouteFilterChain streamCloseChain = PowerMockito.mock(RouteFilterChain.class);
    RouteFilterChain responseFilterChain = PowerMockito.mock(RouteFilterChain.class);

    StreamRouteHandler streamRouteHandler;

    @Before
    public void setUp() throws Exception {
        streamRouteHandler = new StreamRouteHandler(clusterPointLocator, streamCreateChain, responseFilterChain, streamCloseChain);
    }

    @Test
    public void addFilter() throws Exception {
        //given
        RouteFilter responseFilter = new MockEventFilter();
        RouteFilter requestFilter = new MockEventFilter();
        RouteFilter closeFilter = new MockEventFilter();

        streamRouteHandler.addResponseFilter(responseFilter);
        streamRouteHandler.addRequestFilter(requestFilter);
        streamRouteHandler.addCloseFilter(closeFilter);

        //then
        Mockito.verify(responseFilterChain).addLast(responseFilter);
        Mockito.verify(streamCreateChain).addLast(requestFilter);
        Mockito.verify(streamCloseChain).addLast(closeFilter);
    }

    @Test
    public void should_return_EMPTY_REQUEST_when_input_empty_event_onRoute() throws Exception {
        //given
        StreamEvent event = PowerMockito.mock(StreamEvent.class);

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.EMPTY_REQUEST);
    }

    @Test
    public void should_return_NOT_FOUND_when_input_given_event_onRoute() throws Exception {
        //given
        StreamEvent event = PowerMockito.mock(StreamEvent.class);
        TBase tBase = PowerMockito.mock(TBase.class);

        PowerMockito.when(event.getRequestObject()).thenReturn(tBase);
        PowerMockito.when(event.getDeliveryCommand()).thenReturn(new TCommandTransfer());

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.NOT_FOUND);
    }

    @Test
    public void should_return_NOT_SUPPORTED_REQUEST_when_input_given_event_onRoute() throws Exception {
        //given
        StreamEvent event = PowerMockito.mock(StreamEvent.class);
        TBase tBase = PowerMockito.mock(TBase.class);
        TargetClusterPoint point = new MockTargetClusterPoint();

        PowerMockito.when(event.getRequestObject()).thenReturn(tBase);
        PowerMockito.when(event.getDeliveryCommand()).thenReturn(createTCommandTransfer());
        PowerMockito.when(clusterPointLocator.getClusterPointList()).thenReturn(newArrayList(point));

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.NOT_SUPPORTED_REQUEST);
    }


    @Test
    public void should_return_NOT_SUPPORTED_SERVICE_when_input_given_event_onRoute() throws Exception {
        //given
        StreamEvent event = getStreamEvent();
        TargetClusterPoint point = new MockTargetClusterPoint();

        PowerMockito.when(clusterPointLocator.getClusterPointList()).thenReturn(newArrayList(point));

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.NOT_SUPPORTED_SERVICE);
    }

    @Test
    public void should_return_UNKNOWN_SERVICE_when_input_given_event_onRoute() throws Exception {
        //given
        StreamEvent event = getStreamEvent();
        TargetClusterPoint point = getTargetClusterPoint();

        PowerMockito.when(clusterPointLocator.getClusterPointList()).thenReturn(newArrayList(point));

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.UNKNOWN);
    }

    @Test
    public void should_return_OK_when_input_given_event_onRoute() throws Exception {
        //given
        StreamEvent event = getStreamEvent();
        ServerStreamChannel serverStreamChannel = Mockito.mock(ServerStreamChannel.class);
        ServerStreamChannelContext context = new ServerStreamChannelContext(serverStreamChannel);

        Mockito.when(event.getStreamChannelContext()).thenReturn(context);
        TargetClusterPoint point = getTargetClusterPoint();

        PowerMockito.when(clusterPointLocator.getClusterPointList()).thenReturn(newArrayList(point));

        TCommandTransferResponse response = streamRouteHandler.onRoute(event);

        assertEquals(response.getRouteResult(), TRouteResult.OK);
    }

    @Test
    public void close() throws Exception {
        streamRouteHandler.getClass().getDeclaredClasses();

//        streamRouteHandler.close();
    }

    private StreamEvent getStreamEvent() throws Exception {
        StreamEvent event = PowerMockito.mock(StreamEvent.class);
        TBase tBase =  new TCmdActiveThreadDumpRes();
        PowerMockito.when(event.getRequestObject()).thenReturn(tBase);
        PowerMockito.when(event.getDeliveryCommand()).thenReturn(createTCommandTransfer());
        return event;
    }

    private TargetClusterPoint getTargetClusterPoint() {
        PinpointServer pinpointServer = PowerMockito.mock(PinpointServer.class);
        PowerMockito.when(pinpointServer.getChannelProperties()).thenReturn(getProperties());

        ClientStreamChannelContext clientStreamChannelContext = PowerMockito.mock(ClientStreamChannelContext.class);
        PowerMockito.when(pinpointServer.openStream(Mockito.any(byte[].class), Mockito.any(ClientStreamChannelMessageListener.class))).thenReturn(clientStreamChannelContext);
        PowerMockito.when(clientStreamChannelContext.getStreamChannel()).thenReturn(PowerMockito.mock(ClientStreamChannel.class));
        return new MockPinpointServerPoint(pinpointServer);
    }

    private class MockEventFilter implements RouteFilter {

        @Override
        public void doEvent(RouteEvent event) {

        }
    }

    private Map<Object, Object> getProperties() {
        Map<Object, Object> properties = newHashMap();

        properties.put(AgentHandshakePropertyType.VERSION.getName(), "1.5.0-SNAPSHOT");
        properties.put(AgentHandshakePropertyType.APPLICATION_NAME.getName(), "test_APP");
        properties.put(AgentHandshakePropertyType.AGENT_ID.getName(), "test-agent");
        properties.put(AgentHandshakePropertyType.START_TIMESTAMP.getName(), 123L);
        return properties;
    }

    private TCommandTransfer createTCommandTransfer() throws Exception {
        TCommandTransfer tCommandTransfer = new TCommandTransfer();
        tCommandTransfer.setApplicationName("test_APP");
        tCommandTransfer.setAgentId("test-agent");
        tCommandTransfer.setStartTime(123L);
        return tCommandTransfer;
    }

    private class MockTargetClusterPoint implements TargetClusterPoint {
        @Override
        public String getApplicationName() {
            return "test_APP";
        }

        @Override
        public String getAgentId() {
            return "test-agent";
        }

        @Override
        public long getStartTimeStamp() {
            return 123L;
        }

        @Override
        public String gerVersion() {
            return "1.5.0-SNAPSHOT";
        }

        @Override
        public void send(byte[] data) {

        }

        @Override
        public Future request(byte[] data) {
            return null;
        }
    }


    private class MockPinpointServerPoint extends PinpointServerClusterPoint {
        public MockPinpointServerPoint(PinpointServer pinpointServer) {
            super(pinpointServer);
        }
    }
}