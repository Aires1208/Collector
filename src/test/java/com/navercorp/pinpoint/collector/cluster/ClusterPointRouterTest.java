package com.navercorp.pinpoint.collector.cluster;

import com.navercorp.pinpoint.collector.cluster.route.DefaultRouteHandler;
import com.navercorp.pinpoint.collector.cluster.route.RequestEvent;
import com.navercorp.pinpoint.collector.cluster.route.StreamEvent;
import com.navercorp.pinpoint.collector.cluster.route.StreamRouteHandler;
import com.navercorp.pinpoint.rpc.PinpointSocket;
import com.navercorp.pinpoint.rpc.packet.RequestPacket;
import com.navercorp.pinpoint.rpc.packet.SendPacket;
import com.navercorp.pinpoint.rpc.packet.stream.StreamClosePacket;
import com.navercorp.pinpoint.rpc.packet.stream.StreamCode;
import com.navercorp.pinpoint.rpc.packet.stream.StreamCreatePacket;
import com.navercorp.pinpoint.rpc.stream.ServerStreamChannel;
import com.navercorp.pinpoint.rpc.stream.ServerStreamChannelContext;
import com.navercorp.pinpoint.thrift.dto.command.TCommandTransfer;
import com.navercorp.pinpoint.thrift.dto.command.TCommandTransferResponse;
import com.navercorp.pinpoint.thrift.dto.command.TRouteResult;
import com.navercorp.pinpoint.thrift.io.DeserializerFactory;
import com.navercorp.pinpoint.thrift.util.SerializationUtils;
import org.apache.thrift.TBase;
import org.jboss.netty.channel.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.SocketAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

/**
 * Created by root on 17-1-13.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SerializationUtils.class)
public class ClusterPointRouterTest {
    ClusterPointRouter clusterPointRouter;
    static ClusterPointRepository repository;
    static DefaultRouteHandler routeHandler;
    static StreamRouteHandler streamRouteHandler;

    @BeforeClass
    public static void init() throws Exception {
        repository = PowerMockito.mock(ClusterPointRepository.class);
        routeHandler = PowerMockito.mock(DefaultRouteHandler.class);
        streamRouteHandler = PowerMockito.mock(StreamRouteHandler.class);
    }

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SerializationUtils.class);

        clusterPointRouter = new ClusterPointRouter(repository, routeHandler, streamRouteHandler);
    }

    @Test
    public void handleSend() throws Exception {
        SendPacket sendPacket = PowerMockito.mock(SendPacket.class);
        PinpointSocket pinpointSocket = PowerMockito.mock(PinpointSocket.class);

        clusterPointRouter.handleSend(sendPacket, pinpointSocket);
        clusterPointRouter.stop();
        PowerMockito.verifyStatic();
    }

    @Test
    public void should_invoke_handleRouteRequestFail() throws Exception {
        RequestPacket requestPacket = PowerMockito.mock(RequestPacket.class);
        PinpointSocket pinpointSocket = PowerMockito.mock(PinpointSocket.class);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(null);

        clusterPointRouter.handleRequest(requestPacket, pinpointSocket);
    }

    @Test
    public void should_invoke_handleRouteRequest() throws Exception {
        RequestPacket requestPacket = PowerMockito.mock(RequestPacket.class);
        PinpointSocket pinpointSocket = PowerMockito.mock(PinpointSocket.class);
        TBase tBase = PowerMockito.mock(TCommandTransfer.class);
        TCommandTransferResponse response = new TCommandTransferResponse();
        response.setRouteResult(TRouteResult.OK);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);
        PowerMockito.when(routeHandler.onRoute(any(RequestEvent.class))).thenReturn(response);

        clusterPointRouter.handleRequest(requestPacket, pinpointSocket);
        PowerMockito.verifyStatic();
    }

    @Test
    public void should_invoke_handleRouteRequestFail_when_given_TBase_object() throws Exception {
        RequestPacket requestPacket = PowerMockito.mock(RequestPacket.class);
        PinpointSocket pinpointSocket = PowerMockito.mock(PinpointSocket.class);
        TBase tBase = PowerMockito.mock(TBase.class);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);

        clusterPointRouter.handleRequest(requestPacket, pinpointSocket);
    }

    @Test
    public void should_return_TYPE_UNKNOWN_when_given_null_for_handleStreamCreate() throws Exception {
        StreamCreatePacket createPacket = PowerMockito.mock(StreamCreatePacket.class);
        ServerStreamChannelContext context = PowerMockito.mock(ServerStreamChannelContext.class);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(null);

        StreamCode streamCode = clusterPointRouter.handleStreamCreate(context, createPacket);

        assertEquals(streamCode, StreamCode.TYPE_UNKNOWN);
    }

    @Test
    public void should_return_TYPE_UNSUPPORT_when_given_tBase_for_handleStreamCreate() throws Exception {
        StreamCreatePacket createPacket = PowerMockito.mock(StreamCreatePacket.class);
        ServerStreamChannelContext context = PowerMockito.mock(ServerStreamChannelContext.class);
        TBase tBase = PowerMockito.mock(TBase.class);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);

        StreamCode streamCode = clusterPointRouter.handleStreamCreate(context, createPacket);

        assertEquals(streamCode, StreamCode.TYPE_UNSUPPORT);
    }

    @Test
    public void should_return_OK_when_given_tBase_for_handleStreamCreate() throws Exception {
        StreamCreatePacket createPacket = PowerMockito.mock(StreamCreatePacket.class);
        ServerStreamChannelContext context = getMockContext();
        TBase tBase = PowerMockito.mock(TCommandTransfer.class);
        TCommandTransferResponse response = new TCommandTransferResponse();
        response.setRouteResult(TRouteResult.OK);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);
        PowerMockito.when(streamRouteHandler.onRoute(any(StreamEvent.class))).thenReturn(response);

        StreamCode streamCode = clusterPointRouter.handleStreamCreate(context, createPacket);

        assertEquals(streamCode, StreamCode.OK);
    }


    @Test
    public void should_return_ROUTE_ERROR_when_given_tBase_for_handleStreamCreate() throws Exception {
        StreamCreatePacket createPacket = PowerMockito.mock(StreamCreatePacket.class);
        ServerStreamChannelContext context = getMockContext();
        TBase tBase = PowerMockito.mock(TCommandTransfer.class);
        TCommandTransferResponse response = new TCommandTransferResponse();
        response.setRouteResult(TRouteResult.BAD_REQUEST);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);
        PowerMockito.when(streamRouteHandler.onRoute(any(StreamEvent.class))).thenReturn(response);

        StreamCode streamCode = clusterPointRouter.handleStreamCreate(context, createPacket);

        assertEquals(streamCode, StreamCode.ROUTE_ERROR);
    }

    @Test
    public void should_return_CONNECTION_UNSUPPORT_when_given_tBase_for_handleStreamCreate() throws Exception {
        StreamCreatePacket createPacket = PowerMockito.mock(StreamCreatePacket.class);
        ServerStreamChannelContext context = getMockContext();
        TBase tBase = PowerMockito.mock(TCommandTransfer.class);
        TCommandTransferResponse response = new TCommandTransferResponse();
        response.setRouteResult(TRouteResult.NOT_ACCEPTABLE);

        PowerMockito.when(SerializationUtils.deserialize(any(byte[].class), any(DeserializerFactory.class), any(TBase.class))).thenReturn(tBase);
        PowerMockito.when(streamRouteHandler.onRoute(any(StreamEvent.class))).thenReturn(response);

        StreamCode streamCode = clusterPointRouter.handleStreamCreate(context, createPacket);

        assertEquals(streamCode, StreamCode.CONNECTION_UNSUPPORT);
    }

    private ServerStreamChannelContext getMockContext() {
        ServerStreamChannelContext context = PowerMockito.mock(ServerStreamChannelContext.class);
        ServerStreamChannel channel = PowerMockito.mock(ServerStreamChannel.class);
        PowerMockito.when(channel.getChannel()).thenReturn(new MockChannel());
        PowerMockito.when(context.getStreamChannel()).thenReturn(channel);
        return context;
    }

    @Test
    public void handleStreamClose() throws Exception {
        ServerStreamChannelContext context = PowerMockito.mock(ServerStreamChannelContext.class);
        StreamClosePacket closePacket = PowerMockito.mock(StreamClosePacket.class);

        clusterPointRouter.handleStreamClose(context, closePacket);

        PowerMockito.verifyStatic();
    }

    @Test
    public void getTargetClusterPointRepository() throws Exception {
        assertEquals(clusterPointRouter.getTargetClusterPointRepository(), repository);
    }


    private class MockChannel implements org.jboss.netty.channel.Channel {
        @Override
        public Integer getId() {
            return null;
        }

        @Override
        public ChannelFactory getFactory() {
            return null;
        }

        @Override
        public Channel getParent() {
            return null;
        }

        @Override
        public ChannelConfig getConfig() {
            return null;
        }

        @Override
        public ChannelPipeline getPipeline() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isBound() {
            return false;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public ChannelFuture write(Object message) {
            return null;
        }

        @Override
        public ChannelFuture write(Object message, SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture unbind() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture getCloseFuture() {
            return null;
        }

        @Override
        public int getInterestOps() {
            return 0;
        }

        @Override
        public boolean isReadable() {
            return false;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public ChannelFuture setInterestOps(int interestOps) {
            return null;
        }

        @Override
        public ChannelFuture setReadable(boolean readable) {
            return null;
        }

        @Override
        public Object getAttachment() {
            return null;
        }

        @Override
        public void setAttachment(Object attachment) {

        }

        @Override
        public int compareTo(Channel o) {
            return 0;
        }
    }
}