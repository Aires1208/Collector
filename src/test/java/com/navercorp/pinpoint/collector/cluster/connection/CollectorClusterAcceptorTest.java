package com.navercorp.pinpoint.collector.cluster.connection;

import com.navercorp.pinpoint.rpc.MessageListener;
import com.navercorp.pinpoint.rpc.common.SocketStateCode;
import com.navercorp.pinpoint.rpc.packet.HandshakeResponseCode;
import com.navercorp.pinpoint.rpc.packet.PingPacket;
import com.navercorp.pinpoint.rpc.packet.RequestPacket;
import com.navercorp.pinpoint.rpc.packet.SendPacket;
import com.navercorp.pinpoint.rpc.server.PinpointServer;
import com.navercorp.pinpoint.rpc.stream.ServerStreamChannelMessageListener;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by root on 17-1-11.
 */
public class CollectorClusterAcceptorTest {

    @Test
    public void start() throws Exception {
        //given
        CollectorClusterAcceptor acceptor = getCollectorClusterAcceptor();

        //then
        acceptor.start();
        acceptor.stop();
    }

    private CollectorClusterAcceptor getCollectorClusterAcceptor() {
        CollectorClusterConnectionFactory option = mock(CollectorClusterConnectionFactory.class);
        InetSocketAddress bindAddress = new InetSocketAddress("localhost", 0);
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);

        MessageListener listener = mock(MessageListener.class);
        ServerStreamChannelMessageListener messageListener = mock(ServerStreamChannelMessageListener.class);

        //when
        when(option.getClusterId()).thenReturn("");
        when(option.getRouteMessageHandler()).thenReturn(listener);
        when(option.getRouteStreamMessageHandler()).thenReturn(messageListener);

        return new CollectorClusterAcceptor(option, bindAddress, repository);
    }

    @Test
    public void clusterServerMessageListenerTest() throws Exception {
        //given
        MessageListener messageListener = mock(MessageListener.class);
        CollectorClusterAcceptor acceptor = getCollectorClusterAcceptor();
        acceptor.start();
        CollectorClusterAcceptor.ClusterServerMessageListener listener =
                acceptor.new ClusterServerMessageListener("127.0.0.1", messageListener);

        PinpointServer pinpointSocket = mock(PinpointServer.class);
        SendPacket sendPacket = mock(SendPacket.class);
        RequestPacket requestPacket = mock(RequestPacket.class);
        PingPacket pingPacket = mock(PingPacket.class);
        Map properties = mock(Map.class);
        SocketAddress address = mock(SocketAddress.class);

        //when
        when(pinpointSocket.getRemoteAddress()).thenReturn(address);

        listener.handleSend(sendPacket, pinpointSocket);
        listener.handleRequest(requestPacket, pinpointSocket);
        listener.handlePing(pingPacket, pinpointSocket);
        HandshakeResponseCode responseCode = listener.handleHandshake(properties);

        //then
        verify(messageListener).handleRequest(requestPacket, pinpointSocket);
        assertEquals(responseCode, HandshakeResponseCode.DUPLEX_COMMUNICATION);

        acceptor.stop();
    }


    @Test
    public void webClusterServerChannelStateChangeHandler_handleEventTest() throws Exception {
        //given
        CollectorClusterAcceptor acceptor = getCollectorClusterAcceptor();
        acceptor.start();
        CollectorClusterAcceptor.WebClusterServerChannelStateChangeHandler handler =
                acceptor.new WebClusterServerChannelStateChangeHandler();

        PinpointServer pinpointSocket = mock(PinpointServer.class);
        SocketAddress address = mock(SocketAddress.class);

        //when
        when(pinpointSocket.getRemoteAddress()).thenReturn(address);
        handler.eventPerformed(pinpointSocket, SocketStateCode.RUN_DUPLEX);
        handler.eventPerformed(pinpointSocket, SocketStateCode.CLOSED_BY_CLIENT);

        acceptor.stop();
    }

    @Test
    public void webClusterServerChannelStateChangeHandler_ExceptionCaughtTest() throws Exception {
        CollectorClusterAcceptor acceptor = getCollectorClusterAcceptor();
        acceptor.start();
        CollectorClusterAcceptor.WebClusterServerChannelStateChangeHandler handler =
                acceptor.new WebClusterServerChannelStateChangeHandler();

        PinpointServer pinpointSocket = mock(PinpointServer.class);
        handler.exceptionCaught(pinpointSocket, SocketStateCode.CLOSED_BY_CLIENT, new Throwable("test"));

        acceptor.stop();
    }
}