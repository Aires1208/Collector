package com.navercorp.pinpoint.collector.cluster.connection;

import com.navercorp.pinpoint.rpc.PinpointSocket;
import com.navercorp.pinpoint.rpc.server.PinpointServer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by root on 17-1-11.
 */
public class CollectorClusterConnectionManagerTest {
    @Test
    public void start() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        CollectorClusterAcceptor acceptor = mock(CollectorClusterAcceptor.class);

        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        manager.start();

        manager.stop();

        verify(connector).start();
        verify(connector).stop();
        verify(acceptor, times(0)).start();
        verify(acceptor, times(0)).stop();
    }

    @Test
    public void stop() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        CollectorClusterAcceptor acceptor = mock(CollectorClusterAcceptor.class);

        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector, acceptor);

        manager.start();

        manager.stop();

        verify(connector).start();
        verify(connector).stop();
        verify(acceptor).start();
        verify(acceptor).stop();
    }

    @Test
    public void should_return_already_connect_when_given_an_address() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        InetSocketAddress address = new InetSocketAddress("localhost", 0);

        //when
        when(repository.containsKey(any(InetSocketAddress.class))).thenReturn(true);
        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        manager.connectPointIfAbsent(address);

        //then
        verify(repository, times(0)).putIfAbsent(any(SocketAddress.class), any(PinpointSocket.class));
    }

    @Test
    public void should_return_newly_connect_when_given_an_address() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        InetSocketAddress address = new InetSocketAddress("localhost", 0);

        //when
        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        manager.connectPointIfAbsent(address);

        //then
        verify(repository).putIfAbsent(any(SocketAddress.class), any(PinpointSocket.class));
    }

    @Test
    public void should_return_already_disconnect_when_given_an_address() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        InetSocketAddress address = new InetSocketAddress("localhost", 0);

        //when
        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        manager.disconnectPoint(address);
    }

    @Test
    public void should_return_newly_disconnect_when_given_an_address() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        InetSocketAddress address = new InetSocketAddress("localhost", 0);
        PinpointSocket socket = mock(PinpointSocket.class);
        //when
        when(repository.remove(any(SocketAddress.class))).thenReturn(socket);
        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        manager.disconnectPoint(address);
    }

    @Test
    public void getConnectedAddressList() throws Exception {
        CollectorClusterConnectionRepository repository = mock(CollectorClusterConnectionRepository.class);
        CollectorClusterConnector connector = mock(CollectorClusterConnector.class);
        SocketAddress address = new SocketAddress() {};
        List<SocketAddress> sockets = newArrayList(address);

        //when
        when(repository.getAddressList()).thenReturn(sockets);
        CollectorClusterConnectionManager manager =
                new CollectorClusterConnectionManager("", repository, connector);

        List<SocketAddress> socketAddresses = manager.getConnectedAddressList();

        assertEquals(1, socketAddresses.size());
    }

}