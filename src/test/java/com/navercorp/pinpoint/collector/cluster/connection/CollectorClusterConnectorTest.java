package com.navercorp.pinpoint.collector.cluster.connection;

import com.navercorp.pinpoint.rpc.MessageListener;
import com.navercorp.pinpoint.rpc.stream.ServerStreamChannelMessageListener;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 17-1-10.
 */
public class CollectorClusterConnectorTest {

    @Test
    public void start() throws Exception {
        //given
        CollectorClusterConnectionFactory option = mock(CollectorClusterConnectionFactory.class);
        MessageListener listener = mock(MessageListener.class);
        ServerStreamChannelMessageListener messageListener = mock(ServerStreamChannelMessageListener.class);

        //when
        when(option.getClusterId()).thenReturn("");
        when(option.getRouteMessageHandler()).thenReturn(listener);
        when(option.getRouteStreamMessageHandler()).thenReturn(messageListener);

        CollectorClusterConnector connector = new CollectorClusterConnector(option);

        //then
        connector.start();
        connector.stop();
    }

    @Test
    public void should_throw_exception_when_connector_not_start() throws Exception {
        //given
        CollectorClusterConnectionFactory option = mock(CollectorClusterConnectionFactory.class);
        InetSocketAddress addresses = mock(InetSocketAddress.class);

        //when
        CollectorClusterConnector connector = new CollectorClusterConnector(option);

        //then
        try {
            connector.connect(addresses);
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "not started.");
        }
    }

}