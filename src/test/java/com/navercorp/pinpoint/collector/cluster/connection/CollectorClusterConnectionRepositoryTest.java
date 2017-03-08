package com.navercorp.pinpoint.collector.cluster.connection;

import com.navercorp.pinpoint.rpc.PinpointSocket;
import com.navercorp.pinpoint.rpc.client.PinpointClient;
import org.junit.Test;

import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-11.
 */
public class CollectorClusterConnectionRepositoryTest {
    @Test
    public void testRepository() throws Exception {
        //given
        SocketAddress address = new SocketAddress() {
            @Override
            protected Object clone() throws CloneNotSupportedException {
                return super.clone();
            }
        };
        PinpointSocket pinpointSocket = new PinpointClient();

        CollectorClusterConnectionRepository repository = new CollectorClusterConnectionRepository();

        repository.putIfAbsent(address, pinpointSocket);

        boolean containsKey = repository.containsKey(address);
        assertTrue(containsKey);

        List<SocketAddress> socketAddresses = repository.getAddressList();
        assertEquals(socketAddresses.get(0), address);

        List<PinpointSocket> sockets = repository.getClusterSocketList();
        assertEquals(sockets.get(0), pinpointSocket);

        repository.remove(address);
    }
}