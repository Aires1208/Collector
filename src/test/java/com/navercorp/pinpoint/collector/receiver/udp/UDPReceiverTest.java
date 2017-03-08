/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.collector.receiver.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;

import com.codahale.metrics.MetricRegistry;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.pinpoint.collector.receiver.DataReceiver;
import org.springframework.util.SocketUtils;

/**
 * @author emeroad
 */
@RunWith(MockitoJUnitRunner.class)
public class UDPReceiverTest {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final int PORT = SocketUtils.findAvailableUdpPort(10999);

    @Mock
    private MetricRegistry metricRegistry;

    @InjectMocks
    static DataReceiver receiver;

    @BeforeClass
    public static void init() {
        try {
            receiver = new UDPReceiver("test", new PacketHandlerFactory() {
                @Override
                public PacketHandler createPacketHandler() {
                    return null;
                }
            }, "127.0.0.1", PORT, 1024, 1, 10, true);
        } catch (Exception e) {

        }
    }

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void startStop() {
        receiver.start();
        receiver.shutdown();
    }

    @Test
    public void hostNullCheck() {
        InetSocketAddress address = new InetSocketAddress((InetAddress) null, 90);
        logger.debug(address.toString());
    }

    @Test
    public void socketBufferSize() throws SocketException {
        DatagramSocket datagramSocket = new DatagramSocket();
        int receiveBufferSize = datagramSocket.getReceiveBufferSize();
        logger.debug("{}", receiveBufferSize);

        datagramSocket.setReceiveBufferSize(64*1024*10);
        logger.debug("{}", datagramSocket.getReceiveBufferSize());

        datagramSocket.close();
    }

    @Test
    public void sendSocketBufferSize() throws IOException {
        DatagramPacket datagramPacket = new DatagramPacket(new byte[0], 0, 0);

        DatagramSocket datagramSocket = new DatagramSocket();
        datagramSocket.connect(new InetSocketAddress("127.0.0.1", 9995));

        datagramSocket.send(datagramPacket);
        datagramSocket.close();
    }
}
