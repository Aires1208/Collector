package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.rpc.packet.PacketType;
import com.navercorp.pinpoint.rpc.packet.RequestPacket;
import com.navercorp.pinpoint.rpc.packet.SendPacket;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseSerializerFactory;
import com.navercorp.pinpoint.thrift.util.SerializationUtils;
import org.apache.thrift.TBase;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;

/**
 * Created by root on 16-12-27.
 */
public class DispatchTcpMessageHandlerTest {
    private final HeaderTBaseSerializer serializer = HeaderTBaseSerializerFactory.DEFAULT_FACTORY.createSerializer();

    @Mock
    private DispatchHandler dispatchHandler;

    @InjectMocks
    private DispatchTcpMessageHandler dispatchTcpMessageHandler = new DispatchTcpMessageHandler(dispatchHandler);

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_invoke_dispatchSendMessage() throws Exception {
        //given
        Object sendPack = getSendPacket();

        //when
        dispatchTcpMessageHandler.messageReceived(sendPack);
        Thread.sleep(1000);

        //then
        Mockito.verify(dispatchHandler).dispatchSendMessage(any(TBase.class));
    }

    @Test
    public void should_invoke_dispatchRequestMessage() throws Exception {
        //given
        Object requestPack = getRequestPacket();

        //when
        dispatchTcpMessageHandler.messageReceived(requestPack);
        Thread.sleep(1500);

        //then
        Mockito.verify(dispatchHandler).dispatchRequestMessage(any(TBase.class));
    }

    private Object getRequestPacket() {
        Buffer automaticBuffer = new AutomaticBuffer();
        TAgentInfo agentInfo = new TAgentInfo("localhost", "127.0.0.1", "0", "fm-agent", "fm-active", ServiceType.SPRING.getCode(), 123, "version1.5.2", "jdk1.8.0_111", 1L);
        byte[] payload = SerializationUtils.serialize(agentInfo, serializer, null);
        automaticBuffer.put(1000);//requestId
        automaticBuffer.put(payload.length);
        automaticBuffer.put(payload);

        ChannelBuffer buffer = ChannelBuffers.buffer(automaticBuffer.getBuffer().length);
        buffer.writeBytes(automaticBuffer.getBuffer());
        return RequestPacket.readBuffer(PacketType.APPLICATION_REQUEST, buffer);
    }


    private Object getSendPacket() {
        Buffer automaticBuffer = new AutomaticBuffer();
        TAgentInfo agentInfo = new TAgentInfo("localhost", "127.0.0.1", "0", "fm-agent", "fm-active", ServiceType.SPRING.getCode(), 123, "version1.5.2", "jdk1.8.0_111", 1L);
        byte[] payload = SerializationUtils.serialize(agentInfo, serializer, null);
        automaticBuffer.put(payload.length);
        automaticBuffer.put(payload);

        ChannelBuffer buffer = ChannelBuffers.buffer(automaticBuffer.getBuffer().length);
        buffer.writeBytes(automaticBuffer.getBuffer());
        return SendPacket.readBuffer(PacketType.APPLICATION_SEND, buffer);
    }

}