package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.rpc.packet.*;
import com.navercorp.pinpoint.rpc.packet.stream.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-27.
 */
public class MessageDecoderTest {
    @Test
    public void deCodePingPack() throws Exception {
        //given
        byte[] pingPackb = getPingPack();

        //when
        Object pingPack = MessageDecoder.deCode(pingPackb);

        //then
        assertTrue(pingPack instanceof PingPacket);
    }

    @Test
    public void deCodePongPack() throws Exception {
        //given
        byte[] pong = getPongPack();

        //when
        Object pongPack = MessageDecoder.deCode(pong);

        //then
        assertNull(pongPack);
    }

    @Test
    public void deCodeAppSendPack() throws Exception {
        //given
        byte[] pingPackb = getPayloadPack(PacketType.APPLICATION_SEND);

        //when
        Object sendPack = MessageDecoder.deCode(pingPackb);

        //then
        assertTrue(sendPack instanceof SendPacket);
    }

    @Test
    public void deCodeAppReqPack() throws Exception {
        //given
        byte[] reqPack = getIdAndPayloadPack(PacketType.APPLICATION_REQUEST);

        //when
        Object requestPack = MessageDecoder.deCode(reqPack);

        //then
        assertTrue(requestPack instanceof RequestPacket);
    }

    @Test
    public void deCodeAppResponsePack() throws Exception {
        //given
        byte[] responsePackByte = getIdAndPayloadPack(PacketType.APPLICATION_RESPONSE);

        //when
        Object responsePacket = MessageDecoder.deCode(responsePackByte);

        //then
        assertTrue(responsePacket instanceof ResponsePacket);
    }

    @Test
    public void deCodeAppStreamCreatePack() throws Exception {
        //given
        byte[] streamCreate = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_CREATE);

        //when
        Object streamCreatePack = MessageDecoder.deCode(streamCreate);

        //then
        assertTrue(streamCreatePack instanceof StreamCreatePacket);
    }

    @Test
    public void deCodeAppStreamClossePack() throws Exception {
        //given
        byte[] streamClose = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_CLOSE);

        //when
        Object streamClosePack = MessageDecoder.deCode(streamClose);

        //then
        assertTrue(streamClosePack instanceof StreamClosePacket);
    }

    @Test
    public void deCodeAppStreamCreateSuccessPack() throws Exception {
        //given
        byte[] streamCreateSuccess = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_CREATE_SUCCESS);

        //when
        Object streamCreateSuccessPack = MessageDecoder.deCode(streamCreateSuccess);

        //then
        assertTrue(streamCreateSuccessPack instanceof StreamCreateSuccessPacket);
    }

    @Test
    public void deCodeAppStreamCreateFailPack() throws Exception {
        //given
        byte[] streamCreateFail = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_CREATE_FAIL);

        //when
        Object streamCreateFailPack = MessageDecoder.deCode(streamCreateFail);

        //then
        assertTrue(streamCreateFailPack instanceof StreamCreateFailPacket);
    }

    @Test
    public void deCodeAppStreamResponsePack() throws Exception {
        //given
        byte[] streamResponse = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_RESPONSE);

        //when
        Object streamResponsePack = MessageDecoder.deCode(streamResponse);

        //then
        assertTrue(streamResponsePack instanceof StreamResponsePacket);
    }

    @Test
    public void deCodeAppStreamPingPack() throws Exception {
        //given
        byte[] streamPing = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_PING);

        //when
        Object streamPingPack = MessageDecoder.deCode(streamPing);

        //then
        assertTrue(streamPingPack instanceof StreamPingPacket);
    }

    @Test
    public void deCodeAppStreamPongPack() throws Exception {
        //given
        byte[] streamPong = getIdAndPayloadPack(PacketType.APPLICATION_STREAM_PONG);

        //when
        Object streamPongPack = MessageDecoder.deCode(streamPong);

        //then
        assertTrue(streamPongPack instanceof StreamPongPacket);
    }

    @Test
    public void deCodeCtrlClientClosePack() throws Exception {
        //given
        byte[] ctrlCClose = getPayloadPack(PacketType.CONTROL_CLIENT_CLOSE);

        //when
        Object ctrlCClosePack = MessageDecoder.deCode(ctrlCClose);

        //then
        assertTrue(ctrlCClosePack instanceof ClientClosePacket);
    }

    @Test
    public void deCodeCtrlServerClosePack() throws Exception {
        //given
        byte[] ctrlSClose = getPayloadPack(PacketType.CONTROL_SERVER_CLOSE);

        //when
        Object ctrlSClosePack = MessageDecoder.deCode(ctrlSClose);

        //then
        assertTrue(ctrlSClosePack instanceof ServerClosePacket);
    }

    @Test
    public void deCodeHandShakePack() throws Exception {
        //given
        byte[] handShake = getIdAndPayloadPack(PacketType.CONTROL_HANDSHAKE);

        //when
        Object handShakePack = MessageDecoder.deCode(handShake);

        //then
        assertTrue(handShakePack instanceof ControlHandshakePacket);
    }

    @Test
    public void deCodeHandShakeConfirmPack() throws Exception {
        //given
        byte[] handShakeConfirm = getIdAndPayloadPack(PacketType.CONTROL_HANDSHAKE_RESPONSE);

        //when
        Object handShakeConfirmPack = MessageDecoder.deCode(handShakeConfirm);

        //then
        assertTrue(handShakeConfirmPack instanceof ControlHandshakeResponsePacket);
    }

    private byte[] getPayloadPack(short packetType) {
        Buffer buffer = new AutomaticBuffer();
        buffer.put(packetType);
        byte[] payload = Bytes.toBytes("this is a test payload for payload pack.");
        buffer.put(payload.length);
        buffer.put(payload);
        return buffer.getBuffer();
    }

    private byte[] getIdAndPayloadPack(short packetType) {
        Buffer buffer = new AutomaticBuffer();
        buffer.put(packetType);
        buffer.put(10000);//requestId/responseId/channelId
        byte[] payload = Bytes.toBytes("this is a test payload for app Id and payload pack.");
        buffer.put(payload.length);
        buffer.put(payload);
        return buffer.getBuffer();
    }

    private byte[] getPingPack() {
        Buffer buffer = new AutomaticBuffer();
        buffer.put(PacketType.CONTROL_PING);
        buffer.put(1001);
        buffer.put((byte) 0);
        buffer.put((byte) 1);
        return buffer.getBuffer();
    }

    private byte[] getPongPack() {
        Buffer buffer = new AutomaticBuffer();
        buffer.put(PacketType.CONTROL_PONG);
        buffer.put(1L);

        return buffer.getBuffer();
    }

}