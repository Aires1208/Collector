package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.google.common.base.Preconditions;
import com.navercorp.pinpoint.rpc.packet.*;
import com.navercorp.pinpoint.rpc.packet.stream.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class MessageDecoder {
    public static Object deCode(byte[] msg) {
        Preconditions.checkArgument(msg.length > 2, "message length error.");
        ChannelBuffer buffer = ChannelBuffers.buffer(msg.length);
        buffer.writeBytes(msg);

        if (buffer.readableBytes() < 2) {
            return null;
        }
        buffer.markReaderIndex();
        final short packetType = buffer.readShort();
        switch (packetType) {
            case PacketType.APPLICATION_SEND:
                return readSend(packetType, buffer);
            case PacketType.APPLICATION_REQUEST:
                return readRequest(packetType, buffer);
            case PacketType.APPLICATION_RESPONSE:
                return readResponse(packetType, buffer);
            case PacketType.APPLICATION_STREAM_CREATE:
                return readStreamCreate(packetType, buffer);
            case PacketType.APPLICATION_STREAM_CLOSE:
                return readStreamClose(packetType, buffer);
            case PacketType.APPLICATION_STREAM_CREATE_SUCCESS:
                return readStreamCreateSuccess(packetType, buffer);
            case PacketType.APPLICATION_STREAM_CREATE_FAIL:
                return readStreamCreateFail(packetType, buffer);
            case PacketType.APPLICATION_STREAM_RESPONSE:
                return readStreamData(packetType, buffer);
            case PacketType.APPLICATION_STREAM_PING:
                return readStreamPing(packetType, buffer);
            case PacketType.APPLICATION_STREAM_PONG:
                return readStreamPong(packetType, buffer);
            case PacketType.CONTROL_CLIENT_CLOSE:
                return readControlClientClose(packetType, buffer);
            case PacketType.CONTROL_SERVER_CLOSE:
                return readControlServerClose(packetType, buffer);
            case PacketType.CONTROL_PING:
                return readPing(packetType, buffer);
//            case PacketType.CONTROL_PONG:
//                return null;
            case PacketType.CONTROL_HANDSHAKE:
                return readEnableWorker(packetType, buffer);
            case PacketType.CONTROL_HANDSHAKE_RESPONSE:
                return readEnableWorkerConfirm(packetType, buffer);
            default:
                return null;
        }
    }

    private static Object readPing(short packetType, ChannelBuffer buffer) {
        return PingPacket.readBuffer(packetType, buffer);
    }

    private static Object readEnableWorkerConfirm(short packetType, ChannelBuffer buffer) {
        return ControlHandshakeResponsePacket.readBuffer(packetType, buffer);
    }

    private static Object readEnableWorker(short packetType, ChannelBuffer buffer) {
        return ControlHandshakePacket.readBuffer(packetType, buffer);
    }

    private static Object readControlServerClose(short packetType, ChannelBuffer buffer) {
        return ServerClosePacket.readBuffer(packetType, buffer);
    }

    private static Object readControlClientClose(short packetType, ChannelBuffer buffer) {
        return ClientClosePacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamPong(short packetType, ChannelBuffer buffer) {
        return StreamPongPacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamPing(short packetType, ChannelBuffer buffer) {
        return StreamPingPacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamData(short packetType, ChannelBuffer buffer) {
        return StreamResponsePacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamCreateFail(short packetType, ChannelBuffer buffer) {
        return StreamCreateFailPacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamCreateSuccess(short packetType, ChannelBuffer buffer) {
        return StreamCreateSuccessPacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamClose(short packetType, ChannelBuffer buffer) {
        return StreamClosePacket.readBuffer(packetType, buffer);
    }

    private static Object readStreamCreate(short packetType, ChannelBuffer buffer) {
        return StreamCreatePacket.readBuffer(packetType, buffer);
    }

    private static Object readResponse(short packetType, ChannelBuffer buffer) {
        return ResponsePacket.readBuffer(packetType, buffer);
    }

    private static Object readRequest(short packetType, ChannelBuffer buffer) {
        return RequestPacket.readBuffer(packetType, buffer);
    }

    private static Object readSend(short packetType, ChannelBuffer buffer) {
        return SendPacket.readBuffer(packetType, buffer);
    }
}
