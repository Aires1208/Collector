package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import com.navercorp.pinpoint.common.util.ExecutorFactory;
import com.navercorp.pinpoint.common.util.PinpointThreadFactory;
import com.navercorp.pinpoint.rpc.packet.Packet;
import com.navercorp.pinpoint.rpc.packet.PacketType;
import com.navercorp.pinpoint.rpc.packet.RequestPacket;
import com.navercorp.pinpoint.rpc.packet.SendPacket;
import com.navercorp.pinpoint.thrift.io.DeserializerFactory;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializerFactory;
import com.navercorp.pinpoint.thrift.io.ThreadLocalHeaderTBaseDeserializerFactory;
import com.navercorp.pinpoint.thrift.util.SerializationUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class DispatchTcpMessageHandler {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    private DispatchHandler dispatchHandler;
    private final ThreadPoolExecutor worker;
    private final ThreadFactory tcpWorkerThreadFactory = new PinpointThreadFactory("Pinpoint-TCP-Worker");
    private final DeserializerFactory<HeaderTBaseDeserializer> deserializerFactory = new ThreadLocalHeaderTBaseDeserializerFactory<>(new HeaderTBaseDeserializerFactory());

    // TODO: 8/14/16 should pass params from configuration
    public DispatchTcpMessageHandler(DispatchHandler dispatchHandler) {
        this.worker = ExecutorFactory.newFixedThreadPool(2, 1024, tcpWorkerThreadFactory);
        this.dispatchHandler = dispatchHandler;
    }

    public void messageReceived(Object packet) {
        final short packetType = getPacketType(packet);
        switch (packetType) {
            case PacketType.APPLICATION_SEND: {
                handlerSend((SendPacket) packet);
                return;
            }
            case PacketType.APPLICATION_REQUEST: {
                handleRequest((RequestPacket) packet);
                return;
            }
            case PacketType.APPLICATION_RESPONSE:
                logger.debug("responsePacket arrived packet:{}", packet);
                return;
            case PacketType.APPLICATION_STREAM_CREATE:
            case PacketType.APPLICATION_STREAM_CLOSE:
            case PacketType.APPLICATION_STREAM_CREATE_SUCCESS:
            case PacketType.APPLICATION_STREAM_CREATE_FAIL:
            case PacketType.APPLICATION_STREAM_RESPONSE:
            case PacketType.APPLICATION_STREAM_PING:
            case PacketType.APPLICATION_STREAM_PONG:
                logger.debug("StreamPacket arrived packet:{}", packet);
                return;
            case PacketType.CONTROL_HANDSHAKE:
                logger.debug("ControlHandshakePacket arrived packet:{}", packet);
                return;
            case PacketType.CONTROL_CLIENT_CLOSE: {
                logger.debug("ControlClientClosePacket arrived packet:{}", packet);
                return;
            }
            case PacketType.CONTROL_PING: {
                logger.debug("PingPacket arrived packet:{}", packet);
                return;
            }
            default: {
                logger.warn("invalid messageReceived msg:{}", packet);
            }

        }
    }

    private void handleRequest(RequestPacket packet) {
        try {
            worker.execute(new RequestResponseDispatch(packet));
        } catch (RejectedExecutionException e) {
            logger.warn("RejectedExecutionException Caused:{}", e.getMessage(), e);
        }
    }

    private class RequestResponseDispatch implements Runnable {

        private RequestPacket packet;

        public RequestResponseDispatch(RequestPacket packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            byte[] bytes = packet.getPayload();
            try {
                TBase<?, ?> tBase = SerializationUtils.deserialize(bytes, deserializerFactory);
                logger.info("receive RequestPacket, packet:{}", tBase.toString());
                dispatchHandler.dispatchRequestMessage(tBase);
            } catch (TException e) {
                logger.error("RequestPacket handle error : {}", e.getMessage(), e);
            }

        }
    }

    private void handlerSend(SendPacket packet) {
        try {
            worker.execute(new Dispatch(packet.getPayload()));
        } catch (RejectedExecutionException e) {
            logger.error("RejectedExecutionException Caused:{}", e.getMessage(), e);
        }
    }

    private class Dispatch implements Runnable {
        private final byte[] bytes;

        public Dispatch(byte[] bytes) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            this.bytes = bytes;
        }
        @Override
        public void run() {
            try {
                TBase<?, ?> tBase = SerializationUtils.deserialize(bytes, deserializerFactory);
                dispatchHandler.dispatchSendMessage(tBase);
            } catch (TException e) {
                logger.error("dispatchHandler.dispatchSendMessage error : {}", e.getMessage(), e);
            }

        }
    }

    private short getPacketType(Object packet) {
        if (packet == null) {
            return PacketType.UNKNOWN;
        }

        if (packet instanceof Packet) {
            return ((Packet) packet).getPacketType();
        }

        return PacketType.UNKNOWN;
    }
}
