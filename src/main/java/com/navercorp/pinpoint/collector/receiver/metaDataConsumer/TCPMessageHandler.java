package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPMessageHandler implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private KafkaStream stream;
    private DispatchTcpMessageHandler handler;

    public TCPMessageHandler(KafkaStream stream, DispatchHandler dispatchHandler) {
        this.stream = stream;
        this.handler = new DispatchTcpMessageHandler(dispatchHandler);
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            byte[] message = it.next().message();
            byte[] msg = Base64.decodeBase64(message);

            Object packet = MessageDecoder.deCode(msg);

            handler.messageReceived(packet);
        }
        logger.info("TCPMessageHandler exited");
    }
}
