package com.navercorp.pinpoint.collector.receiver.spanstatConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializer;
import com.navercorp.pinpoint.thrift.io.HeaderTBaseDeserializerFactory;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamConsumer implements Runnable {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    private HeaderTBaseDeserializer deserializer = new HeaderTBaseDeserializerFactory().createDeserializer();
    private DispatchHandler dispatchHandler;

    private KafkaStream<byte[], byte[]> kafkaStream;

    public KafkaStreamConsumer(KafkaStream<byte[], byte[]> kafkaStream,DispatchHandler dispatchHandler) {
        this.kafkaStream = kafkaStream;
        this.dispatchHandler = dispatchHandler;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (it.hasNext()) {

            try {
                byte[] message = it.next().message();
                byte[] msg = Base64.decodeBase64(message);
                TBase<?, ?> tBase = deserializer.deserialize(msg);
                logger.info(tBase.toString());
                dispatchHandler.dispatchSendMessage(tBase);

            } catch (TException e) {
                logger.error("handle tBase message error: {}", e.getMessage(), e);
            }
        }
        logger.warn("KafkaStreamConsumer exited.");
    }
}