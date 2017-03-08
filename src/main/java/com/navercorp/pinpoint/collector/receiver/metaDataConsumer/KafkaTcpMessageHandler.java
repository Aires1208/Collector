package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTcpMessageHandler {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());


    public KafkaTcpMessageHandler(DispatchHandler dispatchHandler, String topic, String zkconnect, String group) {
        TcpMessageKafkaConsumer kafkaConsumer = new TcpMessageKafkaConsumer(topic, dispatchHandler, zkconnect, group);
        logger.info("kafkaConsumer.run() is running");
        kafkaConsumer.run();
    }
}
