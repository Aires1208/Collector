package com.navercorp.pinpoint.collector.receiver.spanstatConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;

public class KafkaUDPTopicMessagesHandler {


    public KafkaUDPTopicMessagesHandler(DispatchHandler dispatchHandler, String topic, String zkconnect, String group) {
        if (dispatchHandler == null) {
            throw new NullPointerException("dispatchHandler must not be null");
        }
        KafkaMessagesConsumer kafkaConsumer = new KafkaMessagesConsumer(topic, dispatchHandler, zkconnect, group);
        kafkaConsumer.run();
    }

}
