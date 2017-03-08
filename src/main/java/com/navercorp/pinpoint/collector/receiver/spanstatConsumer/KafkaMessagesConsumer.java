package com.navercorp.pinpoint.collector.receiver.spanstatConsumer;

/**
 * Created by 10183966 on 8/10/16.
 */

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaMessagesConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private DispatchHandler dispatchHandler;

    public KafkaMessagesConsumer(String topic, DispatchHandler dispatchHandler, String zkConnect, String group) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                ConsumerConfigCreator.createConsumerConfig(zkConnect, group));
        this.topic = topic;
        this.dispatchHandler = dispatchHandler;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    logger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted during shutdown, exiting uncleanly");
//                throw new InterruptedException(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, Integer.valueOf(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        executor = Executors.newFixedThreadPool(KafkaProperties.threadNumber);
        for (final KafkaStream stream : streams) {
            executor.submit(new KafkaStreamConsumer(stream, dispatchHandler));
        }
    }
}
