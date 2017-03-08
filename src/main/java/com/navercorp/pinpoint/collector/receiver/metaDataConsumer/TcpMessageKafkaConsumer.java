package com.navercorp.pinpoint.collector.receiver.metaDataConsumer;

import com.navercorp.pinpoint.collector.receiver.DispatchHandler;
import com.navercorp.pinpoint.collector.receiver.spanstatConsumer.ConsumerConfigCreator;
import com.navercorp.pinpoint.collector.receiver.spanstatConsumer.KafkaProperties;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.Maps.newHashMap;
import static kafka.consumer.Consumer.createJavaConsumerConnector;

public class TcpMessageKafkaConsumer {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private String topic;
    private DispatchHandler dispatchHandler;
    private ConsumerConnector consumer;
    private ExecutorService executor = Executors.newFixedThreadPool(1);

    public TcpMessageKafkaConsumer(String topic, DispatchHandler dispatchHandler, String zkconnect, String group) {
        this.topic = topic;
        this.dispatchHandler = dispatchHandler;
        consumer = createJavaConsumerConnector(ConsumerConfigCreator.createConsumerConfig(zkconnect, group));
        logger.info("create metadata consumer, zookeeper info: {}", zkconnect);
    }

    public void run() {
        Map<String, Integer> topicCountMap = newHashMap();
        topicCountMap.put(topic, KafkaProperties.tcpThreadCount);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (final KafkaStream stream : streams) {
            executor.submit(new TCPMessageHandler(stream, dispatchHandler));
        }
    }
}
