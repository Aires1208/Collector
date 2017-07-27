package com.navercorp.pinpoint.collector.receiver.spanstatConsumer;

import kafka.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by aires on 8/9/16.
 */
public class ConsumerConfigCreator {
    public static ConsumerConfig createConsumerConfig(String zkconnect, String group) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkconnect);
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
}
