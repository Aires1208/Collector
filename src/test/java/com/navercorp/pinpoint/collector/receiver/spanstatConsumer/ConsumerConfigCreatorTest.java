package com.navercorp.pinpoint.collector.receiver.spanstatConsumer;

import kafka.consumer.ConsumerConfig;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 16-12-27.
 */
public class ConsumerConfigCreatorTest {
    @Test
    public void createConsumerConfig() throws Exception {
        //given
        String zkconnect = "127.0.0.1:2181";
        String zkGroupId = "group1";

        //when
        ConsumerConfig consumerConfig = ConsumerConfigCreator.createConsumerConfig(zkconnect, zkGroupId);

        //then
        assertEquals(zkconnect, consumerConfig.zkConnect());
        assertEquals(zkGroupId, consumerConfig.groupId());
        assertEquals(40000, consumerConfig.zkConnectionTimeoutMs());
    }

}