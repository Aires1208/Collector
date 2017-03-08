package com.navercorp.pinpoint.collector.eventbus;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration

@ContextConfiguration(locations = {"classpath:servlet-context.xml", "classpath:applicationContext-unit.xml"})
public class TransactionEventCacheTest {
//    @Autowired
//    private TransactionsDao transactionsDao;
//
//    @Autowired
//    private ServiceIndexDao serviceIndexDao;

    @Autowired
    @Qualifier("transactionEventConsumer")
    private EventConsumer consumer;

    @Ignore
    @Test
    public void should_consume_10event_when_happen_10_delay_events_after_wait10s() throws Exception {

        //given
        int expectedCounter = 10;
        TransactionEventCache<TransactionEventKey, TransactionEventValue> cache = new TransactionEventCache<TransactionEventKey, TransactionEventValue>(consumer);

        //when
        for (int i = 0; i < expectedCounter; i++) {

            TransactionEventKey key = new TransactionEventKey("test-agent", 1467022742471L, 2L);
            TransactionEventValue value = new TransactionEventValue(key, "TEST_123", System.currentTimeMillis());
            cache.put(key, value, 3, TimeUnit.SECONDS);
        }

        Thread.sleep(1000 * 20);
        //then
        System.out.println("End");
    }
}
