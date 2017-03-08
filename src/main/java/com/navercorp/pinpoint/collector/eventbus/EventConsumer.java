package com.navercorp.pinpoint.collector.eventbus;

/**
 * Created by 10018761 on 16-7-14.
 */
public interface EventConsumer {
    void lister(TransactionEventValue value);
}