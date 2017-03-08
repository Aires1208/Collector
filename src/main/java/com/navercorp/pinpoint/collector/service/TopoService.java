package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;

/**
 * Created by root on 16-12-13.
 */
public interface TopoService {
    void updateTopo(TransactionEventValue value, long timestamp);
}
