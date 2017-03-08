package com.navercorp.pinpoint.collector.eventbus;

import com.google.common.eventbus.Subscribe;
import com.navercorp.pinpoint.collector.service.TopoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("transactionEventConsumer")
public class TransactionEventConsumer implements EventConsumer {

    @Autowired
    private TopoService topoService;

    @Subscribe
    @Override
    public void lister(TransactionEventValue value){
        topoService.updateTopo(value, value.getStartTime());
    }
}