package com.navercorp.pinpoint.collector.eventbus;

import com.navercorp.pinpoint.collector.service.TopoService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;

/**
 * Created by root on 17-1-10.
 */
public class TransactionEventConsumerTest {

    @Mock
    private TopoService topoService;

    @InjectMocks
    private TransactionEventConsumer consumer = new TransactionEventConsumer();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void lister() throws Exception {
        //given
        TransactionEventKey key = new TransactionEventKey("fm-active", 1L, 1L);
        TransactionEventValue value = new TransactionEventValue(key, "fm_history", 1L);

        //when
        consumer.lister(value);

        //then
        Mockito.verify(topoService).updateTopo(value, 1L);
    }

}