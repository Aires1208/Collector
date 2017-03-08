package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.InstanceIndexDao;
import com.navercorp.pinpoint.collector.dao.ServiceIndexDao;
import com.navercorp.pinpoint.collector.dao.TransactionsDao;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.service.ServiceTypeRegistryService;
import com.navercorp.pinpoint.common.topo.domain.TopoLine;
import com.navercorp.pinpoint.common.trace.ServiceType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

/**
 * Created by root on 16-12-22.
 */
public class TopoServiceImplTest {

    @Mock
    private ServiceTypeRegistryService registryService;

    @Mock
    private TransactionsDao transactionsDao;

    @Mock
    private ServiceIndexDao serviceIndexDao;

    @Mock
    private InstanceIndexDao instanceIndexDao;

    @InjectMocks
    private TopoService topoService = new TopoServiceImpl();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void updateTopo() throws Exception {
        //given
        long timestamp = System.currentTimeMillis();
        List<SpanBo> spanBos = buildTraces(timestamp);
        TransactionEventValue value = getTraceValue(timestamp);

        //when
        Mockito.when(transactionsDao.selectSpans(any(TransactionEventKey.class))).thenReturn(spanBos);
        topoService.updateTopo(value, timestamp);

        //then
        Mockito.verify(serviceIndexDao).update(anyString(), anyLong(), any(TopoLine.class));
        Mockito.verify(instanceIndexDao).update(anyString(), anyLong(), any(TopoLine.class));
    }

    private TransactionEventValue getTraceValue(long timestamp) {
        TransactionEventKey key = new TransactionEventKey("fm-agent", timestamp, 12345L);

        return new TransactionEventValue(key, "fm_active", timestamp);
    }

    private List<SpanBo> buildTraces(long timestamp) {
        SpanBo spanBo1 = new SpanBo("fm-agent", timestamp, 12345L, timestamp, 300, 23456L);
        spanBo1.setApplicationId("fm-active");
        spanBo1.setErrCode(0);
        spanBo1.setServiceType(ServiceType.SPRING.getCode());

        return newArrayList(spanBo1);
    }

}