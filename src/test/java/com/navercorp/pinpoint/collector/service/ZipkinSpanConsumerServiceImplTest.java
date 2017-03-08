package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.*;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.collector.manage.controller.ConverterTest;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import zipkin.Span;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;

public class ZipkinSpanConsumerServiceImplTest {

    @Mock
    private ApplicationTraceIndexDao applicationTraceIndexDao;

    @Mock
    private TracesDao tracesDao;

    @Mock
    private TransactionListDao transactionListDao;

    @Mock
    private AgentInfoDao agentInfoDao;

    @Mock
    private ApplicationIndexDao applicationIndexDao;

    @Mock
    private ServiceTraceIdIndexDao serviceTraceIdIndexDao;

    @Mock
    private InstanceTraceIdIndexDao instanceTraceIdIndexDao;

    @Mock
    private ApiMetaDataDao apiMetaDataDao;

    @Mock
    private TopoService topoService;

    @Mock
    private SqlMetaDataDao sqlMetaDataDao;

    @InjectMocks
    private ZipkinSpanConsumerService zipkinSpanConsumerService = new ZipkinSpanConsumerServiceImpl();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void consume() throws Exception {
        //given
        List<Span> zipkinspans = ConverterTest.ZipkinSpanBuilder.read("sleuth-spans.json");

        //when
        zipkinSpanConsumerService.consume(zipkinspans);

        //then
        Mockito.verify(applicationTraceIndexDao, times(2)).insert(any(TSpan.class));
        Mockito.verify(instanceTraceIdIndexDao).update(any(TSpan.class));
        Mockito.verify(apiMetaDataDao, times(5)).insert(any(TApiMetaData.class));
        Mockito.verify(applicationIndexDao, times(2)).insert(any(TAgentInfo.class));
        Mockito.verify(topoService).updateTopo(any(TransactionEventValue.class), anyLong());
        Mockito.verify(sqlMetaDataDao).insert(any(TSqlMetaData.class));
    }

}