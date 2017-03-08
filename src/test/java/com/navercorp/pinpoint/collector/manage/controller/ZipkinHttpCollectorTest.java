package com.navercorp.pinpoint.collector.manage.controller;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.eventbus.ZipkinTraceEventConsumer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;

import static org.junit.Assert.assertEquals;

public class ZipkinHttpCollectorTest {

    @Mock
    private ZipkinTraceDao zipkinTraceDao;

    @Mock
    private ZipkinTraceEventConsumer consumer;

    @InjectMocks
    private ZipkinHttpCollector zipkinHttpCollector = new ZipkinHttpCollector();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void should_return_success_when_input_json_span() throws Exception {
        //given
        String zipkinspans = ConverterTest.ZipkinSpanBuilder.readJson("sleuth-spans.json");
        byte[] postBody = Bytes.toBytes(zipkinspans);

        //when
        DeferredResult<ResponseEntity<?>> result = zipkinHttpCollector.receiveSpans(postBody);

        //then
        assertEquals(result.hasResult(), true);
    }

    @Test
    public void should_resturn_fail_when_input_thrift_span() throws Exception {
        //given
        byte[] thriftSpan = Bytes.toBytes(ConverterTest.ZipkinSpanBuilder.readJson("sleuth-spans.json"));

        //when
        DeferredResult<ResponseEntity<?>> result = zipkinHttpCollector.uploadSpansThrift(thriftSpan);

        //then
        assertEquals(result.hasResult(), true);
        assertEquals(result.getResult(), false);
    }

}