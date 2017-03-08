package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.eventbus.ZipkinTraceEventConsumer;
import com.navercorp.pinpoint.collector.manage.controller.ConverterTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import zipkin.Span;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 16-12-23.
 */
public class ZipKinAsyncStorageServiceImplTest {

    @Mock
    private ZipkinTraceDao zipkinTraceDao;

    @Mock
    private ZipkinTraceEventConsumer consumer;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void acceptSpans() throws Exception {
        //given
        List<Span> spans = ConverterTest.ZipkinSpanBuilder.read("sleuth-spans.json");

        ZipkinSpanAsyncConsumer zipkinSpanAsyncConsumer = new ZipkinSpanAsyncConsumer(zipkinTraceDao, consumer);

        //when
        ZipKinAsyncStorageServiceImpl storageService = new ZipKinAsyncStorageServiceImpl(zipkinSpanAsyncConsumer);

        storageService.acceptSpans(spans);
        storageService.acceptSpansCallback(spans);

        //then
    }

    @Test
    public void appendSpanIds() throws Exception {
        //given
        List<Span> spans = ConverterTest.ZipkinSpanBuilder.read("sleuth-spans.json");
        StringBuilder message = new StringBuilder("zipkin span accepted.");

        //when
        StringBuilder msg = ZipKinAsyncStorageServiceImpl.appendSpanIds(spans, message);

        //then
        assertEquals(msg, message);
    }

}