package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.eventbus.ZipkinTraceEventConsumer;
import com.navercorp.pinpoint.collector.manage.controller.ConverterTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import zipkin.Span;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;

/**
 * Created by root on 16-12-8.
 */
public class ZipkinSpanAsyncConsumerTest {

    @Mock
    private ZipkinTraceDao zipkinTraceDao;

    @Mock
    private ZipkinTraceEventConsumer zipkinTraceEventConsumer;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void consume() throws Exception {
        //given
        List<Span> spans = ConverterTest.ZipkinSpanBuilder.read("sleuth-spans.json");

        ZipkinSpanAsyncConsumer zipkinSpanAsyncConsumer = new ZipkinSpanAsyncConsumer(zipkinTraceDao, zipkinTraceEventConsumer);

        //when
        zipkinSpanAsyncConsumer.accept(spans);

        //then
        Thread.sleep(1000);
        Mockito.verify(zipkinTraceDao, times(5)).insert(any(Span.class));
    }

}