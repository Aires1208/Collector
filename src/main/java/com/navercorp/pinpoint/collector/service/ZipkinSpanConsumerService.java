package com.navercorp.pinpoint.collector.service;

import zipkin.Span;

import java.util.List;

/**
 * Created by root on 16-12-8.
 */
public interface ZipkinSpanConsumerService {
    void consume(List<Span> spans);
}
