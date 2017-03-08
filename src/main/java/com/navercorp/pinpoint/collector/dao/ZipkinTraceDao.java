package com.navercorp.pinpoint.collector.dao;

import zipkin.Span;

import java.io.IOException;
import java.util.List;

/**
 * Created by root on 16-12-1.
 */
public interface ZipkinTraceDao {
    void insert(Span span);

    List<Span> selectSpan(long traceId) throws IOException;
}
