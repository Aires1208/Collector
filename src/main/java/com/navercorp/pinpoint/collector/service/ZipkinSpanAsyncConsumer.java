package com.navercorp.pinpoint.collector.service;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.eventbus.*;
import zipkin.Span;
import zipkin.storage.StorageAdapters;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by root on 16-12-7.
 */
public class ZipkinSpanAsyncConsumer implements StorageAdapters.SpanConsumer {
    private static final String DEFAULT_AGENT = "default_agent";
    private final static long WAIT_TIME = 30L;

    private ZipkinTraceDao zipkinTraceDao;

    private EventConsumer consumer;

    private TransactionEventCache cache;

    private ExecutorService executor = Executors.newCachedThreadPool();

    public ZipkinSpanAsyncConsumer(ZipkinTraceDao zipkinTraceDao, EventConsumer consumer) {
        this.zipkinTraceDao = zipkinTraceDao;
        this.consumer = consumer;
    }

    @Override
    public void accept(List<Span> spans) {
        for (Span span : spans) {
            // 1), insert zipkin span to temporary table
            executor.submit(new CacheZipkinSpanTask(span));

            // 2), put root span into eventbus
            if (span.parentId == null) {
                putToEventBus(span);
            }
        }
    }

    private void putToEventBus(Span span) {
        if (cache == null) {
            cache = new TransactionEventCache(consumer);
        }

        long acceptTime = span.timestamp != null ? span.timestamp : System.nanoTime() / 1000L;
        TransactionEventKey key = new TransactionEventKey(DEFAULT_AGENT, acceptTime, span.traceId);
        TransactionEventValue value = new TransactionEventValue(key, DEFAULT_AGENT, acceptTime);
        cache.put(key, value, WAIT_TIME, TimeUnit.SECONDS);
    }

    private class CacheZipkinSpanTask implements Runnable {
        private Span span;

        public CacheZipkinSpanTask(Span span) {
            this.span = span;
        }

        @Override
        public void run() {
            zipkinTraceDao.insert(span);
        }
    }
}
