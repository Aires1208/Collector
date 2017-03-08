package com.navercorp.pinpoint.collector.service;


import zipkin.Span;
import zipkin.internal.Nullable;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.Callback;
import zipkin.storage.StorageAdapters;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import static zipkin.storage.StorageAdapters.blockingToAsync;

public class ZipKinAsyncStorageServiceImpl {


    final Executor callingThread = new Executor() {

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    private AsyncSpanConsumer asyncConsumer;

    public ZipKinAsyncStorageServiceImpl(StorageAdapters.SpanConsumer zipKinAsyncSpanConsumer) {
        this.asyncConsumer = blockingToAsync(zipKinAsyncSpanConsumer, callingThread);

    }

    public void acceptSpans(List<Span> spans) {

        asyncConsumer.accept(spans, acceptSpansCallback(spans));

        System.out.println("Spans size is " + spans.size());
    }

    Callback<Void> acceptSpansCallback(final List<Span> spans) {
        return new Callback<Void>() {
            @Override
            public void onSuccess(@Nullable Void aVoid) {
                appendSpanIds(spans, new StringBuilder("Accept spans(")).append(")").toString();
            }

            @Override
            public void onError(Throwable t) {
                appendSpanIds(spans, new StringBuilder("Reject spans(")).append(")").toString();
            }
        };
    }

    static StringBuilder appendSpanIds(List<Span> spans, StringBuilder message) {
        message.append("[");
        for (Iterator<Span> iterator = spans.iterator(); iterator.hasNext(); ) {
            message.append(iterator.next().idString());
            if (iterator.hasNext()) message.append(", ");
        }

        return message.append("]");
    }


}
