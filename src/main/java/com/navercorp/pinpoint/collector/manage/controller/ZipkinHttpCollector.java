package com.navercorp.pinpoint.collector.manage.controller;

import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.eventbus.EventConsumer;
import com.navercorp.pinpoint.collector.service.ZipKinAsyncStorageServiceImpl;
import com.navercorp.pinpoint.collector.service.ZipkinSpanAsyncConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.async.DeferredResult;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.Nullable;
import zipkin.storage.Callback;

import java.util.Arrays;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Controller
@RequestMapping("/zipkin")
public class ZipkinHttpCollector {
    private static final String APPLICATION_THRIFT = "application/x-thrift";
    private static final ResponseEntity<?> SUCCESS = ResponseEntity.accepted().build();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ZipKinAsyncStorageServiceImpl zipKinAsyncStorageService;

    @Autowired
    private ZipkinTraceDao zipkinTraceDao;

    @Autowired
    @Qualifier("zipkinTraceEventConsumer")
    private EventConsumer consumer;

    @RequestMapping(value = "/api/v1/spans", method = POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public DeferredResult<ResponseEntity<?>> receiveSpans(@RequestBody byte[] body) {
        final DeferredResult<ResponseEntity<?>> result = new DeferredResult<>();
        this.acceptSpans(body, Codec.JSON, new AsynCallBack(result));
        return result;

    }

    @RequestMapping(value = "/api/v1/spans", method = POST, consumes = APPLICATION_THRIFT)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public DeferredResult<ResponseEntity<?>> uploadSpansThrift(
            @RequestBody byte[] body) {
        final DeferredResult<ResponseEntity<?>> result = new DeferredResult<>();
        this.acceptSpans(body, Codec.THRIFT, new AsynCallBack(result));

        return result;
    }

    public void acceptSpans(byte[] serializedSpans, Codec codec, Callback<Void> callback) {
        logger.debug("accept zipkin span: " + Arrays.toString(serializedSpans));

        try {
            List<Span> spans = codec.readSpans(serializedSpans);

            logger.info(spans.toString());

            if (zipKinAsyncStorageService == null) {
                ZipkinSpanAsyncConsumer zipkinSpanAsyncConsumer = new ZipkinSpanAsyncConsumer(zipkinTraceDao, consumer);
                zipKinAsyncStorageService = new ZipKinAsyncStorageServiceImpl(zipkinSpanAsyncConsumer);
            }

            zipKinAsyncStorageService.acceptSpans(spans);
            callback.onSuccess(null);
            logger.debug("Spans size is " + spans.size());
        } catch (Exception e) {
            callback.onError(e);
            logger.error("handle zipkin span error", e);
        }
    }


    private static class AsynCallBack implements Callback<Void> {
        private DeferredResult<ResponseEntity<?>> result;

        public AsynCallBack(DeferredResult<ResponseEntity<?>> result) {
            this.result = result;
        }


        @Override
        public void onSuccess(@Nullable Void aVoid) {
            result.setResult(SUCCESS);
        }

        @Override
        public void onError(Throwable t) {
            String message = t.getMessage();
            result.setErrorResult(message.startsWith("Cannot store"));
        }
    }

}
