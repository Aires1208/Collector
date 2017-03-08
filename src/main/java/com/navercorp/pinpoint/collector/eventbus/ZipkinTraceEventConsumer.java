package com.navercorp.pinpoint.collector.eventbus;

import com.google.common.eventbus.Subscribe;
import com.navercorp.pinpoint.collector.dao.ZipkinTraceDao;
import com.navercorp.pinpoint.collector.service.ZipkinSpanConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import zipkin.Span;

import java.io.IOException;
import java.util.List;

/**
 * Created by root on 16-12-12.
 */
@Component("zipkinTraceEventConsumer")
public class ZipkinTraceEventConsumer implements EventConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ZipkinTraceDao zipkinTraceDao;

    @Autowired
    ZipkinSpanConsumerService zipkinSpanConsumerService;

    @Subscribe
    @Override
    public void lister(TransactionEventValue value) {
        TransactionEventKey key = value.getTransactionEventKey();
        try {
            List<Span> zipkinSpans = zipkinTraceDao.selectSpan(key.getTransactionSequence());
            if (zipkinSpans != null && !zipkinSpans.isEmpty()) {
                zipkinSpanConsumerService.consume(zipkinSpans);
            }
        } catch (IOException e) {
            logger.error("select zipkin span from database failed.", e);
        }
    }
}
