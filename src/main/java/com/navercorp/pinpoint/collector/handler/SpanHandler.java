package com.navercorp.pinpoint.collector.handler;

import com.navercorp.pinpoint.collector.dao.*;
import com.navercorp.pinpoint.collector.eventbus.EventConsumer;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventCache;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventKey;
import com.navercorp.pinpoint.collector.eventbus.TransactionEventValue;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class SpanHandler implements SimpleHandler {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final static long WAIT_TIME = 30 * 1000L;

    private ExecutorService executor = Executors.newCachedThreadPool();

    @Autowired
    private TransactionListDao transactionListDao;

    @Autowired
    private TracesDao traceDao;

    @Autowired
    private ApplicationTraceIndexDao applicationTraceIndexDao;

    @Autowired
    private ServiceTraceIdIndexDao serviceTraceIdIndexDao;

    @Autowired
    private InstanceTraceIdIndexDao instanceTraceIdIndexDao;

    @Autowired
    @Qualifier("transactionEventConsumer")
    private EventConsumer consumer;

    @Autowired
    private RpcStatisticDao rpcStatisticDao;

    private TransactionEventCache cache;

    @Override
    public void handleSimple(TBase<?, ?> tbase) {

        if (!(tbase instanceof TSpan)) {
            throw new IllegalArgumentException("unexpected tbase:" + tbase + " expected:" + this.getClass().getName());
        }

        try {
            final TSpan span = (TSpan) tbase;
            if (logger.isDebugEnabled()) {
                logger.info("Received SPAN={}", span);
            }

            executor.submit(new TraceDataTask(span));
//            traceDao.insert(span);
            executor.submit(new ApplicationTraceIdIndexTask(span));
//            applicationTraceIndexDao.insert(span);
            executor.submit(new UrlStatisticTask(span));
//            rpcStatisticDao.update(span);

            //insert
            if (span.getParentSpanId() == -1L) {
                //insert statistics info for TOPO(node and link)
                insertServiceTopoLine(span);
                //insert traceName list
                executor.submit(new TraceListTask(span));
//                transactionListDao.insert(span);
                //insert traceId index table
                executor.submit(new TraceIndexTask(span));
//                insertTraceIdIndex(span);
            }
        } catch (Exception e) {
            logger.warn("Span handle error. Caused:{}. Span:{}", e.getMessage(), tbase, e);
        }
    }

    private void insertServiceTopoLine(TSpan span) {
        TransactionId id = TransactionIdUtils.parseTransactionId(span.getTransactionId());
        String agentId = id.getAgentId() == null ? span.getAgentId() : id.getAgentId();
        TransactionEventKey key = new TransactionEventKey(agentId, id.getAgentStartTime(), id.getTransactionSequence());
        TransactionEventValue value = new TransactionEventValue(key, span.getApplicationName(), span.getStartTime());
        if (cache == null) {
            cache = new TransactionEventCache<>(consumer);
        }

        cache.put(key, value, WAIT_TIME, TimeUnit.MILLISECONDS);
    }

    private class TraceDataTask implements Runnable {
        private TSpan span;
        public TraceDataTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            traceDao.insert(span);
        }
    }

    private class ApplicationTraceIdIndexTask implements Runnable {
        private TSpan span;
        public ApplicationTraceIdIndexTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            applicationTraceIndexDao.insert(span);
        }
    }

    private class UrlStatisticTask implements Runnable {
        private TSpan span;
        public UrlStatisticTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            rpcStatisticDao.update(span);
        }
    }

    private class TopoServiceTask implements Runnable {
        private TSpan span;
        public TopoServiceTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            insertServiceTopoLine(span);
        }
    }

    private class TraceListTask implements Runnable {
        private TSpan span;
        public TraceListTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            transactionListDao.insert(span);
        }
    }

    private class TraceIndexTask implements Runnable {
        private TSpan span;
        public TraceIndexTask(TSpan span) {
            this.span = span;
        }

        @Override
        public void run() {
            serviceTraceIdIndexDao.update(span);
            instanceTraceIdIndexDao.update(span);
        }
    }
}
