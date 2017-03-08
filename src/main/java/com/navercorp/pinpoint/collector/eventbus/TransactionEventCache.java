package com.navercorp.pinpoint.collector.eventbus;

import com.google.common.eventbus.EventBus;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TransactionEventCache<K, V> {
    private static final Logger LOG = Logger.getLogger(TransactionEventCache.class.getName());

    private ConcurrentMap<K, V> cacheObjMap = new ConcurrentHashMap<>();

    private DelayQueue<DelayItem<Pair<K, V>>> q = new DelayQueue<>();

    private Thread daemonThread;

    private EventBus eventBus = new EventBus();

    @Autowired
    private EventConsumer consumer;

    public TransactionEventCache(EventConsumer consumer) {

        eventBus.register(consumer);
        Runnable daemonTask = new Runnable() {
            @Override
            public void run() {
                daemonCheck();
            }
        };

        daemonThread = new Thread(daemonTask);
        daemonThread.setDaemon(true);
        daemonThread.setName("com.TransactionEventCache Daemon");
        daemonThread.start();
    }

    private void daemonCheck() {

        if (LOG.isLoggable(Level.INFO))
            LOG.info("cache service started.");

        for (; ; ) {
            try {
                DelayItem<Pair<K, V>> delayItem = q.take();
                if (delayItem != null) {
                    Pair<K, V> pair = delayItem.getItem();

                    cacheObjMap.remove(pair.first, pair.second); // compare and remove
                    eventBus.post(pair.second);
                }
            } catch (InterruptedException e) {
                if (LOG.isLoggable(Level.SEVERE))
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (LOG.isLoggable(Level.INFO))
            LOG.info("cache service stopped.");
    }

    public void put(K key, V value, long time, TimeUnit unit) {
        V oldValue = cacheObjMap.put(key, value);
        if (oldValue != null)
            q.remove(key);

        long nanoTime = TimeUnit.NANOSECONDS.convert(time, unit);
        q.put(new DelayItem<>(new Pair<>(key, value), nanoTime));
    }

    public V get(K key) {
        return cacheObjMap.get(key);
    }

}