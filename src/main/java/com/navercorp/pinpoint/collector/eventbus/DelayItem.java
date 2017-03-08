package com.navercorp.pinpoint.collector.eventbus;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DelayItem<T> implements Delayed {
    private static final long NANO_ORIGIN = System.nanoTime();
    private static final AtomicLong sequencer = new AtomicLong(0);

    private final long sequenceNumber;
    private final long time;
    private final T item;

    public DelayItem(T submit, long timeout) {
        this.time = now() + timeout;
        this.item = submit;
        this.sequenceNumber = sequencer.getAndIncrement();
    }

    final static long now() {
        return System.nanoTime() - NANO_ORIGIN;
    }

    public T getItem() {
        return this.item;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(time - now(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other == this) // compare zero ONLY if same object
            return 0;
        if (other instanceof DelayItem) {
            DelayItem x = (DelayItem) other;
            long diff = time - x.time;

            if (diff < 0 || sequenceNumber < x.sequenceNumber) {
                return -1;
            } else {
                return 1;
            }

        }
        long d = getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS);
        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }
}