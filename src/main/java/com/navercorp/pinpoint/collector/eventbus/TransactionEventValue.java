package com.navercorp.pinpoint.collector.eventbus;

import com.google.common.base.Preconditions;

/**
 * Created by root on 16-7-15.
 */
public class TransactionEventValue {
    private TransactionEventKey transactionEventKey;
    private String serviceName;

    private long startTime = 0L;

    public TransactionEventValue(TransactionEventKey transactionEventKey, String serviceName) {
        this.transactionEventKey = transactionEventKey;
        this.serviceName = serviceName;
    }

    public TransactionEventValue(TransactionEventKey key, String serviceName, long startTime) {
        this(key, serviceName);
        this.startTime = startTime;
    }

    public String getServiceName() {
        return serviceName;
    }

    public TransactionEventKey getTransactionEventKey() {
        return transactionEventKey;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getAppName() {
        Preconditions.checkArgument(serviceName != null, "original serviceName must not be empty");

        int index = serviceName.indexOf('_');
        if (index < 0)
        {
            return serviceName;
        }

        return serviceName.substring(0, index);
    }
}
