package com.navercorp.pinpoint.collector.eventbus;

import com.navercorp.pinpoint.common.PinpointConstants;
import com.navercorp.pinpoint.common.util.BytesUtils;
import com.navercorp.pinpoint.common.util.TransactionId;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.AGENT_NAME_MAX_LEN;

public class TransactionEventKey extends TransactionId {
    public TransactionEventKey(String agentId, long agentStartTime, long transactionSequence) {
        super(agentId, agentStartTime, transactionSequence);
    }

    public static TransactionEventKey buildTransacionKey(byte[] transactionId, int offset) {
        if (transactionId == null) {
            throw new NullPointerException("transactionId must not be null");
        }
        if (transactionId.length < BytesUtils.LONG_LONG_BYTE_LENGTH + AGENT_NAME_MAX_LEN + offset) {
            throw new IllegalArgumentException("invalid transactionId length:" + transactionId.length);
        }

        String agentId = BytesUtils.toStringAndRightTrim(transactionId, offset, AGENT_NAME_MAX_LEN);
        long  agentStartTime = BytesUtils.bytesToLong(transactionId, offset + AGENT_NAME_MAX_LEN);
        long transactionSequence = BytesUtils.bytesToLong(transactionId, offset + BytesUtils.LONG_BYTE_LENGTH + AGENT_NAME_MAX_LEN);

        return new TransactionEventKey(agentId, agentStartTime, transactionSequence);
    }

    public byte[] getBytes() {
        return BytesUtils.stringLongLongToBytes(agentId
                , PinpointConstants.AGENT_NAME_MAX_LEN, agentStartTime
                , transactionSequence);
    }

}
